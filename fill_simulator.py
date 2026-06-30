"""fill_simulator.py — conservative, queue-aware fill simulation.

This module decides whether (and how much of) a *resting* limit order would have
been filled, given the historical event stream produced by
:func:`data_loader.iter_market_events`. It is deliberately pessimistic; every
modelling choice errs on the side of *under*-filling rather than reporting an
optimistic fill that would not have happened in live trading.

Fill / queue policy (the heart of this module — read ASSUMPTIONS.md §3, §4):

1. FILLS happen ONLY on observed trade prints (``last_trade_price``). There is
   no "price touched my level => I'm filled" rule. A resting order is filled
   only by an aggressive trade on the opposite side that prints *at our exact
   price level*.

2. QUEUE ADVANCEMENT uses the conservative clamp invariant:

       queue_ahead <= L_new   (always)
       queue_ahead := min(queue_ahead, L_new)

   where ``L_new`` is the level's resting size *excluding our own order* (the
   historical book never contains our hypothetical order, so the data value IS
   "others"). ``queue_ahead`` is the portion of that resting size sitting *in
   front of* us.

   We cannot observe whether a reduction at the level happened in front of or
   behind our order. We therefore assume — conservatively — that reductions
   occur *behind* us first, which advances our queue as little as possible. The
   clamp expresses exactly this: a reduction only moves us up once it is too
   large to be explained by the size that could be sitting behind us.

   * others 100 -> 60, queue_ahead 30  =>  min(30, 60) = 30  (no advance)
   * others 100 -> 20, queue_ahead 30  =>  min(30, 20) = 20  (forced advance)

   Size *increases* never move us backward (new orders join behind us).

Dependencies: standard library only. Consumes :mod:`data_loader` types.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import List, Optional

from data_loader import MarketEvent, OrderBook, Price

# Default adverse-selection measurement window (seconds of ``local_timestamp``).
# NOT hardcoded at the call sites — exposed here and as a constructor argument so
# a sensitivity analysis can sweep it. See ASSUMPTIONS.md §5.
DEFAULT_ADVERSE_WINDOW_SECONDS: float = 1.0

# Numerical tolerance for size/price comparisons.
_EPS: float = 1e-9


def clamp_queue_ahead(queue_ahead: float, level_size: float) -> float:
    """Conservative queue advance: ``min(queue_ahead, level_size)``.

    Encodes the "reductions happen behind me first" assumption. ``queue_ahead``
    can never exceed the resting size still at the level, and never moves
    backward (a level that grows does not push us back).

    The two asserts make the invariant executable so it is caught in tests and
    in any future refactor.
    """
    new_q = min(queue_ahead, level_size)
    # Invariant 1: we are never ahead of more size than actually rests there.
    assert new_q <= level_size + _EPS, (new_q, level_size)
    # Invariant 2: queue position never moves backward.
    assert new_q <= queue_ahead + _EPS, (new_q, queue_ahead)
    return new_q


@dataclass
class Fill:
    """A single (possibly partial) execution against our order."""

    local_timestamp: float
    price: Price
    qty: float
    remaining_after: float
    queue_ahead_before: float  # our queue position just before this trade
    mid_before: Optional[float]  # mid at fill time
    # Filled in once the adverse window elapses (see resolve below):
    mid_after: Optional[float] = None
    # When mid_after was ACTUALLY measured (local_timestamp), and the elapsed
    # time from the fill. These let an adverse-window sensitivity analysis see
    # whether the measurement landed near the intended window or much later.
    mid_after_local_ts: Optional[float] = None
    measured_dt: Optional[float] = None
    # adverse is tri-state:
    #   True  -> measurable, mid moved against our position
    #   False -> measurable, mid did not move against us
    #   None  -> UNMEASURABLE (no valid mid available after the window); such
    #            fills must be EXCLUDED from PnL / adverse attribution, never
    #            counted as not-adverse.
    adverse: Optional[bool] = None


@dataclass
class MyOrder:
    """A resting limit order to be simulated.

    ``side`` is "BUY" (a resting bid) or "SELL" (a resting ask). The order joins
    the back of the queue at ``price`` when first processed at/after
    ``place_local_ts``.
    """

    side: str
    price: Price
    size: float
    asset_id: str
    place_local_ts: float

    # runtime state (initialised on first processed event):
    remaining: Optional[float] = None
    queue_ahead: Optional[float] = None
    filled: float = 0.0
    fills: List[Fill] = field(default_factory=list)
    initialised: bool = False
    active: bool = True

    def __post_init__(self) -> None:
        self.price = self.price if isinstance(self.price, Decimal) else Decimal(str(self.price))
        self.side = self.side.upper()


def _trade_fills_order(order: MyOrder, trade) -> bool:
    """Return True if ``trade`` can execute against ``order``.

    Conservative matching:
      * opposite aggressor side — a resting BID (BUY) is filled only by a SELL
        (taker selling into bids); a resting ASK (SELL) only by a BUY taker; and
      * the trade prints at our *exact* price level.

    Requiring an exact price match can *miss* fills from sweeping trades that
    print only at their final level (we then under-fill), which is the safe
    direction. See ASSUMPTIONS.md §3.
    """
    if trade.asset_id != order.asset_id:
        return False
    if trade.price != order.price:
        return False
    if order.side == "BUY":
        return trade.side == "SELL"
    return trade.side == "BUY"


class FillSimulator:
    """Drive a single :class:`MyOrder` through a stream of market events.

    Usage::

        order = MyOrder(side="SELL", price=Decimal("0.67"), size=50,
                        asset_id=PRIMARY, place_local_ts=t0)
        sim = FillSimulator(order)
        for ev in iter_market_events(path, primary_asset=PRIMARY):
            sim.process(ev)
        sim.finalize(last_mid=...)  # resolve any pending adverse windows
    """

    def __init__(
        self,
        order: MyOrder,
        *,
        adverse_window_seconds: float = DEFAULT_ADVERSE_WINDOW_SECONDS,
    ) -> None:
        self.order = order
        self.adverse_window_seconds = float(adverse_window_seconds)
        # Fills whose adverse window has not yet elapsed.
        self._pending_adverse: List[Fill] = []

    # -- internal helpers --------------------------------------------------
    def _book(self, event: MarketEvent) -> Optional[OrderBook]:
        return event.books.get(self.order.asset_id)

    def _maybe_initialise(self, event: MarketEvent) -> None:
        """Join the queue the first time we see an event at/after placement."""
        if self.order.initialised:
            return
        if event.local_timestamp < self.order.place_local_ts:
            return
        book = self._book(event)
        level = book.level_size(self.order.price, self.order.side) if book else 0.0
        self.order.queue_ahead = level  # we sit behind everyone currently resting
        self.order.remaining = self.order.size
        self.order.initialised = True

    def _advance_queue(self, event: MarketEvent) -> None:
        """Apply the conservative clamp using the level's current resting size."""
        book = self._book(event)
        if book is None or self.order.queue_ahead is None:
            return
        level = book.level_size(self.order.price, self.order.side)
        self.order.queue_ahead = clamp_queue_ahead(self.order.queue_ahead, level)

    def _apply_trade(self, event: MarketEvent) -> None:
        """Consume queue then fill on a matching trade print (partial-aware)."""
        trade = event.trade
        if trade is None or not self.order.active:
            return
        if not _trade_fills_order(self.order, trade):
            return

        book = self._book(event)
        mid_before = book.mid() if book is not None else None
        queue_before = self.order.queue_ahead or 0.0

        volume = trade.size
        # Trades consume the queue *ahead of us* first (price-time priority).
        consumed = min(queue_before, volume)
        self.order.queue_ahead = queue_before - consumed
        through = volume - consumed  # volume that reaches our order

        fill_qty = min(self.order.remaining, through)
        if fill_qty <= _EPS:
            return

        self.order.remaining -= fill_qty
        self.order.filled += fill_qty
        fill = Fill(
            local_timestamp=event.local_timestamp,
            price=self.order.price,
            qty=fill_qty,
            remaining_after=self.order.remaining,
            queue_ahead_before=queue_before,
            mid_before=mid_before,
        )
        self.order.fills.append(fill)
        self._pending_adverse.append(fill)
        if self.order.remaining <= _EPS:
            self.order.active = False

    def _resolve_adverse(self, event: MarketEvent) -> None:
        """Close out adverse-selection windows whose deadline has passed.

        Adverse selection = the mid moved *against* our resulting position
        shortly after we were filled:
          * we BOUGHT (resting bid filled)  -> adverse if mid fell.
          * we SOLD  (resting ask filled)   -> adverse if mid rose.

        Measurement rule (see ASSUMPTIONS.md §5):
          * Once the window has elapsed, the fill is resolved at the FIRST event
            at/after the deadline that has a VALID mid. The mid and the actual
            measurement time/elapsed-Δt are recorded.
          * If the window has elapsed but no valid mid is available yet, the fill
            stays pending (we keep waiting for a measurable mid).
          * "mid unchanged" is a measured outcome -> adverse=False.
          * "no valid mid" is NOT measured -> resolved to adverse=None by
            finalize(), and must be excluded from attribution.
        """
        if not self._pending_adverse:
            return
        book = self._book(event)
        mid_now = book.mid() if book is not None else None
        still_pending: List[Fill] = []
        for fill in self._pending_adverse:
            elapsed = event.local_timestamp - fill.local_timestamp
            if elapsed >= self.adverse_window_seconds and mid_now is not None:
                fill.mid_after = mid_now
                fill.mid_after_local_ts = event.local_timestamp
                fill.measured_dt = elapsed
                fill.adverse = self._classify_adverse(fill.mid_before, mid_now)
            else:
                still_pending.append(fill)
        self._pending_adverse = still_pending

    def _classify_adverse(
        self, mid_before: Optional[float], mid_after: Optional[float]
    ) -> Optional[bool]:
        """Tri-state adverse classification.

        Returns None when either mid is missing (unmeasurable); otherwise True if
        the mid moved against our position, False if not.
        """
        if mid_before is None or mid_after is None:
            return None
        if self.order.side == "BUY":
            return mid_after < mid_before - _EPS  # bought, then price fell
        return mid_after > mid_before + _EPS  # sold, then price rose

    # -- public API --------------------------------------------------------
    def process(self, event: MarketEvent) -> None:
        """Advance the simulation by one market event.

        Order of operations within an event matters and is deliberate:
        initialise -> resolve any due adverse windows -> advance queue from the
        book state -> apply trade fills. Trade prints arrive *before* the
        matching book ``price_change`` in the recorded data, so consuming the
        queue on the trade and clamping on the later book update does not
        double-advance. See ASSUMPTIONS.md §4.
        """
        self._maybe_initialise(event)
        if not self.order.initialised:
            return
        self._resolve_adverse(event)
        if event.event_type in ("book", "price_change"):
            self._advance_queue(event)
        if event.trade is not None:
            self._apply_trade(event)

    def finalize(self, last_mid: Optional[float] = None) -> MyOrder:
        """Resolve any still-pending adverse windows at end of stream.

        A fill is still pending here only because its window never elapsed within
        the data, or no valid mid was ever observed after the deadline. Either
        way it is UNMEASURABLE: ``adverse`` is set to ``None`` (not False) so PnL
        / adverse attribution can exclude it. ``last_mid`` is accepted for
        backward compatibility but intentionally NOT used to force a measurement
        outside the window. Returns the (now fully populated) order.
        """
        for fill in self._pending_adverse:
            fill.mid_after = None
            fill.mid_after_local_ts = None
            fill.measured_dt = None
            fill.adverse = None
        self._pending_adverse = []
        return self.order


def simulate(order: MyOrder, events, **kwargs) -> MyOrder:
    """Convenience runner: feed ``events`` (an iterable of MarketEvent) through
    a :class:`FillSimulator` and return the resolved order.

    The last seen mid is used to close out any pending adverse windows.
    """
    sim = FillSimulator(order, **kwargs)
    last_mid: Optional[float] = None
    for ev in events:
        sim.process(ev)
        book = ev.books.get(order.asset_id)
        if book is not None and book.mid() is not None:
            last_mid = book.mid()
    return sim.finalize(last_mid=last_mid)


def adverse_summary(order: MyOrder) -> dict:
    """Aggregate adverse-selection outcomes, EXCLUDING unmeasurable fills.

    Returns counts and an ``adverse_rate`` computed over *measurable* fills only
    (adverse in {True, False}); fills with ``adverse is None`` are reported
    separately as ``unmeasurable`` and never folded into the rate. Files 3 & 4
    (PnL attribution) should use this so unmeasurable fills are excluded by
    construction. See ASSUMPTIONS.md §5.
    """
    measurable = [f for f in order.fills if f.adverse is not None]
    adverse_true = sum(1 for f in measurable if f.adverse)
    return {
        "total_fills": len(order.fills),
        "measurable": len(measurable),
        "unmeasurable": len(order.fills) - len(measurable),
        "adverse_true": adverse_true,
        "adverse_false": len(measurable) - adverse_true,
        # adverse_rate is None when nothing was measurable (avoid 0/0).
        "adverse_rate": (adverse_true / len(measurable)) if measurable else None,
    }


__all__ = [
    "DEFAULT_ADVERSE_WINDOW_SECONDS",
    "clamp_queue_ahead",
    "Fill",
    "MyOrder",
    "FillSimulator",
    "simulate",
    "adverse_summary",
]
