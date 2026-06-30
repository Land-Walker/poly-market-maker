"""data_loader.py — Polymarket L2 order-book reconstruction for backtesting.

This module reads recorded Polymarket WebSocket JSONL logs and reconstructs the
limit order book for *both* outcome tokens (YES/NO) of a binary market, then
yields strictly time-ordered :class:`MarketEvent` objects for a backtest loop to
consume.

Design decisions (see ASSUMPTIONS.md for the full rationale):

* **Ordering key** — the top-level ``local_timestamp`` (the recorder's receive
  wall-clock) is the ONLY ordering key. The exchange ``timestamp`` is preserved
  for diagnostics but never used for ordering. A market-making bot can only react
  to information at the moment it *receives* it, so ordering by arrival time is
  what keeps the backtest live-consistent and free of look-ahead.
* **Two-token reconstruction** — each ``asset_id`` gets its own independent
  :class:`OrderBook`. Only the snapshotted token is fully reliable; a token that
  never receives a snapshot is marked ``synced=False`` (see ASSUMPTIONS.md §2).
* **No look-ahead** — a message's state change is applied and only *then* is the
  event yielded. The consumer can never observe a future message.

Dependencies: standard library only (``json``, ``decimal``).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, Iterator, List, Optional, Tuple

# Prices are kept as :class:`decimal.Decimal` so that tick-aligned levels like
# "0.66" compare and hash exactly (binary floats would not). Sizes are floats.
Price = Decimal
Size = float


# ---------------------------------------------------------------------------
# Order book
# ---------------------------------------------------------------------------
@dataclass
class TopOfBook:
    """Immutable snapshot of the best bid/ask captured at one event."""

    best_bid: Optional[Price]
    best_bid_size: Optional[Size]
    best_ask: Optional[Price]
    best_ask_size: Optional[Size]

    @property
    def mid(self) -> Optional[float]:
        """Mid-price, or ``None`` if either side is empty (one-sided book)."""
        if self.best_bid is None or self.best_ask is None:
            return None
        return (float(self.best_bid) + float(self.best_ask)) / 2.0


class OrderBook:
    """A single token's L2 book, reconstructed incrementally.

    The book stores only *other* participants' resting size — it never contains
    our hypothetical backtest order. ``bids``/``asks`` map ``Price -> Size``.
    """

    def __init__(self, asset_id: str) -> None:
        self.asset_id = asset_id
        self.bids: Dict[Price, Size] = {}
        self.asks: Dict[Price, Size] = {}
        # ``synced`` becomes True once a *snapshot* (event_type "book") has
        # initialised this book. A delta-only book (no snapshot ever) cannot be
        # trusted for absolute levels and stays False — see ASSUMPTIONS.md §2.
        self.synced: bool = False
        self.last_exchange_ts: Optional[int] = None

    # -- mutation ----------------------------------------------------------
    def apply_snapshot(
        self,
        bids: List[dict],
        asks: List[dict],
        exchange_ts: Optional[str] = None,
    ) -> None:
        """Replace the whole book from a snapshot ("book") message.

        ``bids``/``asks`` are lists of ``{"price": str, "size": str}``. Levels
        with non-positive size are dropped. Marks the book ``synced``.
        """
        self.bids = {
            Decimal(b["price"]): float(b["size"])
            for b in bids
            if float(b["size"]) > 0
        }
        self.asks = {
            Decimal(a["price"]): float(a["size"])
            for a in asks
            if float(a["size"]) > 0
        }
        self.synced = True
        if exchange_ts is not None:
            self.last_exchange_ts = int(exchange_ts)

    def apply_change(
        self,
        price: str,
        size: str,
        side: str,
        exchange_ts: Optional[str] = None,
    ) -> None:
        """Apply one incremental L2 update from a "price_change" message.

        ``size`` is the new *absolute* resting size at ``price`` (NOT a delta);
        a size of 0 removes the level. ``side`` is "BUY" (bid) or "SELL" (ask).
        """
        p = Decimal(price)
        s = float(size)
        book = self.bids if side.upper() == "BUY" else self.asks
        if s <= 0:
            book.pop(p, None)
        else:
            book[p] = s
        if exchange_ts is not None:
            self.last_exchange_ts = int(exchange_ts)

    # -- queries -----------------------------------------------------------
    def best_bid(self) -> Optional[Price]:
        """Highest bid price, or ``None`` if there are no bids."""
        return max(self.bids) if self.bids else None

    def best_ask(self) -> Optional[Price]:
        """Lowest ask price, or ``None`` if there are no asks."""
        return min(self.asks) if self.asks else None

    def level_size(self, price: Price, side: str) -> Size:
        """Resting size at ``price`` on ``side`` (0.0 if the level is absent).

        This is *other* participants' size only — exactly the quantity the fill
        simulator treats as the queue at that level.
        """
        p = price if isinstance(price, Decimal) else Decimal(str(price))
        book = self.bids if side.upper() == "BUY" else self.asks
        return book.get(p, 0.0)

    def top_of_book(self) -> TopOfBook:
        """Return the current :class:`TopOfBook`."""
        bb = self.best_bid()
        ba = self.best_ask()
        return TopOfBook(
            best_bid=bb,
            best_bid_size=self.bids.get(bb) if bb is not None else None,
            best_ask=ba,
            best_ask_size=self.asks.get(ba) if ba is not None else None,
        )

    def mid(self) -> Optional[float]:
        """Mid-price, or ``None`` if the book is one-sided/empty."""
        return self.top_of_book().mid

    def depth(
        self, levels: int = 10
    ) -> Tuple[List[Tuple[Price, Size]], List[Tuple[Price, Size]]]:
        """Return up to ``levels`` of depth as ``(bids_desc, asks_asc)``.

        Bids are sorted best (highest) first; asks best (lowest) first.
        """
        bids = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:levels]
        asks = sorted(self.asks.items(), key=lambda kv: kv[0])[:levels]
        return bids, asks


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------
@dataclass
class Trade:
    """An observed trade print (``last_trade_price`` message).

    ``side`` is the taker/aggressor side: "BUY" lifts asks (fills resting sell
    orders); "SELL" hits bids (fills resting buy orders). Verified against the
    sample data — see ASSUMPTIONS.md §3.
    """

    asset_id: str
    price: Price
    size: Size
    side: str  # taker side: "BUY" or "SELL"


@dataclass
class MarketEvent:
    """One time-ordered event handed to the backtest loop.

    ``books`` holds the *live* :class:`OrderBook` objects (current state at the
    moment this event is yielded). Read them inside the loop iteration; do not
    stash a reference and read it later, as the books mutate as the stream
    advances. This is the only look-ahead foot-gun and it is the consumer's
    responsibility to avoid it.
    """

    local_timestamp: float
    exchange_timestamp: Optional[int]
    event_type: Optional[str]  # "book" | "price_change" | "last_trade_price"
    asset_ids: Tuple[str, ...]  # tokens touched by this message
    trade: Optional[Trade]
    books: Dict[str, OrderBook]
    primary_asset: Optional[str]

    def book(self, asset_id: Optional[str] = None) -> Optional[OrderBook]:
        """Return the :class:`OrderBook` for ``asset_id`` (default primary)."""
        aid = asset_id or self.primary_asset
        return self.books.get(aid) if aid is not None else None

    def top_of_book(self, asset_id: Optional[str] = None) -> Optional[TopOfBook]:
        """Top-of-book for ``asset_id`` (default primary), or ``None``."""
        ob = self.book(asset_id)
        return ob.top_of_book() if ob is not None else None

    def depth(self, asset_id: Optional[str] = None, levels: int = 10):
        """N-level depth for ``asset_id`` (default primary), or ``None``."""
        ob = self.book(asset_id)
        return ob.depth(levels) if ob is not None else None

    def parity_deviation(
        self, yes_asset: str, no_asset: str
    ) -> Tuple[Optional[float], bool]:
        """Binary-market sanity diagnostic: ``(YES_mid + NO_mid) - 1``.

        For a binary market the two token prices should sum to 1. The deviation
        from 1 is a data-quality / arbitrage diagnostic.

        Returns ``(deviation, reliable)``. ``reliable`` is False whenever either
        book has never been snapshotted (e.g. the NO token in the sample data,
        which is delta-only) or a mid is unavailable — callers MUST treat an
        unreliable value as untrustworthy. See ASSUMPTIONS.md §2.
        """
        yb = self.books.get(yes_asset)
        nb = self.books.get(no_asset)
        if yb is None or nb is None:
            return None, False
        my = yb.mid()
        mn = nb.mid()
        reliable = yb.synced and nb.synced and my is not None and mn is not None
        if my is None or mn is None:
            return None, reliable
        return (my + mn) - 1.0, reliable


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------
def _iter_json_lines(path: str) -> Iterator[dict]:
    """Yield parsed JSON objects from a JSONL file, skipping blank lines."""
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def _apply_message(
    books: Dict[str, OrderBook],
    m: dict,
    local_ts: float,
    primary_asset: Optional[str],
) -> MarketEvent:
    """Apply ONE message dict ``m`` to ``books`` and return its MarketEvent.

    ``m`` is a single message carrying its own ``event_type`` (book /
    price_change / last_trade_price). A snapshot-shaped message that omits
    ``event_type`` but carries ``bids``/``asks`` is treated as a book snapshot
    (back-compat with the recorder's list-of-book frames).
    """
    touched: List[str] = []
    trade: Optional[Trade] = None
    exch_ts: Optional[int] = int(m["timestamp"]) if m.get("timestamp") is not None else None
    event_type = m.get("event_type")
    if event_type is None and ("bids" in m or "asks" in m):
        event_type = "book"

    if event_type == "book":
        aid = m["asset_id"]
        ob = books.setdefault(aid, OrderBook(aid))
        ob.apply_snapshot(m.get("bids", []), m.get("asks", []), m.get("timestamp"))
        touched.append(aid)
    elif event_type == "price_change":
        for c in m["price_changes"]:
            aid = c["asset_id"]
            ob = books.setdefault(aid, OrderBook(aid))
            ob.apply_change(c["price"], c["size"], c["side"], m.get("timestamp"))
            if aid not in touched:
                touched.append(aid)
    elif event_type == "last_trade_price":
        aid = m["asset_id"]
        books.setdefault(aid, OrderBook(aid))  # ensure book exists
        trade = Trade(
            asset_id=aid,
            price=Decimal(m["price"]),
            size=float(m["size"]),
            side=str(m["side"]).upper(),
        )
        touched.append(aid)
    else:
        # Unknown event type: do not guess at semantics, do not mutate state.
        pass

    primary = primary_asset if primary_asset is not None else (touched[0] if touched else None)
    return MarketEvent(
        local_timestamp=local_ts,
        exchange_timestamp=exch_ts,
        event_type=event_type,
        asset_ids=tuple(touched),
        trade=trade,
        books=books,
        primary_asset=primary,
    )


def build_market_events(
    books: Dict[str, OrderBook],
    msg: dict,
    *,
    primary_asset: Optional[str] = None,
) -> Iterator[MarketEvent]:
    """Apply one raw recorded frame to ``books`` and yield one MarketEvent per
    message it contains.

    A frame's ``data`` is EITHER a single message dict OR a list of independent
    message dicts (Polymarket CLOB may batch several updates per frame). Each
    element is dispatched by its OWN ``event_type`` — a ``price_change`` updates
    both tokens' books and a ``last_trade_price`` produces a ``Trade``. The old
    "a list means snapshots" assumption was wrong (it dropped price_change/trade
    messages) and is removed here.

    Each message is applied to ``books`` immediately before its event is yielded,
    so a consumer iterating lazily never sees a later message from the same frame
    (no intra-frame look-ahead). ``msg`` is ``{"local_timestamp", "slug",
    "data"}``; ordering across frames is the caller's responsibility.
    """
    local_ts = float(msg["local_timestamp"])
    data = msg["data"]
    messages = data if isinstance(data, list) else [data]
    for m in messages:
        yield _apply_message(books, m, local_ts, primary_asset)


def build_market_event(
    books: Dict[str, OrderBook],
    msg: dict,
    *,
    primary_asset: Optional[str] = None,
) -> MarketEvent:
    """Backward-compatible single-event helper: apply every message in the frame
    and return the LAST resulting MarketEvent. Code that must not drop batched
    messages should iterate :func:`build_market_events` instead.
    """
    last: Optional[MarketEvent] = None
    for ev in build_market_events(books, msg, primary_asset=primary_asset):
        last = ev
    if last is None:  # empty frame (data == [])
        return MarketEvent(
            local_timestamp=float(msg["local_timestamp"]),
            exchange_timestamp=None, event_type=None, asset_ids=(),
            trade=None, books=books, primary_asset=primary_asset,
        )
    return last


def iter_market_events(
    path: str,
    *,
    primary_asset: Optional[str] = None,
    strict_ordering: bool = True,
) -> Iterator[MarketEvent]:
    """Stream :class:`MarketEvent` objects from a recorded JSONL log.

    Messages are consumed in *file order*, which equals ``local_timestamp``
    arrival order. The state change for each message is applied first and the
    event is yielded afterwards, so the consumer can never see the future.

    Args:
        path: Path to the JSONL log.
        primary_asset: ``asset_id`` to treat as the primary (the token the bot
            quotes). If ``None``, the first token touched by each event is used.
        strict_ordering: If True (default), a ``local_timestamp`` that goes
            backwards raises ``ValueError`` rather than being silently accepted.

    Yields:
        One :class:`MarketEvent` per message, in strict time order
        (a frame may carry several messages).
    """
    books: Dict[str, OrderBook] = {}
    last_local: Optional[float] = None

    for lineno, msg in enumerate(_iter_json_lines(path)):
        local_ts = float(msg["local_timestamp"])
        if last_local is not None and local_ts < last_local:
            if strict_ordering:
                raise ValueError(
                    f"out-of-order local_timestamp at line {lineno}: "
                    f"{local_ts!r} < {last_local!r}"
                )
        last_local = local_ts

        yield from build_market_events(books, msg, primary_asset=primary_asset)


__all__ = [
    "build_market_event",
    "build_market_events",
    "Price",
    "Size",
    "TopOfBook",
    "OrderBook",
    "Trade",
    "MarketEvent",
    "iter_market_events",
]
