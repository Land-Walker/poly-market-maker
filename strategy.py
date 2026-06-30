"""strategy.py — logit-space Avellaneda-Stoikov market maker + backtest loop.

This is the *minimal* strategy whose only job is to exercise the PnL-attribution
engine (analytics.py) on a simple, hand-checkable fill/inventory path. It runs a
single bid+ask pair, but every interface here is written to accept N concurrent
orders (multiple levels) so that BAND/AMM strategies can later feed the same
FillSimulator + analytics with no rework. It does NOT replace those strategies.

Consumes Phase-1 types unchanged: data_loader.iter_market_events / MarketEvent /
OrderBook and fill_simulator.FillSimulator / MyOrder / Fill.

Design decisions (see ASSUMPTIONS.md Phase 2):
* Single token (...5616) is quoted; the two-token skeleton is preserved but only
  the primary token drives quotes/PnL. No complement-seeding of the NO token.
* logit space: x = ln(p/(1-p)); quotes are formed in logit space and mapped back
  with sigmoid so reservation price and half-spread use ONE consistent mapping.
* Horizon H=(T-t): default constant `tau` (infinite-horizon approximation),
  switchable to a terminal countdown via config (hook). Rationale: sparse data
  near expiry makes a countdown blow quotes up; constant tau isolates validation.
* Requote: cancel/replace only when the target price moves >= `requote_tick_
  threshold` ticks (default 1). A fixed-interval mode is also provided for
  sensitivity sweeps. Requoting every event is intentionally NOT the default
  (queue resets would suppress nearly all fills).
* kappa: constant from config; an online estimate is an explicit extension point
  (kappa_mode="estimate" is a documented hook, not active) — 77 trade prints are
  too few to estimate fill-intensity decay stably.

Dependencies: standard library only (math, decimal, collections).
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Deque, List, Optional, Tuple

from data_loader import MarketEvent, OrderBook, iter_market_events
from fill_simulator import Fill, FillSimulator, MyOrder


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class StrategyConfig:
    """All tunables exposed for sweeping (see analytics.adverse_window_sweep and
    analytics.param_sweep)."""

    gamma: float = 1.0              # risk aversion
    kappa: float = 10.0             # order-flow intensity (constant start)
    kappa_mode: str = "const"       # "const" | "estimate" (estimate = extension hook)
    sigma_window: int = 50          # events in the rolling logit-return std
    horizon_mode: str = "const"     # "const" (tau) | "terminal" (countdown hook)
    tau: float = 1.0                # constant horizon H when horizon_mode="const"
    t_expiry: Optional[float] = None  # epoch secs; used only if horizon_mode="terminal"
    horizon_scale: float = 300.0    # seconds scaling for terminal countdown
    min_horizon: float = 1e-3       # floor for terminal H near expiry
    max_inventory: float = 100.0    # |inventory| cap; stop quoting the breaching side
    order_size: float = 10.0        # size per quoted order
    requote_mode: str = "tick"      # "tick" | "interval"
    requote_tick_threshold: int = 1 # ticks of price move that trigger a requote
    requote_interval_seconds: float = 5.0  # used when requote_mode="interval"
    tick_size: float = 0.01
    price_eps: float = 1e-4         # clip p into [eps, 1-eps] before logit
    sigma_floor: float = 1e-6       # floor on estimated sigma
    adverse_window_seconds: float = 1.0  # passed through to FillSimulator


# ---------------------------------------------------------------------------
# logit / price helpers
# ---------------------------------------------------------------------------
def logit(p: float) -> float:
    """Logit transform x = ln(p/(1-p)). Caller must pass p in (0,1)."""
    return math.log(p / (1.0 - p))


def sigmoid(x: float) -> float:
    """Inverse logit p = 1/(1+e^-x); always returns a value in (0,1)."""
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def clip_prob(p: float, eps: float) -> float:
    """Clip a probability into [eps, 1-eps] to keep the logit finite."""
    return min(1.0 - eps, max(eps, p))


def price_to_tick(p_float: float, tick: float, side: str) -> Decimal:
    """Round a price to the tick grid: bids floor, asks ceil, kept in [tick,1-tick].

    Flooring bids / ceiling asks keeps quotes from crossing the intended level and
    is the conservative direction (slightly wider than the raw A-S price).
    """
    d = Decimal(str(p_float))
    t = Decimal(str(tick))
    n = d / t
    ticks = n.to_integral_value(rounding=ROUND_FLOOR if side == "bid" else ROUND_CEILING)
    price = ticks * t
    lo, hi = t, Decimal("1") - t
    if price < lo:
        price = lo
    if price > hi:
        price = hi
    return price


def horizon(cfg: StrategyConfig, ts: float) -> float:
    """Effective A-S horizon H=(T-t).

    Default constant ``tau``. If ``horizon_mode="terminal"`` and ``t_expiry`` is
    set, returns a floored, scaled countdown to expiry. The countdown is an
    explicit hook; constant tau is the validated default.
    """
    if cfg.horizon_mode == "terminal" and cfg.t_expiry is not None:
        return max((cfg.t_expiry - ts) / cfg.horizon_scale, cfg.min_horizon)
    return cfg.tau


def as_quotes_logit(
    x: float, q: float, sigma: float, cfg: StrategyConfig, ts: float
) -> Tuple[float, float, float, float]:
    """Avellaneda-Stoikov reservation price and half-spread in LOGIT space.

    Returns ``(bid_logit, ask_logit, reservation, delta)`` where:
        reservation r = x - q * gamma * sigma^2 * H        (inventory skew)
        half-spread  delta = gamma*sigma^2*H/2 + (1/gamma)*ln(1+gamma/kappa)
    sigma is the volatility of the logit-mid, so r and delta live in the same
    space and map back to price with one sigmoid — this consistency is unit
    tested.
    """
    H = horizon(cfg, ts)
    r = x - q * cfg.gamma * sigma * sigma * H
    delta = 0.5 * cfg.gamma * sigma * sigma * H + (1.0 / cfg.gamma) * math.log(
        1.0 + cfg.gamma / cfg.kappa
    )
    return r - delta, r + delta, r, delta


# ---------------------------------------------------------------------------
# Quoting engine (stateless A-S given mid + inventory)
# ---------------------------------------------------------------------------
class ASMarketMaker:
    """Tracks rolling logit volatility and produces tick-aligned bid/ask quotes."""

    def __init__(self, cfg: StrategyConfig) -> None:
        self.cfg = cfg
        self._rets: Deque[float] = deque(maxlen=cfg.sigma_window)
        self._last_x: Optional[float] = None

    def observe_mid(self, mid: float) -> float:
        """Update the rolling logit-return buffer with a new mid; return its logit."""
        x = logit(clip_prob(mid, self.cfg.price_eps))
        if self._last_x is not None:
            self._rets.append(x - self._last_x)
        self._last_x = x
        return x

    def sigma(self) -> float:
        """Sample std-dev of recent logit returns (floored)."""
        if len(self._rets) < 2:
            return self.cfg.sigma_floor
        m = sum(self._rets) / len(self._rets)
        var = sum((r - m) ** 2 for r in self._rets) / (len(self._rets) - 1)
        return max(math.sqrt(var), self.cfg.sigma_floor)

    def quotes(
        self, x: float, q: float, ts: float
    ) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Return ``(bid_price, ask_price)`` as tick-aligned Decimals (or None).

        A side is None when the inventory cap is hit on that side. bid<ask is
        enforced (widened by one tick if rounding collapsed them).
        """
        sigma = self.sigma()
        bid_l, ask_l, _r, _delta = as_quotes_logit(x, q, sigma, self.cfg, ts)
        bid = price_to_tick(sigmoid(bid_l), self.cfg.tick_size, "bid")
        ask = price_to_tick(sigmoid(ask_l), self.cfg.tick_size, "ask")
        if q >= self.cfg.max_inventory:
            bid = None  # too long -> stop bidding
        if q <= -self.cfg.max_inventory:
            ask = None  # too short -> stop offering
        if bid is not None and ask is not None and bid >= ask:
            ask = bid + Decimal(str(self.cfg.tick_size))
        return bid, ask


# ---------------------------------------------------------------------------
# Backtest engine (N-order ready)
# ---------------------------------------------------------------------------
@dataclass
class _ActiveOrder:
    order: MyOrder
    sim: FillSimulator
    processed: int = 0          # fills already booked into inventory/cash
    cancel_ts: Optional[float] = None  # set when moved to draining


@dataclass
class FillRecord:
    """A flat, side-tagged fill record consumed by analytics. Works identically
    for one pair or many levels."""

    local_timestamp: float
    side: str                   # "BUY" (bid filled) or "SELL" (ask filled)
    qty: float
    fill_price: float
    mid_before: Optional[float]
    mid_after: Optional[float]
    adverse: Optional[bool]
    measured_dt: Optional[float]
    queue_ahead_before: float


@dataclass
class BacktestResult:
    fills: List[FillRecord]
    inventory_series: List[Tuple[float, float]]
    mid_series: List[Tuple[float, Optional[float]]]
    equity_series: List[Tuple[float, Optional[float]]]
    cash_final: float
    inv_final: float
    mid_final: Optional[float]
    quoted_qty: float
    n_quotes: int
    parity_samples: List[Tuple[float, Optional[float], bool]]
    config: StrategyConfig


def _should_requote(ao: _ActiveOrder, target: Decimal, ts: float, cfg: StrategyConfig) -> bool:
    """Requote decision for an existing order on a side."""
    if not ao.order.active:
        return True
    if cfg.requote_mode == "interval":
        return (ts - ao.order.place_local_ts) >= cfg.requote_interval_seconds
    ticks = abs(ao.order.price - target) / Decimal(str(cfg.tick_size))
    return ticks >= cfg.requote_tick_threshold


def run_backtest(
    path: str,
    cfg: StrategyConfig,
    *,
    primary_asset: str,
    yes_asset: Optional[str] = None,
    no_asset: Optional[str] = None,
) -> BacktestResult:
    """Run the single-pair A-S backtest over a recorded JSONL log.

    Inventory/cash convention (initial 0/0): a BUY (bid) fill does
    ``inv += qty; cash -= qty*price``; a SELL (ask) fill does
    ``inv -= qty; cash += qty*price``. Mark-to-market equity = cash + inv*mid.

    Adverse selection is measured by FillSimulator over ``cfg.adverse_window_
    seconds``. To measure it correctly even after a quote is cancelled/filled,
    cancelled orders are *drained* (kept receiving events, but with
    ``order.active=False`` so they cannot fill again) until their adverse window
    elapses; only then are they finalised. See ASSUMPTIONS.md Phase 2.
    """
    mm = ASMarketMaker(cfg)
    active: List[_ActiveOrder] = []     # currently quoting (the bid+ask pair)
    draining: List[_ActiveOrder] = []   # cancelled/filled, resolving adverse only
    ledger: List[Tuple[str, Fill]] = []

    inv = 0.0
    cash = 0.0
    quoted_qty = 0.0
    n_quotes = 0
    last_mid: Optional[float] = None

    inventory_series: List[Tuple[float, float]] = []
    mid_series: List[Tuple[float, Optional[float]]] = []
    equity_series: List[Tuple[float, Optional[float]]] = []
    parity_samples: List[Tuple[float, Optional[float], bool]] = []

    def book_fills(ao: _ActiveOrder) -> None:
        nonlocal inv, cash
        while ao.processed < len(ao.order.fills):
            f = ao.order.fills[ao.processed]
            ao.processed += 1
            price = float(ao.order.price)
            sq = f.qty if ao.order.side == "BUY" else -f.qty
            inv += sq
            cash -= sq * price  # BUY: cash-=qty*price; SELL: cash+=qty*price
            ledger.append((ao.order.side, f))

    for ev in iter_market_events(path, primary_asset=primary_asset):
        ts = ev.local_timestamp
        book = ev.book(primary_asset)
        mid = book.mid() if book is not None else None
        if mid is not None:
            last_mid = mid

        # 1) advance active orders, book any new fills
        for ao in list(active):
            ao.sim.process(ev)
            book_fills(ao)
            if not ao.order.active:  # fully filled -> drain for adverse window
                ao.cancel_ts = ts
                active.remove(ao)
                draining.append(ao)

        # 2) advance draining orders (resolve adverse only; cannot fill)
        for ao in list(draining):
            ao.sim.process(ev)
            book_fills(ao)  # no-op (inactive) but safe
            if ao.cancel_ts is not None and (ts - ao.cancel_ts) >= cfg.adverse_window_seconds:
                ao.sim.finalize(last_mid=mid if mid is not None else last_mid)
                draining.remove(ao)

        # 3) requote on a valid, synced two-sided mid
        if mid is not None and book is not None and book.synced:
            x = mm.observe_mid(mid)
            tgt_bid, tgt_ask = mm.quotes(x, inv, ts)
            for side, target in (("BUY", tgt_bid), ("SELL", tgt_ask)):
                existing = [ao for ao in active if ao.order.side == side]
                if target is None:
                    for ao in existing:  # cancel: stop filling, drain for adverse
                        ao.order.active = False
                        ao.cancel_ts = ts
                        active.remove(ao)
                        draining.append(ao)
                    continue
                if existing:
                    ao = existing[0]
                    if not _should_requote(ao, target, ts, cfg):
                        continue
                    ao.order.active = False
                    ao.cancel_ts = ts
                    active.remove(ao)
                    draining.append(ao)
                order = MyOrder(side=side, price=target, size=cfg.order_size,
                                asset_id=primary_asset, place_local_ts=ts)
                sim = FillSimulator(order, adverse_window_seconds=cfg.adverse_window_seconds)
                active.append(_ActiveOrder(order=order, sim=sim))
                quoted_qty += cfg.order_size
                n_quotes += 1

        # 4) record series
        inventory_series.append((ts, inv))
        mid_series.append((ts, mid))
        equity_series.append((ts, (cash + inv * mid) if mid is not None else None))
        if yes_asset is not None and no_asset is not None:
            dev, reliable = ev.parity_deviation(yes_asset, no_asset)
            parity_samples.append((ts, dev, reliable))

    # finalise everything still open (resolves/None-marks remaining adverse)
    for ao in active + draining:
        ao.sim.finalize(last_mid=last_mid)
        book_fills(ao)

    # build flat fill records (adverse now resolved)
    fills: List[FillRecord] = []
    for side, f in ledger:
        fills.append(
            FillRecord(
                local_timestamp=f.local_timestamp,
                side=side,
                qty=f.qty,
                fill_price=float(f.price),
                mid_before=f.mid_before,
                mid_after=f.mid_after,
                adverse=f.adverse,
                measured_dt=f.measured_dt,
                queue_ahead_before=f.queue_ahead_before,
            )
        )

    return BacktestResult(
        fills=fills,
        inventory_series=inventory_series,
        mid_series=mid_series,
        equity_series=equity_series,
        cash_final=cash,
        inv_final=inv,
        mid_final=last_mid,
        quoted_qty=quoted_qty,
        n_quotes=n_quotes,
        parity_samples=parity_samples,
        config=cfg,
    )


__all__ = [
    "StrategyConfig",
    "logit",
    "sigmoid",
    "clip_prob",
    "price_to_tick",
    "horizon",
    "as_quotes_logit",
    "ASMarketMaker",
    "FillRecord",
    "BacktestResult",
    "run_backtest",
]
