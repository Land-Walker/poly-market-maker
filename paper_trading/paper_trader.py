"""paper_trader.py — live PAPER trading (virtual orders only) for the A-S strategy.

Phase-1 goal is *continuity*, not profit: receive the live Polymarket order book,
quote with the validated A-S strategy, judge fills with the validated conservative
queue/fill engine, and have those virtual fills reconcile in analytics. Zero real
orders are ever sent.

  ┌──────────────┐   ws frame    ┌───────────────────────────────────────────┐
  │ Polymarket   │ ───────────▶  │ handle_message()                          │
  │ market WSS   │               │  1. append {local_timestamp,slug,data} log │
  └──────────────┘               │  2. data_loader.build_market_event -> book │
                                  │  3. VirtualOrderManager.step  -> fills     │
                                  │  4. (requote gate) ASMarketMaker.quotes    │
                                  └───────────────────────────────────────────┘

Reuse map (root modules only — poly_market_maker/ is NEVER imported or modified):
  * strategy        : StrategyConfig, ASMarketMaker(observe_mid/sigma/quotes),
                      _should_requote, FillRecord
  * fill_simulator  : FillSimulator, MyOrder, Fill  (queue + tri-state adverse)
  * data_loader     : OrderBook, Trade, MarketEvent, build_market_event
  * analytics       : attribute_pnl, adverse_diagnostics
``run_backtest`` is deliberately NOT used; its per-event orchestration
(quotes / _should_requote / fill judging) is reproduced here for the live loop.

SAFETY: there is no order-routing code in this module. No place/cancel/submit
API, no signing, no private keys, no funds path. Orders exist only as in-memory
``MyOrder`` objects judged against the historical/live tape. It is structurally
impossible for this file to move money.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

# Put the repo root on sys.path BEFORE importing the shared engine modules,
# since this file now lives in paper_trading/ while the engine stays at root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analytics import adverse_diagnostics, attribute_pnl  # noqa: E402
from data_loader import MarketEvent, OrderBook, Trade, build_market_events  # noqa: E402
from fill_simulator import Fill, FillSimulator, MyOrder  # noqa: E402
from strategy import ASMarketMaker, FillRecord, StrategyConfig, _should_requote  # noqa: E402

WSS_URI = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
@dataclass
class PaperConfig:
    token_id: str
    slug: str = "paper"
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    log_path: Optional[str] = None        # JSONL tape (same format as the recorder)
    state_path: Optional[str] = None      # periodic inventory/cash/ledger snapshot
    reload_state: bool = True             # restore inv/cash/ledger on startup
    adverse_resolve_interval: float = 1.0  # seconds between heartbeat resolves
    state_save_interval: float = 30.0      # seconds between state snapshots
    reconnect_max_backoff: float = 30.0


# ---------------------------------------------------------------------------
# Virtual order manager (N-order ready) — mirrors run_backtest orchestration
# ---------------------------------------------------------------------------
@dataclass
class _ActiveOrder:
    order: MyOrder
    sim: FillSimulator
    processed: int = 0
    cancel_ts: Optional[float] = None


class VirtualOrderManager:
    """Manages virtual resting orders and judges fills via FillSimulator.

    VIRTUAL ONLY: nothing here sends an order anywhere. Cancelled/filled orders
    are *drained* (kept receiving events but marked inactive so they can never
    fill again) until their adverse window elapses, then finalised — identical to
    the backtest engine, so the conservative queue assumption is preserved.
    """

    def __init__(self, asset_id: str) -> None:
        self.asset_id = asset_id
        self.active: List[_ActiveOrder] = []
        self.draining: List[_ActiveOrder] = []

    def step(
        self, event: MarketEvent, mid: Optional[float], ts: float, cfg: PaperConfig
    ) -> List[Tuple[str, Fill]]:
        """Advance all virtual orders by one event; return newly booked fills."""
        new_fills: List[Tuple[str, Fill]] = []
        for ao in list(self.active):
            ao.sim.process(event)
            while ao.processed < len(ao.order.fills):
                new_fills.append((ao.order.side, ao.order.fills[ao.processed]))
                ao.processed += 1
            if not ao.order.active:        # fully filled -> drain for adverse window
                ao.cancel_ts = ts
                self.active.remove(ao)
                self.draining.append(ao)
        for ao in list(self.draining):
            ao.sim.process(event)
            while ao.processed < len(ao.order.fills):  # safety (inactive: normally none)
                new_fills.append((ao.order.side, ao.order.fills[ao.processed]))
                ao.processed += 1
            if ao.cancel_ts is not None and (ts - ao.cancel_ts) >= cfg.strategy.adverse_window_seconds:
                ao.sim.finalize(last_mid=mid)
                self.draining.remove(ao)
        return new_fills

    def requote(
        self, book: OrderBook, mid: float, ts: float, inv: float,
        mm: ASMarketMaker, cfg: PaperConfig,
    ) -> Tuple[int, float]:
        """Recompute desired quotes and cancel/replace per the requote gate.

        Returns ``(n_new_orders, quoted_qty_added)``.
        """
        x = mm.observe_mid(mid)
        tgt_bid, tgt_ask = mm.quotes(x, inv, ts)
        n_new = 0
        qty_added = 0.0
        for side, target in (("BUY", tgt_bid), ("SELL", tgt_ask)):
            existing = [ao for ao in self.active if ao.order.side == side]
            if target is None:                      # inventory cap -> pull the side
                for ao in existing:
                    ao.order.active = False
                    ao.cancel_ts = ts
                    self.active.remove(ao)
                    self.draining.append(ao)
                continue
            if existing:
                ao = existing[0]
                if not _should_requote(ao, target, ts, cfg.strategy):
                    continue
                ao.order.active = False             # cancel: give up queue, drain
                ao.cancel_ts = ts
                self.active.remove(ao)
                self.draining.append(ao)
            order = MyOrder(side=side, price=target, size=cfg.strategy.order_size,
                            asset_id=self.asset_id, place_local_ts=ts)
            sim = FillSimulator(order, adverse_window_seconds=cfg.strategy.adverse_window_seconds)
            self.active.append(_ActiveOrder(order=order, sim=sim))
            n_new += 1
            qty_added += cfg.strategy.order_size
        return n_new, qty_added


# ---------------------------------------------------------------------------
# Paper trader
# ---------------------------------------------------------------------------
class PaperTrader:
    def __init__(self, cfg: PaperConfig) -> None:
        self.cfg = cfg
        self.books: Dict[str, OrderBook] = {}
        self.mm = ASMarketMaker(cfg.strategy)
        self.vom = VirtualOrderManager(cfg.token_id)
        self.inv = 0.0
        self.cash = 0.0
        self.last_mid: Optional[float] = None
        self.n_quotes = 0
        self.quoted_qty = 0.0
        self.ledger: List[Tuple[str, Fill]] = []
        self._stop = False
        if cfg.reload_state:
            self.load_state()

    # -- accounting --------------------------------------------------------
    def _book_fill(self, side: str, f: Fill) -> None:
        """Apply a virtual fill to inventory/cash (initial 0/0)."""
        price = float(f.price)
        sq = f.qty if side == "BUY" else -f.qty
        self.inv += sq
        self.cash -= sq * price     # BUY: cash-=qty*price ; SELL: cash+=qty*price
        self.ledger.append((side, f))

    # -- message handling (SYNC, unit-testable, no network) ----------------
    def handle_message(self, data, local_ts: Optional[float] = None) -> None:
        """Process one raw WS message ``data`` (the inner payload of a frame).

        ``data`` is exactly what the recorder stored under the ``"data"`` key, so
        the live tape is byte-compatible with backtest input.
        """
        if local_ts is None:
            local_ts = time.time()
        msg = {"local_timestamp": local_ts, "slug": self.cfg.slug, "data": data}
        self._log_record(msg)

        token = self.cfg.token_id
        # A frame may batch several messages (e.g. YES + NO price_change, or a
        # list of mixed book/price_change/trade). Process each independently.
        for event in build_market_events(self.books, msg, primary_asset=token):
            book = event.book(token)
            mid = book.mid() if book is not None else None
            if mid is not None:
                self.last_mid = mid

            # Fill judging always runs: fill_simulator isolates by asset_id, so
            # only OUR token's virtual orders can ever fill.
            for side, f in self.vom.step(event, mid, local_ts, self.cfg):
                self._book_fill(side, f)

            # Quoting / volatility update are HARD-GATED to events that actually
            # touched our token. A NO-token-only update never re-quotes and never
            # pushes into the sigma buffer (single-token isolation).
            if (token in event.asset_ids) and book is not None and book.synced and mid is not None:
                n, q = self.vom.requote(book, mid, local_ts, self.inv, self.mm, self.cfg)
                self.n_quotes += n
                self.quoted_qty += q

    def heartbeat(self, ts: Optional[float] = None) -> None:
        """Feed a no-op event so the engine resolves pending adverse windows
        during quiet periods (no new fills, no queue change)."""
        if ts is None:
            ts = time.time()
        book = self.books.get(self.cfg.token_id)
        mid = book.mid() if book is not None else None
        ev = MarketEvent(local_timestamp=ts, exchange_timestamp=None, event_type=None,
                         asset_ids=(self.cfg.token_id,), trade=None, books=self.books,
                         primary_asset=self.cfg.token_id)
        for side, f in self.vom.step(ev, mid, ts, self.cfg):
            self._book_fill(side, f)

    # -- logging / persistence --------------------------------------------
    def _log_record(self, msg: dict) -> None:
        if not self.cfg.log_path:
            return
        with open(self.cfg.log_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(msg) + "\n")

    def _serialize_fill(self, side: str, f: Fill) -> dict:
        return {
            "side": side, "local_timestamp": f.local_timestamp,
            "price": str(f.price), "qty": f.qty,
            "remaining_after": f.remaining_after,
            "queue_ahead_before": f.queue_ahead_before,
            "mid_before": f.mid_before, "mid_after": f.mid_after,
            "mid_after_local_ts": f.mid_after_local_ts,
            "measured_dt": f.measured_dt, "adverse": f.adverse,
        }

    def persist_state(self) -> None:
        """Atomically snapshot inventory/cash/ledger/last_mid to disk."""
        if not self.cfg.state_path:
            return
        state = {
            "inv": self.inv, "cash": self.cash, "last_mid": self.last_mid,
            "n_quotes": self.n_quotes, "quoted_qty": self.quoted_qty,
            "ledger": [self._serialize_fill(s, f) for s, f in self.ledger],
        }
        tmp = self.cfg.state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(state, fh)
        os.replace(tmp, self.cfg.state_path)

    def load_state(self) -> None:
        """Restore cumulative inv/cash/ledger/last_mid. Open virtual orders and
        queue positions are intentionally NOT restored — after downtime the book
        has changed and any queue position is meaningless; the next requote lays
        fresh quotes."""
        path = self.cfg.state_path
        if not path or not os.path.exists(path):
            return
        with open(path, "r", encoding="utf-8") as fh:
            state = json.load(fh)
        self.inv = state.get("inv", 0.0)
        self.cash = state.get("cash", 0.0)
        self.last_mid = state.get("last_mid")
        self.n_quotes = state.get("n_quotes", 0)
        self.quoted_qty = state.get("quoted_qty", 0.0)
        self.ledger = []
        for d in state.get("ledger", []):
            f = Fill(
                local_timestamp=d["local_timestamp"], price=Decimal(d["price"]),
                qty=d["qty"], remaining_after=d.get("remaining_after", 0.0),
                queue_ahead_before=d.get("queue_ahead_before", 0.0),
                mid_before=d.get("mid_before"), mid_after=d.get("mid_after"),
                mid_after_local_ts=d.get("mid_after_local_ts"),
                measured_dt=d.get("measured_dt"), adverse=d.get("adverse"),
            )
            self.ledger.append((d["side"], f))

    # -- analytics ---------------------------------------------------------
    def fill_records(self) -> List[FillRecord]:
        return [
            FillRecord(
                local_timestamp=f.local_timestamp, side=side, qty=f.qty,
                fill_price=float(f.price), mid_before=f.mid_before,
                mid_after=f.mid_after, adverse=f.adverse,
                measured_dt=f.measured_dt, queue_ahead_before=f.queue_ahead_before,
            )
            for side, f in self.ledger
        ]

    def analytics_snapshot(self) -> Optional[dict]:
        """Run PnL attribution + reconciliation on the live ledger (same engine
        as backtest). Returns None if no valid mid has been seen yet."""
        if self.last_mid is None:
            return None
        fills = self.fill_records()
        attr = attribute_pnl(fills, mid_final=self.last_mid, cash_final=self.cash,
                             inv_final=self.inv, assert_reconcile=False)
        adv = adverse_diagnostics(fills)
        filled_qty = sum(f.qty for f in fills)
        return {
            "n_fills": len(fills), "inv": self.inv, "cash": self.cash,
            "last_mid": self.last_mid, "n_quotes": self.n_quotes,
            "quoted_qty": self.quoted_qty,
            "fill_ratio": (filled_qty / self.quoted_qty) if self.quoted_qty else 0.0,
            "spread_capture": attr.spread_capture,
            "inventory_pnl": attr.inventory_pnl,
            "adverse_selection_cost": attr.adverse_selection_cost,
            "total_pnl": attr.total_pnl,
            "reconciliation_error": attr.reconciliation_error,
            "adverse": adv,
        }

    # -- async runtime -----------------------------------------------------
    async def _adverse_resolver_loop(self) -> None:
        while not self._stop:
            await asyncio.sleep(self.cfg.adverse_resolve_interval)
            self.heartbeat()

    async def _state_saver_loop(self) -> None:
        while not self._stop:
            await asyncio.sleep(self.cfg.state_save_interval)
            self.persist_state()

    async def run(self) -> None:
        """Connect, subscribe, and stream messages with reconnect/backoff.

        ``websockets`` is imported lazily so the module (and its tests) import
        without the dependency installed.
        """
        import websockets  # lazy: not needed for unit tests

        tasks = [
            asyncio.ensure_future(self._adverse_resolver_loop()),
            asyncio.ensure_future(self._state_saver_loop()),
        ]
        backoff = 1.0
        try:
            while not self._stop:
                try:
                    async with websockets.connect(
                        WSS_URI, ping_interval=20, ping_timeout=20
                    ) as ws:
                        await ws.send(json.dumps(
                            {"type": "market", "assets_ids": [self.cfg.token_id]}
                        ))
                        backoff = 1.0
                        async for raw in ws:
                            frame = json.loads(raw)
                            # The recorder stored each frame as the ``data`` field;
                            # build_market_event handles both dict and list frames.
                            self.handle_message(frame)
                except Exception as exc:  # noqa: BLE001 — keep the loop alive
                    print(f"[paper_trader] ws error: {exc!r}; reconnecting in {backoff:.0f}s")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, self.cfg.reconnect_max_backoff)
        finally:
            self._stop = True
            for t in tasks:
                t.cancel()
            self.persist_state()

    def stop(self) -> None:
        self._stop = True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _build_config(argv: Optional[List[str]] = None) -> PaperConfig:
    p = argparse.ArgumentParser(description="Polymarket A-S PAPER trader (virtual orders only).")
    p.add_argument("--token-id", required=True, help="YES outcome token_id to quote")
    p.add_argument("--slug", default="paper", help="market slug (for the tape/log)")
    p.add_argument("--log", dest="log_path", default=None, help="JSONL tape output path")
    p.add_argument("--state", dest="state_path", default=None, help="state snapshot path")
    p.add_argument("--no-reload", dest="reload_state", action="store_false",
                   help="start fresh (inv/cash=0) instead of restoring state")
    p.add_argument("--gamma", type=float, default=StrategyConfig.gamma)
    p.add_argument("--kappa", type=float, default=StrategyConfig.kappa)
    p.add_argument("--order-size", type=float, default=StrategyConfig.order_size)
    p.add_argument("--max-inventory", type=float, default=StrategyConfig.max_inventory)
    p.add_argument("--adverse-window", type=float, default=StrategyConfig.adverse_window_seconds)
    a = p.parse_args(argv)
    scfg = StrategyConfig(
        gamma=a.gamma, kappa=a.kappa, order_size=a.order_size,
        max_inventory=a.max_inventory, adverse_window_seconds=a.adverse_window,
    )
    return PaperConfig(
        token_id=a.token_id, slug=a.slug, log_path=a.log_path,
        state_path=a.state_path, reload_state=a.reload_state, strategy=scfg,
    )


def main(argv: Optional[List[str]] = None) -> None:
    cfg = _build_config(argv)
    trader = PaperTrader(cfg)
    try:
        asyncio.run(trader.run())
    except KeyboardInterrupt:
        trader.stop()
        print("\n[paper_trader] stopped.")
        snap = trader.analytics_snapshot()
        if snap is not None:
            print(json.dumps(snap, indent=2, default=str))


if __name__ == "__main__":
    main()


__all__ = ["WSS_URI", "PaperConfig", "VirtualOrderManager", "PaperTrader"]
