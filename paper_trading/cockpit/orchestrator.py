"""orchestrator.py — multi-market cockpit layer over paper_trader (VIRTUAL ONLY).

Runs N independent ``PaperTrader`` instances as asyncio tasks in ONE event
loop, and exposes runtime control:

  * ``MarketRunner.apply_params``  — live parameter updates (validate -> mutate
    the shared StrategyConfig in place -> queue reset -> immediate requote ->
    event log). The engine reads its config at every requote, so an in-place
    field mutation is picked up with ZERO engine changes.
  * ``MarketRunner.snapshot``      — dashboard state (book + best-level queue
    sizes, my quotes, inventory/cash/equity, PnL attribution, recent fills,
    current params, history ring buffer).
  * start / stop / loss-limit auto-stop, add / remove markets.

Reuse map (additive layer — nothing below is modified):
  * paper_trading.paper_trader : PaperTrader, PaperConfig
  * strategy                   : StrategyConfig
  * analytics                  : via PaperTrader.analytics_snapshot()
  * poly_market_maker/         : NEVER imported (unchanged rule)

SAFETY: there is no order-routing code in this module. No place/cancel/submit
API, no signing, no private keys, no funds path. "Orders" are the in-memory
``MyOrder`` objects of the fill simulator. Structurally incapable of trading.

Concurrency note: every trader WS task AND every control-server handler runs
on the same asyncio event loop, so ``apply_params`` can never interleave with
event processing — plain in-place mutation is atomic here. No locks.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

# cockpit/ lives inside paper_trading/; make both paper_trading/ (for
# paper_trader) and the repo root (for the engine modules) importable.
_HERE = os.path.dirname(os.path.abspath(__file__))
_PT_DIR = os.path.dirname(_HERE)
_ROOT = os.path.dirname(_PT_DIR)
for _p in (_PT_DIR, _ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from paper_trader import PaperConfig, PaperTrader  # noqa: E402
from strategy import StrategyConfig  # noqa: E402


# ---------------------------------------------------------------------------
# Live-tunable parameters (the cockpit's four levers)
# ---------------------------------------------------------------------------
# sigma_window etc. are deliberately NOT here: the rolling deque's maxlen is
# fixed at ASMarketMaker init, so it cannot be changed at runtime safely.
_PARAM_RULES = {
    "gamma":         (lambda v: v > 0.0,  "gamma must be > 0 (0 -> division by zero)"),
    "kappa":         (lambda v: v > 0.0,  "kappa must be > 0"),
    "order_size":    (lambda v: v > 0.0,  "order_size must be > 0"),
    "max_inventory": (lambda v: v >= 0.0, "max_inventory must be >= 0"),
}
TUNABLE_PARAMS = tuple(_PARAM_RULES)


def validate_params(updates: dict) -> tuple:
    """Pure validation: returns ``(clean, errors)``. All-or-nothing semantics
    are enforced by the caller (apply nothing if ``errors`` is non-empty)."""
    errors: Dict[str, str] = {}
    clean: Dict[str, float] = {}
    for k, v in updates.items():
        if k not in _PARAM_RULES:
            errors[k] = "unknown parameter"
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            errors[k] = "not a number"
            continue
        check, msg = _PARAM_RULES[k]
        if math.isnan(f) or math.isinf(f) or not check(f):
            errors[k] = msg
        else:
            clean[k] = f
    return clean, errors


# ---------------------------------------------------------------------------
# Market spec (one entry of markets.json)
# ---------------------------------------------------------------------------
@dataclass
class MarketSpec:
    token_id: str
    slug: str
    label: str = ""                    # e.g. "thin" | "mid" | "thick" (display only)
    params: Dict[str, float] = field(default_factory=dict)
    loss_limit: Optional[float] = None  # auto-stop when equity < loss_limit

    @classmethod
    def from_dict(cls, d: dict) -> "MarketSpec":
        clean, errors = validate_params(d.get("params", {}))
        if errors:
            raise ValueError(f"market {d.get('slug')!r}: bad params: {errors}")
        if not d.get("token_id"):
            raise ValueError(f"market {d.get('slug')!r}: token_id is required "
                             "(run select_markets.py --liquidity-spread to pick)")
        return cls(token_id=str(d["token_id"]), slug=str(d.get("slug", "market")),
                   label=str(d.get("label", "")), params=clean,
                   loss_limit=d.get("loss_limit"))


# ---------------------------------------------------------------------------
# MarketRunner — one market: trader + task + control + history
# ---------------------------------------------------------------------------
class MarketRunner:
    def __init__(self, spec: MarketSpec, log_dir: str, history_len: int = 300) -> None:
        self.spec = spec
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self.events_path = os.path.join(log_dir, f"{spec.slug}.events.jsonl")
        self.history: Deque[dict] = deque(maxlen=history_len)
        self.task = None                    # asyncio.Task while running
        self.running = False
        self.stopped_reason: Optional[str] = None
        self.last_param_error: Optional[dict] = None
        self.trader = self._make_trader()

    # -- construction -------------------------------------------------------
    def _make_trader(self) -> PaperTrader:
        scfg = StrategyConfig(**self.spec.params) if self.spec.params else StrategyConfig()
        cfg = PaperConfig(
            token_id=self.spec.token_id,
            slug=self.spec.slug,
            strategy=scfg,
            # tape keeps recording (backtest asset + replay/review material)
            log_path=os.path.join(self.log_dir, f"{self.spec.slug}.tape.jsonl"),
            state_path=os.path.join(self.log_dir, f"{self.spec.slug}.state.json"),
            reload_state=True,
        )
        return PaperTrader(cfg)

    # -- event log (learning asset: what did I touch, what happened) --------
    def _log_event(self, event: dict) -> None:
        with open(self.events_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(event) + "\n")

    # -- lifecycle -----------------------------------------------------------
    def start(self) -> None:
        """(Re)start the live WS loop. Recreates the trader after a stop:
        inv/cash/ledger reload from the state snapshot; the book and sigma
        buffer rebuild from the stream (same semantics as paper_trader after
        downtime — stale queue positions are meaningless anyway)."""
        if self.running:
            return
        if self.task is not None:           # restarting after a stop
            self.trader = self._make_trader()
        self.task = asyncio.ensure_future(self.trader.run())
        self.running = True
        self.stopped_reason = None
        self._log_event({"type": "start", "ts": time.time(),
                         "params": self.current_params()})

    async def stop(self, reason: str = "manual") -> None:
        """Cancel the WS task and persist state. Never routes anything."""
        self.running = False
        self.stopped_reason = reason
        if self.task is not None:
            self.trader.stop()
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:  # BaseException in py>=3.8 — must be
                pass                        # caught explicitly or it kills the
            except Exception:               # calling HTTP handler
                pass
        self.trader.persist_state()
        self._log_event({"type": "stop", "ts": time.time(), "reason": reason})

    def breached_loss_limit(self) -> bool:
        """True when mark-to-market equity has fallen below the loss limit."""
        if self.spec.loss_limit is None:
            return False
        eq = self._equity()
        return eq is not None and eq < self.spec.loss_limit

    # -- live parameter updates (the cockpit's core) -------------------------
    def apply_params(self, updates: dict) -> dict:
        """Validate -> mutate in place -> queue reset -> immediate requote -> log.

        VIRTUAL ONLY: this touches in-memory MyOrder objects; no order routing
        exists anywhere in this codebase path.
        """
        # 1) validate everything before changing anything (atomic)
        clean, errors = validate_params(updates)
        if errors or not clean:
            if not clean and not errors:
                errors = {"_request": "no parameters given"}
            self.last_param_error = errors
            return {"ok": False, "errors": errors}
        self.last_param_error = None

        # 2) in-place mutation — trader.mm.cfg and trader.cfg.strategy are the
        #    SAME StrategyConfig object; every requote reads it afresh.
        scfg = self.trader.cfg.strategy
        old = {k: getattr(scfg, k) for k in clean}
        for k, f in clean.items():
            setattr(scfg, k, f)

        # 3) queue reset — identical to VirtualOrderManager.requote's cancel
        #    path: deactivate and drain (adverse window still resolves).
        now = time.time()
        vom = self.trader.vom
        for ao in list(vom.active):
            ao.order.active = False
            ao.cancel_ts = now
            vom.active.remove(ao)
            vom.draining.append(ao)

        # 4) immediate requote with the new params (don't wait for the next
        #    event). Known micro-tradeoff: vom.requote calls mm.observe_mid,
        #    re-observing the same mid -> one 0-return in the sigma buffer.
        #    Accepted (engine stays unmodified) and recorded in the log.
        requoted = False
        book = self.trader.books.get(self.trader.cfg.token_id)
        if book is not None and book.synced:
            mid = book.mid()
            if mid is not None:
                n, q = vom.requote(book, mid, now, self.trader.inv,
                                   self.trader.mm, self.trader.cfg)
                self.trader.n_quotes += n
                self.trader.quoted_qty += q
                requoted = True

        # 5) review log: what changed, and whether it re-quoted immediately
        self._log_event({"type": "param_change", "ts": now, "old": old,
                         "new": clean, "requoted": requoted,
                         "sigma_zero_return_noted": requoted})
        return {"ok": True, "applied": clean, "requoted": requoted}

    def current_params(self) -> dict:
        scfg = self.trader.cfg.strategy
        return {k: getattr(scfg, k) for k in TUNABLE_PARAMS}

    # -- snapshot / history ---------------------------------------------------
    def _book_top(self) -> dict:
        book = self.trader.books.get(self.trader.cfg.token_id)
        out = {"mid": None, "best_bid": None, "best_ask": None,
               "bid_queue": None, "ask_queue": None, "synced": False}
        if book is None:
            return out
        out["synced"] = book.synced
        out["mid"] = book.mid()
        if book.bids:
            bb = max(book.bids)
            out["best_bid"] = float(bb)
            out["bid_queue"] = book.bids[bb]      # best-level queue size
        if book.asks:
            ba = min(book.asks)
            out["best_ask"] = float(ba)
            out["ask_queue"] = book.asks[ba]
        return out

    def _my_quotes(self) -> dict:
        quotes = {"bid": None, "ask": None}
        for ao in self.trader.vom.active:
            key = "bid" if ao.order.side == "BUY" else "ask"
            rem = ao.order.remaining if ao.order.remaining is not None else ao.order.size
            quotes[key] = {"price": float(ao.order.price), "size": rem,
                           "placed_ts": ao.order.place_local_ts}
        return quotes

    def _equity(self) -> Optional[float]:
        t = self.trader
        if t.last_mid is None:
            return None
        return t.cash + t.inv * t.last_mid

    def record_history(self) -> None:
        """Append one time-series point (called by the server's push loop)."""
        t = self.trader
        snap = t.analytics_snapshot()      # None until the first valid mid
        top = self._book_top()
        q = self._my_quotes()
        self.history.append({
            "ts": time.time(),
            "mid": top["mid"] if top["mid"] is not None else t.last_mid,
            "my_bid": q["bid"]["price"] if q["bid"] else None,
            "my_ask": q["ask"]["price"] if q["ask"] else None,
            "inv": t.inv,
            "equity": self._equity(),
            "cum_spread": snap["spread_capture"] if snap else 0.0,
            "cum_adverse": snap["adverse_selection_cost"] if snap else 0.0,
        })

    def snapshot(self) -> dict:
        t = self.trader
        snap = t.analytics_snapshot()
        recent = [
            {"ts": f.local_timestamp, "side": side, "price": float(f.price),
             "qty": f.qty, "adverse": f.adverse}
            for side, f in t.ledger[-5:]
        ]
        scfg = t.cfg.strategy
        return {
            "slug": self.spec.slug,
            "label": self.spec.label,
            "token_id": self.spec.token_id,
            "running": self.running,
            "stopped_reason": self.stopped_reason,
            "loss_limit": self.spec.loss_limit,
            "book": self._book_top(),
            "quotes": self._my_quotes(),
            "quoting_halted": {          # inventory-cap indicator per side
                "bid": t.inv >= scfg.max_inventory,
                "ask": t.inv <= -scfg.max_inventory,
            },
            "position": {"inv": t.inv, "cash": t.cash, "equity": self._equity()},
            "fills": {
                "n_fills": len(t.ledger),
                "fill_ratio": snap["fill_ratio"] if snap else 0.0,
                "recent": recent,
            },
            "pnl": {
                "spread_capture": snap["spread_capture"] if snap else 0.0,
                "inventory_pnl": snap["inventory_pnl"] if snap else 0.0,
                "adverse_cost": snap["adverse_selection_cost"] if snap else 0.0,
                "total": snap["total_pnl"] if snap else 0.0,
            },
            "params": self.current_params(),
            "param_error": self.last_param_error,
            "history": {
                k: [p[k] for p in self.history]
                for k in ("ts", "mid", "my_bid", "my_ask", "inv",
                          "cum_spread", "cum_adverse")
            },
        }


# ---------------------------------------------------------------------------
# Orchestrator — the fleet
# ---------------------------------------------------------------------------
class Orchestrator:
    def __init__(self, specs: List[MarketSpec], log_dir: str,
                 history_len: int = 300) -> None:
        self.log_dir = log_dir
        self.history_len = history_len
        self.runners: Dict[str, MarketRunner] = {}
        for spec in specs:
            self.add_market(spec, start=False)

    def add_market(self, spec: MarketSpec, start: bool = True) -> MarketRunner:
        if spec.slug in self.runners:
            raise ValueError(f"market {spec.slug!r} already exists")
        runner = MarketRunner(spec, self.log_dir, self.history_len)
        self.runners[spec.slug] = runner
        if start:
            runner.start()
        return runner

    async def remove_market(self, slug: str) -> None:
        runner = self.runners.pop(slug)
        await runner.stop(reason="removed")

    def start_all(self) -> None:
        for r in self.runners.values():
            r.start()

    async def stop_all(self) -> None:
        for r in self.runners.values():
            if r.running:
                await r.stop(reason="shutdown")

    def snapshot_all(self) -> dict:
        return {slug: r.snapshot() for slug, r in self.runners.items()}

    def breached_runners(self) -> List[MarketRunner]:
        """Runners whose loss limit is breached (server stops them)."""
        return [r for r in self.runners.values()
                if r.running and r.breached_loss_limit()]


__all__ = [
    "MarketSpec", "MarketRunner", "Orchestrator",
    "validate_params", "TUNABLE_PARAMS",
]
