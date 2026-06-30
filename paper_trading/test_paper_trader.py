"""Smoke/unit tests for paper_trader.py — no network, fake message sequence.

Drives one full cycle: receive -> book update -> virtual quote -> virtual fill
-> PnL reconcile, plus heartbeat adverse resolution and state persist/restore.
Runnable directly:  python test_paper_trader.py
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from decimal import Decimal

# Make this folder importable (for `import paper_trader`) and add the repo root
# (for the shared engine modules) BEFORE importing either.
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _p in (_HERE, _ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from paper_trader import PaperConfig, PaperTrader  # noqa: E402
from strategy import StrategyConfig  # noqa: E402

TOKEN = "A"


def _snapshot(bids, asks):
    return {"asset_id": TOKEN, "timestamp": "1", "event_type": "book",
            "bids": [{"price": p, "size": s} for p, s in bids],
            "asks": [{"price": p, "size": s} for p, s in asks]}


def _trade(price, size, side):
    return {"asset_id": TOKEN, "timestamp": "2", "price": price, "size": size,
            "side": side, "event_type": "last_trade_price"}


def _new_trader(**cfg_over):
    scfg = StrategyConfig(order_size=10.0, adverse_window_seconds=1.0, max_inventory=100.0)
    cfg = PaperConfig(token_id=TOKEN, slug="test", strategy=scfg,
                      reload_state=False, **cfg_over)
    return PaperTrader(cfg)


def test_snapshot_triggers_quotes():
    pt = _new_trader()
    pt.handle_message(_snapshot([("0.45", "100"), ("0.44", "50")],
                                [("0.55", "100"), ("0.56", "50")]), local_ts=100.0)
    assert pt.books[TOKEN].synced
    assert pt.n_quotes >= 2  # a bid and an ask were placed
    sides = {ao.order.side for ao in pt.vom.active}
    assert sides == {"BUY", "SELL"}


def test_full_cycle_fill_and_reconcile():
    pt = _new_trader()
    pt.handle_message(_snapshot([("0.45", "100")], [("0.55", "100")]), local_ts=100.0)
    sell = [ao for ao in pt.vom.active if ao.order.side == "SELL"][0]
    price = str(sell.order.price)
    # Aggressive BUY trade at our ask price -> our virtual SELL fills.
    pt.handle_message(_trade(price, "1000", "BUY"), local_ts=100.5)
    assert pt.inv < 0          # we sold -> short inventory
    assert len(pt.ledger) >= 1
    snap = pt.analytics_snapshot()
    assert snap is not None
    assert snap["n_fills"] >= 1
    assert abs(snap["reconciliation_error"]) < 1e-6   # live ledger reconciles


def test_heartbeat_resolves_adverse():
    pt = _new_trader()
    pt.handle_message(_snapshot([("0.45", "100")], [("0.55", "100")]), local_ts=100.0)
    sell = [ao for ao in pt.vom.active if ao.order.side == "SELL"][0]
    pt.handle_message(_trade(str(sell.order.price), "1000", "BUY"), local_ts=100.5)
    _, f = pt.ledger[0]
    assert f.adverse is None              # window not elapsed yet
    pt.heartbeat(ts=102.0)                # > adverse window later
    assert f.adverse is not None          # now measured (True/False, not None)
    assert f.measured_dt is not None


def test_state_persist_and_restore_inv_cash_only():
    fd, spath = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    try:
        pt = _new_trader(state_path=spath)
        pt.handle_message(_snapshot([("0.45", "100")], [("0.55", "100")]), local_ts=100.0)
        sell = [ao for ao in pt.vom.active if ao.order.side == "SELL"][0]
        pt.handle_message(_trade(str(sell.order.price), "1000", "BUY"), local_ts=100.5)
        pt.persist_state()
        inv0, cash0, n_led = pt.inv, pt.cash, len(pt.ledger)

        # restart with reload
        scfg = StrategyConfig(order_size=10.0, adverse_window_seconds=1.0)
        cfg = PaperConfig(token_id=TOKEN, slug="test", strategy=scfg,
                          state_path=spath, reload_state=True)
        pt2 = PaperTrader(cfg)
        assert abs(pt2.inv - inv0) < 1e-12       # inventory carried over
        assert abs(pt2.cash - cash0) < 1e-12     # cash carried over
        assert len(pt2.ledger) == n_led          # ledger restored
        assert pt2.vom.active == [] and pt2.vom.draining == []  # NO open orders restored
    finally:
        os.remove(spath)


def test_no_real_order_api_present():
    """Guard: the module exposes no order-routing surface."""
    import paper_trader
    forbidden = ["place_order", "submit_order", "cancel_order", "post_order", "send_order"]
    for name in forbidden:
        assert not hasattr(paper_trader.PaperTrader, name)
        assert not hasattr(paper_trader.VirtualOrderManager, name)


def test_jsonl_tape_is_backtest_compatible():
    """The tape written by the paper trader must be re-readable by data_loader."""
    fd, lpath = tempfile.mkstemp(suffix=".jsonl")
    os.close(fd)
    try:
        pt = _new_trader(log_path=lpath)
        pt.handle_message(_snapshot([("0.45", "100")], [("0.55", "100")]), local_ts=100.0)
        pt.handle_message(_trade("0.55", "5", "BUY"), local_ts=100.5)
        from data_loader import iter_market_events
        events = list(iter_market_events(lpath, primary_asset=TOKEN))
        assert len(events) == 2
        assert events[0].book(TOKEN).synced
        assert events[1].trade is not None
    finally:
        os.remove(lpath)


def test_yes_no_frame_updates_both_books_quotes_yes_only():
    """A single price_change frame carrying YES+NO updates both books, but quotes
    are computed from the YES book only (all virtual orders are YES)."""
    pt = _new_trader()
    pt.handle_message(_snapshot([("0.45", "100")], [("0.55", "100")]), local_ts=100.0)
    frame = {"event_type": "price_change", "timestamp": "2", "price_changes": [
        {"asset_id": "B", "price": "0.46", "size": "500", "side": "BUY"},
        {"asset_id": TOKEN, "price": "0.46", "size": "500", "side": "BUY"}]}
    pt.handle_message(frame, local_ts=101.0)
    # both books updated
    assert pt.books["B"].level_size(Decimal("0.46"), "BUY") == 500.0
    assert pt.books[TOKEN].level_size(Decimal("0.46"), "BUY") == 500.0
    # every virtual order belongs to the YES token only
    assert pt.vom.active and all(ao.order.asset_id == TOKEN
                                 for ao in pt.vom.active + pt.vom.draining)


def test_no_only_frame_does_not_requote_or_observe():
    """A NO-token-only update must NOT re-quote and must NOT push into the YES
    volatility buffer (hard single-token gate)."""
    pt = _new_trader()
    pt.handle_message(_snapshot([("0.45", "100")], [("0.55", "100")]), local_ts=100.0)
    n_q = pt.n_quotes
    sig_len = len(pt.mm._rets)            # sigma return buffer length
    frame = {"event_type": "price_change", "timestamp": "2", "price_changes": [
        {"asset_id": "B", "price": "0.46", "size": "500", "side": "BUY"}]}
    pt.handle_message(frame, local_ts=101.0)
    assert pt.n_quotes == n_q             # no requote triggered by NO change
    assert len(pt.mm._rets) == sig_len    # observe_mid NOT called (no sigma pollution)
    assert "B" in pt.books                # but the NO book WAS updated
    assert pt.books["B"].level_size(Decimal("0.46"), "BUY") == 500.0


def test_list_frame_trade_reaches_fill_engine():
    """A trade batched inside a LIST frame must reach the fill engine (the old
    list=snapshot path dropped it)."""
    pt = _new_trader()
    pt.handle_message(_snapshot([("0.45", "100")], [("0.55", "100")]), local_ts=100.0)
    sell = [ao for ao in pt.vom.active if ao.order.side == "SELL"][0]
    price = str(sell.order.price)
    # Trade first (while our quote is still resting), then a price_change. Both
    # are batched in ONE list frame; the trade must not be dropped.
    frame = [
        {"asset_id": TOKEN, "event_type": "last_trade_price", "timestamp": "2",
         "price": price, "size": "1000", "side": "BUY"},
        {"asset_id": TOKEN, "event_type": "price_change", "timestamp": "3",
         "price_changes": [{"asset_id": TOKEN, "price": "0.54", "size": "10", "side": "SELL"}]},
    ]
    pt.handle_message(frame, local_ts=100.5)
    assert pt.inv < 0          # the batched trade filled our SELL -> not dropped
    assert len(pt.ledger) >= 1


def _run_all():
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn(); print(f"PASS {fn.__name__}")
    print(f"\n{len(fns)} paper_trader tests passed.")


if __name__ == "__main__":
    _run_all()
