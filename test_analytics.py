"""Unit tests for analytics.py: PnL reconciliation + adverse None exclusion."""

from __future__ import annotations

import json
import os
import tempfile

from analytics import (
    adverse_diagnostics,
    attribute_pnl,
    summarize,
    time_bucket_attribution,
)
from strategy import FillRecord, StrategyConfig, run_backtest


def _engine_cash_inv(fills):
    """Replicate the engine's independent accounting from fills."""
    cash = 0.0
    inv = 0.0
    for f in fills:
        sq = f.qty if f.side == "BUY" else -f.qty
        inv += sq
        cash -= sq * f.fill_price
    return cash, inv


def _fr(ts, side, qty, price, mid_before, mid_after=None, adverse=None):
    return FillRecord(ts, side, qty, price, mid_before, mid_after, adverse, None, 0.0)


def test_reconciliation_single_pair():
    fills = [
        _fr(1.0, "BUY", 10, 0.40, 0.42),
        _fr(2.0, "SELL", 10, 0.60, 0.58),
    ]
    cash, inv = _engine_cash_inv(fills)
    a = attribute_pnl(fills, mid_final=0.50, cash_final=cash, inv_final=inv)
    assert abs(a.total_pnl - 2.0) < 1e-9
    assert abs(a.spread_capture - 0.4) < 1e-9
    assert abs(a.inventory_pnl - 1.6) < 1e-9
    assert abs(a.adverse_selection_cost) < 1e-9
    assert abs(a.reconciliation_error) < 1e-9


def test_reconciliation_multi_fill_with_adverse():
    fills = [
        _fr(1.0, "BUY", 7, 0.41, 0.43, mid_after=0.42, adverse=True),   # bought, mid fell
        _fr(2.0, "SELL", 3, 0.59, 0.57, mid_after=0.58, adverse=False),
        _fr(3.0, "BUY", 5, 0.45, None, adverse=None),                   # unmeasurable mid
        _fr(4.0, "SELL", 9, 0.62, 0.60, mid_after=None, adverse=None),
    ]
    cash, inv = _engine_cash_inv(fills)
    a = attribute_pnl(fills, mid_final=0.55, cash_final=cash, inv_final=inv)
    # The three-term decomposition must reconcile exactly with engine PnL.
    assert abs(a.reconciliation_error) < 1e-9
    assert a.n_mid_before_missing == 1  # one fill had mid_before=None


def test_spread_capture_positive_for_buy_below_mid():
    fills = [_fr(1.0, "BUY", 10, 0.40, 0.45)]  # bought 5c below mid
    cash, inv = _engine_cash_inv(fills)
    a = attribute_pnl(fills, mid_final=0.45, cash_final=cash, inv_final=inv)
    assert a.spread_capture > 0  # captured edge is positive


def test_adverse_diagnostics_excludes_none():
    fills = [
        _fr(1.0, "BUY", 10, 0.40, 0.42, mid_after=0.41, adverse=True),
        _fr(2.0, "SELL", 10, 0.60, 0.58, mid_after=0.59, adverse=False),
        _fr(3.0, "BUY", 10, 0.40, 0.42, mid_after=None, adverse=None),
    ]
    d = adverse_diagnostics(fills)
    assert d["measurable"] == 2
    assert d["unmeasurable"] == 1
    assert d["adverse_true"] == 1
    assert abs(d["adverse_rate"] - 0.5) < 1e-9
    # cost = -(+10*(0.41-0.42)) = 0.1 from the single adverse-true buy
    assert abs(d["adverse_cost"] - 0.1) < 1e-9


def test_time_buckets_sum_to_global():
    fills = [
        _fr(0.5, "BUY", 7, 0.41, 0.43, mid_after=0.42, adverse=True),
        _fr(3.5, "SELL", 9, 0.62, 0.60, mid_after=0.61, adverse=False),
        _fr(6.0, "BUY", 4, 0.50, 0.49, adverse=None),
    ]
    cash, inv = _engine_cash_inv(fills)
    glob = attribute_pnl(fills, mid_final=0.55, cash_final=cash, inv_final=inv)
    buckets = time_bucket_attribution(fills, mid_final=0.55, bucket_seconds=2.0)
    assert abs(sum(b["spread"] for b in buckets) - glob.spread_capture) < 1e-9
    assert abs(sum(b["inventory"] for b in buckets) - glob.inventory_pnl) < 1e-9
    assert abs(sum(b["adverse_cost"] for b in buckets) - glob.adverse_selection_cost) < 1e-9


def _write_synthetic_jsonl():
    A = "PRIMARY"
    rows = []
    rows.append({"local_timestamp": 1.0, "slug": "syn", "data": {
        "asset_id": A, "timestamp": "1000", "event_type": "book",
        "bids": [{"price": "0.45", "size": "100"}, {"price": "0.44", "size": "50"}],
        "asks": [{"price": "0.55", "size": "100"}, {"price": "0.56", "size": "50"}]}})
    # move the book a bit
    for i, (ts, bp, bs, ap, as_) in enumerate([
        (2.0, "0.46", "80", "0.56", "90"),
        (3.0, "0.47", "70", "0.57", "80"),
        (4.0, "0.46", "60", "0.56", "70"),
    ]):
        rows.append({"local_timestamp": ts, "slug": "syn", "data": {
            "market": "0x", "timestamp": str(1000 + i), "event_type": "price_change",
            "price_changes": [
                {"asset_id": A, "price": bp, "size": bs, "side": "BUY"},
                {"asset_id": A, "price": ap, "size": as_, "side": "SELL"}]}})
    # a couple of trades (may or may not hit our exact quote; reconciliation must hold regardless)
    rows.append({"local_timestamp": 5.0, "slug": "syn", "data": {
        "asset_id": A, "timestamp": "1005", "price": "0.56", "size": "200",
        "side": "BUY", "event_type": "last_trade_price"}})
    rows.append({"local_timestamp": 9.0, "slug": "syn", "data": {
        "asset_id": A, "timestamp": "1009", "price": "0.46", "size": "200",
        "side": "SELL", "event_type": "last_trade_price"}})
    fd, path = tempfile.mkstemp(suffix=".jsonl")
    with os.fdopen(fd, "w") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
    return path, A


def test_end_to_end_backtest_reconciles():
    path, A = _write_synthetic_jsonl()
    try:
        cfg = StrategyConfig(order_size=10.0, adverse_window_seconds=1.0)
        res = run_backtest(path, cfg, primary_asset=A)
        s = summarize(res)  # asserts reconciliation internally
        assert abs(s.attribution.reconciliation_error) < 1e-6
        assert res.mid_final is not None
    finally:
        os.remove(path)


def _run_all():
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn(); print(f"PASS {fn.__name__}")
    print(f"\n{len(fns)} analytics tests passed.")


if __name__ == "__main__":
    _run_all()
