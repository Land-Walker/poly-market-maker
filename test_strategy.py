"""Unit tests for strategy.py (logit/price consistency + A-S quoting)."""

from __future__ import annotations

import math
from decimal import Decimal

from strategy import (
    ASMarketMaker,
    StrategyConfig,
    as_quotes_logit,
    clip_prob,
    logit,
    price_to_tick,
    sigmoid,
)


def test_logit_sigmoid_roundtrip():
    for p in (0.01, 0.1, 0.33, 0.5, 0.67, 0.9, 0.99):
        assert abs(sigmoid(logit(p)) - p) < 1e-12


def test_reservation_equals_mid_when_flat():
    cfg = StrategyConfig()
    x = logit(0.5)
    bid_l, ask_l, r, delta = as_quotes_logit(x, q=0.0, sigma=0.2, cfg=cfg, ts=0.0)
    assert abs(r - x) < 1e-12            # no skew when inventory is flat
    assert abs((x - bid_l) - delta) < 1e-12   # symmetric around mid in logit space
    assert abs((ask_l - x) - delta) < 1e-12
    assert delta > 0


def test_inventory_skew_sign():
    cfg = StrategyConfig()
    x = logit(0.5)
    _, _, r_long, _ = as_quotes_logit(x, q=10.0, sigma=0.2, cfg=cfg, ts=0.0)
    _, _, r_short, _ = as_quotes_logit(x, q=-10.0, sigma=0.2, cfg=cfg, ts=0.0)
    assert r_long < x    # long inventory -> reservation below mid (skew to sell)
    assert r_short > x   # short inventory -> reservation above mid (skew to buy)


def test_price_to_tick_bounds_and_rounding():
    assert price_to_tick(0.6649, 0.01, "bid") == Decimal("0.66")  # floor
    assert price_to_tick(0.6651, 0.01, "ask") == Decimal("0.67")  # ceil
    assert price_to_tick(0.0, 0.01, "bid") == Decimal("0.01")     # clamped to tick
    assert price_to_tick(1.0, 0.01, "ask") == Decimal("0.99")     # clamped to 1-tick


def test_quotes_ordered_and_in_bounds():
    cfg = StrategyConfig()
    mm = ASMarketMaker(cfg)
    x = mm.observe_mid(0.5)
    bid, ask = mm.quotes(x, q=0.0, ts=0.0)
    assert bid is not None and ask is not None
    assert Decimal("0") < bid < ask < Decimal("1")


def test_inventory_cap_skips_side():
    cfg = StrategyConfig(max_inventory=5.0)
    mm = ASMarketMaker(cfg)
    x = mm.observe_mid(0.5)
    bid, ask = mm.quotes(x, q=5.0, ts=0.0)   # at +cap
    assert bid is None and ask is not None
    bid2, ask2 = mm.quotes(x, q=-5.0, ts=0.0)  # at -cap
    assert ask2 is None and bid2 is not None


def test_terminal_horizon_switch():
    base = StrategyConfig()
    term = StrategyConfig(horizon_mode="terminal", t_expiry=100.0, horizon_scale=100.0)
    from strategy import horizon
    assert horizon(base, ts=50.0) == base.tau            # const ignores ts
    assert abs(horizon(term, ts=50.0) - 0.5) < 1e-12     # (100-50)/100
    assert horizon(term, ts=100.0) == term.min_horizon   # floored at expiry


def _run_all():
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn(); print(f"PASS {fn.__name__}")
    print(f"\n{len(fns)} strategy tests passed.")


if __name__ == "__main__":
    _run_all()
