"""Tests for the cockpit layer (orchestrator + param plumbing). No network.

Covers the cockpit's core promises:
  * live parameter updates reflect in the very next quote (shared-config mutation)
  * queue reset + immediate requote on parameter change
  * strict validation (reject, don't crash — atomic all-or-nothing)
  * multi-market isolation
  * loss-limit auto-stop condition
  * tape + param-change event logging (review/learning asset)
  * NO real-order API anywhere in the cockpit (guard, same spirit as
    paper_trading's test_no_real_order_api_present)
  * select_markets liquidity buckets

Runnable directly:  python paper_trading/cockpit/test_cockpit.py
(server.py is deliberately not imported: its safety guard scans source text,
so these tests run without aiohttp installed.)
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
_PT_DIR = os.path.dirname(_HERE)
_ROOT = os.path.dirname(_PT_DIR)
for _p in (_HERE, _PT_DIR, _ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from orchestrator import (  # noqa: E402
    MarketRunner, MarketSpec, Orchestrator, TUNABLE_PARAMS, validate_params,
)
from select_markets import Candidate, pick_liquidity_spread  # noqa: E402

TOKEN = "A"


# ---------------------------------------------------------------------------
# helpers (fake WS messages, mirrors test_paper_trader.py)
# ---------------------------------------------------------------------------
def _snapshot(bids, asks, token=TOKEN):
    return {"asset_id": token, "timestamp": "1", "event_type": "book",
            "bids": [{"price": p, "size": s} for p, s in bids],
            "asks": [{"price": p, "size": s} for p, s in asks]}


def _spec(slug="mkt", token=TOKEN, **kw):
    return MarketSpec(token_id=token, slug=slug, **kw)


def _new_runner(tmp, slug="mkt", token=TOKEN, **spec_kw) -> MarketRunner:
    r = MarketRunner(_spec(slug=slug, token=token, **spec_kw), log_dir=tmp)
    return r


def _feed_book(r: MarketRunner, bid=("0.45", "100"), ask=("0.55", "100")):
    r.trader.handle_message(_snapshot([bid], [ask], token=r.spec.token_id),
                            local_ts=100.0)


def _quotes(r: MarketRunner) -> dict:
    out = {}
    for ao in r.trader.vom.active:
        out[ao.order.side] = float(ao.order.price)
    return out


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------
def test_validate_params_rules():
    bad = [{"gamma": 0}, {"gamma": -1}, {"gamma": float("nan")},
           {"gamma": float("inf")}, {"kappa": 0}, {"kappa": -0.1},
           {"order_size": 0}, {"max_inventory": -1},
           {"gamma": "abc"}, {"sigma_window": 10}, {"nope": 1}]
    for updates in bad:
        clean, errors = validate_params(updates)
        assert errors and not clean, updates
    clean, errors = validate_params(
        {"gamma": "2.5", "kappa": 1, "order_size": 5, "max_inventory": 0})
    assert not errors and clean["gamma"] == 2.5 and clean["max_inventory"] == 0.0


def test_apply_params_atomic_rejection():
    """One bad value rejects the WHOLE update; nothing is mutated."""
    tmp = tempfile.mkdtemp()
    try:
        r = _new_runner(tmp)
        before = r.current_params()
        res = r.apply_params({"gamma": 2.0, "kappa": 0})   # kappa invalid
        assert res["ok"] is False and "kappa" in res["errors"]
        assert r.current_params() == before                # gamma NOT applied
        assert r.last_param_error is not None              # surfaced to dashboard
        assert r.apply_params({})["ok"] is False           # empty update rejected
    finally:
        shutil.rmtree(tmp)


# ---------------------------------------------------------------------------
# live reflection + queue reset
# ---------------------------------------------------------------------------
def test_shared_config_identity():
    """The mechanism itself: mm.cfg IS cfg.strategy (one mutable object)."""
    tmp = tempfile.mkdtemp()
    try:
        r = _new_runner(tmp)
        assert r.trader.mm.cfg is r.trader.cfg.strategy
    finally:
        shutil.rmtree(tmp)


def test_param_change_reflects_in_next_quote():
    """kappa 10 -> 0.5 must widen the spread on the immediate requote
    (delta = (1/gamma)*ln(1+gamma/kappa) with sigma at the floor)."""
    tmp = tempfile.mkdtemp()
    try:
        r = _new_runner(tmp)
        _feed_book(r)
        q1 = _quotes(r)
        assert "BUY" in q1 and "SELL" in q1
        spread1 = q1["SELL"] - q1["BUY"]
        res = r.apply_params({"kappa": 0.5})
        assert res["ok"] and res["requoted"]
        q2 = _quotes(r)
        spread2 = q2["SELL"] - q2["BUY"]
        assert spread2 > spread1, (spread1, spread2)
    finally:
        shutil.rmtree(tmp)


def test_apply_params_queue_reset_and_new_size():
    """Old virtual orders are cancelled into draining (queue given up, adverse
    window still resolving); replacements carry the new order_size."""
    tmp = tempfile.mkdtemp()
    try:
        r = _new_runner(tmp)
        _feed_book(r)
        old_orders = [ao.order for ao in r.trader.vom.active]
        assert old_orders
        res = r.apply_params({"order_size": 33.0})
        assert res["ok"] and res["requoted"]
        for o in old_orders:                       # cancelled, moved to draining
            assert o.active is False
        draining_orders = [ao.order for ao in r.trader.vom.draining]
        for o in old_orders:
            assert o in draining_orders
        for ao in r.trader.vom.active:             # fresh quotes, new size
            assert ao.order.size == 33.0
            assert ao.order not in old_orders
    finally:
        shutil.rmtree(tmp)


def test_max_inventory_zero_pulls_both_sides():
    """Extreme-but-valid input: max_inventory=0 halts quoting on both sides
    (inv cap logic already in the engine) — shown, not crashed."""
    tmp = tempfile.mkdtemp()
    try:
        r = _new_runner(tmp)
        _feed_book(r)
        assert _quotes(r)
        res = r.apply_params({"max_inventory": 0.0})
        assert res["ok"]
        assert not r.trader.vom.active             # both sides pulled
        snap = r.snapshot()
        assert snap["quoting_halted"]["bid"] and snap["quoting_halted"]["ask"]
        assert snap["quotes"]["bid"] is None and snap["quotes"]["ask"] is None
    finally:
        shutil.rmtree(tmp)


# ---------------------------------------------------------------------------
# multi-market
# ---------------------------------------------------------------------------
def test_multimarket_isolation():
    tmp = tempfile.mkdtemp()
    try:
        orch = Orchestrator([_spec(slug="a", token="A"), _spec(slug="b", token="B")],
                            log_dir=tmp)
        ra, rb = orch.runners["a"], orch.runners["b"]
        _feed_book(ra)
        _feed_book(rb)
        b_orders = list(rb.trader.vom.active)
        res = ra.apply_params({"gamma": 5.0, "kappa": 0.5})
        assert res["ok"]
        assert ra.current_params()["gamma"] == 5.0
        assert rb.current_params()["gamma"] != 5.0          # B untouched
        assert list(rb.trader.vom.active) == b_orders       # B's queue intact
        assert ra.trader.cfg.strategy is not rb.trader.cfg.strategy
    finally:
        shutil.rmtree(tmp)


def test_orchestrator_add_remove():
    tmp = tempfile.mkdtemp()
    try:
        orch = Orchestrator([_spec(slug="a")], log_dir=tmp)
        orch.add_market(_spec(slug="c", token="C"), start=False)
        assert set(orch.snapshot_all()) == {"a", "c"}
        try:
            orch.add_market(_spec(slug="a"), start=False)
            assert False, "duplicate slug must raise"
        except ValueError:
            pass
        asyncio.run(orch.remove_market("c"))
        assert set(orch.runners) == {"a"}
    finally:
        shutil.rmtree(tmp)


# ---------------------------------------------------------------------------
# safety rails
# ---------------------------------------------------------------------------
def test_loss_limit_autostop_condition():
    tmp = tempfile.mkdtemp()
    try:
        orch = Orchestrator([_spec(slug="a", loss_limit=-5.0)], log_dir=tmp)
        r = orch.runners["a"]
        r.running = True                    # simulate live (no network needed)
        _feed_book(r)
        assert not r.breached_loss_limit()  # equity 0 > -5
        r.trader.cash = -100.0              # force equity below the limit
        assert r.breached_loss_limit()
        assert orch.breached_runners() == [r]
        asyncio.run(r.stop(reason="loss_limit"))
        assert r.running is False and r.stopped_reason == "loss_limit"
        assert r.snapshot()["stopped_reason"] == "loss_limit"
        assert os.path.exists(r.trader.cfg.state_path)      # state persisted
    finally:
        shutil.rmtree(tmp)


def test_no_real_order_api_present():
    """Guard: the cockpit exposes no order-routing surface (same spirit as
    paper_trading.test_paper_trader.test_no_real_order_api_present)."""
    import orchestrator as orch_mod
    forbidden_attrs = ["place_order", "submit_order", "cancel_order",
                       "post_order", "send_order", "create_order"]
    for cls in (orch_mod.MarketRunner, orch_mod.Orchestrator):
        for name in forbidden_attrs:
            assert not hasattr(cls, name), (cls, name)
    # source scan (server.py + dashboard are checked WITHOUT importing them,
    # so this guard also runs where aiohttp isn't installed)
    forbidden_tokens = forbidden_attrs + ["private_key", "signTypedData",
                                          "eth_account", "clob_api", "py_clob"]
    for fname in ("orchestrator.py", "server.py",
                  os.path.join("static", "index.html")):
        with open(os.path.join(_HERE, fname), "r", encoding="utf-8") as fh:
            src = fh.read()
        for tok in forbidden_tokens:
            assert tok not in src, f"{tok!r} found in {fname}"
    # and the original engine rule still holds: never import poly_market_maker
    for fname in ("orchestrator.py", "server.py"):
        with open(os.path.join(_HERE, fname), "r", encoding="utf-8") as fh:
            for line in fh:
                stripped = line.strip()
                if stripped.startswith(("import ", "from ")):
                    assert "poly_market_maker" not in stripped, (fname, stripped)


# ---------------------------------------------------------------------------
# logging (learning asset)
# ---------------------------------------------------------------------------
def test_event_log_and_tape():
    """Param changes land in the events JSONL (what did I touch, when, did it
    requote) and the market tape keeps recording in backtest-compatible form."""
    tmp = tempfile.mkdtemp()
    try:
        r = _new_runner(tmp)
        _feed_book(r)
        r.apply_params({"gamma": 3.0})
        asyncio.run(r.stop(reason="manual"))
        with open(r.events_path, "r", encoding="utf-8") as fh:
            events = [json.loads(l) for l in fh]
        kinds = [e["type"] for e in events]
        assert "param_change" in kinds and "stop" in kinds
        pc = next(e for e in events if e["type"] == "param_change")
        assert pc["old"]["gamma"] == 1.0 and pc["new"]["gamma"] == 3.0
        assert pc["requoted"] is True
        # tape re-readable by the backtest loader
        from data_loader import iter_market_events
        evs = list(iter_market_events(r.trader.cfg.log_path, primary_asset=TOKEN))
        assert len(evs) == 1 and evs[0].book(TOKEN).synced
    finally:
        shutil.rmtree(tmp)


# ---------------------------------------------------------------------------
# snapshot correctness
# ---------------------------------------------------------------------------
def test_snapshot_structure_and_best_queue():
    tmp = tempfile.mkdtemp()
    try:
        r = _new_runner(tmp)
        _feed_book(r, bid=("0.45", "120"), ask=("0.55", "3400"))
        s = r.snapshot()
        for key in ("slug", "running", "book", "quotes", "quoting_halted",
                    "position", "fills", "pnl", "params", "history"):
            assert key in s, key
        assert s["book"]["mid"] == 0.5
        assert s["book"]["best_bid"] == 0.45 and s["book"]["bid_queue"] == 120.0
        assert s["book"]["best_ask"] == 0.55 and s["book"]["ask_queue"] == 3400.0
        assert s["quotes"]["bid"] is not None and s["quotes"]["ask"] is not None
        assert set(s["params"]) == set(TUNABLE_PARAMS)
        r.record_history()
        h = r.snapshot()["history"]
        assert len(h["ts"]) == 1 and h["mid"][0] == 0.5 and h["inv"][0] == 0.0
    finally:
        shutil.rmtree(tmp)


def test_market_spec_validation():
    try:
        MarketSpec.from_dict({"slug": "x", "token_id": ""})
        assert False, "empty token_id must raise"
    except ValueError:
        pass
    try:
        MarketSpec.from_dict({"slug": "x", "token_id": "T", "params": {"gamma": 0}})
        assert False, "gamma=0 must raise"
    except ValueError:
        pass
    spec = MarketSpec.from_dict({"slug": "x", "token_id": "T",
                                 "params": {"gamma": 2}, "loss_limit": -10})
    assert spec.params == {"gamma": 2.0} and spec.loss_limit == -10
    # the shipped markets.json parses (token ids are placeholders to fill)
    with open(os.path.join(_HERE, "markets.json"), "r", encoding="utf-8") as fh:
        cfgd = json.load(fh)
    specs = [MarketSpec.from_dict(m) for m in cfgd["markets"]]
    assert len(specs) == 3 and {s.label for s in specs} == {"thin", "mid", "thick"}


# ---------------------------------------------------------------------------
# select_markets liquidity buckets
# ---------------------------------------------------------------------------
def test_liquidity_spread_buckets():
    def cand(slug, liq):
        return Candidate(slug=slug, question="q", volume24hr=1.0, liquidity=liq,
                         end_date=None, neg_risk=False, yes_token="Y",
                         no_token="N", tick_size=0.01, min_size=5.0)
    cands = [cand(f"m{i}", liq) for i, liq in
             enumerate([50, 80, 120, 4000, 5000, 6000, 80000, 90000, 99999])]
    b = pick_liquidity_spread(cands)
    assert b["thin"][0].liquidity == 50          # thinnest: fills, adverse risk
    assert b["thick"][0].liquidity == 99999      # thickest: the Hormuz control
    assert 4000 <= b["mid"][0].liquidity <= 6000
    assert pick_liquidity_spread([]) == {"thin": [], "mid": [], "thick": []}


def _run_all():
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn(); print(f"PASS {fn.__name__}")
    print(f"\n{len(fns)} cockpit tests passed.")


if __name__ == "__main__":
    _run_all()
