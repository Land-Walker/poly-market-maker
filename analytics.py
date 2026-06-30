"""analytics.py — PnL attribution (3-way decomposition) + diagnostics.

The core is a fill-level decomposition of total PnL into three components whose
signed sum reconciles, within tolerance, with the independently-tracked
cash+inventory PnL from the backtest engine. Because attribution is fill-level,
the exact same logic applies to a single bid+ask pair or to many levels
(BAND/AMM): you just feed more FillRecords.

Reconciliation identity (initial cash/inventory = 0), with
``signed_qty = +qty`` for BUY (bid) fills and ``-qty`` for SELL (ask) fills,
``mid_fill = mid_before`` (or fill_price if mid_before is None), and
``mid_after`` measured over the adverse window:

    spread_capture        = Σ  signed_qty * (mid_fill  - fill_price)
    adverse_selection_cost = -Σ_{adverse==True, measurable} signed_qty * (mid_after - mid_fill)
    inventory_pnl         = Σ  signed_qty * (mid_final - base)
                            where base = mid_after for adverse&measurable fills,
                                         mid_fill   otherwise

    spread_capture + inventory_pnl - adverse_selection_cost == total_pnl
    total_pnl = cash_final + inv_final * mid_final

This is an exact algebraic identity (see ASSUMPTIONS.md Phase 2 for the proof),
so reconciliation failure indicates an accounting bug, not modelling drift.

NOTE on sign: spread_capture uses ``(mid_fill - fill_price)`` (positive = edge
captured), the negation of the literal ``(fill_price - mid)`` so the three-term
identity closes with the correct sign. Confirmed with the user.

Dependencies: standard library only. matplotlib is optional (guarded import).
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from strategy import BacktestResult, FillRecord, StrategyConfig, run_backtest


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _signed_qty(f: FillRecord) -> float:
    return f.qty if f.side == "BUY" else -f.qty


def _mid_fill(f: FillRecord) -> float:
    """Reference mid at fill; falls back to fill_price when mid was unavailable
    (keeps the identity exact: that fill contributes 0 spread, all inventory)."""
    return f.mid_before if f.mid_before is not None else f.fill_price


# ---------------------------------------------------------------------------
# core attribution
# ---------------------------------------------------------------------------
@dataclass
class Attribution:
    spread_capture: float
    inventory_pnl: float
    adverse_selection_cost: float
    decomposed_total: float        # spread + inventory - adverse
    total_pnl: float               # cash_final + inv_final*mid_final
    reconciliation_error: float    # decomposed_total - total_pnl
    n_fills: int
    n_mid_before_missing: int


def attribute_pnl(
    fills: Sequence[FillRecord],
    *,
    mid_final: float,
    cash_final: float,
    inv_final: float,
    tol: float = 1e-6,
    assert_reconcile: bool = True,
) -> Attribution:
    """Decompose total PnL into spread / inventory / adverse and reconcile.

    ``mid_final`` is the last valid mid; ``cash_final``/``inv_final`` come from the
    engine's independent accounting. Raises AssertionError if the decomposition
    does not match the engine PnL within ``tol`` (set ``assert_reconcile=False``
    to only report the error instead of raising).
    """
    if mid_final is None:
        raise ValueError("mid_final is required for reconciliation (no valid mid).")

    spread = 0.0
    inventory = 0.0
    adverse_cost = 0.0
    n_missing = 0

    for f in fills:
        sq = _signed_qty(f)
        mid_fill = _mid_fill(f)
        if f.mid_before is None:
            n_missing += 1
        spread += sq * (mid_fill - f.fill_price)
        if f.adverse is True and f.mid_after is not None:
            base = f.mid_after
            adverse_cost += -(sq * (f.mid_after - mid_fill))
        else:
            base = mid_fill
        inventory += sq * (mid_final - base)

    decomposed = spread + inventory - adverse_cost
    total = cash_final + inv_final * mid_final
    err = decomposed - total
    if assert_reconcile:
        assert abs(err) <= tol, (
            f"PnL reconciliation failed: decomposed={decomposed:.10f} "
            f"total={total:.10f} err={err:.3e} tol={tol:.1e}"
        )
    return Attribution(
        spread_capture=spread,
        inventory_pnl=inventory,
        adverse_selection_cost=adverse_cost,
        decomposed_total=decomposed,
        total_pnl=total,
        reconciliation_error=err,
        n_fills=len(fills),
        n_mid_before_missing=n_missing,
    )


# ---------------------------------------------------------------------------
# diagnostics
# ---------------------------------------------------------------------------
def fill_diagnostics(result: BacktestResult) -> Dict[str, float]:
    """Fill ratio and average queue position at fill."""
    filled_qty = sum(f.qty for f in result.fills)
    qa = [f.queue_ahead_before for f in result.fills]
    return {
        "n_fills": len(result.fills),
        "filled_qty": filled_qty,
        "quoted_qty": result.quoted_qty,
        "n_quotes": result.n_quotes,
        "fill_ratio": (filled_qty / result.quoted_qty) if result.quoted_qty else 0.0,
        "avg_queue_ahead_at_fill": (sum(qa) / len(qa)) if qa else 0.0,
    }


def inventory_diagnostics(result: BacktestResult) -> Dict[str, float]:
    invs = [v for _, v in result.inventory_series]
    return {
        "inv_final": result.inv_final,
        "inv_max": max(invs) if invs else 0.0,
        "inv_min": min(invs) if invs else 0.0,
    }


def adverse_diagnostics(fills: Sequence[FillRecord]) -> Dict[str, Optional[float]]:
    """Aggregate adverse outcomes, EXCLUDING unmeasurable (adverse is None)."""
    measurable = [f for f in fills if f.adverse is not None]
    adverse_true = [f for f in measurable if f.adverse]
    cost = 0.0
    for f in adverse_true:
        if f.mid_after is not None:
            sq = _signed_qty(f)
            cost += -(sq * (f.mid_after - _mid_fill(f)))
    return {
        "n_fills": len(fills),
        "measurable": len(measurable),
        "unmeasurable": len(fills) - len(measurable),
        "adverse_true": len(adverse_true),
        "adverse_rate": (len(adverse_true) / len(measurable)) if measurable else None,
        "adverse_cost": cost,
    }


def parity_summary(
    parity_samples: Sequence[Tuple[float, Optional[float], bool]]
) -> Dict[str, object]:
    """Summarise parity_deviation; explicitly flag when never reliable."""
    if not parity_samples:
        return {"available": False}
    reliable = [d for _, d, ok in parity_samples if ok and d is not None]
    any_reliable = len(reliable) > 0
    out: Dict[str, object] = {
        "available": True,
        "reliable": any_reliable,
        "n_samples": len(parity_samples),
        "n_reliable": len(reliable),
    }
    if any_reliable:
        out["mean_deviation"] = sum(reliable) / len(reliable)
        out["max_abs_deviation"] = max(abs(d) for d in reliable)
    else:
        out["note"] = "parity always unreliable (NO token is delta-only / unsynced)"
    return out


def time_bucket_attribution(
    fills: Sequence[FillRecord],
    *,
    mid_final: float,
    bucket_seconds: float,
) -> List[Dict[str, float]]:
    """Per-time-bucket 3-way decomposition (sums to the global decomposition).

    Each fill is attributed individually, so bucketing by fill time partitions
    spread/inventory/adverse cleanly; the bucket sums equal the global totals.
    """
    if not fills:
        return []
    t0 = min(f.local_timestamp for f in fills)
    buckets: Dict[int, Dict[str, float]] = {}
    for f in fills:
        b = int((f.local_timestamp - t0) // bucket_seconds)
        sq = _signed_qty(f)
        mid_fill = _mid_fill(f)
        d = buckets.setdefault(
            b, {"bucket": b, "t_start": t0 + b * bucket_seconds,
                 "spread": 0.0, "inventory": 0.0, "adverse_cost": 0.0, "n": 0.0}
        )
        d["spread"] += sq * (mid_fill - f.fill_price)
        if f.adverse is True and f.mid_after is not None:
            base = f.mid_after
            d["adverse_cost"] += -(sq * (f.mid_after - mid_fill))
        else:
            base = mid_fill
        d["inventory"] += sq * (mid_final - base)
        d["n"] += 1
    out = []
    for b in sorted(buckets):
        d = buckets[b]
        d["total"] = d["spread"] + d["inventory"] - d["adverse_cost"]
        out.append(d)
    return out


# ---------------------------------------------------------------------------
# sweeps (re-run the backtest while varying a config field)
# ---------------------------------------------------------------------------
def adverse_window_sweep(
    path: str,
    cfg: StrategyConfig,
    *,
    primary_asset: str,
    windows: Sequence[float] = (0.5, 1.0, 2.0),
    yes_asset: Optional[str] = None,
    no_asset: Optional[str] = None,
) -> Dict[float, Dict[str, Optional[float]]]:
    """Re-run the backtest at each adverse window; report adverse_rate & cost.

    Adverse classification depends on the window, so this re-runs the engine for
    each value (rather than re-labelling existing fills).
    """
    out: Dict[float, Dict[str, Optional[float]]] = {}
    for w in windows:
        c = dataclasses.replace(cfg, adverse_window_seconds=w)
        res = run_backtest(path, c, primary_asset=primary_asset,
                           yes_asset=yes_asset, no_asset=no_asset)
        out[w] = adverse_diagnostics(res.fills)
    return out


def config_sweep(
    path: str,
    cfg: StrategyConfig,
    field_name: str,
    values: Sequence[object],
    *,
    primary_asset: str,
    yes_asset: Optional[str] = None,
    no_asset: Optional[str] = None,
) -> Dict[object, Dict[str, object]]:
    """Generic sweep over any StrategyConfig field (e.g. kappa, gamma,
    requote_interval_seconds, requote_mode). Returns attribution + key diagnostics
    per value."""
    out: Dict[object, Dict[str, object]] = {}
    for v in values:
        c = dataclasses.replace(cfg, **{field_name: v})
        res = run_backtest(path, c, primary_asset=primary_asset,
                           yes_asset=yes_asset, no_asset=no_asset)
        attr = attribute_pnl(res.fills, mid_final=res.mid_final,
                             cash_final=res.cash_final, inv_final=res.inv_final,
                             assert_reconcile=False)
        out[v] = {
            "total_pnl": attr.total_pnl,
            "spread_capture": attr.spread_capture,
            "inventory_pnl": attr.inventory_pnl,
            "adverse_selection_cost": attr.adverse_selection_cost,
            "reconciliation_error": attr.reconciliation_error,
            "fill_ratio": fill_diagnostics(res)["fill_ratio"],
        }
    return out


# ---------------------------------------------------------------------------
# top-level summary
# ---------------------------------------------------------------------------
@dataclass
class Summary:
    attribution: Attribution
    fills: Dict[str, float]
    inventory: Dict[str, float]
    adverse: Dict[str, Optional[float]]
    parity: Dict[str, object]


def summarize(result: BacktestResult, *, tol: float = 1e-6) -> Summary:
    """Build a human-readable summary (also asserts reconciliation)."""
    attr = attribute_pnl(
        result.fills, mid_final=result.mid_final,
        cash_final=result.cash_final, inv_final=result.inv_final, tol=tol,
    )
    return Summary(
        attribution=attr,
        fills=fill_diagnostics(result),
        inventory=inventory_diagnostics(result),
        adverse=adverse_diagnostics(result.fills),
        parity=parity_summary(result.parity_samples),
    )


def print_summary(summary: Summary) -> None:
    """Print the summary as a readable block."""
    a = summary.attribution
    print("=== PnL attribution ===")
    print(f"  spread_capture        : {a.spread_capture:+.6f}")
    print(f"  inventory_pnl         : {a.inventory_pnl:+.6f}")
    print(f"  adverse_selection_cost: {a.adverse_selection_cost:+.6f}")
    print(f"  decomposed_total      : {a.decomposed_total:+.6f}")
    print(f"  total_pnl (engine)    : {a.total_pnl:+.6f}")
    print(f"  reconciliation_error  : {a.reconciliation_error:+.3e}")
    print(f"  fills={a.n_fills} (mid_before missing: {a.n_mid_before_missing})")
    print("=== fills ===")
    for k, v in summary.fills.items():
        print(f"  {k}: {v}")
    print("=== inventory ===")
    for k, v in summary.inventory.items():
        print(f"  {k}: {v}")
    print("=== adverse (None excluded) ===")
    for k, v in summary.adverse.items():
        print(f"  {k}: {v}")
    print("=== parity ===")
    for k, v in summary.parity.items():
        print(f"  {k}: {v}")


def plot_series(result: BacktestResult):  # pragma: no cover (optional)
    """Plot inventory & equity time series if matplotlib is available."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not available; skipping plot.")
        return None
    ts_inv = [t for t, _ in result.inventory_series]
    inv = [v for _, v in result.inventory_series]
    eq = [v for _, v in result.equity_series]
    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True)
    ax1.plot(ts_inv, inv); ax1.set_ylabel("inventory")
    ax2.plot(ts_inv, eq); ax2.set_ylabel("equity"); ax2.set_xlabel("local_timestamp")
    return fig


__all__ = [
    "Attribution",
    "attribute_pnl",
    "fill_diagnostics",
    "inventory_diagnostics",
    "adverse_diagnostics",
    "parity_summary",
    "time_bucket_attribution",
    "adverse_window_sweep",
    "config_sweep",
    "Summary",
    "summarize",
    "print_summary",
    "plot_series",
]
