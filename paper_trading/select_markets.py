#!/usr/bin/env python3
"""select_markets.py — list "pure binary, reasonably active" Polymarket markets
as candidates for paper-trading live runs.

This is NOT the full discovery system (that is a later phase) — it only LISTS
candidates from the Gamma API and lets you pick a market by hand. Selection
criteria (all required):

  * negRisk == False          (pure binary; negRisk markets have a different book
                               structure that breaks the single-token premise —
                               the Messi market was negRisk=True and unsuitable)
  * enableOrderBook == True, acceptingOrders == True, active == True, closed == False
  * volume24hr within [min, max]  (not too thin -> no fills; not too thick -> deep
                                   queue, hard to fill)
  * endDate at least N days away  (near-resolution books skew one-sided; MM moot)
  * exactly two outcome tokens (binary)

Also prints orderPriceMinTickSize and orderMinSize (needed for the strategy
config). Output is sorted by 24h volume (descending).

Dependencies: standard library only (urllib). The filter/rank logic is a pure
function (`filter_and_rank`) verified offline by ``--self-test`` (no network).

Usage:
    python paper_trading/select_markets.py                  # live, default filters
    python paper_trading/select_markets.py --min-vol24 20000 --min-days 21 --top 15
    python paper_trading/select_markets.py --self-test      # offline logic check
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

GAMMA_URL = "https://gamma-api.polymarket.com/markets"


# ---------------------------------------------------------------------------
# Normalisation helpers (Gamma returns camelCase, often as strings)
# ---------------------------------------------------------------------------
def _as_float(v, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _as_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes")
    return bool(v)


def _as_list(v) -> list:
    """clobTokenIds / outcomes come either as a JSON string or a real list."""
    if isinstance(v, list):
        return v
    if isinstance(v, str) and v.strip():
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _parse_end_date(v) -> Optional[datetime]:
    if not v or not isinstance(v, str):
        return None
    s = v.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


@dataclass
class Candidate:
    slug: str
    question: str
    volume24hr: float
    liquidity: float
    end_date: Optional[datetime]
    neg_risk: bool
    yes_token: Optional[str]
    no_token: Optional[str]
    tick_size: Optional[float]
    min_size: Optional[float]


def normalize(m: dict) -> dict:
    """Flatten one raw Gamma market dict into the fields we care about."""
    tokens = _as_list(m.get("clobTokenIds"))
    return {
        "slug": m.get("slug", ""),
        "question": m.get("question", ""),
        "volume24hr": _as_float(m.get("volume24hr"), 0.0) or 0.0,
        "liquidity": _as_float(m.get("liquidity", m.get("liquidityNum")), 0.0) or 0.0,
        "end_date": _parse_end_date(m.get("endDate")),
        "neg_risk": _as_bool(m.get("negRisk")),
        "enable_order_book": _as_bool(m.get("enableOrderBook")),
        "accepting_orders": _as_bool(m.get("acceptingOrders")),
        "active": _as_bool(m.get("active")),
        "closed": _as_bool(m.get("closed")),
        "tokens": tokens,
        "tick_size": _as_float(m.get("orderPriceMinTickSize")),
        "min_size": _as_float(m.get("orderMinSize")),
    }


# ---------------------------------------------------------------------------
# Pure filter + rank (unit-tested offline)
# ---------------------------------------------------------------------------
def filter_and_rank(
    markets: List[dict],
    *,
    min_vol24: float,
    max_vol24: Optional[float],
    min_days: float,
    now: Optional[datetime] = None,
) -> List[Candidate]:
    """Return pure-binary, active, suitably-liquid candidates sorted by 24h volume
    (descending). ``markets`` is a list of RAW Gamma dicts; ``now`` defaults to
    the current UTC time (injectable for deterministic tests)."""
    if now is None:
        now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=min_days)

    out: List[Candidate] = []
    for raw in markets:
        m = normalize(raw)
        if m["neg_risk"]:
            continue                      # pure binary only
        if not (m["enable_order_book"] and m["accepting_orders"]
                and m["active"] and not m["closed"]):
            continue
        if len(m["tokens"]) != 2:
            continue                      # binary = exactly two outcome tokens
        vol = m["volume24hr"]
        if vol < min_vol24:
            continue
        if max_vol24 is not None and vol > max_vol24:
            continue
        if m["end_date"] is None or m["end_date"] < cutoff:
            continue                      # must have enough time left
        out.append(Candidate(
            slug=m["slug"], question=m["question"], volume24hr=vol,
            liquidity=m["liquidity"], end_date=m["end_date"], neg_risk=m["neg_risk"],
            yes_token=str(m["tokens"][0]), no_token=str(m["tokens"][1]),
            tick_size=m["tick_size"], min_size=m["min_size"],
        ))
    out.sort(key=lambda c: c.volume24hr, reverse=True)
    return out


# ---------------------------------------------------------------------------
# Network + presentation
# ---------------------------------------------------------------------------
def fetch_markets(limit: int, timeout: float) -> List[dict]:
    """Fetch active, open markets from the Gamma API (raw dicts)."""
    url = (f"{GAMMA_URL}?active=true&closed=false&archived=false"
           f"&limit={int(limit)}&order=volume24hr&ascending=false")
    req = urllib.request.Request(url, headers={"User-Agent": "poly-bot-test/select_markets"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    # Gamma may return a bare list or {"data": [...]} depending on version.
    if isinstance(data, dict):
        data = data.get("data", [])
    return data if isinstance(data, list) else []


def _trunc(s: str, n: int) -> str:
    s = s or ""
    return s if len(s) <= n else s[: n - 1] + "…"


def format_table(cands: List[Candidate], top: int) -> str:
    rows = cands[:top]
    lines = []
    header = (f"{'#':>2}  {'vol24h':>11}  {'liq':>10}  {'endDate':>10}  "
              f"{'tick':>5}  {'min':>5}  {'slug':<40}  question")
    lines.append(header)
    lines.append("-" * len(header))
    for i, c in enumerate(rows, 1):
        end = c.end_date.strftime("%Y-%m-%d") if c.end_date else "?"
        tick = f"{c.tick_size:g}" if c.tick_size is not None else "?"
        mins = f"{c.min_size:g}" if c.min_size is not None else "?"
        lines.append(
            f"{i:>2}  {c.volume24hr:>11,.0f}  {c.liquidity:>10,.0f}  {end:>10}  "
            f"{tick:>5}  {mins:>5}  {_trunc(c.slug, 40):<40}  {_trunc(c.question, 60)}"
        )
    # token ids printed separately (long) so the table stays readable
    lines.append("")
    lines.append("clobTokenIds (YES / NO):")
    for i, c in enumerate(rows, 1):
        lines.append(f"  {i:>2}. {c.slug}")
        lines.append(f"      YES={c.yes_token}")
        lines.append(f"      NO ={c.no_token}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Offline self-test (no network)
# ---------------------------------------------------------------------------
def _self_test() -> None:
    now = datetime(2026, 6, 29, tzinfo=timezone.utc)
    far = "2026-08-01T00:00:00Z"   # ~33 days out
    soon = "2026-07-01T00:00:00Z"  # ~2 days out

    def mk(**over):
        base = dict(slug="s", question="q", volume24hr="10000", liquidity="5000",
                    endDate=far, negRisk=False, enableOrderBook=True,
                    acceptingOrders=True, active=True, closed=False,
                    clobTokenIds=json.dumps(["YES", "NO"]),
                    orderPriceMinTickSize="0.01", orderMinSize="5")
        base.update(over)
        return base

    markets = [
        mk(slug="good-hi", volume24hr="50000"),                 # pass (highest vol)
        mk(slug="good-lo", volume24hr="12000"),                 # pass
        mk(slug="negrisk", negRisk=True),                       # reject: negRisk
        mk(slug="no-book", enableOrderBook=False),              # reject: no book
        mk(slug="not-accepting", acceptingOrders=False),        # reject
        mk(slug="closed", closed=True),                         # reject
        mk(slug="too-thin", volume24hr="500"),                  # reject: < min_vol
        mk(slug="too-thick", volume24hr="9000000"),             # reject: > max_vol
        mk(slug="ends-soon", endDate=soon),                     # reject: near expiry
        mk(slug="not-binary", clobTokenIds=json.dumps(["A", "B", "C"])),  # reject
    ]
    cands = filter_and_rank(markets, min_vol24=5000, max_vol24=1_000_000,
                            min_days=14, now=now)
    slugs = [c.slug for c in cands]
    assert slugs == ["good-hi", "good-lo"], slugs           # only the two, vol-sorted
    assert cands[0].yes_token == "YES" and cands[0].no_token == "NO"
    assert cands[0].tick_size == 0.01 and cands[0].min_size == 5.0
    # string-typed numbers and JSON-string token lists were coerced correctly
    assert isinstance(cands[0].volume24hr, float)
    print("self-test passed: filtering, coercion, and ranking all correct.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="List pure-binary, active Polymarket candidates.")
    p.add_argument("--limit", type=int, default=500, help="markets to fetch from Gamma")
    p.add_argument("--min-vol24", type=float, default=5000.0, help="min 24h volume")
    p.add_argument("--max-vol24", type=float, default=2_000_000.0,
                   help="max 24h volume (avoid ultra-deep books)")
    p.add_argument("--min-days", type=float, default=14.0, help="min days until endDate")
    p.add_argument("--top", type=int, default=20, help="how many candidates to show")
    p.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout (s)")
    p.add_argument("--list-only", action="store_true", default=True,
                   help="list candidates only (selection is manual; always on)")
    p.add_argument("--self-test", action="store_true", help="run offline logic check and exit")
    a = p.parse_args(argv)

    if a.self_test:
        _self_test()
        return 0

    try:
        raw = fetch_markets(a.limit, a.timeout)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as exc:
        print(f"[select_markets] could not reach Gamma API: {exc}", file=sys.stderr)
        print("Run this on a machine with network access (e.g. your Oracle VM).",
              file=sys.stderr)
        return 2

    cands = filter_and_rank(raw, min_vol24=a.min_vol24, max_vol24=a.max_vol24,
                            min_days=a.min_days)
    print(f"Fetched {len(raw)} markets; {len(cands)} pass the pure-binary filters.\n")
    print(format_table(cands, a.top))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
