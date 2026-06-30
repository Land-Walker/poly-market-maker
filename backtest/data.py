"""
data.py — Market data replay for the Polymarket backtest.

Responsibilities:
  1. Parse Polymarket WebSocket JSONL logs into typed events.
  2. Yield events in chronological order (single market, single file).
  3. Provide a derived `is_fill` field on price_change events by cross-
     referencing nearby last_trade_price events.

Non-responsibilities (deliberately):
  - Maintaining book state: the simulator does that.
  - Cross-market merging: handled by a future MultiMarketReplay.
  - Data quality reporting: that's a separate validate.py script.
  - Trade inference beyond fill-vs-cancel: out of scope for the loader.

Event semantics (from Polymarket docs, verified):
  - `book`: full L2 snapshot. Use to initialise or resync book state.
  - `price_change.size`: ABSOLUTE total size at that price level after
    the change. NOT a delta. size=0 means the level was removed.
  - `last_trade_price`: aggressive trade print. Used both as an event
    in the stream and to label price_change events as fills.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Union
import json


# ---------------------------------------------------------------------------
# Event dataclasses
# ---------------------------------------------------------------------------
#
# Design notes:
#
# - All three event types carry both `timestamp` (exchange clock, from the
#   message body) and `local_timestamp` (your collector's wall clock).
#   These differ by feed latency; the simulator may use either depending on
#   what it's modelling. Keep both.
#
# - Timestamps are normalised to float seconds since epoch. The raw
#   Polymarket `timestamp` field is a string of milliseconds; convert at
#   load time so downstream code never has to.
#
# - Prices and sizes are floats. The raw fields are strings (avoiding
#   JSON-number precision issues on the wire); convert at load time.
#
# - `bids` are stored sorted descending by price (best bid first).
#   `asks` are stored sorted ascending by price (best ask first).
#   The raw data is mostly already sorted this way but don't trust it.

@dataclass(frozen=True, slots=True)
class BookSnapshot:
    timestamp: float
    local_timestamp: float
    market: str
    asset_id: str
    bids: tuple[tuple[float, float], ...]   # (price, size), best bid first
    asks: tuple[tuple[float, float], ...]   # (price, size), best ask first
    tick_size: float
    # `hash` and `last_trade_price` from the raw payload are dropped;
    # add them back if a future module needs them.


@dataclass(frozen=True, slots=True)
class PriceChange:
    timestamp: float
    local_timestamp: float
    market: str
    asset_id: str
    price: float
    size: float                # absolute new total at this level
    side: str                  # "BUY" or "SELL"
    best_bid: float
    best_ask: float
    # Derived at load time by cross-referencing trades. None means
    # "not yet computed" (raw event); True means the size change
    # coincided with a trade print; False means cancel-only.
    is_fill: bool | None = None


@dataclass(frozen=True, slots=True)
class Trade:
    timestamp: float
    local_timestamp: float
    market: str
    asset_id: str
    price: float
    size: float
    side: str                  # taker side: "BUY" or "SELL"


Event = Union[BookSnapshot, PriceChange, Trade]


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
#
# Each JSONL line is one collector record:
#   {"local_timestamp": <float>, "slug": "...", "data": <payload>}
#
# `data` is either:
#   - a list of dicts (typically just the initial `book` snapshots,
#     one per asset_id), or
#   - a single dict (`price_change`, `last_trade_price`, etc.)
#
# Dispatch on the inner event_type. Unknown event_types should be
# silently ignored — Polymarket adds new types over time (e.g.
# tick_size_change, best_bid_ask) and the loader shouldn't crash on
# data it doesn't understand. Log a warning the first time you see
# each unknown type; don't spam.

def _parse_book(raw: dict, local_ts: float) -> BookSnapshot:
    """Parse a single `book` payload into a BookSnapshot.

    Conversions:
      - timestamp string of ms -> float seconds
      - bid/ask list of {price, size} dicts -> tuple of (float, float)
      - sort bids desc, asks asc (don't trust input ordering)
    """
    with open('')
    raise NotImplementedError


def _parse_price_change(raw: dict, local_ts: float) -> list[PriceChange]:
    """Parse a `price_change` payload. May contain multiple `price_changes`
    entries (one per affected asset_id when both Yes/No moved together).
    Returns one PriceChange per entry.

    `is_fill` is left None here; it's filled in by `_label_fills`.
    """
    raise NotImplementedError


def _parse_trade(raw: dict, local_ts: float) -> Trade:
    """Parse a `last_trade_price` payload."""
    raise NotImplementedError


# ---------------------------------------------------------------------------
# Fill labelling
# ---------------------------------------------------------------------------
#
# Heuristic: a price_change is labelled `is_fill=True` if a Trade event
# with the same asset_id and price occurred within FILL_WINDOW_S of it,
# AND the size decreased (i.e., this isn't a new order arrival).
#
# Edge cases worth knowing:
#   - A single large trade can sweep multiple levels. Each level produces
#     a separate price_change, each should be labelled as a fill.
#   - A partial fill at one level produces both a Trade and a price_change
#     with the new (lower) size. Same labelling logic applies.
#   - Cancel + new order at the same price within FILL_WINDOW_S can look
#     like a fill if you're not careful. Check that size DECREASED.
#   - Two trades very close in time may share a single price_change if
#     the collector or feed batches them. The label captures "there was
#     a trade nearby" — close enough for κ calibration purposes.
#
# This is a derived field with documented imprecision. Don't pretend it's
# ground truth. Use it for calibration and analysis, not for anything
# safety-critical in the backtest itself.

FILL_WINDOW_S = 0.1  # 100ms cross-reference window

def _label_fills(events: list[Event]) -> list[Event]:
    """Walk the event stream once; for each PriceChange with size DECREASE,
    check whether a Trade with the same (asset_id, price) occurred within
    +/- FILL_WINDOW_S. If so, return a copy with is_fill=True; otherwise
    is_fill=False.

    Implementation hint: maintain a small deque of recent trades indexed
    by (asset_id, price); prune entries older than FILL_WINDOW_S. Also
    maintain the previous size at each (asset_id, price) to detect
    decrease vs increase. The walk is O(n) in event count.
    """
    raise NotImplementedError


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class MarketReplay:
    """Replays a single Polymarket WebSocket JSONL log as an Event stream.

    Usage:
        replay = MarketReplay("log_btc-updown-5m-1776351600.jsonl")
        for event in replay.stream():
            ... # simulator consumes here

    The full file is loaded into memory at construction time. For the
    short markets you're collecting (a few minutes, ~3MB), this is fine.
    If you later need to replay multi-hour markets, switch to a streaming
    parser — the Event interface won't change.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._events: list[Event] = self._load_and_label()

    def _load_and_label(self) -> list[Event]:
        """Load all events from disk, then label fills.

        Two-pass design (load, then label) is deliberate: the fill
        labelling needs to look backward AND forward in time at each
        price_change to find nearby trades. Single-pass is possible but
        more fiddly; do it later if memory becomes a concern.
        """
        raw_events: list[Event] = []
        # ... iterate jsonl, dispatch on event_type, append to raw_events
        # Don't sort: events should already be chronological by collection
        # order. If they're not, that's a data bug worth raising loudly.
        labeled = _label_fills(raw_events)
        return labeled

    def stream(self) -> Iterator[Event]:
        """Yield events in chronological order."""
        yield from self._events

    # Convenience methods — implement if/when needed, not preemptively.
    # def trades(self) -> Iterator[Trade]: ...
    # def snapshots(self) -> Iterator[BookSnapshot]: ...
    # def time_range(self) -> tuple[float, float]: ...
    # def __len__(self) -> int: ...