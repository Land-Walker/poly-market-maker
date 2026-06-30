"""Unit tests for data_loader.py using small synthetic JSONL data.

Runnable with pytest, or directly:  python test_data_loader.py
"""

from __future__ import annotations

import json
import os
import tempfile
from decimal import Decimal

from data_loader import iter_market_events

YES = "YES_ASSET"
NO = "NO_ASSET"


def _write_jsonl(records) -> str:
    fd, path = tempfile.mkstemp(suffix=".jsonl")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")
    return path


def _snapshot(local_ts, asset, bids, asks, exch="1000"):
    return {
        "local_timestamp": local_ts,
        "slug": "synthetic",
        "data": {
            "asset_id": asset,
            "timestamp": exch,
            "bids": [{"price": p, "size": s} for p, s in bids],
            "asks": [{"price": p, "size": s} for p, s in asks],
            "event_type": "book",
        },
    }


def _price_change(local_ts, changes, exch="1001"):
    return {
        "local_timestamp": local_ts,
        "slug": "synthetic",
        "data": {
            "market": "0xmarket",
            "timestamp": exch,
            "event_type": "price_change",
            "price_changes": [
                {"asset_id": a, "price": p, "size": s, "side": side}
                for (a, p, s, side) in changes
            ],
        },
    }


def _trade(local_ts, asset, price, size, side, exch="1002"):
    return {
        "local_timestamp": local_ts,
        "slug": "synthetic",
        "data": {
            "asset_id": asset,
            "timestamp": exch,
            "price": price,
            "size": size,
            "side": side,
            "event_type": "last_trade_price",
        },
    }


def test_snapshot_and_delta_reconstruction():
    """Snapshot initialises the book; a price_change sets an absolute level; a
    size-0 change removes a level."""
    records = [
        _snapshot(1.0, YES, bids=[("0.40", "100"), ("0.39", "50")], asks=[("0.60", "80")]),
        _price_change(1.1, [(YES, "0.40", "60", "BUY")]),  # absolute new size
        _price_change(1.2, [(YES, "0.39", "0", "BUY")]),   # remove level
    ]
    path = _write_jsonl(records)
    try:
        events = list(iter_market_events(path, primary_asset=YES))
    finally:
        os.remove(path)

    last = events[-1]
    book = last.book(YES)
    assert book.synced is True
    assert book.best_bid() == Decimal("0.40")
    assert book.level_size(Decimal("0.40"), "BUY") == 60.0  # delta applied
    assert book.level_size(Decimal("0.39"), "BUY") == 0.0   # removed
    assert book.best_ask() == Decimal("0.60")
    top = book.top_of_book()
    assert top.best_bid_size == 60.0
    assert top.mid == (0.40 + 0.60) / 2.0


def test_strict_ordering_raises_on_backward_timestamp():
    records = [
        _snapshot(5.0, YES, bids=[("0.40", "100")], asks=[("0.60", "80")]),
        _price_change(4.0, [(YES, "0.40", "90", "BUY")]),  # goes backward
    ]
    path = _write_jsonl(records)
    try:
        gen = iter_market_events(path, primary_asset=YES)
        next(gen)  # ok
        raised = False
        try:
            next(gen)
        except ValueError:
            raised = True
        assert raised, "expected ValueError on out-of-order local_timestamp"
    finally:
        os.remove(path)


def test_parity_unreliable_when_no_token_never_snapshotted():
    """NO token is delta-only -> parity_deviation must be flagged unreliable."""
    records = [
        _snapshot(1.0, YES, bids=[("0.40", "100")], asks=[("0.60", "80")]),
        # NO token only ever appears via price_change (mirrors the real data).
        _price_change(1.1, [(NO, "0.40", "70", "BUY"), (NO, "0.60", "70", "SELL")]),
    ]
    path = _write_jsonl(records)
    try:
        events = list(iter_market_events(path, primary_asset=YES))
    finally:
        os.remove(path)

    dev, reliable = events[-1].parity_deviation(YES, NO)
    assert reliable is False  # NO book never synced
    assert events[-1].books[NO].synced is False
    assert events[-1].books[YES].synced is True


def test_trade_event_exposed_without_mutating_book():
    records = [
        _snapshot(1.0, YES, bids=[("0.40", "100")], asks=[("0.67", "80")]),
        _trade(1.1, YES, "0.67", "10", "BUY"),
    ]
    path = _write_jsonl(records)
    try:
        events = list(iter_market_events(path, primary_asset=YES))
    finally:
        os.remove(path)

    ev = events[-1]
    assert ev.event_type == "last_trade_price"
    assert ev.trade is not None
    assert ev.trade.price == Decimal("0.67")
    assert ev.trade.size == 10.0
    assert ev.trade.side == "BUY"
    # Book unchanged by the trade print itself.
    assert ev.book(YES).level_size(Decimal("0.67"), "SELL") == 80.0


def test_list_frame_dispatches_each_message_by_type():
    """A frame whose data is a LIST of mixed messages must dispatch each element
    by its own event_type (book/price_change/last_trade_price) with NO drops."""
    from data_loader import build_market_events
    books = {}
    frame = {"local_timestamp": 1.0, "slug": "syn", "data": [
        {"asset_id": YES, "event_type": "book", "timestamp": "1",
         "bids": [{"price": "0.45", "size": "100"}],
         "asks": [{"price": "0.55", "size": "100"}]},
        {"asset_id": YES, "event_type": "price_change", "timestamp": "2",
         "price_changes": [{"asset_id": YES, "price": "0.55", "size": "40", "side": "SELL"}]},
        {"asset_id": YES, "event_type": "last_trade_price", "timestamp": "3",
         "price": "0.55", "size": "7", "side": "BUY"},
    ]}
    evs = list(build_market_events(books, frame, primary_asset=YES))
    assert [e.event_type for e in evs] == ["book", "price_change", "last_trade_price"]
    assert evs[-1].trade is not None and evs[-1].trade.side == "BUY"   # trade preserved
    assert books[YES].synced                                          # book applied
    assert books[YES].level_size(Decimal("0.55"), "SELL") == 40.0     # price_change applied


def test_both_tokens_updated_in_one_price_change_frame():
    """A single price_change message carrying both tokens updates both books."""
    from data_loader import build_market_events
    books = {}
    frame = {"local_timestamp": 1.0, "slug": "syn", "data": {
        "event_type": "price_change", "timestamp": "1", "price_changes": [
            {"asset_id": NO, "price": "0.46", "size": "500", "side": "BUY"},
            {"asset_id": YES, "price": "0.54", "size": "500", "side": "SELL"}]}}
    evs = list(build_market_events(books, frame, primary_asset=YES))
    assert len(evs) == 1                                  # one message -> one event
    assert set(evs[0].asset_ids) == {YES, NO}
    assert books[YES].level_size(Decimal("0.54"), "SELL") == 500.0
    assert books[NO].level_size(Decimal("0.46"), "BUY") == 500.0


def _run_all():
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"\n{len(fns)} data_loader tests passed.")


if __name__ == "__main__":
    _run_all()
