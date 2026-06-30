"""Unit tests for fill_simulator.py using small synthetic events.

Runnable with pytest, or directly:  python test_fill_simulator.py
"""

from __future__ import annotations

from decimal import Decimal

from data_loader import MarketEvent, OrderBook, Trade
from fill_simulator import FillSimulator, MyOrder, adverse_summary, clamp_queue_ahead

ASSET = "A"


def _event(local_ts, book, *, event_type="price_change", trade=None):
    """Wrap a single live OrderBook into a MarketEvent."""
    return MarketEvent(
        local_timestamp=local_ts,
        exchange_timestamp=None,
        event_type=event_type,
        asset_ids=(ASSET,),
        trade=trade,
        books={ASSET: book},
        primary_asset=ASSET,
    )


# Queue-advance invariant — the exact numeric cases requested for review.
def test_clamp_no_advance_case_a():
    assert clamp_queue_ahead(30.0, 60.0) == 30.0  # others 100->60, q=30 stays 30


def test_clamp_forced_advance_case_b():
    assert clamp_queue_ahead(30.0, 20.0) == 20.0  # others 100->20, q=30 -> 20


def test_clamp_never_moves_backward_on_growth():
    assert clamp_queue_ahead(30.0, 80.0) == 30.0  # level grows -> no backward


# No fill without a trade print (no "touch => fill" optimism).
def test_no_fill_on_book_touch_without_trade():
    book = OrderBook(ASSET)
    book.apply_snapshot(bids=[{"price": "0.65", "size": "10"}],
                        asks=[{"price": "0.67", "size": "100"}])
    order = MyOrder(side="SELL", price=Decimal("0.67"), size=50,
                    asset_id=ASSET, place_local_ts=0.0)
    sim = FillSimulator(order)
    sim.process(_event(0.0, book, event_type="book"))
    book.apply_change("0.67", "0", "SELL")  # everyone cancels, no trade
    sim.process(_event(0.1, book))
    assert order.filled == 0.0
    assert order.remaining == 50.0
    assert order.queue_ahead == 0.0


# Queue consumption + partial fills driven only by trade prints.
def test_queue_consume_then_partial_fills():
    book = OrderBook(ASSET)
    book.apply_snapshot(bids=[{"price": "0.65", "size": "10"}],
                        asks=[{"price": "0.67", "size": "100"}])
    order = MyOrder(side="SELL", price=Decimal("0.67"), size=50,
                    asset_id=ASSET, place_local_ts=0.0)
    sim = FillSimulator(order)
    sim.process(_event(0.0, book, event_type="book"))
    assert order.queue_ahead == 100.0
    book.apply_change("0.67", "60", "SELL")
    sim.process(_event(0.1, book))
    assert order.queue_ahead == 60.0
    book.apply_change("0.67", "20", "SELL")
    sim.process(_event(0.2, book))
    assert order.queue_ahead == 20.0
    sim.process(_event(0.3, book, event_type="last_trade_price",
                       trade=Trade(ASSET, Decimal("0.67"), 30.0, "BUY")))
    assert order.queue_ahead == 0.0
    assert abs(order.filled - 10.0) < 1e-9
    assert abs(order.remaining - 40.0) < 1e-9
    sim.process(_event(0.4, book, event_type="last_trade_price",
                       trade=Trade(ASSET, Decimal("0.67"), 100.0, "BUY")))
    assert abs(order.filled - 50.0) < 1e-9
    assert abs(order.remaining) < 1e-9
    assert order.active is False
    assert len(order.fills) == 2
    assert [round(f.qty, 6) for f in order.fills] == [10.0, 40.0]


def test_wrong_side_trade_does_not_fill():
    book = OrderBook(ASSET)
    book.apply_snapshot(bids=[{"price": "0.65", "size": "0"}],
                        asks=[{"price": "0.67", "size": "0"}])
    order = MyOrder(side="SELL", price=Decimal("0.67"), size=50,
                    asset_id=ASSET, place_local_ts=0.0)
    sim = FillSimulator(order)
    sim.process(_event(0.0, book, event_type="book"))
    sim.process(_event(0.1, book, event_type="last_trade_price",
                       trade=Trade(ASSET, Decimal("0.67"), 100.0, "SELL")))
    assert order.filled == 0.0


# Adverse selection — tri-state (True / False / None=unmeasurable).
def test_adverse_selection_true_when_mid_moves_against_buy():
    book = OrderBook(ASSET)
    book.apply_snapshot(bids=[{"price": "0.65", "size": "5"}],
                        asks=[{"price": "0.67", "size": "50"}])  # mid 0.66
    order = MyOrder(side="BUY", price=Decimal("0.65"), size=10,
                    asset_id=ASSET, place_local_ts=0.0)
    sim = FillSimulator(order, adverse_window_seconds=1.0)
    sim.process(_event(1.0, book, event_type="book"))
    sim.process(_event(1.0, book, event_type="last_trade_price",
                       trade=Trade(ASSET, Decimal("0.65"), 10.0, "SELL")))
    assert abs(order.filled - 5.0) < 1e-9
    book.apply_change("0.67", "0", "SELL")
    book.apply_change("0.63", "50", "SELL")  # best ask 0.63 -> mid 0.64
    sim.process(_event(2.5, book))
    sim.finalize(last_mid=book.mid())
    f = order.fills[0]
    assert f.mid_before == 0.66
    assert f.adverse is True
    assert f.mid_after_local_ts == 2.5
    assert abs(f.measured_dt - 1.5) < 1e-9
    assert f.mid_after == 0.64


def test_adverse_selection_false_when_mid_moves_favorably_for_buy():
    book = OrderBook(ASSET)
    book.apply_snapshot(bids=[{"price": "0.65", "size": "5"}],
                        asks=[{"price": "0.67", "size": "50"}])
    order = MyOrder(side="BUY", price=Decimal("0.65"), size=10,
                    asset_id=ASSET, place_local_ts=0.0)
    sim = FillSimulator(order, adverse_window_seconds=1.0)
    sim.process(_event(1.0, book, event_type="book"))
    sim.process(_event(1.0, book, event_type="last_trade_price",
                       trade=Trade(ASSET, Decimal("0.65"), 10.0, "SELL")))
    book.apply_change("0.67", "0", "SELL")
    book.apply_change("0.71", "50", "SELL")  # best ask 0.71 -> mid 0.68
    sim.process(_event(2.5, book))
    sim.finalize(last_mid=book.mid())
    assert order.fills[0].adverse is False


def test_adverse_unmeasurable_is_none_not_false():
    book = OrderBook(ASSET)
    book.apply_snapshot(bids=[{"price": "0.65", "size": "5"}],
                        asks=[{"price": "0.67", "size": "50"}])
    order = MyOrder(side="BUY", price=Decimal("0.65"), size=10,
                    asset_id=ASSET, place_local_ts=0.0)
    sim = FillSimulator(order, adverse_window_seconds=1.0)
    sim.process(_event(1.0, book, event_type="book"))
    sim.process(_event(1.0, book, event_type="last_trade_price",
                       trade=Trade(ASSET, Decimal("0.65"), 10.0, "SELL")))
    assert abs(order.filled - 5.0) < 1e-9
    book.apply_change("0.67", "0", "SELL")  # one-sided -> mid None rest of stream
    sim.process(_event(3.0, book))
    sim.finalize()
    f = order.fills[0]
    assert f.adverse is None
    assert f.mid_after is None
    assert f.measured_dt is None
    s = adverse_summary(order)
    assert s["unmeasurable"] == 1
    assert s["measurable"] == 0
    assert s["adverse_rate"] is None


def test_adverse_summary_excludes_none():
    from fill_simulator import Fill
    order = MyOrder(side="BUY", price=Decimal("0.65"), size=10,
                    asset_id=ASSET, place_local_ts=0.0)
    order.fills = [
        Fill(1.0, Decimal("0.65"), 1, 9, 0, 0.66, mid_after=0.64, adverse=True),
        Fill(1.0, Decimal("0.65"), 1, 8, 0, 0.66, mid_after=0.67, adverse=False),
        Fill(1.0, Decimal("0.65"), 1, 7, 0, 0.66, mid_after=None, adverse=None),
    ]
    s = adverse_summary(order)
    assert s["total_fills"] == 3
    assert s["measurable"] == 2
    assert s["unmeasurable"] == 1
    assert s["adverse_true"] == 1
    assert s["adverse_false"] == 1
    assert abs(s["adverse_rate"] - 0.5) < 1e-9


def _run_all():
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"\n{len(fns)} fill_simulator tests passed.")


if __name__ == "__main__":
    _run_all()
