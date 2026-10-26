"""Negative controls for point-in-time history and deterministic output."""

from datetime import datetime
from itertools import permutations

from repeatlab.temporal import Order, build_snapshot

SNAPSHOT = datetime(2011, 1, 1)
OBSERVED_UNTIL = datetime(2011, 2, 1)


def test_future_sales_change_target_but_not_past_features() -> None:
    history = (Order("A", datetime(2010, 12, 10), "sale"),)
    before = build_snapshot(history, snapshot_time=SNAPSHOT, observed_until=OBSERVED_UNTIL)
    after = build_snapshot(
        (*history, Order("A", datetime(2011, 1, 5), "sale")),
        snapshot_time=SNAPSHOT,
        observed_until=OBSERVED_UNTIL,
    )

    assert before[0].history_order_count == after[0].history_order_count == 1
    assert before[0].label == 0
    assert after[0].label == 1


def test_future_cancellation_does_not_rewrite_history_or_create_purchase() -> None:
    history = (Order("A", datetime(2010, 12, 10), "sale"),)
    before = build_snapshot(history, snapshot_time=SNAPSHOT, observed_until=OBSERVED_UNTIL)
    after = build_snapshot(
        (*history, Order("A", datetime(2011, 1, 5), "cancellation")),
        snapshot_time=SNAPSHOT,
        observed_until=OBSERVED_UNTIL,
    )

    assert after == before
    assert after[0].history_order_count == 1
    assert after[0].label == 0


def test_unobserved_events_do_not_change_snapshot() -> None:
    history = (Order("A", datetime(2010, 12, 10), "sale"),)
    before = build_snapshot(history, snapshot_time=SNAPSHOT, observed_until=OBSERVED_UNTIL)
    after = build_snapshot(
        (
            *history,
            Order("A", OBSERVED_UNTIL, "sale"),
            Order("new", datetime(2011, 2, 5), "sale"),
        ),
        snapshot_time=SNAPSHOT,
        observed_until=OBSERVED_UNTIL,
    )

    assert after == before


def test_input_order_does_not_change_results() -> None:
    orders = (
        Order("B", datetime(2010, 12, 10), "sale"),
        Order("A", datetime(2010, 12, 10), "sale"),
        Order("A", datetime(2010, 12, 20), "sale"),
        Order("A", datetime(2011, 1, 5), "sale"),
    )
    for permuted in permutations(orders):
        rows = build_snapshot(permuted, snapshot_time=SNAPSHOT, observed_until=OBSERVED_UNTIL)
        assert [(row.customer_id, row.history_order_count, row.label) for row in rows] == [
            ("A", 2, 1),
            ("B", 1, 0),
        ]
