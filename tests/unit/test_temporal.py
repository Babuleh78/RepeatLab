"""Reference cases for temporal purchase labels."""

import csv
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from repeatlab.temporal import Order, OrderKind, build_snapshot

SNAPSHOT = datetime(2011, 1, 1)
HORIZON_END = datetime(2011, 1, 31)


@pytest.fixture
def reference_orders() -> tuple[Order, ...]:
    path = Path(__file__).resolve().parents[2] / "data/fixtures/minimal_orders.csv"
    with path.open(encoding="utf-8", newline="") as source:
        return tuple(
            Order(
                customer_id=row["customer_id"] or None,
                event_time=datetime.fromisoformat(row["event_time"]),
                kind=cast(OrderKind, row["kind"]),
            )
            for row in csv.DictReader(source)
        )


def test_reference_customers_have_expected_labels(reference_orders: tuple[Order, ...]) -> None:
    rows = build_snapshot(
        reference_orders,
        snapshot_time=SNAPSHOT,
        observed_until=datetime(2011, 2, 1),
    )

    assert [(row.customer_id, row.history_order_count, row.label) for row in rows] == [
        ("A", 1, 1),
        ("B", 1, 0),
        ("C", 1, 0),
        ("E", 1, 1),
    ]
    assert all(row.status == "complete" for row in rows)
    assert all(row.snapshot_time == SNAPSHOT for row in rows)
    assert all(row.label_end == HORIZON_END for row in rows)


def test_incomplete_horizons_have_no_binary_label(
    reference_orders: tuple[Order, ...],
) -> None:
    rows = build_snapshot(
        reference_orders,
        snapshot_time=SNAPSHOT,
        observed_until=datetime(2011, 1, 15),
    )

    assert [(row.customer_id, row.status, row.label) for row in rows] == [
        ("A", "censored", None),
        ("B", "censored", None),
        ("C", "censored", None),
        ("E", "censored", None),
    ]


def test_observation_end_equal_to_label_end_is_complete(
    reference_orders: tuple[Order, ...],
) -> None:
    rows = build_snapshot(
        reference_orders,
        snapshot_time=SNAPSHOT,
        observed_until=HORIZON_END,
    )

    assert [(row.customer_id, row.label) for row in rows] == [
        ("A", 1),
        ("B", 0),
        ("C", 0),
        ("E", 1),
    ]
    assert all(row.status == "complete" for row in rows)


def test_history_includes_left_boundary_and_excludes_snapshot() -> None:
    orders = (
        Order("included", datetime(2010, 7, 5), "sale"),
        Order("too_old", datetime(2010, 7, 4, 23, 59, 59, 999999), "sale"),
        Order("new", SNAPSHOT, "sale"),
        Order("cancelled_only", datetime(2010, 12, 1), "cancellation"),
    )

    rows = build_snapshot(orders, snapshot_time=SNAPSHOT, observed_until=HORIZON_END)

    assert [(row.customer_id, row.history_order_count, row.label) for row in rows] == [
        ("included", 1, 0),
    ]


def test_nondefault_windows_use_the_requested_bounds() -> None:
    orders = (
        Order("A", datetime(2010, 12, 25), "sale"),
        Order("A", datetime(2011, 1, 8), "sale"),
        Order("B", datetime(2010, 12, 24), "sale"),
    )

    rows = build_snapshot(
        orders,
        snapshot_time=SNAPSHOT,
        observed_until=datetime(2011, 1, 8),
        horizon_days=7,
        lookback_days=7,
    )

    assert [(row.customer_id, row.label, row.label_end) for row in rows] == [
        ("A", 0, datetime(2011, 1, 8)),
    ]


@pytest.mark.parametrize("horizon_days,lookback_days", [(0, 180), (-1, 180), (30, 0), (30, -1)])
def test_nonpositive_windows_are_rejected(horizon_days: int, lookback_days: int) -> None:
    with pytest.raises(ValueError):
        build_snapshot(
            (),
            snapshot_time=SNAPSHOT,
            observed_until=HORIZON_END,
            horizon_days=horizon_days,
            lookback_days=lookback_days,
        )


def test_timezone_aware_orders_are_rejected() -> None:
    with pytest.raises(ValueError):
        Order("A", datetime(2010, 12, 1, tzinfo=UTC), "sale")


@pytest.mark.parametrize("aware_field", ["snapshot_time", "observed_until"])
def test_timezone_aware_boundaries_are_rejected(aware_field: str) -> None:
    snapshot_time = SNAPSHOT.replace(tzinfo=UTC) if aware_field == "snapshot_time" else SNAPSHOT
    observed_until = (
        HORIZON_END.replace(tzinfo=UTC) if aware_field == "observed_until" else HORIZON_END
    )
    with pytest.raises(ValueError):
        build_snapshot((), snapshot_time=snapshot_time, observed_until=observed_until)


def test_observation_end_before_snapshot_is_rejected() -> None:
    with pytest.raises(ValueError):
        build_snapshot((), snapshot_time=SNAPSHOT, observed_until=datetime(2010, 12, 31))


def test_unknown_event_kind_is_rejected() -> None:
    with pytest.raises(ValueError):
        Order("A", SNAPSHOT, cast(OrderKind, "refund"))


def test_no_orders_produces_no_customers() -> None:
    assert build_snapshot((), snapshot_time=SNAPSHOT, observed_until=HORIZON_END) == ()
