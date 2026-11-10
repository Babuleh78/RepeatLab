"""The real clean exchange preserves half-open snapshot boundaries."""

import importlib
from datetime import datetime
from pathlib import Path

from test_java_cleaning import run_clean

from repeatlab.temporal import build_snapshot


def test_future_sales_and_cancellations_do_not_rewrite_history(tmp_path: Path) -> None:
    read = importlib.import_module("repeatlab.cleaning").read_clean_dataset
    history = [
        ("HA", "P", "1", "2010-12-10T00:00:00", "1", "A"),
        ("HB", "P", "1", "2010-12-10T00:00:00", "1", "B"),
        ("HC", "P", "1", "2010-12-10T00:00:00", "1", "C"),
    ]
    future = [
        ("AT", "P", "1", "2011-01-01T00:00:00", "1", "A"),
        ("END", "P", "1", "2011-01-31T00:00:00", "1", "B"),
        ("CC", "P", "-1", "2011-01-05T00:00:00", "1", "C"),
    ]
    first = read(run_clean(tmp_path / "history", history)).aggregate()
    second = read(run_clean(tmp_path / "full", history + future)).aggregate()
    t = datetime(2011, 1, 1)
    before = build_snapshot(
        (order.to_temporal_order() for order in first.orders),
        snapshot_time=t,
        observed_until=datetime(2011, 2, 1),
    )
    after = build_snapshot(
        (order.to_temporal_order() for order in second.orders),
        snapshot_time=t,
        observed_until=datetime(2011, 2, 1),
    )
    assert [row.history_order_count for row in before] == [1, 1, 1]
    assert [row.history_order_count for row in after] == [1, 1, 1]
    assert [row.label for row in after] == [1, 0, 0]
    censored = build_snapshot(
        (order.to_temporal_order() for order in second.orders),
        snapshot_time=t,
        observed_until=datetime(2011, 1, 15),
    )
    assert all(row.label is None and row.status == "censored" for row in censored)
