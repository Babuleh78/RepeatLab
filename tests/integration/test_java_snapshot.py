"""Compare historical invoice decisions at the feature and label cutoffs."""

import hashlib
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from test_java_cleaning import run_clean
from test_java_ingestion import JAR, java_command

from repeatlab.cleaning import read_clean_dataset
from repeatlab.temporal import build_snapshot


def snapshot(
    source: Path, until: str, output: Path, *options: str
) -> tuple[int, list[dict[str, Any]], dict[str, Any]]:
    result = subprocess.run(
        [
            java_command(),
            "-jar",
            str(JAR),
            "snapshot",
            "--input",
            str(source / "raw"),
            "--config",
            str(source / "config.json"),
            "--snapshot-time",
            "2020-02-01T00:00:00",
            "--observed-until",
            until,
            "--output",
            str(output),
            *options,
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=30,
        check=False,
    )
    assert result.returncode in (0, 2), result.stderr
    rows = (output / "snapshots.jsonl").read_bytes()
    manifest = json.loads((output / "manifest.json").read_bytes())
    assert manifest["artifacts"]["snapshots.jsonl"] == hashlib.sha256(rows).hexdigest()
    return result.returncode, [json.loads(line) for line in rows.splitlines()], manifest


def test_later_invoice_conflicts_cannot_rewrite_a_past_snapshot(tmp_path: Path) -> None:
    history = [
        ("I", "P", "1", "2020-01-03T00:00:00", "1", "A"),
        ("I", "P", "2", "2020-01-03T00:00:00", "1", "A"),
        ("J", "P", "1", "2020-01-04T00:00:00", "1", "B"),
        ("OLD", "P", "1", "2019-01-01T00:00:00", "1", "A"),
        ("FEE", "SERVICE", "1", "2020-01-03T00:00:00", "1", "Z"),
    ]
    future = [
        ("I", "P", "1", "2020-02-10T00:00:00", "1", "A"),
        ("OLD", "P", "1", "2020-02-01T00:00:00", "1", "A"),
        ("NEW", "P", "1", "2020-02-01T00:00:00", "1", "B"),
        ("NEW", "P", "1", "2020-03-02T00:00:00", "1", "B"),
        ("I", "P", "1", "missing-date", "1", "A"),
    ]
    reference, extended = tmp_path / "before", tmp_path / "extended"
    run_clean(reference, history)
    clean = run_clean(extended, list(reversed(history + future)))
    # The full journal quarantines the collided invoices, including their original lines.
    assert "I" not in {
        json.loads(line)["invoice_id"]
        for line in (clean / "clean_events.jsonl").read_bytes().splitlines()
    }
    _, first, _ = snapshot(reference, "2020-03-05T00:00:00", tmp_path / "baseline")
    code, actual, manifest = snapshot(extended, "2020-03-05T00:00:00", tmp_path / "full")
    assert code == 2
    assert manifest["counts"]["undated_rows"] == 1
    assert manifest["counts"]["quarantined_target_invoices"] == 2
    assert (
        [(r["customer_id"], r["history_order_count"]) for r in first]
        == [(r["customer_id"], r["history_order_count"]) for r in actual]
        == [("A", 1), ("B", 1)]
    )
    assert [(r["label"], r["status"]) for r in actual] == [(0, "complete"), (1, "complete")]
    _, partial, _ = snapshot(extended, "2020-02-05T00:00:00", tmp_path / "partial")
    assert all(r["label"] is None and r["status"] == "censored" for r in partial)


def test_snapshot_matches_order_contract_on_consistent_invoices(tmp_path: Path) -> None:
    source = tmp_path / "source"
    clean = run_clean(
        source,
        [
            ("EXACT", "P", "1", "2020-01-25T00:00:00", "1", "A"),
            ("EARLY", "P", "1", "2020-01-24T23:59:59.9999999", "1", "X"),
            ("B", "Q", "1", "2020-01-31T23:59:59.9999999", "1", "B"),
            ("MISSING", "P", "1", "2020-01-26", "1", ""),
            ("SERVICE", "SERVICE", "1", "2020-01-27", "1", "X"),
            ("A2", "Q", "1", "2020-02-01T00:00:00", "1", "A"),
            ("A2", "Q", "2", "2020-02-01T00:00:00", "1", "A"),
            ("C1", "Q", "-1", "2020-02-02", "1", "B"),
            ("LATE", "P", "1", "2020-02-08", "1", "B"),
        ],
    )
    until = "2020-02-08T00:00:00"
    orders = read_clean_dataset(clean).aggregate().orders
    expected = build_snapshot(
        (o.to_temporal_order() for o in orders),
        snapshot_time=datetime(2020, 2, 1),
        observed_until=datetime.fromisoformat(until),
        horizon_days=7,
        lookback_days=7,
    )
    code, actual, _ = snapshot(
        source, until, tmp_path / "result", "--horizon-days", "7", "--lookback-days", "7"
    )
    assert code == 0
    assert [
        (r["customer_id"], r["history_order_count"], r["label"], r["label_end"]) for r in actual
    ] == [
        (r.customer_id, r.history_order_count, r.label, r.label_end.isoformat()) for r in expected
    ]
