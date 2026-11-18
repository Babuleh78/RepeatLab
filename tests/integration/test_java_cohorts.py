"""Run cohort analytics against completed orders using the packaged Java CLI."""

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any
from zipfile import ZipFile

import pytest
from test_java_analytics import analyze, records
from test_java_cleaning import run_clean
from test_java_ingestion import JAR, java_command

from repeatlab.orders import write_orders_dataset

FROM = "2020-01-01T00:00:00"
UNTIL = "2020-04-15T00:00:00"


def run_cohorts(
    orders: Path,
    output: Path,
    *,
    observed_from: str = FROM,
    observed_until: str = UNTIL,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            java_command(),
            "-jar",
            str(JAR),
            "cohorts",
            "--input",
            str(orders),
            "--observed-from",
            observed_from,
            "--observed-until",
            observed_until,
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=90,
        check=False,
    )


def document(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = json.loads(path.read_bytes())
    return result


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_completed_orders_feed_deterministic_cohort_artifacts(tmp_path: Path) -> None:
    clean = run_clean(
        tmp_path / "source",
        [
            ("A1", "P", "1", "2020-01-10T00:00:00", "1", "A"),
            ("A2", "P", "1", "2020-01-10T00:00:00", "1", "A"),
            ("A3", "P", "1", "2020-02-09T00:00:00", "1", "A"),
            ("A4", "P", "1", "2020-02-10T00:00:00", "1", "A"),
            ("SALE-C1", "P", "1", "2020-01-10T00:00:00", "1", "C"),
            ("CC1", "P", "-1", "2020-02-04T00:00:00", "1", "C"),
            ("B1", "P", "1", "2020-03-25T00:00:00", "1", "B"),
            ("B2", "P", "1", "2020-03-26T00:00:00", "1", "B"),
            ("ANON", "P", "1", FROM, "1", ""),
            ("BAD", "P", "1", FROM, "invalid", "BAD"),
        ],
    )
    analytics = tmp_path / "analytics"
    result = analyze(clean, analytics)
    assert result.returncode == 2, result.stderr
    orders = analytics / "orders"
    first, second = tmp_path / "first", tmp_path / "second"

    for output in (first, second):
        result = run_cohorts(orders, output)
        assert result.returncode == 2, result.stderr

    assert [
        (
            row["horizon_days"],
            row["total_customers"],
            row["eligible_customers"],
            row["censored_customers"],
            row["repeat_customers"],
            row["repeat_share"],
        )
        for row in records(first / "repeat_rates.jsonl")
    ] == [
        (30, 3, 2, 1, 0, {"numerator": 0, "denominator": 2}),
        (60, 3, 2, 1, 1, {"numerator": 1, "denominator": 2}),
        (90, 3, 2, 1, 1, {"numerator": 1, "denominator": 2}),
    ]
    assert [
        (
            row["cohort_month"],
            row["age_months"],
            row["activity_month"],
            row["cohort_customers"],
            row["status"],
            row["active_customers"],
            row["activity_share"],
        )
        for row in records(first / "cohort_activity.jsonl")
    ] == [
        ("2020-01", 0, "2020-01", 2, "complete", 2, {"numerator": 2, "denominator": 2}),
        ("2020-01", 1, "2020-02", 2, "complete", 1, {"numerator": 1, "denominator": 2}),
        ("2020-01", 2, "2020-03", 2, "complete", 0, {"numerator": 0, "denominator": 2}),
        ("2020-01", 3, "2020-04", 2, "censored", None, None),
        ("2020-03", 0, "2020-03", 1, "complete", 1, {"numerator": 1, "denominator": 1}),
        ("2020-03", 1, "2020-04", 1, "censored", None, None),
    ]

    manifest = document(first / "manifest.json")
    audit = document(first / "audit.json")
    order_manifest = document(orders / "manifest.json")
    assert manifest["schema"] == "repeatlab-cohorts"
    assert manifest["schema_version"] == 1
    assert manifest["policy_version"] == "cohorts-v1"
    assert manifest["status"] == "completed"
    assert manifest["data_status"] == audit["data_status"] == "quarantined"
    assert (
        manifest["observation"]
        == audit["observation"]
        == {
            "observed_from": FROM,
            "observed_until": UNTIL,
        }
    )
    assert manifest["input"] == {
        "orders_manifest_sha256": sha256(orders / "manifest.json"),
        "clean_manifest_sha256": sha256(clean / "manifest.json"),
    }
    assert audit["orders_counts"] == order_manifest["counts"]
    assert audit["source_clean_counts"] == order_manifest["source_clean_counts"]
    assert (audit["repeat_rates_rows"], audit["cohort_activity_rows"]) == (3, 6)
    assert manifest["engine"]["name"] == "DuckDB"
    assert manifest["artifacts"] == {
        name: sha256(first / name)
        for name in ("repeat_rates.jsonl", "cohort_activity.jsonl", "audit.json")
    }
    with ZipFile(JAR) as jar:
        assert manifest["sql_sha256"] == {
            name: hashlib.sha256(jar.read(f"sql/{name}")).hexdigest()
            for name in ("repeat_rates.sql", "cohort_activity.sql")
        }
    stable = {
        name: (first / name).read_bytes() for name in (*manifest["artifacts"], "manifest.json")
    }
    assert stable == {name: (second / name).read_bytes() for name in stable}
    assert not list((first / ".partial").glob("*"))
    assert (first / "run.json").is_file()

    result = run_cohorts(orders, first)
    assert result.returncode == 1, result.stderr
    assert stable == {name: (first / name).read_bytes() for name in stable}
    assert not (first / "fatal.json").exists()


def test_cohorts_reuse_exact_orders_after_monetary_sql_failure(tmp_path: Path) -> None:
    clean = run_clean(
        tmp_path / "source",
        [
            ("A1", "P", "1", FROM, "1E+38", "A"),
            ("A2", "P", "1", "2020-01-15T00:00:00", "1", "A"),
        ],
    )
    analytics = tmp_path / "monetary-sql"
    result = analyze(clean, analytics)
    assert result.returncode == 1, result.stderr
    assert document(analytics / "fatal.json")["stage"] == "sql"
    assert not (analytics / "manifest.json").exists()

    output = tmp_path / "cohorts"
    result = run_cohorts(analytics / "orders", output, observed_until="2020-04-01T00:00:00")
    assert result.returncode == 0, result.stderr
    assert [row["repeat_customers"] for row in records(output / "repeat_rates.jsonl")] == [1, 1, 1]
    assert [row["activity_month"] for row in records(output / "cohort_activity.jsonl")] == [
        "2020-01",
        "2020-02",
        "2020-03",
    ]


@pytest.mark.parametrize(
    "out_of_bounds",
    [
        ("ANON", "P", "1", UNTIL, "1", ""),
        ("CX", "P", "-1", "2019-12-31T23:59:59.999999", "1", "A"),
    ],
)
def test_all_orders_must_fit_declared_coverage(
    tmp_path: Path, out_of_bounds: tuple[str, str, str, str, str, str]
) -> None:
    clean = run_clean(tmp_path / "source", [("A1", "P", "1", FROM, "1", "A"), out_of_bounds])
    orders = tmp_path / "orders"
    write_orders_dataset(clean, orders)
    original = {path.name: sha256(path) for path in orders.iterdir()}
    output = tmp_path / "cohorts"

    result = run_cohorts(orders, output)
    assert result.returncode == 1, result.stderr
    assert document(output / "fatal.json")["stage"] == "observation_validation"
    assert not (output / "manifest.json").exists()
    assert original == {path.name: sha256(path) for path in orders.iterdir()}


@pytest.mark.parametrize(
    ("alias_for", "observed_until"),
    [
        (None, UNTIL),
        (None, "2020-01-10T00:00:00"),
        ("input", UNTIL),
        ("output_parent", UNTIL),
    ],
    ids=["direct", "direct-out-of-window", "input-alias", "output-parent-alias"],
)
def test_cohorts_reject_output_inside_orders(
    tmp_path: Path, alias_for: str | None, observed_until: str
) -> None:
    orders = tmp_path / "orders"
    output_parent = orders
    source = orders
    if alias_for is not None:
        output_parent = orders / "child"
        alias = tmp_path / "orders-alias"
        target = orders if alias_for == "input" else output_parent
        try:
            alias.symlink_to(target, target_is_directory=True)
        except OSError as error:
            if getattr(error, "winerror", None) in (5, 1314):
                pytest.skip("creating symlinks requires host permission")
            raise
        if alias_for == "input":
            source = alias
        else:
            output_parent = alias

    clean = run_clean(tmp_path / "source", [("A1", "P", "1", "2020-01-15T00:00:00", "1", "A")])
    write_orders_dataset(clean, orders)
    (orders / "child").mkdir()

    def snapshot() -> dict[Path, bytes | None]:
        return {
            path.relative_to(orders): path.read_bytes() if path.is_file() else None
            for path in orders.rglob("*")
        }

    original = snapshot()
    output = output_parent / "nested-result"
    result = run_cohorts(source, output, observed_until=observed_until)
    assert result.returncode == 1, result.stderr
    assert snapshot() == original
    assert not output.exists()
    assert json.loads(result.stderr.splitlines()[-1])["stage"] == "output_reservation"


def test_cohorts_missing_orders_writes_external_diagnostics(tmp_path: Path) -> None:
    orders = tmp_path / "missing-orders"
    output = tmp_path / "cohorts"

    result = run_cohorts(orders, output)
    assert result.returncode == 1, result.stderr
    assert document(output / "fatal.json")["stage"] == "orders_validation"
    assert not (output / "manifest.json").exists()
    assert not orders.exists()


@pytest.mark.parametrize(
    ("observed_from", "observed_until"),
    [
        (FROM, FROM),
        (FROM, UNTIL + "Z"),
        (FROM + ".0000001", UNTIL),
        (FROM, "2020-02-30T00:00:00"),
        ("0000-01-01T00:00:00", UNTIL),
    ],
)
def test_invalid_cli_bounds_leave_no_output(
    tmp_path: Path, observed_from: str, observed_until: str
) -> None:
    output = tmp_path / "cohorts"
    result = run_cohorts(
        tmp_path / "orders",
        output,
        observed_from=observed_from,
        observed_until=observed_until,
    )
    assert result.returncode == 64, result.stderr
    assert "cohorts --input" in result.stderr
    assert not output.exists()


def test_service_only_input_has_no_cohorts_and_modified_orders_fail(tmp_path: Path) -> None:
    clean = run_clean(tmp_path / "source", [("S", "SERVICE", "1", FROM, "1", "A")])
    orders = tmp_path / "orders"
    write_orders_dataset(clean, orders)
    output = tmp_path / "orders-cohorts"

    result = run_cohorts(orders, output)
    assert result.returncode == 0, result.stderr
    assert records(output / "cohort_activity.jsonl") == []
    counts = ("total_customers", "eligible_customers", "censored_customers", "repeat_customers")
    rates = records(output / "repeat_rates.jsonl")
    assert [row["horizon_days"] for row in rates] == [30, 60, 90]
    for row in rates:
        assert all(row[field] == 0 for field in counts)
        assert row["repeat_share"] is None

    (orders / "orders.jsonl").write_bytes(b"\n")
    corrupt_output = tmp_path / "corrupt"
    result = run_cohorts(orders, corrupt_output)
    assert result.returncode == 1, result.stderr
    assert document(corrupt_output / "fatal.json")["stage"] == "orders_validation"
    assert not (corrupt_output / "manifest.json").exists()
