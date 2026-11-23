"""Exercise the sales profile using the packaged CLI and verified orders."""

import hashlib
import json
import subprocess
from pathlib import Path
from zipfile import ZipFile

import pytest
from test_java_analytics import records
from test_java_cleaning import run_clean
from test_java_cohorts import document, run_cohorts, sha256
from test_java_ingestion import JAR, java_command

from repeatlab.orders import write_orders_dataset

FROM = "2020-01-01T00:00:00"
UNTIL = "2020-04-01T00:00:00"
ARTIFACTS = ("purchase_intervals.jsonl", "sales_concentration.jsonl", "audit.json")


def run_sales_profile(
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
            "sales-profile",
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


def test_published_sales_profile_reuses_verified_orders_and_packaged_queries(
    tmp_path: Path,
) -> None:
    clean = run_clean(
        tmp_path / "source",
        [
            ("A1", "P", "1", FROM, "10", "A"),
            ("A2", "P", "1", FROM, "5", "A"),
            ("A3", "P", "1", "2020-01-04T00:00:00", "10", "A"),
            ("A4", "P", "1", "2020-01-09T00:00:00", "15", "A"),
            ("CA", "P", "-1", "2020-01-05T00:00:00", "1E+38", "A"),
            ("B", "P", "1", FROM, "20", "B"),
            ("D", "P", "1", FROM, "20", "D"),
            ("ANON", "P", "1", "2020-03-31T23:59:59.999999", "100", ""),
            ("BAD", "P", "1", FROM, "invalid", "BAD"),
            *((f"SALE-{i}", "P", "1", FROM, "1", f"Buyer-{i}") for i in range(22)),
        ],
    )
    orders = tmp_path / "orders"
    write_orders_dataset(clean, orders)
    first, second = tmp_path / "first", tmp_path / "second"

    result = run_sales_profile(orders, first, observed_from=FROM + ".000000")
    assert result.returncode == 2, result.stderr
    result = run_sales_profile(orders, second)
    assert result.returncode == 2, result.stderr

    intervals = records(first / "purchase_intervals.jsonl")
    assert [
        (
            row["segment"],
            row["interval_count"],
            row["customer_count"],
            row["min_gap_microseconds"],
            row["max_gap_microseconds"],
            [bucket["interval_count"] for bucket in row["buckets"]],
        )
        for row in intervals
    ] == [
        ("first", 1, 1, 259200000000, 259200000000, [0, 1, 0, 0, 0]),
        ("later", 1, 1, 432000000000, 432000000000, [0, 1, 0, 0, 0]),
    ]
    assert intervals[0]["buckets"] == [
        {"lower_microseconds": 0, "upper_microseconds": 86400000000, "interval_count": 0},
        {
            "lower_microseconds": 86400000000,
            "upper_microseconds": 604800000000,
            "interval_count": 1,
        },
        {
            "lower_microseconds": 604800000000,
            "upper_microseconds": 2592000000000,
            "interval_count": 0,
        },
        {
            "lower_microseconds": 2592000000000,
            "upper_microseconds": 7776000000000,
            "interval_count": 0,
        },
        {"lower_microseconds": 7776000000000, "upper_microseconds": None, "interval_count": 0},
    ]
    concentrations = records(first / "sales_concentration.jsonl")
    assert [
        (row["top_percent"], row["eligible_buyers"], row["top_buyers"], row["top_sales"])
        for row in concentrations
    ] == [(1, 25, 1, "40"), (5, 25, 2, "60"), (10, 25, 3, "80")]
    for row, top_sales in zip(concentrations, ("40", "60", "80"), strict=True):
        assert (row["known_sales"], row["all_sales"]) == ("102", "202")
        assert row["share_of_known_sales"] == {"numerator": top_sales, "denominator": "102"}
        assert row["share_of_all_sales"] == {"numerator": top_sales, "denominator": "202"}

    manifest = document(first / "manifest.json")
    audit = document(first / "audit.json")
    orders_manifest = document(orders / "manifest.json")
    for entry in [manifest, audit, *intervals, *concentrations]:
        assert (entry["schema"], entry["schema_version"]) == ("repeatlab-sales-profile", 1)
    assert (manifest["policy_version"], manifest["status"]) == ("sales-profile-v1", "completed")
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
    assert manifest["engine"]["name"] == "DuckDB" and manifest["engine"]["version"]
    assert manifest["decimal_scale"] == audit["decimal_scale"] == 0
    assert audit["orders_counts"] == orders_manifest["counts"]
    assert audit["source_clean_counts"] == orders_manifest["source_clean_counts"]
    assert (audit["purchase_intervals_rows"], audit["sales_concentration_rows"]) == (2, 3)
    assert manifest["artifacts"] == {name: sha256(first / name) for name in ARTIFACTS}
    with ZipFile(JAR) as jar:
        assert manifest["sql_sha256"] == {
            name: hashlib.sha256(jar.read(f"sql/{name}")).hexdigest()
            for name in ("purchase_intervals.sql", "sales_concentration.sql")
        }
    stable_files = (*ARTIFACTS, "manifest.json")
    assert {name: sha256(first / name) for name in stable_files} == {
        name: sha256(second / name) for name in stable_files
    }
    assert not list((first / ".partial").glob("*"))
    assert (first / "run.json").is_file()

    original = {path.name: path.read_bytes() for path in first.iterdir() if path.is_file()}
    result = run_sales_profile(orders, first)
    assert result.returncode == 1, result.stderr
    assert original == {path.name: path.read_bytes() for path in first.iterdir() if path.is_file()}


@pytest.mark.parametrize("anonymous", [False, True], ids=["empty", "anonymous-only"])
def test_sales_profile_with_no_eligible_buyers(tmp_path: Path, anonymous: bool) -> None:
    rows = [("SERVICE-ONLY", "SERVICE", "1", FROM, "50", "Buyer")]
    if anonymous:
        rows.append(("ANON", "P", "1", FROM, "0.335", ""))
    clean = run_clean(tmp_path / "source", rows)
    orders, output = tmp_path / "orders", tmp_path / "sales-profile"
    write_orders_dataset(clean, orders)

    result = run_sales_profile(orders, output)
    assert result.returncode == 0, result.stderr
    intervals = records(output / "purchase_intervals.jsonl")
    assert [row["segment"] for row in intervals] == ["first", "later"]
    for row in intervals:
        assert row["interval_count"] == row["customer_count"] == 0
        assert row["min_gap_microseconds"] is row["max_gap_microseconds"] is None
        assert [bucket["interval_count"] for bucket in row["buckets"]] == [0, 0, 0, 0, 0]

    concentrations = records(output / "sales_concentration.jsonl")
    assert [row["top_percent"] for row in concentrations] == [1, 5, 10]
    zero = "0.000" if anonymous else "0"
    for row in concentrations:
        assert row["eligible_buyers"] == row["top_buyers"] == 0
        assert row["top_sales"] == row["known_sales"] == zero
        assert row["share_of_known_sales"] is None
        assert row["all_sales"] == ("0.335" if anonymous else "0")
        assert row["share_of_all_sales"] == (
            {"numerator": "0.000", "denominator": "0.335"} if anonymous else None
        )


@pytest.mark.parametrize(
    ("observed_from", "observed_until"),
    [(FROM, FROM), (FROM, UNTIL + "Z"), (FROM + ".0000001", UNTIL), ("0000" + FROM[4:], UNTIL)],
)
def test_sales_profile_rejects_invalid_cli_bounds(
    tmp_path: Path, observed_from: str, observed_until: str
) -> None:
    output = tmp_path / "sales-profile"
    result = run_sales_profile(
        tmp_path / "missing-orders",
        output,
        observed_from=observed_from,
        observed_until=observed_until,
    )
    assert result.returncode == 64, result.stderr
    assert "sales-profile --input" in result.stderr
    assert not output.exists()


@pytest.mark.parametrize(
    "outside",
    [
        ("ANON", "P", "1", UNTIL, "1", ""),
        ("CA", "P", "-1", "2019-12-31T23:59:59.999999", "1", "A"),
    ],
    ids=["anonymous-at-end", "cancellation-before-start"],
)
def test_sales_profile_validates_coverage_of_every_order(
    tmp_path: Path, outside: tuple[str, str, str, str, str, str]
) -> None:
    clean = run_clean(tmp_path / "source", [("A", "P", "1", FROM, "1", "Buyer"), outside])
    orders, output = tmp_path / "orders", tmp_path / "sales-profile"
    write_orders_dataset(clean, orders)
    original = {path.name: sha256(path) for path in orders.iterdir()}

    result = run_sales_profile(orders, output)
    assert result.returncode == 1, result.stderr
    assert document(output / "fatal.json")["stage"] == "observation_validation"
    assert not (output / "manifest.json").exists()
    assert original == {path.name: sha256(path) for path in orders.iterdir()}


def test_sales_profile_rejects_corrupt_orders_lineage(tmp_path: Path) -> None:
    clean = run_clean(tmp_path / "source", [("A", "P", "1", FROM, "1", "Buyer")])
    orders, output = tmp_path / "orders", tmp_path / "sales-profile"
    write_orders_dataset(clean, orders)
    (orders / "lineage.jsonl").write_bytes(b"\n")

    result = run_sales_profile(orders, output)
    assert result.returncode == 1, result.stderr
    assert document(output / "fatal.json")["stage"] == "orders_validation"
    assert not (output / "manifest.json").exists()


@pytest.mark.parametrize("aliased", [False, True], ids=["direct", "aliased"])
def test_sales_profile_rejects_output_inside_input_orders(tmp_path: Path, aliased: bool) -> None:
    clean = run_clean(tmp_path / "source", [("A", "P", "1", FROM, "1", "Buyer")])
    orders = tmp_path / "orders"
    write_orders_dataset(clean, orders)
    output_parent = orders
    if aliased:
        output_parent = tmp_path / "orders-alias"
        try:
            output_parent.symlink_to(orders, target_is_directory=True)
        except OSError as error:
            if getattr(error, "winerror", None) in (5, 1314):
                pytest.skip("creating symlinks requires host permission")
            raise
    original = {path.name: sha256(path) for path in orders.iterdir()}
    output = output_parent / "nested-result"

    result = run_sales_profile(orders, output)
    assert result.returncode == 1, result.stderr
    assert json.loads(result.stderr.splitlines()[-1])["stage"] == "output_reservation"
    assert not output.exists()
    assert original == {path.name: sha256(path) for path in orders.iterdir()}


@pytest.mark.parametrize(
    "prices",
    [
        ("1E-39",),
        ("9" * 38, "1"),
    ],
    ids=["unrepresentable-scale", "overflowing-sum"],
)
def test_sales_profile_numeric_failure_preserves_orders_for_cohorts(
    tmp_path: Path, prices: tuple[str, ...]
) -> None:
    clean = run_clean(
        tmp_path / "source",
        [(f"A{i}", "P", "1", FROM, price, "Buyer") for i, price in enumerate(prices)],
    )
    orders, output = tmp_path / "orders", tmp_path / "sales-profile"
    write_orders_dataset(clean, orders)
    original = {path.name: sha256(path) for path in orders.iterdir()}

    result = run_sales_profile(orders, output)
    assert result.returncode == 1, result.stderr
    assert document(output / "fatal.json")["stage"] == "sql"
    assert not (output / "manifest.json").exists()
    assert original == {path.name: sha256(path) for path in orders.iterdir()}

    cohorts = run_cohorts(orders, tmp_path / "cohorts")
    assert cohorts.returncode == 0, cohorts.stderr
