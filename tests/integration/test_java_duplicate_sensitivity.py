"""Exercise the packaged CLI, installed worker and monthly comparisons together."""

import hashlib
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any
from zipfile import ZipFile

import pytest
from test_java_analytics import install_test_worker, records
from test_java_cleaning import run_clean
from test_java_cohorts import document, sha256
from test_java_ingestion import JAR, ROOT, java_command

JAN = "2020-01-31T23:59:59.999999"
FEB = "2020-02-01T00:00:00"
REPORT = "duplicate_sensitivity.jsonl"


def run_comparison(
    clean: Path, output: Path, *, python: str = sys.executable
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            java_command(),
            "-jar",
            str(JAR),
            "duplicate-sensitivity",
            "--input",
            str(clean),
            "--python",
            python,
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=90,
        check=False,
    )


def measures(row: dict[str, Any]) -> tuple[int, int, int, Decimal]:
    assert isinstance(row["units"], str) and isinstance(row["amount"], str)
    return row["order_count"], row["product_line_count"], int(row["units"]), Decimal(row["amount"])


def test_monthly_comparison_preserves_clean_and_binds_reproducible_outputs(tmp_path: Path) -> None:
    clean = run_clean(
        tmp_path / "source",
        [
            ("S0", "P", "1", JAN, "10.0", "A"),
            ("S0", "P", "1", JAN, "10.00", "A"),
            ("S0", "Q", "1", JAN, "5", "A"),
            ("S1", "P", "1", JAN, "10.00", "A"),
            ("S2", "P", "3", JAN, "2", ""),
            ("S2", "P", "3", JAN, "2", ""),
            ("CK", "X", "-2", JAN, "0.335", "A"),
            ("CK", "X", "-2", JAN, "0.335", "A"),
            ("CF", "X", "-1", FEB, "1", ""),
            ("S0", "SERVICE", "5", JAN, "8", "A"),
            ("S0", "SERVICE", "5", JAN, "8", "A"),
            ("BAD", "P", "0", FEB, "3", "A"),
        ],
    )
    original = {p.name: sha256(p) for p in clean.iterdir() if p.is_file()}
    first, second = tmp_path / "first", tmp_path / "second"
    for output in (first, second):
        result = run_comparison(clean, output)
        assert result.returncode == 2, result.stderr

    rows = records(first / REPORT)
    expected = {
        ("2020-01", "sale", True): ((2, 4, 4, 35), (2, 3, 3, 25)),
        ("2020-01", "sale", False): ((1, 2, 6, 12), (1, 1, 3, 6)),
        ("2020-01", "cancellation", True): (
            (1, 2, -4, Decimal("-1.340")),
            (1, 1, -2, Decimal("-0.670")),
        ),
        ("2020-02", "cancellation", False): ((1, 1, -1, -1), (1, 1, -1, -1)),
    }
    assert len(rows) == len(expected)
    assert {
        (r["month"], r["kind"], r["customer_known"]): (
            measures(r["retained"]),
            measures(r["collapsed"]),
        )
        for r in rows
    } == expected
    manifest = document(first / "manifest.json")
    audit = document(first / "audit.json")
    for row in [*rows, audit, manifest]:
        assert (row["schema"], row["schema_version"]) == ("repeatlab-duplicate-sensitivity", 1)
    assert (manifest["policy_version"], manifest["status"]) == (
        "duplicate-sensitivity-v1",
        "completed",
    )
    assert manifest["data_status"] == audit["data_status"] == "quarantined"
    assert audit["source_clean_counts"] == document(clean / "manifest.json")["counts"]
    assert (audit["orders"], audit["retained_product_rows"], audit["collapsed_product_rows"]) == (
        5,
        9,
        6,
    )
    assert audit["duplicate_sensitivity_rows"] == len(rows)
    assert manifest["input"]["clean_manifest_sha256"] == original["manifest.json"]
    assert manifest["engine"]["name"] == "DuckDB" and manifest["engine"]["version"]
    assert manifest["artifacts"] == {name: sha256(first / name) for name in (REPORT, "audit.json")}
    with ZipFile(JAR) as jar:
        sql = jar.read("sql/duplicate_sensitivity.sql")
        assert sql == (ROOT / "sql/duplicate_sensitivity.sql").read_bytes()
    assert manifest["sql_sha256"] == {"duplicate_sensitivity.sql": hashlib.sha256(sql).hexdigest()}
    for name in (*manifest["artifacts"], "manifest.json"):
        assert (first / name).read_bytes() == (second / name).read_bytes()
    assert not list((first / ".partial").iterdir())
    assert {p.name: sha256(p) for p in clean.iterdir() if p.is_file()} == original
    assert run_comparison(clean, first).returncode == 1
    assert sha256(first / "manifest.json") == sha256(second / "manifest.json")


def test_all_service_rows_produce_an_empty_completed_report(tmp_path: Path) -> None:
    clean = run_clean(tmp_path / "source", [("S0", "SERVICE", "1", JAN, "10", "A")] * 2)
    output = tmp_path / "out"
    result = run_comparison(clean, output)
    assert result.returncode == 0, result.stderr
    assert (output / REPORT).read_bytes() == b""
    audit = document(output / "audit.json")
    assert audit["orders"] == audit["retained_product_rows"] == audit["collapsed_product_rows"] == 0


@pytest.mark.parametrize("price", ["1E-39", "1E+38"])
def test_money_outside_sql_range_leaves_clean_available(tmp_path: Path, price: str) -> None:
    clean = run_clean(tmp_path / "source", [("S0", "P", "1", JAN, price, "A")])
    output = tmp_path / "out"
    digest = sha256(clean / "clean_events.jsonl")
    assert run_comparison(clean, output).returncode == 1
    assert (output / "fatal.json").is_file()
    assert not (output / "manifest.json").exists()
    assert sha256(clean / "clean_events.jsonl") == digest


def test_units_beyond_38_digits_remain_exact(tmp_path: Path) -> None:
    units = 10**38
    clean = run_clean(tmp_path / "source", [("S0", "P", str(units), JAN, "1e-38", "A")] * 2)
    output = tmp_path / "out"
    result = run_comparison(clean, output)
    assert result.returncode == 0, result.stderr
    [row] = records(output / REPORT)
    assert measures(row["retained"]) == (1, 2, 2 * units, Decimal(2))
    assert measures(row["collapsed"]) == (1, 1, units, Decimal(1))


def test_corrupt_input_and_output_overlap_are_rejected(tmp_path: Path) -> None:
    clean = run_clean(tmp_path / "source", [("S0", "P", "1", JAN, "2", "A")])
    nested = clean / "out"
    assert run_comparison(clean, nested).returncode == 1
    assert not nested.exists()
    missing = tmp_path / "missing"
    assert run_comparison(missing, missing).returncode == 1
    assert not missing.exists()
    (clean / "clean_events.jsonl").write_bytes(b"")
    output = tmp_path / "out"
    assert run_comparison(clean, output).returncode == 1
    assert (output / "fatal.json").is_file()
    assert not (output / "manifest.json").exists()


@pytest.mark.parametrize(
    ("worker_script", "stage"),
    [
        ("raise SystemExit(2)", "worker"),
        ("(clean / 'clean_events.jsonl').write_bytes(b'')", "worker"),
        ("facts.write_bytes(b'')", "sql"),
    ],
    ids=["wrong-quality", "changed-snapshot", "dropped-order"],
)
def test_faulty_worker_cannot_publish_a_completed_report(
    tmp_path: Path, worker_script: str, stage: str
) -> None:
    clean = run_clean(tmp_path / "source", [("I", "P", "1", JAN, "1", "A")])
    original = sha256(clean / "clean_events.jsonl")
    script = (
        "import sys\nfrom pathlib import Path\n"
        "clean = Path(sys.argv[sys.argv.index('--input') + 1])\n"
        "facts = Path(sys.argv[sys.argv.index('--output') + 1])\n" + worker_script
    )
    interpreter = install_test_worker(tmp_path / "worker", script, module="duplicate_sensitivity")
    output = tmp_path / "out"
    result = run_comparison(clean, output, python=str(interpreter))
    assert result.returncode == 1, result.stderr
    assert document(output / "fatal.json")["stage"] == stage
    assert not (output / "manifest.json").exists()
    assert sha256(clean / "clean_events.jsonl") == original
