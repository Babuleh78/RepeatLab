"""Installed Python worker and the actual embedded SQL engine, through the Java CLI."""

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from test_java_cleaning import run_clean
from test_java_ingestion import JAR, java_command


def analyze(clean: Path, output: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            java_command(),
            "-jar",
            str(JAR),
            "analyze",
            "--input",
            str(clean),
            "--python",
            sys.executable,
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=90,
        check=False,
    )


def records(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_exact_marts_quality_and_stable_hashes(tmp_path: Path) -> None:
    time = "2020-01-01T12:00:00.123456"
    clean = run_clean(
        tmp_path / "source",
        [
            ("I1", "P", "2", time, "0.1000", "A"),
            ("I1", "P", "2", time, "0.1000", "A"),
            ("I1", "Q", "1", time, "0.335", "A"),
            ("I1", "SERVICE", "1", time, "99", "A"),
            ("I2", "P", "1", time, "1.25", ""),
            ("CI1", "P", "-1", time, "0.1000", "A"),
            ("BAD", "P", "1", time, "invalid", "A"),
        ],
    )
    first, second = tmp_path / "first", tmp_path / "second"
    for output in (first, second):
        result = analyze(clean, output)
        assert result.returncode == 2, result.stderr
    order = next(row for row in records(first / "orders/orders.jsonl") if row["invoice_id"] == "I1")
    assert (order["line_count"], order["quantity"], order["stock_code_count"]) == (3, 5, 2)
    assert order["total_amount"] == "0.7350"
    assert len(records(first / "orders/lineage.jsonl")) == 6
    assert len(records(first / "orders/duplicates.jsonl")) == 1
    audit = json.loads((first / "audit.json").read_text(encoding="utf-8"))
    assert audit["source_clean_counts"]["quarantined_rows"] == 1
    monthly = records(first / "monthly_activity.jsonl")
    sale = next(row for row in monthly if row["kind"] == "sale")
    assert (sale["order_count"], sale["customer_count"], sale["product_line_count"]) == (2, 1, 4)
    assert (
        sale["total_amount"],
        sale["identified_total_amount"],
        sale["missing_customer_total_amount"],
    ) == ("1.985", "0.735", "1.250")
    assert next(row for row in monthly if row["kind"] == "cancellation")["total_amount"] == "-0.100"
    missing = next(
        row for row in records(first / "missing_customer.jsonl") if row["kind"] == "sale"
    )
    assert missing["order_share"] == {"numerator": 1, "denominator": 2}
    assert missing["line_share"] == {"numerator": 1, "denominator": 4}
    assert missing["amount_share"] == {"numerator": "1.250", "denominator": "1.985"}
    for name in (
        "manifest.json",
        "audit.json",
        "monthly_activity.jsonl",
        "missing_customer.jsonl",
        "orders/manifest.json",
    ):
        assert (first / name).read_bytes() == (second / name).read_bytes()
    before = hashlib.sha256((first / "manifest.json").read_bytes()).hexdigest()
    assert analyze(clean, first).returncode == 1
    assert hashlib.sha256((first / "manifest.json").read_bytes()).hexdigest() == before


@pytest.mark.parametrize(
    "amount", ["0.000000000000000000000000000000000000001", "1E+38", "1E+1000"]
)
def test_sql_numeric_failure_retains_exact_orders(tmp_path: Path, amount: str) -> None:
    clean = run_clean(tmp_path / "source", [("I", "P", "1", "2020-01-01T00:00:00", amount, "A")])
    output = tmp_path / "result"
    result = analyze(clean, output)
    assert result.returncode == 1, result.stderr
    assert (output / "orders/manifest.json").is_file()
    assert not (output / "manifest.json").exists()
    assert json.loads((output / "fatal.json").read_text())["stage"] == "sql"


def test_large_quantity_fitting_amount_unicode_and_service_only(tmp_path: Path) -> None:
    time = "2020-01-01T00:00:00"
    clean = run_clean(
        tmp_path / "source",
        [
            ("\ue000", "P", "100000000000000000000", time, "0.00000000000000000001", "A"),
            ("\U0001f600", "P", "1", time, "0.335", "B"),
            ("SERVICE", "SERVICE", "1", time, "99", "A"),
        ],
    )
    output = tmp_path / "result"
    result = analyze(clean, output)
    assert result.returncode == 0, result.stderr
    orders = records(output / "orders/orders.jsonl")
    assert [row["invoice_id"] for row in orders] == ["\ue000", "\U0001f600"]
    assert orders[0]["quantity"] == 100000000000000000000
    assert records(output / "monthly_activity.jsonl")[0]["total_amount"] == "1.335"


@pytest.mark.parametrize(
    "rows", [[], [("SERVICE", "SERVICE", "1", "2020-01-01T00:00:00", "99", "A")]]
)
def test_empty_accepted_product_subset(
    tmp_path: Path, rows: list[tuple[str, str, str, str, str, str]]
) -> None:
    clean = run_clean(tmp_path / "source", rows)
    output = tmp_path / "result"
    result = analyze(clean, output)
    assert result.returncode == 0, result.stderr
    assert (output / "monthly_activity.jsonl").read_bytes() == b""
    assert (output / "missing_customer.jsonl").read_bytes() == b""


@pytest.mark.parametrize(
    "mode", ["no_manifest", "nonzero", "exit_mismatch", "changed_snapshot", "publication_io"]
)
def test_worker_protocol_cannot_publish_false_completion(tmp_path: Path, mode: str) -> None:
    clean = run_clean(tmp_path / "source", [("I", "P", "1", "2020-01-01T00:00:00", "0.335", "A")])
    reference = tmp_path / "reference"
    result = analyze(clean, reference)
    assert result.returncode == 0, result.stderr
    fake = tmp_path / "fake"
    package = fake / "repeatlab"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    # The real child imports from its explicit working directory to test the protocol boundary.
    script = (
        "import pathlib, shutil, sys\n"
        "print('o' * 1100000)\nprint('e' * 1100000, file=sys.stderr)\n"
        f"mode = {mode!r}\n"
        "output = pathlib.Path(sys.argv[sys.argv.index('--output') + 1])\n"
        "source = pathlib.Path(sys.argv[sys.argv.index('--input') + 1])\n"
        f"template = pathlib.Path({str(reference / 'orders')!r})\n"
        "if mode in ('exit_mismatch', 'changed_snapshot', 'publication_io'):\n"
        "    shutil.copytree(template, output)\n"
        "if mode == 'publication_io':\n"
        "    (output.parent / 'monthly_activity.jsonl').mkdir()\n"
        "if mode == 'changed_snapshot':\n"
        "    (source / 'clean_events.jsonl').write_text('changed')\n"
        "sys.exit(7 if mode == 'nonzero' else 2 if mode == 'exit_mismatch' else 0)\n"
    )
    (package / "orders.py").write_text(script, encoding="utf-8")
    output = tmp_path / "result"
    result = subprocess.run(
        [
            java_command(),
            "-jar",
            str(JAR),
            "analyze",
            "--input",
            str(clean),
            "--python",
            sys.executable,
            "--output",
            str(output),
        ],
        cwd=fake,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=60,
        check=False,
    )
    assert result.returncode == 1, result.stderr
    assert not (output / "manifest.json").exists()
    assert (output / "fatal.json").is_file()
    assert (output / "worker.stdout.log").stat().st_size > 1_000_000
    assert (output / "worker.stderr.log").stat().st_size > 1_000_000
    if mode == "publication_io":
        assert (output / "orders/manifest.json").is_file()
        assert json.loads((output / "fatal.json").read_text())["stage"] == "publication"


def test_grouped_sum_overflow_retains_orders(tmp_path: Path) -> None:
    amount = "99999999999999999999999999999999999999"
    clean = run_clean(
        tmp_path / "source",
        [
            ("I", "P", "1", "2020-01-01T00:00:00", amount, "A"),
            ("J", "P", "1", "2020-01-01T00:00:00", amount, "B"),
        ],
    )
    output = tmp_path / "result"
    result = analyze(clean, output)
    assert result.returncode == 1, result.stderr
    assert (output / "orders/manifest.json").is_file()
    assert not (output / "manifest.json").exists()
    assert json.loads((output / "fatal.json").read_text())["stage"] == "sql"
