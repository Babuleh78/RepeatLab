"""Invoke the installed worker through its real module entry point."""

import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import pytest
from test_java_cleaning import run_clean


def run_worker(source: Path, output: Path, cwd: Path) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [
            sys.executable,
            "-I",
            "-m",
            "repeatlab.orders",
            "--input",
            str(source),
            "--output",
            str(output),
        ],
        cwd=cwd,
        capture_output=True,
        timeout=60,
        check=False,
    )


@pytest.mark.parametrize(
    "customer,bad,expected_exit,expected_quality",
    [("A", False, 0, "ok"), ("", False, 0, "warnings"), ("A", True, 2, "quarantined")],
)
def test_installed_worker_inherits_quality_and_exit_status(
    tmp_path: Path, customer: str, bad: bool, expected_exit: int, expected_quality: str
) -> None:
    rows = [("I", "P", "1", "2020-01-01T00:00:00", "0.335", customer)]
    if bad:
        rows.append(("BAD", "P", "0", "2020-01-01T00:00:00", "1", "A"))
    source = run_clean(tmp_path, rows)
    output = tmp_path / "orders"
    process = run_worker(source, output, tmp_path)
    assert process.returncode == expected_exit, process.stderr
    assert json.loads((output / "manifest.json").read_bytes())["data_status"] == expected_quality
    order = json.loads((output / "orders.jsonl").read_bytes())
    assert order["total_amount"] == "0.335"
    assert order["customer_id"] == (customer or None)
    assert order["line_count"] == 1


def test_installed_worker_accepts_huge_quantity_with_fitting_total(tmp_path: Path) -> None:
    quantity = "1" + "0" * 4400
    source = run_clean(
        tmp_path,
        [("\U0001f600", "P", quantity, "2020-01-01T00:00:00", "1E-4400", "Å")],
    )
    output = tmp_path / "orders"
    process = run_worker(source, output, tmp_path)
    assert process.returncode == 0, process.stderr
    payload = (output / "orders.jsonl").read_bytes()
    assert b'"quantity":' + quantity.encode() in payload
    order = json.loads(payload, parse_int=lambda value: int(Decimal(value)))
    assert order["quantity"] == 10**4400
    assert order["invoice_id"] == "\U0001f600"
    assert order["customer_id"] == "Å"
    assert Decimal(order["total_amount"]) == Decimal(1)


@pytest.mark.parametrize("arguments", [[], ["--input", "missing"], ["--unknown"], ["--in", "x"]])
def test_installed_worker_invalid_arguments_exit_64(tmp_path: Path, arguments: list[str]) -> None:
    process = subprocess.run(
        [sys.executable, "-I", "-m", "repeatlab.orders", *arguments],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=30,
        check=False,
    )
    assert process.returncode == 64, process.stderr
    assert not list(tmp_path.iterdir())


def test_installed_worker_invalid_input_exits_1_with_fatal_diagnostics(tmp_path: Path) -> None:
    source, output = tmp_path / "missing", tmp_path / "orders"
    process = run_worker(source, output, tmp_path)
    assert process.returncode == 1
    assert process.stderr
    assert not (output / "manifest.json").exists()
    assert json.loads((output / "fatal.json").read_bytes())["stage"] == "snapshot_clean"


def test_installed_worker_preserves_existing_output_on_error(tmp_path: Path) -> None:
    output = tmp_path / "orders"
    output.mkdir()
    (output / "keep").write_bytes(b"preserve")
    process = run_worker(tmp_path / "missing", output, tmp_path)
    assert process.returncode == 1
    assert set(item.name for item in output.iterdir()) == {"keep"}
    assert (output / "keep").read_bytes() == b"preserve"
