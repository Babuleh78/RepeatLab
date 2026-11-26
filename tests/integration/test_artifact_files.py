"""Operator-selected inputs may use aliases, while named dataset members must be regular."""

import os
import subprocess
import sys
from pathlib import Path

import pytest
from test_java_analytics import analyze
from test_java_cleaning import run_clean
from test_java_cohorts import run_cohorts
from test_java_ingestion import JAR, java_command

from repeatlab.cleaning import read_clean_dataset
from repeatlab.orders import write_orders_dataset


def cli(*args: str) -> int:
    return subprocess.run(
        [java_command(), "-jar", str(JAR), *args], capture_output=True, check=False, timeout=30
    ).returncode


def test_cli_accepts_selected_aliases_and_rejects_linked_members(tmp_path: Path) -> None:
    clean = tmp_path / "clean"
    selected = tmp_path / "selected"
    try:
        selected.symlink_to(clean, target_is_directory=True)
    except OSError:
        pytest.skip("creating symlinks requires host permission")
    run_clean(tmp_path, [("I", "P", "1", "2020-02-01T00:00:00", "1", "A")])
    book = tmp_path / "selected.xlsx"
    book.symlink_to(tmp_path / "source.xlsx")
    raw = tmp_path / "second-raw"
    assert cli("ingest", "--input", str(book), "--output", str(raw)) == 0
    config = tmp_path / "selected-config.json"
    config.symlink_to(tmp_path / "config.json")
    assert (
        cli(
            "clean",
            "--input",
            str(raw),
            "--config",
            str(config),
            "--output",
            str(tmp_path / "from-alias"),
        )
        == 0
    )
    assert len(read_clean_dataset(selected).events) == 1
    orders = tmp_path / "orders"
    assert write_orders_dataset(selected, orders) == "ok"
    assert analyze(selected, tmp_path / "valid-analytics").returncode == 0

    for member in (raw / "raw_transactions.jsonl", clean / "audit.json", orders / "orders.jsonl"):
        target = member.rename(tmp_path / member.name)
        member.symlink_to(target)
    results = [tmp_path / name for name in ("failed-clean", "failed-analyze", "failed-cohorts")]
    assert (
        cli(
            "clean",
            "--input",
            str(raw),
            "--output",
            str(results[0]),
            "--config",
            str(tmp_path / "config.json"),
        )
        == 1
    )
    assert analyze(selected, results[1]).returncode == 1
    assert run_cohorts(orders, results[2]).returncode == 1
    for output in results:
        assert (output / "fatal.json").is_file()
        assert not (output / "manifest.json").exists()


def test_special_inputs_are_rejected_before_reading(tmp_path: Path) -> None:
    make_fifo = getattr(os, "mkfifo", None)
    if make_fifo is None:
        pytest.skip("POSIX named pipe")
    source = tmp_path / "clean"
    source.mkdir()
    pipe = source / "manifest.json"
    make_fifo(pipe)
    assert cli("ingest", "--input", str(pipe), "--output", str(tmp_path / "raw")) == 1
    assert analyze(source, tmp_path / "analytics").returncode == 1
    for module, destination in (("orders", "orders"), ("duplicate_sensitivity", "facts.jsonl")):
        output = tmp_path / destination
        result = subprocess.run(
            [
                sys.executable,
                "-I",
                "-m",
                f"repeatlab.{module}",
                "--input",
                str(source),
                "--output",
                str(output),
            ],
            capture_output=True,
            check=False,
            timeout=10,
        )
        assert result.returncode == 1
    assert not (tmp_path / "orders" / "manifest.json").exists()
    assert not (tmp_path / "facts.jsonl").exists()
