"""Exercise the two exact order scenarios on classified source rows."""

import os
import stat
from dataclasses import replace
from decimal import Decimal, localcontext
from hashlib import sha256
from pathlib import Path

import pytest
from test_orders import clean_dataset, records

from repeatlab import duplicate_sensitivity
from repeatlab.cleaning import read_clean_dataset
from repeatlab.duplicate_sensitivity import main


def run_worker(source: Path, output: Path) -> int:
    return main(["--input", str(source), "--output", str(output)])


def test_scenarios_preserve_orders_and_collapse_only_equal_contents(tmp_path: Path) -> None:
    source, output = tmp_path / "clean", tmp_path / "facts.jsonl"
    clean_dataset(
        source,
        [
            ("I", "007", "P", 2, "10.00"),
            ("I", "007", "P", 2, "10.0"),
            ("I", "007", "P", 3, "10.00"),
            ("I", "007", "Q", 2, "10.00"),
            ("I", "007", "P", 2, "10.01"),
            ("I", "007", "SERVICE", 3, "78"),
            ("I", "007", "SERVICE", 3, "78"),
            ("OTHER", "007", "P", 2, "10.00"),
            ("C1", None, "Q", -3, "2.55"),
            ("C1", None, "Q", -3, "2.55"),
            ("S", "007", "SERVICE", 1, "1"),
        ],
        quarantine=True,
    )
    source_bytes = {p.name: p.read_bytes() for p in source.iterdir()}
    with localcontext() as context:
        context.prec = 2
        assert run_worker(source, output) == 2
    if os.name == "posix":
        assert stat.S_IMODE(output.stat().st_mode) == 0o600
    assert {p.name: p.read_bytes() for p in source.iterdir()} == source_bytes
    rows = records(output)
    assert [r["order_key"] for r in rows] == ["C1", "I", "OTHER"]
    assert rows[0] == {
        "order_key": "C1",
        "event_time": "2020-01-01T12:34:56.123456",
        "kind": "cancellation",
        "customer_known": False,
        "retained": {"lines": 2, "units": "-6", "amount": "-15.30"},
        "collapsed": {"lines": 1, "units": "-3", "amount": "-7.65"},
    }
    assert rows[1]["retained"] == {"lines": 5, "units": "11", "amount": "110.02"}
    assert rows[1]["collapsed"] == {"lines": 4, "units": "9", "amount": "90.02"}
    assert rows[1]["customer_known"] is True and rows[1]["kind"] == "sale"
    assert (
        rows[2]["retained"]
        == rows[2]["collapsed"]
        == {
            "lines": 1,
            "units": "2",
            "amount": "20.00",
        }
    )
    repeat = tmp_path / "second.jsonl"
    assert run_worker(source, repeat) == 2
    assert repeat.read_bytes() == output.read_bytes()


@pytest.mark.parametrize("quarantine", [True, False])
def test_empty_output_inherits_input_quality(
    tmp_path: Path, quarantine: bool, capsys: pytest.CaptureFixture[str]
) -> None:
    source, output = tmp_path / "clean", tmp_path / "facts.jsonl"
    clean_dataset(source, [], quarantine=quarantine)
    assert run_worker(source, output) == (2 if quarantine else 0)
    assert output.read_bytes() == b""
    assert capsys.readouterr().out == expected_receipt(source, output)


def expected_receipt(source: Path, output: Path) -> str:
    manifest_hash = sha256((source / "manifest.json").read_bytes()).hexdigest()
    facts_hash = sha256(output.read_bytes()).hexdigest()
    return (
        f'{{"protocol_version":1,"clean_manifest_sha256":"{manifest_hash}",'
        f'"facts_sha256":"{facts_hash}"}}\n'
    )


def test_large_quantities_and_unicode_order_keys(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, output = tmp_path / "clean", tmp_path / "facts.jsonl"
    units = "9" * 4500
    clean_dataset(
        source,
        [
            ("\U0001f4e6", None, "A", units, "0.01"),
            ("\U0001f4e6", None, "A", units, "0.01"),
            ("\ue000", "A", "B", 1, "1"),
        ],
    )
    assert run_worker(source, output) == 0
    assert capsys.readouterr().out == expected_receipt(source, output)
    rows = records(output)
    assert [r["order_key"] for r in rows] == ["\ue000", "\U0001f4e6"]
    assert rows[1]["retained"]["units"] == "1" + "9" * 4499 + "8"
    assert rows[1]["collapsed"]["units"] == units
    assert Decimal(rows[1]["collapsed"]["amount"]) == Decimal(units[:-2] + ".99")


def test_invalid_input_does_not_create_facts_and_existing_output_is_preserved(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source, output = tmp_path / "clean", tmp_path / "facts.jsonl"
    clean_dataset(source, [("I", "A", "P", 1, "10")])
    output.write_bytes(b"existing")
    assert run_worker(source, output) == 1
    assert output.read_bytes() == b"existing"
    (source / "audit.json").write_bytes(b"{}")
    other = tmp_path / "other.jsonl"
    assert run_worker(source, other) == 1
    assert not other.exists()
    assert main(["--input", str(source)]) == 64
    assert capsys.readouterr().out == ""


def test_misaligned_scenario_fails_before_creating_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, output = tmp_path / "clean", tmp_path / "facts.jsonl"
    clean_dataset(source, [("I", "A", "P", 1, "1"), ("I", "A", "P", 1, "1")])
    collapsed = read_clean_dataset(source).aggregate()
    wrong_order = replace(collapsed.orders[0], customer_id="B")
    monkeypatch.setattr(
        duplicate_sensitivity,
        "aggregate_orders",
        lambda *args, **kwargs: replace(collapsed, orders=(wrong_order,)),
    )
    assert run_worker(source, output) == 1
    assert not output.exists()


def test_changed_manifest_cannot_receive_a_success_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, output = tmp_path / "clean", tmp_path / "facts.jsonl"
    clean_dataset(source, [("I", "A", "P", 1, "10")])
    write_facts = duplicate_sensitivity.write_facts

    def change_manifest_after_writing(clean_path: Path, output_path: Path) -> str:
        status = write_facts(clean_path, output_path)
        with (clean_path / "manifest.json").open("ab") as stream:
            stream.write(b"\n")
        return status

    monkeypatch.setattr(duplicate_sensitivity, "write_facts", change_manifest_after_writing)
    assert run_worker(source, output) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "clean manifest changed" in captured.err
    assert not output.exists()
