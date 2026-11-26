"""Installed Python worker and the actual embedded SQL engine, through the Java CLI."""

import hashlib
import json
import os
import subprocess
import sys
import venv
from pathlib import Path
from typing import Any

import pytest
from test_java_cleaning import run_clean
from test_java_ingestion import JAR, java_command

from repeatlab.cleaning import read_clean_dataset


def analyze(
    clean: Path,
    output: Path,
    *,
    python: Path | None = None,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            java_command(),
            "-jar",
            str(JAR),
            "analyze",
            "--input",
            str(clean),
            "--python",
            str(python) if python is not None else sys.executable,
            "--output",
            str(output),
        ],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=90,
        check=False,
    )


def records(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


@pytest.mark.parametrize(
    ("command", "overlap"),
    [
        (cmd, case)
        for cmd in ("analyze", "clean", "ingest")
        for case in ("equal", "ancestor", "nested")
    ]
    + [("clean", "config")],
)
def test_output_reservation_preserves_source_paths(
    tmp_path: Path, command: str, overlap: str
) -> None:
    output = tmp_path / "result"
    source = output if overlap == "equal" else output / "input"
    config = tmp_path / "policy.json"
    if overlap == "nested":
        source = tmp_path / "input"
        source.mkdir()
        output = source / "result"
    if overlap == "config":
        source, config = tmp_path / "input", output
    extra = ["--config", str(config)] if command == "clean" else ["--python", sys.executable]
    if command == "ingest":
        extra = []
    result = subprocess.run(
        [
            java_command(),
            "-jar",
            str(JAR),
            command,
            "--input",
            str(source),
            *extra,
            "--output",
            str(output),
        ],
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 1
    assert b"overlap" in result.stderr
    assert not output.exists()


@pytest.mark.parametrize("shadow_source", ["cwd", "pythonpath"])
def test_analyze_uses_installed_worker_despite_caller_import_paths(
    tmp_path: Path, shadow_source: str
) -> None:
    clean = run_clean(tmp_path / "source", [("I", "P", "1", "2020-01-01T00:00:00", "0.335", "A")])
    shadow = tmp_path / "shadow"
    package = shadow / "repeatlab"
    package.mkdir(parents=True)
    marker = tmp_path / "shadow-imported"
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "orders.py").write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).touch()\nraise SystemExit(73)\n",
        encoding="utf-8",
    )
    output = tmp_path / "result"
    result = analyze(
        clean,
        output,
        cwd=shadow if shadow_source == "cwd" else tmp_path,
        env={**os.environ, "PYTHONPATH": str(shadow)} if shadow_source == "pythonpath" else None,
    )
    assert result.returncode == 0, result.stderr
    assert not marker.exists()
    assert json.loads((output / "manifest.json").read_bytes())["status"] == "completed"
    assert records(output / "monthly_activity.jsonl")[0]["total_amount"] == "0.335"


@pytest.mark.parametrize("identifier", ["\u001c", "\u001d", "\u001e", "\u001f"])
@pytest.mark.parametrize("field", ["invoice_id", "customer_id"])
def test_analyze_preserves_control_identifiers_accepted_by_clean_policy(
    tmp_path: Path, identifier: str, field: str
) -> None:
    clean = run_clean(tmp_path / "source", [("I", "P", "1", "2020-01-01T00:00:00", "0.335", "A")])
    # These literal control IDs are valid in the JSON exchange, but not in an XML1.0 workbook.
    event = records(clean / "clean_events.jsonl")[0]
    event[field] = identifier
    (clean / "clean_events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")
    if field == "invoice_id":
        disposition = records(clean / "dispositions.jsonl")[0]
        disposition["invoice_id"] = identifier
        (clean / "dispositions.jsonl").write_text(json.dumps(disposition) + "\n", encoding="utf-8")
    manifest = json.loads((clean / "manifest.json").read_bytes())
    manifest["artifacts"] = {
        name: hashlib.sha256((clean / name).read_bytes()).hexdigest()
        for name in manifest["artifacts"]
    }
    (clean / "manifest.json").write_text(json.dumps(manifest) + "\n", encoding="utf-8")
    assert getattr(read_clean_dataset(clean).events[0], field) == identifier
    output = tmp_path / "result"
    result = analyze(clean, output)
    assert result.returncode == 0, result.stderr
    assert records(output / "orders/orders.jsonl")[0][field] == identifier
    monthly = records(output / "monthly_activity.jsonl")[0]
    assert (monthly["customer_count"], monthly["order_count"], monthly["total_amount"]) == (
        1,
        1,
        "0.335",
    )


def install_test_worker(environment: Path, script: str, *, module: str = "orders") -> Path:
    """Install a controlled stdlib-only package into a disposable interpreter's site-packages."""
    venv.EnvBuilder(with_pip=False).create(environment)
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    probe = subprocess.run(
        [str(python), "-I", "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=30,
        check=True,
    )
    site = Path(probe.stdout.strip()).resolve()
    assert site.is_relative_to(environment.resolve())
    package = site / "repeatlab"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / f"{module}.py").write_text(script, encoding="utf-8")
    return python


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
    # Select a disposable interpreter with a test-only installed worker at the process boundary.
    script = (
        "import pathlib, shutil, sys\n"
        "print('o' * 3000000)\nprint('e' * 3000000, file=sys.stderr)\n"
        f"mode = {mode!r}\n"
        "output = pathlib.Path(sys.argv[sys.argv.index('--output') + 1])\n"
        "source = pathlib.Path(sys.argv[sys.argv.index('--input') + 1])\n"
        f"template = pathlib.Path({str(reference / 'orders')!r})\n"
        "if mode in ('exit_mismatch', 'changed_snapshot', 'publication_io'):\n"
        "    shutil.copytree(template, output)\n"
        "if mode == 'publication_io':\n"
        "    (output.parent.parent / 'monthly_activity.jsonl').mkdir()\n"
        "if mode == 'changed_snapshot':\n"
        "    (source / 'clean_events.jsonl').write_text('changed')\n"
        "sys.exit(7 if mode == 'nonzero' else 2 if mode == 'exit_mismatch' else 0)\n"
    )
    python = install_test_worker(tmp_path / "worker-venv", script)
    output = tmp_path / "result"
    result = analyze(clean, output, python=python, cwd=tmp_path)
    assert result.returncode == 1, result.stderr
    assert not (output / "manifest.json").exists()
    assert (output / "fatal.json").is_file()
    for log_name in ("worker.stdout.log", "worker.stderr.log"):
        diagnostic = (output / log_name).read_bytes()
        assert 2 * 1024**2 < len(diagnostic) < 2 * 1024**2 + 64
        assert diagnostic.endswith(b"[worker log truncated]\n")
    if mode == "publication_io":
        assert (output / "orders/manifest.json").is_file()
        assert json.loads((output / "fatal.json").read_text())["stage"] == "publication"
    else:
        assert not (output / "orders").exists()
    assert not (output / ".partial").exists()


def test_worker_timeout_aborts_before_orders_publication(tmp_path: Path) -> None:
    clean = run_clean(tmp_path / "source", [])
    python = install_test_worker(tmp_path / "worker", "import time\ntime.sleep(120)\n")
    output = tmp_path / "result"
    env = dict(os.environ)
    env["JAVA_TOOL_OPTIONS"] = (
        env.get("JAVA_TOOL_OPTIONS", "") + " -Drepeatlab.worker.timeoutSeconds=1"
    )
    result = analyze(clean, output, python=python, env=env)
    assert result.returncode == 1
    fatal = json.loads((output / "fatal.json").read_bytes())
    assert fatal["stage"] == "worker" and "timeout" in fatal["message"]
    assert not (output / "orders").exists()
    assert not (output / "manifest.json").exists()


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
