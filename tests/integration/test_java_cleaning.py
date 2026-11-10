"""Synthetic workbook → executable clean stage → verified Python consumer."""

import importlib
import json
import subprocess
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from xml.sax.saxutils import escape
from zipfile import ZIP_DEFLATED, ZipFile

from test_java_ingestion import JAR, java_command, workbook

from repeatlab.temporal import build_snapshot


def synthetic(path: Path, rows: list[tuple[str, str, str, str, str, str]]) -> Path:
    """Each tuple is invoice, product, quantity, ISO time, price, customer."""
    workbook(path)
    with ZipFile(path) as archive:
        parts = {name: archive.read(name) for name in archive.namelist()}
    name = "xl/worksheets/sheet1.xml"
    original = parts[name].decode()
    header = original[original.index('<row r="1">') : original.index("</row>") + 6]
    output = [header]
    for index, (invoice, stock, quantity, date, price, customer) in enumerate(rows, 2):
        values = [invoice, stock, "item", quantity, date, price, customer, "Testland"]
        cells = []
        for column, value in enumerate(values):
            ref = f"{chr(65 + column)}{index}"
            if column in (3, 5):
                cells.append(f'<c r="{ref}"><v>{escape(value)}</v></c>')
            else:
                cells.append(
                    f'<c r="{ref}" t="inlineStr"><is><t xml:space="preserve">'
                    f"{escape(value)}</t></is></c>"
                )
        output.append(f'<row r="{index}">{"".join(cells)}</row>')
    parts[name] = (
        original[: original.index("<sheetData>") + 11]
        + "".join(output)
        + "</sheetData></worksheet>"
    ).encode()
    with ZipFile(path, "w", ZIP_DEFLATED) as archive:
        for part, content in parts.items():
            archive.writestr(part, content)
    return path


def run_clean(tmp_path: Path, rows: list[tuple[str, str, str, str, str, str]]) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = synthetic(tmp_path / "source.xlsx", rows)
    raw = tmp_path / "raw"
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "policy_version": "clean-v1",
                "service_codes": ["SERVICE", "\U0001f600", "\ue000", "\x1b", "\\u001B"],
            }
        ),
        encoding="utf-8",
    )
    clean = tmp_path / "clean"
    for args in (
        ["ingest", "--input", str(source), "--output", str(raw)],
        ["clean", "--input", str(raw), "--config", str(config), "--output", str(clean)],
    ):
        result = subprocess.run(
            [java_command(), "-jar", str(JAR), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=60,
            check=False,
        )
        assert result.returncode in (0, 2), result.stderr
    return clean


def test_clean_orders_snapshot_and_rearranged_rows(tmp_path: Path) -> None:
    module = importlib.import_module("repeatlab.cleaning")
    rows = [
        ("OLD", "P", "2", "2010-12-10T00:00:00", "0.1000", "A"),
        ("OLD", "P", "2", "2010-12-10T00:00:00", "0.1000", "A"),
        ("OLD", "Q", "1", "2010-12-10T00:00:00", "0.335", "A"),
        ("OLD", "SERVICE", "1", "2010-12-10T00:00:00", "99", "A"),
        ("NOW", "P", "1", "2011-01-01T00:00:00", "1", "A"),
        ("COLD", "P", "-1", "2011-01-05T00:00:00", "1", "A"),
        ("ANON", "P", "1", "2010-12-10T00:00:00", "1", "\u00a0"),
        ("BAD", "P", "1", "2010-12-10T00:00:00", "1", "A"),
        ("BAD", "SERVICE", "1", "2010-12-10T00:00:00", "invalid", "A"),
    ]
    dataset = module.read_clean_dataset(run_clean(tmp_path / "first", rows))
    result = dataset.aggregate()
    old = next(order for order in result.orders if order.invoice_id == "OLD")
    assert (old.line_count, old.quantity, old.total_amount) == (3, 5, Decimal("0.7350"))
    assert len(result.excluded_service_event_ids) == 1
    assert len(result.possible_duplicate_event_ids) == 1
    assert next(order for order in result.orders if order.invoice_id == "ANON").customer_id is None
    assert dataset.counts["quarantined_rows"] == 2
    assert len(dataset.events) == 7
    snapshots = build_snapshot(
        (order.to_temporal_order() for order in result.orders),
        snapshot_time=datetime(2011, 1, 1),
        observed_until=datetime(2011, 2, 1),
    )
    assert len(snapshots) == 1
    assert (snapshots[0].history_order_count, snapshots[0].label) == (1, 1)
    reordered = module.read_clean_dataset(run_clean(tmp_path / "reordered", list(reversed(rows))))
    assert reordered.aggregate().orders == result.orders
