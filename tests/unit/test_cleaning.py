"""The verified dataset boundary rejects incomplete and altered exchanges."""

import hashlib
import importlib
import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest


def canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def fixture(path: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    provenance = {"source_sha256": "a" * 64, "sheet_index": 1, "sheet_name": "S", "row_number": 2}
    event_id = hashlib.sha256(f"{'a' * 64}:1:2".encode()).hexdigest()
    envelope = {"schema": "repeatlab-clean", "schema_version": 1}
    event = {
        **envelope,
        "event_id": event_id,
        "invoice_id": "I",
        "customer_id": "A",
        "stock_code": "SERVICE",
        "kind": "sale",
        "quantity": 2,
        "unit_price": "0.1000",
        "event_time": "2020-01-01T00:00:00",
        "provenance": provenance,
        "quality_flags": [],
        "is_service": True,
    }
    disposition = {
        **envelope,
        "event_id": event_id,
        "invoice_id": "I",
        "provenance": provenance,
        "status": "accepted_service",
        "reasons": [],
        "quality_flags": [],
    }
    config = {"policy_version": "clean-v1", "service_codes": ["SERVICE"]}
    counts = {
        "data_rows": 1,
        "accepted_rows": 1,
        "accepted_product_rows": 0,
        "accepted_service_rows": 1,
        "quarantined_rows": 0,
        "missing_customer_rows": 0,
        "header_rows": 1,
        "empty_rows": 0,
        "warning_rows": 0,
    }
    manifest = {
        **envelope,
        "tool_version": "0.1.0",
        "policy_version": "clean-v1",
        "status": "completed",
        "data_status": "ok",
        "config": config,
        "config_sha256": hashlib.sha256(canonical(config)).hexdigest(),
        "counts": counts,
        "input": {
            "manifest_sha256": "b" * 64,
            "source_sha256": "a" * 64,
            "date_system": "1900",
            "artifacts": {
                "raw_transactions.jsonl": "c" * 64,
                "issues.jsonl": "d" * 64,
                "audit.json": "e" * 64,
            },
        },
        "artifacts": {},
    }
    audit = {**envelope, "counts": counts, "reason_counts": {}, "coverage_complete": True}
    (path / "audit.json").write_bytes(canonical(audit) + b"\n")
    return event, disposition, manifest


def publish(
    path: Path, event: dict[str, Any], disposition: dict[str, Any], manifest: dict[str, Any]
) -> None:
    (path / "clean_events.jsonl").write_bytes(canonical(event) + b"\n")
    (path / "dispositions.jsonl").write_bytes(canonical(disposition) + b"\n")
    manifest["artifacts"] = {
        name: hashlib.sha256((path / name).read_bytes()).hexdigest()
        for name in ("clean_events.jsonl", "dispositions.jsonl", "audit.json")
    }
    (path / "manifest.json").write_bytes(canonical(manifest) + b"\n")


def reader() -> Any:
    assert importlib.util.find_spec("repeatlab.cleaning") is not None
    return importlib.import_module("repeatlab.cleaning").read_clean_dataset


def test_reader_requires_a_completed_dataset(tmp_path: Path) -> None:
    read = reader()
    (tmp_path / "clean_events.jsonl").write_text("", encoding="utf-8")
    with pytest.raises(ValueError):
        read(tmp_path)


def test_safe_default_excludes_services_and_metadata_is_immutable(tmp_path: Path) -> None:
    publish(tmp_path, *fixture(tmp_path))
    dataset = reader()(tmp_path)
    assert dataset.aggregate().orders == ()
    assert dataset.events[0].unit_price.as_tuple().exponent == -4
    assert dataset.service_codes == frozenset({"SERVICE"})
    with pytest.raises(TypeError):
        dataset.counts["data_rows"] = 99


@pytest.mark.parametrize("sheet,row", [(1, 1), (2147483648, 2)])
def test_impossible_raw_data_coordinates_are_rejected(tmp_path: Path, sheet: int, row: int) -> None:
    event, disposition, manifest = fixture(tmp_path)
    event["provenance"]["sheet_index"] = sheet
    event["provenance"]["row_number"] = row
    identifier = hashlib.sha256(f"{'a' * 64}:{sheet}:{row}".encode()).hexdigest()
    event["event_id"] = disposition["event_id"] = identifier
    publish(tmp_path, event, disposition, manifest)
    with pytest.raises(ValueError):
        reader()(tmp_path)


@pytest.mark.parametrize(
    "field,value",
    [
        ("quantity", True),
        ("quantity", 2.0),
        ("unit_price", 0.1),
        ("unit_price", "NaN"),
        ("unit_price", "Infinity"),
        ("unit_price", "0"),
        ("event_time", "2020-01-01T00:00:00.1234567"),
        ("event_time", "2020-01-01T00:00:00Z"),
        ("event_time", "0000-01-01T00:00:00"),
        ("is_service", False),
        ("customer_id", "\u00a0"),
        ("invoice_id", "C1"),
        ("schema_version", True),
        ("quality_flags", ["UNKNOWN"]),
    ],
)
def test_rehashed_invalid_event_is_rejected(tmp_path: Path, field: str, value: object) -> None:
    event, disposition, manifest = fixture(tmp_path)
    event[field] = value
    publish(tmp_path, event, disposition, manifest)
    with pytest.raises(ValueError):
        reader()(tmp_path)


@pytest.mark.parametrize(
    "problem",
    [
        "checksum",
        "duplicate_key",
        "duplicate_event",
        "duplicate_disposition",
        "config",
        "counts",
        "artifact_path",
        "status",
        "provenance",
        "accepted_reason",
        "quarantine",
    ],
)
def test_corrupt_or_inconsistent_dataset_is_rejected(tmp_path: Path, problem: str) -> None:
    event, disposition, manifest = fixture(tmp_path)
    if problem == "config":
        manifest["config"]["service_codes"] = []
    elif problem == "counts":
        manifest["counts"]["data_rows"] = 2
    elif problem == "status":
        manifest["status"] = "failed"
    elif problem == "provenance":
        event["provenance"]["source_sha256"] = "f" * 64
    elif problem == "accepted_reason":
        disposition["reasons"] = ["INVALID_QUANTITY"]
    elif problem == "quarantine":
        disposition["status"] = "quarantined"
        disposition["reasons"] = ["INVALID_QUANTITY"]
    publish(tmp_path, event, disposition, manifest)
    if problem == "artifact_path":
        manifest["artifacts"]["../events.jsonl"] = manifest["artifacts"].pop("clean_events.jsonl")
    elif problem in ("checksum", "duplicate_key", "duplicate_event", "duplicate_disposition"):
        name = "dispositions.jsonl" if problem == "duplicate_disposition" else "clean_events.jsonl"
        payload = (tmp_path / name).read_bytes()
        if problem == "duplicate_key":
            payload = payload.replace(b'"quantity":2', b'"quantity":2,"quantity":2')
        elif problem == "checksum":
            payload += b" "
        else:
            payload += payload
        (tmp_path / name).write_bytes(payload)
        if problem != "checksum":
            manifest["artifacts"][name] = hashlib.sha256(payload).hexdigest()
    (tmp_path / "manifest.json").write_bytes(canonical(manifest) + b"\n")
    with pytest.raises(ValueError):
        reader()(tmp_path)
