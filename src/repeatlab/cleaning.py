"""Verify a completed clean exchange before exposing immutable typed events."""

import json
import re
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast

from repeatlab.aggregation import AggregationResult, CleanEvent, aggregate_orders
from repeatlab.temporal import OrderKind

_SCHEMA = "repeatlab-clean"
_POLICY = "clean-v1"
_ARTIFACTS = {"clean_events.jsonl", "dispositions.jsonl", "audit.json"}
_INPUT_ARTIFACTS = {"raw_transactions.jsonl", "issues.jsonl", "audit.json"}
_COUNTS = {
    "data_rows",
    "accepted_rows",
    "accepted_product_rows",
    "accepted_service_rows",
    "quarantined_rows",
    "missing_customer_rows",
    "header_rows",
    "empty_rows",
    "warning_rows",
}
_FLAGS = {
    "MISSING_CUSTOMER_ID",
    "NORMALIZED_INVOICE_ID",
    "NORMALIZED_STOCK_CODE",
    "NORMALIZED_CUSTOMER_ID",
    "TIME_PRECISION_FLOORED",
    "NONCRITICAL_FORMULA",
}
_REASONS = {
    "INVALID_INVOICE_ID",
    "INVALID_STOCK_CODE",
    "INVALID_CUSTOMER_ID",
    "INVALID_QUANTITY",
    "INVALID_UNIT_PRICE",
    "INVALID_EVENT_TIME",
    "ZERO_QUANTITY",
    "NONPOSITIVE_UNIT_PRICE",
    "CANCELLATION_MISMATCH",
    "INVOICE_INVALID",
    "INVOICE_CONFLICT_CUSTOMER_ID",
    "INVOICE_CONFLICT_KIND",
    "INVOICE_CONFLICT_EVENT_TIME",
}
_TIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?")
_DECIMAL = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")
_WHITESPACE = frozenset(
    [
        *range(9, 14),
        32,
        0x85,
        0xA0,
        0x1680,
        *range(0x2000, 0x200B),
        0x2028,
        0x2029,
        0x202F,
        0x205F,
        0x3000,
    ]
)


@dataclass(frozen=True, slots=True)
class CleanDataset:
    """A fully checked clean subset, with quality and the matching service policy."""

    events: tuple[CleanEvent, ...]
    service_codes: frozenset[str]
    data_status: str
    counts: Mapping[str, int]
    reason_counts: Mapping[str, int]
    config_sha256: str
    input_manifest_sha256: str

    def aggregate(self) -> AggregationResult:
        """Apply the dataset's service exclusions to the existing order builder."""
        return aggregate_orders(self.events, service_codes=self.service_codes)


def _require(valid: bool, message: str) -> None:
    if not valid:
        raise ValueError(message)


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        _require(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def _nonfinite(value: str) -> Any:
    raise ValueError(f"nonfinite JSON number: {value}")


def _parse(payload: bytes) -> Any:
    return json.loads(
        payload.decode("utf-8"),
        object_pairs_hook=_pairs,
        parse_constant=_nonfinite,
        parse_int=lambda value: int(Decimal(value)),
    )


def _object(value: Any, keys: set[str]) -> dict[str, Any]:
    _require(type(value) is dict and set(value) == keys, "unexpected or missing object fields")
    return cast(dict[str, Any], value)


def _text(value: Any) -> str:
    _require(type(value) is str, "expected string")
    result = cast(str, value)
    _require(not any(0xD800 <= ord(char) <= 0xDFFF for char in result), "invalid Unicode")
    return result


def _identifier(value: Any) -> str:
    result = _text(value)
    _require(not all(ord(char) in _WHITESPACE for char in result), "missing identifier")
    return result


def _hash(value: Any) -> str:
    result = _text(value)
    _require(re.fullmatch(r"[0-9a-f]{64}", result) is not None, "invalid SHA-256")
    return result


def _integer(value: Any, minimum: int = 0) -> int:
    _require(type(value) is int and value >= minimum, "invalid integer")
    return cast(int, value)


def _envelope(node: dict[str, Any]) -> None:
    _require(node["schema"] == _SCHEMA, "unsupported clean schema")
    _require(
        type(node["schema_version"]) is int and node["schema_version"] == 1,
        "unsupported clean schema version",
    )


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _codes(value: Any, allowed: set[str]) -> tuple[str, ...]:
    _require(type(value) is list, "expected code array")
    codes = tuple(_text(code) for code in value)
    _require(
        list(codes) == sorted(set(codes)) and set(codes) <= allowed, "invalid or unordered codes"
    )
    return codes


def _counts(value: Any) -> dict[str, int]:
    node = _object(value, _COUNTS)
    return {key: _integer(number) for key, number in node.items()}


def _origin(value: Any, source_hash: str, event_id: Any) -> tuple[int, int, str]:
    node = _object(value, {"source_sha256", "sheet_index", "sheet_name", "row_number"})
    _require(_hash(node["source_sha256"]) == source_hash, "source provenance mismatch")
    sheet = _integer(node["sheet_index"], 1)
    _require(sheet <= 2147483647, "invalid source sheet")
    row = _integer(node["row_number"], 2)
    _require(row <= 1048576, "invalid source row")
    name = _text(node["sheet_name"])
    _require(bool(name), "empty sheet name")
    expected = sha256(f"{source_hash}:{sheet}:{row}".encode()).hexdigest()
    _require(_hash(event_id) == expected, "event identifier does not match provenance")
    return sheet, row, name


def _event(
    node: Any, services: frozenset[str], source_hash: str
) -> tuple[CleanEvent, tuple[str, ...]]:
    value = _object(
        node,
        {
            "schema",
            "schema_version",
            "event_id",
            "invoice_id",
            "customer_id",
            "event_time",
            "kind",
            "stock_code",
            "quantity",
            "unit_price",
            "provenance",
            "quality_flags",
            "is_service",
        },
    )
    _envelope(value)
    _origin(value["provenance"], source_hash, value["event_id"])
    invoice, stock = _identifier(value["invoice_id"]), _identifier(value["stock_code"])
    customer = None if value["customer_id"] is None else _identifier(value["customer_id"])
    flags = _codes(value["quality_flags"], _FLAGS)
    _require((customer is None) == ("MISSING_CUSTOMER_ID" in flags), "customer flag mismatch")
    _require(
        type(value["is_service"]) is bool and value["is_service"] == (stock in services),
        "service flag does not match configuration",
    )
    time_text = _text(value["event_time"])
    _require(_TIME.fullmatch(time_text) is not None, "invalid time or excess time precision")
    time = datetime.fromisoformat(time_text)
    price_text = _text(value["unit_price"])
    _require(_DECIMAL.fullmatch(price_text) is not None, "invalid decimal representation")
    price = Decimal(price_text)
    _require(price.is_finite() and price > 0, "price must be finite and positive")
    quantity = value["quantity"]
    _require(type(quantity) is int, "quantity must be an exact integer")
    kind = _text(value["kind"])
    _require(kind in ("sale", "cancellation"), "invalid event kind")
    _require(
        invoice.startswith(("c", "C")) == (kind == "cancellation"), "cancellation prefix mismatch"
    )
    event = CleanEvent(
        _hash(value["event_id"]),
        invoice,
        customer,
        time,
        cast(OrderKind, kind),
        stock,
        quantity,
        price,
    )
    return event, flags


def read_clean_dataset(path: str | Path) -> CleanDataset:
    """Read only a completed directory whose entire verified exchange is consistent.

    Artifact bytes are hashed and parsed from the same in-memory snapshot. No
    usable events are returned on checksum, schema, coverage or policy errors.
    """
    try:
        return _read(Path(path))
    except (
        OSError,
        UnicodeError,
        json.JSONDecodeError,
        InvalidOperation,
        TypeError,
        KeyError,
        OverflowError,
    ) as error:
        raise ValueError("invalid clean dataset") from error


def _read(path: Path) -> CleanDataset:
    manifest = _object(
        _parse((path / "manifest.json").read_bytes()),
        {
            "schema",
            "schema_version",
            "tool_version",
            "policy_version",
            "status",
            "data_status",
            "input",
            "config",
            "config_sha256",
            "counts",
            "artifacts",
        },
    )
    _envelope(manifest)
    _require(manifest["tool_version"] == "0.1.0", "unsupported clean tool version")
    _require(manifest["policy_version"] == _POLICY, "unsupported cleaning policy")
    _require(manifest["status"] == "completed", "clean dataset is not completed")
    config = _object(manifest["config"], {"policy_version", "service_codes"})
    _require(config["policy_version"] == _POLICY, "configuration policy mismatch")
    _require(type(config["service_codes"]) is list, "invalid service codes")
    codes = [_text(code) for code in config["service_codes"]]
    _require(codes == sorted(set(codes)), "configuration is not canonical")
    services = frozenset(codes)
    config_hash = _hash(manifest["config_sha256"])
    _require(
        sha256(_canonical(config)).hexdigest() == config_hash, "configuration checksum mismatch"
    )
    source = _object(
        manifest["input"], {"manifest_sha256", "source_sha256", "date_system", "artifacts"}
    )
    input_hash = _hash(source["manifest_sha256"])
    source_hash = _hash(source["source_sha256"])
    _require(source["date_system"] in ("1900", "1904"), "invalid source date system")
    for value in _object(source["artifacts"], _INPUT_ARTIFACTS).values():
        _hash(value)
    expected_counts = _counts(manifest["counts"])
    hashes = _object(manifest["artifacts"], _ARTIFACTS)
    payloads: dict[str, bytes] = {}
    for name, digest in hashes.items():
        payload = (path / name).read_bytes()
        _require(sha256(payload).hexdigest() == _hash(digest), f"checksum mismatch: {name}")
        payloads[name] = payload
    audit = _object(
        _parse(payloads.pop("audit.json")),
        {
            "schema",
            "schema_version",
            "counts",
            "reason_counts",
            "coverage_complete",
        },
    )
    _envelope(audit)
    _require(audit["coverage_complete"] is True, "incomplete coverage")
    _require(_counts(audit["counts"]) == expected_counts, "audit counts mismatch")
    _require(type(audit["reason_counts"]) is dict, "invalid reason counts")
    expected_reasons = {
        _text(key): _integer(value, 1) for key, value in audit["reason_counts"].items()
    }
    _require(set(expected_reasons) <= _REASONS, "unknown reason count")

    events: list[CleanEvent] = []
    by_id: dict[str, tuple[CleanEvent, dict[str, Any], tuple[str, ...]]] = {}
    invoice_fields: dict[str, tuple[str | None, datetime, OrderKind]] = {}
    previous = (0, 0)
    for line in payloads.pop("clean_events.jsonl").splitlines():
        node = _parse(line)
        event, flags = _event(node, services, source_hash)
        sheet, row, _ = _origin(node["provenance"], source_hash, event.event_id)
        _require((sheet, row) > previous, "duplicate or unordered event origin")
        previous = sheet, row
        _require(event.event_id not in by_id, "duplicate event identifier")
        by_id[event.event_id] = event, node["provenance"], flags
        fields = (event.customer_id, event.event_time, event.kind)
        _require(
            invoice_fields.setdefault(event.invoice_id, fields) == fields,
            "inconsistent clean invoice",
        )
        events.append(event)

    actual = {key: 0 for key in _COUNTS}
    actual["header_rows"], actual["empty_rows"] = (
        expected_counts["header_rows"],
        expected_counts["empty_rows"],
    )
    reasons: Counter[str] = Counter()
    previous = (0, 0)
    sheet_names: dict[int, str] = {}
    quarantined_invoices: set[str] = set()
    seen_ids: set[str] = set()
    accepted_ids: list[str] = []
    for line in payloads.pop("dispositions.jsonl").splitlines():
        node = _object(
            _parse(line),
            {
                "schema",
                "schema_version",
                "event_id",
                "invoice_id",
                "provenance",
                "status",
                "reasons",
                "quality_flags",
            },
        )
        _envelope(node)
        sheet, row, name = _origin(node["provenance"], source_hash, node["event_id"])
        _require((sheet, row) > previous, "duplicate or unordered disposition origin")
        previous = sheet, row
        _require(sheet_names.setdefault(sheet, name) == name, "inconsistent sheet name")
        event_id = _hash(node["event_id"])
        _require(event_id not in seen_ids, "duplicate disposition identifier")
        seen_ids.add(event_id)
        invoice = None if node["invoice_id"] is None else _identifier(node["invoice_id"])
        flags = _codes(node["quality_flags"], _FLAGS)
        row_reasons = _codes(node["reasons"], _REASONS)
        actual["data_rows"] += 1
        actual["warning_rows"] += bool(flags)
        actual["missing_customer_rows"] += "MISSING_CUSTOMER_ID" in flags
        status = _text(node["status"])
        if status == "quarantined":
            _require(bool(row_reasons) and event_id not in by_id, "invalid quarantine disposition")
            if invoice is not None:
                quarantined_invoices.add(invoice)
            actual["quarantined_rows"] += 1
            reasons.update(row_reasons)
        else:
            _require(
                status in ("accepted_product", "accepted_service"), "invalid disposition status"
            )
            _require(not row_reasons and event_id in by_id, "accepted disposition mismatch")
            event, provenance, event_flags = by_id[event_id]
            _require(
                invoice == event.invoice_id
                and provenance == node["provenance"]
                and flags == event_flags,
                "event/disposition mismatch",
            )
            expected_status = (
                "accepted_service" if event.stock_code in services else "accepted_product"
            )
            _require(status == expected_status, "disposition service mismatch")
            actual["accepted_rows"] += 1
            actual[status + "_rows"] += 1
            accepted_ids.append(event_id)
    _require(accepted_ids == [event.event_id for event in events], "incomplete event coverage")
    _require(not quarantined_invoices.intersection(invoice_fields), "partially quarantined invoice")
    _require(actual == expected_counts, "disposition counts mismatch")
    _require(dict(reasons) == expected_reasons, "reason counts mismatch")
    status = (
        "quarantined"
        if actual["quarantined_rows"]
        else "warnings"
        if actual["warning_rows"]
        else "ok"
    )
    _require(manifest["data_status"] == status, "quality status mismatch")
    return CleanDataset(
        tuple(events),
        services,
        status,
        MappingProxyType(actual),
        MappingProxyType(dict(reasons)),
        config_hash,
        input_hash,
    )
