"""Persist the exact aggregate with complete, deterministic source accounting."""

import hashlib
import importlib
import importlib.util
import json
import os
import shutil
import sys
from dataclasses import replace
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any

import pytest

from repeatlab.aggregation import QuarantinedInvoice
from repeatlab.cleaning import CleanDataset


def canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def clean_dataset(
    path: Path,
    rows: list[tuple[str, str | None, str, int | str, str]],
    *,
    quarantine: bool = False,
) -> dict[str, Any]:
    """Build a complete clean exchange with literal classified source rows."""
    path.mkdir()
    envelope = {"schema": "repeatlab-clean", "schema_version": 1}
    events: list[dict[str, Any]] = []
    dispositions: list[dict[str, Any]] = []
    for row, (invoice, customer, stock, quantity, price) in enumerate(rows, 2):
        origin = {
            "source_sha256": "a" * 64,
            "sheet_index": 1,
            "sheet_name": "Source",
            "row_number": row,
        }
        event_id = hashlib.sha256(f"{'a' * 64}:1:{row}".encode()).hexdigest()
        flags = ["MISSING_CUSTOMER_ID"] if customer is None else []
        event: dict[str, Any] = {
            **envelope,
            "event_id": event_id,
            "invoice_id": invoice,
            "customer_id": customer,
            "stock_code": stock,
            "quantity": quantity,
            "unit_price": price,
            "kind": "cancellation" if invoice.startswith(("C", "c")) else "sale",
            "event_time": "2020-01-01T12:34:56.123456",
            "provenance": origin,
            "quality_flags": flags,
            "is_service": stock == "SERVICE",
        }
        events.append(event)
        dispositions.append(
            {
                **envelope,
                "event_id": event_id,
                "invoice_id": invoice,
                "provenance": origin,
                "quality_flags": flags,
                "status": "accepted_service" if stock == "SERVICE" else "accepted_product",
                "reasons": [],
            }
        )
    if quarantine:
        row = len(rows) + 2
        dispositions.append(
            {
                **envelope,
                "event_id": hashlib.sha256(f"{'a' * 64}:1:{row}".encode()).hexdigest(),
                "invoice_id": "BAD",
                "provenance": {
                    "source_sha256": "a" * 64,
                    "sheet_index": 1,
                    "sheet_name": "Source",
                    "row_number": row,
                },
                "status": "quarantined",
                "reasons": ["INVALID_QUANTITY"],
                "quality_flags": [],
            }
        )
    missing = sum(customer is None for _, customer, _, _, _ in rows)
    services = sum(stock == "SERVICE" for _, _, stock, _, _ in rows)
    counts = {
        "data_rows": len(rows) + int(quarantine),
        "accepted_rows": len(rows),
        "accepted_product_rows": len(rows) - services,
        "accepted_service_rows": services,
        "quarantined_rows": int(quarantine),
        "missing_customer_rows": missing,
        "header_rows": 1,
        "empty_rows": 0,
        "warning_rows": missing,
    }
    config = {"policy_version": "clean-v1", "service_codes": ["SERVICE"]}
    manifest: dict[str, Any] = {
        **envelope,
        "tool_version": "0.1.0",
        "policy_version": "clean-v1",
        "status": "completed",
        "data_status": "quarantined" if quarantine else "warnings" if missing else "ok",
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
    payload = b""
    for event in events:
        # Encode huge integer source tokens without changing interpreter-wide limits.
        quantity = event["quantity"]
        if isinstance(quantity, str):
            event["quantity"] = 1
            line = canonical(event).replace(b'"quantity":1', b'"quantity":' + quantity.encode())
        else:
            line = canonical(event)
        payload += line + b"\n"
    (path / "clean_events.jsonl").write_bytes(payload)
    (path / "dispositions.jsonl").write_bytes(
        b"".join(canonical(item) + b"\n" for item in dispositions)
    )
    (path / "audit.json").write_bytes(
        canonical(
            {
                **envelope,
                "counts": counts,
                "reason_counts": {"INVALID_QUANTITY": 1} if quarantine else {},
                "coverage_complete": True,
            }
        )
        + b"\n"
    )
    rehash(path, manifest)
    return manifest


def rehash(path: Path, manifest: dict[str, Any]) -> None:
    manifest["artifacts"] = {
        name: hashlib.sha256((path / name).read_bytes()).hexdigest()
        for name in ("clean_events.jsonl", "dispositions.jsonl", "audit.json")
    }
    (path / "manifest.json").write_bytes(canonical(manifest) + b"\n")


def worker() -> Any:
    assert importlib.util.find_spec("repeatlab.orders") is not None
    return importlib.import_module("repeatlab.orders")


def records(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line, parse_int=lambda value: int(Decimal(value)))
        for line in path.read_bytes().splitlines()
    ]


def test_exact_orders_keep_duplicates_missing_customers_and_service_lineage(tmp_path: Path) -> None:
    source = tmp_path / "clean"
    source_manifest = clean_dataset(
        source,
        [
            ("I1", "A", "P", 2, "0.1000"),
            ("I1", "A", "P", 2, "0.1000"),
            ("I1", "A", "Q", 1, "0.335"),
            ("I1", "A", "SERVICE", 1, "99"),
            ("I2", None, "P", 1, "1.25"),
            ("CI1", "A", "P", -1, "0.1000"),
        ],
    )
    output = tmp_path / "orders"
    with localcontext() as context:
        context.prec = 2
        assert worker().write_orders_dataset(source, output) == "warnings"
    orders = records(output / "orders.jsonl")
    assert [item["invoice_id"] for item in orders] == ["CI1", "I1", "I2"]
    assert [item["total_amount"] for item in orders] == ["-0.1000", "0.7350", "1.25"]
    assert orders[1] == {
        "schema": "repeatlab-orders",
        "schema_version": 1,
        "order_key": "I1",
        "invoice_id": "I1",
        "customer_id": "A",
        "event_time": "2020-01-01T12:34:56.123456",
        "kind": "sale",
        "line_count": 3,
        "quantity": 5,
        "stock_code_count": 2,
        "total_amount": "0.7350",
    }
    assert orders[2]["customer_id"] is None
    lineage = records(output / "lineage.jsonl")
    assert len(lineage) == 6
    assert [item["event_id"] for item in lineage] == sorted(item["event_id"] for item in lineage)
    excluded = [item for item in lineage if item["disposition"] == "excluded_service"]
    assert len(excluded) == 1 and excluded[0]["order_key"] is None
    assert all(item["order_key"] == item["invoice_id"] for item in lineage if item not in excluded)
    duplicates = records(output / "duplicates.jsonl")
    assert duplicates == [
        {
            "schema": "repeatlab-orders",
            "schema_version": 1,
            "event_ids": sorted(
                hashlib.sha256(f"{'a' * 64}:1:{row}".encode()).hexdigest() for row in (2, 3)
            ),
        }
    ]
    manifest = json.loads((output / "manifest.json").read_bytes())
    audit = json.loads((output / "audit.json").read_bytes())
    counts = {
        "orders": 3,
        "sale_orders": 2,
        "cancellation_orders": 1,
        "missing_customer_orders": 1,
        "included_product_rows": 5,
        "excluded_service_rows": 1,
        "duplicate_groups": 1,
    }
    assert manifest == {
        "schema": "repeatlab-orders",
        "schema_version": 1,
        "policy_version": "orders-v1",
        "worker_protocol_version": 1,
        "status": "completed",
        "data_status": "warnings",
        "input": {
            "clean_manifest_sha256": hashlib.sha256(
                (source / "manifest.json").read_bytes()
            ).hexdigest(),
            "clean_artifacts": source_manifest["artifacts"],
            "config_sha256": source_manifest["config_sha256"],
        },
        "counts": counts,
        "source_clean_counts": source_manifest["counts"],
        "artifacts": {
            name: hashlib.sha256((output / name).read_bytes()).hexdigest()
            for name in ("orders.jsonl", "lineage.jsonl", "duplicates.jsonl", "audit.json")
        },
    }
    assert audit == {
        "schema": "repeatlab-orders",
        "schema_version": 1,
        "counts": counts,
        "source_clean_counts": source_manifest["counts"],
    }
    assert set(item.name for item in output.iterdir()) == {
        "manifest.json",
        "orders.jsonl",
        "lineage.jsonl",
        "duplicates.jsonl",
        "audit.json",
        "run.json",
    }


@pytest.mark.parametrize("rows", [[], [("I", "A", "SERVICE", 1, "99")]])
def test_empty_and_all_service_inputs_publish_complete_empty_orders(
    tmp_path: Path, rows: list[tuple[str, str | None, str, int | str, str]]
) -> None:
    source, output = tmp_path / "clean", tmp_path / "orders"
    clean_dataset(source, rows)
    assert worker().write_orders_dataset(source, output) == "ok"
    assert records(output / "orders.jsonl") == []
    assert len(records(output / "lineage.jsonl")) == len(rows)
    assert records(output / "duplicates.jsonl") == []


def test_huge_integer_quantity_keeps_fitting_money_and_global_digit_limit(tmp_path: Path) -> None:
    source, output = tmp_path / "clean", tmp_path / "orders"
    quantity = "1" + "0" * 4400
    clean_dataset(source, [("I", "A", "P", quantity, "1E-4400")])
    previous_limit = sys.get_int_max_str_digits()
    assert worker().write_orders_dataset(source, output) == "ok"
    order = records(output / "orders.jsonl")[0]
    assert order["quantity"] == 10**4400
    assert Decimal(order["total_amount"]) == Decimal(1)
    assert b'"quantity":' + quantity.encode() in (output / "orders.jsonl").read_bytes()
    assert sys.get_int_max_str_digits() == previous_limit


@pytest.mark.parametrize("price", ["1E+2147483648", "1E-2147483649"])
def test_exact_decimal_exponents_survive_without_plain_string_expansion(
    tmp_path: Path, price: str
) -> None:
    source, output = tmp_path / "clean", tmp_path / "orders"
    clean_dataset(source, [("I", "A", "P", 1, price)])
    assert worker().write_orders_dataset(source, output) == "ok"
    assert records(output / "orders.jsonl")[0]["total_amount"] == price


def test_quarantine_quality_and_source_counts_are_inherited(tmp_path: Path) -> None:
    source, output = tmp_path / "clean", tmp_path / "orders"
    manifest = clean_dataset(source, [("I", "A", "P", 1, "1")], quarantine=True)
    assert worker().write_orders_dataset(source, output) == "quarantined"
    result = json.loads((output / "manifest.json").read_bytes())
    assert result["data_status"] == "quarantined"
    assert result["source_clean_counts"] == manifest["counts"]
    assert result["counts"]["orders"] == 1
    assert len(records(output / "lineage.jsonl")) == 1


@pytest.mark.parametrize(
    "problem",
    ["tampered", "duplicate_key", "duplicate_id", "counts", "status", "invoice", "service"],
)
def test_worker_rejects_corrupt_clean_exchange_without_completion(
    tmp_path: Path, problem: str
) -> None:
    source, output = tmp_path / "clean", tmp_path / "orders"
    manifest = clean_dataset(source, [("I", "A", "P", 1, "1")])
    event_path = source / "clean_events.jsonl"
    payload = event_path.read_bytes()
    if problem == "tampered":
        event_path.write_bytes(payload + b" ")
    elif problem == "duplicate_key":
        event_path.write_bytes(payload.replace(b'"quantity":1', b'"quantity":1,"quantity":1'))
    elif problem == "duplicate_id":
        event_path.write_bytes(payload + payload)
    elif problem == "invoice":
        event_path.write_bytes(payload.replace(b'"invoice_id":"I"', b'"invoice_id":"CI"'))
    elif problem == "service":
        event_path.write_bytes(payload.replace(b'"is_service":false', b'"is_service":true'))
    elif problem == "status":
        manifest["data_status"] = "quarantined"
    else:
        manifest["counts"]["accepted_rows"] = 9
    if problem != "tampered":
        rehash(source, manifest)
    with pytest.raises(ValueError):
        worker().write_orders_dataset(source, output)
    assert not (output / "manifest.json").exists()
    fatal = json.loads((output / "fatal.json").read_bytes())
    assert fatal["status"] == "failed"
    assert fatal["stage"] == "verify_clean"


@pytest.mark.parametrize("existing", ["file", "directory"])
def test_existing_output_is_preserved(tmp_path: Path, existing: str) -> None:
    source, output = tmp_path / "clean", tmp_path / "orders"
    clean_dataset(source, [])
    if existing == "file":
        output.write_bytes(b"preserve")
    else:
        output.mkdir()
        (output / "manifest.json").write_bytes(b"preserve")
    with pytest.raises(FileExistsError):
        worker().write_orders_dataset(source, output)
    target = output if existing == "file" else output / "manifest.json"
    assert target.read_bytes() == b"preserve"


@pytest.mark.parametrize(
    "name", ["manifest.json", "audit.json", "clean_events.jsonl", "dispositions.jsonl"]
)
@pytest.mark.parametrize("kind", ["directory", "symlink"])
def test_clean_readers_require_regular_members(tmp_path: Path, name: str, kind: str) -> None:
    from repeatlab.cleaning import read_clean_dataset

    source, output = tmp_path / "clean", tmp_path / "orders"
    clean_dataset(source, [])
    member = source / name
    target = member.rename(source / "same-bytes")
    if kind == "directory":
        member.mkdir()
    else:
        try:
            member.symlink_to(target)
        except OSError:
            pytest.skip("creating symlinks requires host permission")
    with pytest.raises(ValueError, match="regular artifact"):
        read_clean_dataset(source)
    with pytest.raises(ValueError, match="regular artifact"):
        worker().write_orders_dataset(source, output)
    assert not (output / "manifest.json").exists()


def test_existing_output_symlink_is_preserved(tmp_path: Path) -> None:
    source, output, target = tmp_path / "clean", tmp_path / "orders", tmp_path / "target"
    clean_dataset(source, [])
    target.mkdir()
    (target / "keep").write_bytes(b"preserve")
    try:
        output.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("creating symlinks requires host permission")
    with pytest.raises(FileExistsError):
        worker().write_orders_dataset(source, output)
    assert (target / "keep").read_bytes() == b"preserve"
    assert set(item.name for item in target.iterdir()) == {"keep"}


@pytest.mark.skipif(os.name != "posix", reason="POSIX directory permissions")
def test_new_orders_directory_is_owner_only(tmp_path: Path) -> None:
    source, output = tmp_path / "clean", tmp_path / "orders"
    clean_dataset(source, [])
    original = os.umask(0o022)
    try:
        worker().write_orders_dataset(source, output)
    finally:
        os.umask(original)
    assert output.stat().st_mode & 0o777 == 0o700


def test_snapshot_binds_input_hashes_to_processed_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, output = tmp_path / "clean", tmp_path / "orders"
    manifest = clean_dataset(source, [("I", "A", "P", 1, "0.335")])
    original_hash = hashlib.sha256((source / "manifest.json").read_bytes()).hexdigest()
    module = worker()
    original_read = module.read_clean_dataset

    def mutate_source_then_read(snapshot: Path) -> CleanDataset:
        (source / "manifest.json").write_bytes(b"replaced")
        (source / "clean_events.jsonl").write_bytes(b"replaced")
        return original_read(snapshot)  # type: ignore[no-any-return]

    monkeypatch.setattr(module, "read_clean_dataset", mutate_source_then_read)
    assert module.write_orders_dataset(source, output) == "ok"
    result = json.loads((output / "manifest.json").read_bytes())
    assert result["input"]["clean_manifest_sha256"] == original_hash
    assert result["input"]["clean_artifacts"] == manifest["artifacts"]
    assert records(output / "orders.jsonl")[0]["total_amount"] == "0.335"


def test_unexpected_aggregation_quarantine_fails_without_partial_orders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, output = tmp_path / "clean", tmp_path / "orders"
    clean_dataset(source, [("I", "A", "P", 1, "1")])
    original = CleanDataset.aggregate

    def inconsistent(dataset: CleanDataset) -> Any:
        return replace(
            original(dataset),
            quarantined_invoices=(QuarantinedInvoice("I", ("customer_id",), dataset.events),),
        )

    monkeypatch.setattr(CleanDataset, "aggregate", inconsistent)
    with pytest.raises(ValueError):
        worker().write_orders_dataset(source, output)
    assert not (output / "manifest.json").exists()
    assert json.loads((output / "fatal.json").read_bytes())["stage"] == "aggregate"


def test_interrupted_aggregation_cannot_publish_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, output = tmp_path / "clean", tmp_path / "orders"
    clean_dataset(source, [("I", "A", "P", 1, "1")])

    def interrupt(dataset: CleanDataset) -> Any:
        raise KeyboardInterrupt

    monkeypatch.setattr(CleanDataset, "aggregate", interrupt)
    with pytest.raises(KeyboardInterrupt):
        worker().write_orders_dataset(source, output)
    assert not (output / "manifest.json").exists()
    assert json.loads((output / "fatal.json").read_bytes())["status"] == "failed"


def test_failed_atomic_publication_leaves_diagnostics_without_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, output = tmp_path / "clean", tmp_path / "orders"
    clean_dataset(source, [])

    def fail_publication(*args: object) -> None:
        raise OSError("injected publication failure")

    monkeypatch.setattr(worker().os, "replace", fail_publication)
    with pytest.raises(OSError):
        worker().write_orders_dataset(source, output)
    assert not (output / "manifest.json").exists()
    assert json.loads((output / "fatal.json").read_bytes())["stage"] == "publish_manifest"
    assert {path.name for path in output.iterdir()} == {"fatal.json"}


def test_worker_only_discards_its_own_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, output = tmp_path / "clean", tmp_path / "orders"
    clean_dataset(source, [])
    original_open = Path.open

    def intercept(path: Path, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        if path == output / "lineage.jsonl" and mode == "xb":
            # The first output is replaced, and the second has already been claimed.
            first = output / "orders.jsonl"
            first.rename(tmp_path / "moved-by-caller")
            first.write_bytes(b"replacement")
            with original_open(path, "xb") as stream:
                stream.write(b"already here")
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", intercept)
    with pytest.raises(FileExistsError):
        worker().write_orders_dataset(source, output)
    assert (output / "orders.jsonl").read_bytes() == b"replacement"
    assert (output / "lineage.jsonl").read_bytes() == b"already here"
    assert {path.name for path in output.iterdir()} == {
        "orders.jsonl",
        "lineage.jsonl",
        "fatal.json",
    }


@pytest.mark.parametrize("error", [KeyboardInterrupt, FileNotFoundError])
def test_orders_survive_interruption_after_successful_rename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: type[BaseException]
) -> None:
    source, output = tmp_path / "clean", tmp_path / "orders"
    clean_dataset(source, [])
    replace_file = os.replace

    def interrupt_after_rename(src: Path, dst: Path) -> None:
        replace_file(src, dst)
        raise error

    monkeypatch.setattr(worker().os, "replace", interrupt_after_rename)
    with pytest.raises(error):
        worker().write_orders_dataset(source, output)
    manifest = json.loads((output / "manifest.json").read_bytes())
    assert manifest["status"] == "completed"
    for name, digest in manifest["artifacts"].items():
        assert hashlib.sha256((output / name).read_bytes()).hexdigest() == digest
    assert not (output / "fatal.json").exists()


def test_worker_continues_cleanup_after_an_unremovable_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, output = tmp_path / "clean", tmp_path / "orders"
    clean_dataset(source, [])
    original_unlink = Path.unlink

    def fail_unlink(path: Path, missing_ok: bool = False) -> None:
        if path == output / "lineage.jsonl":
            raise PermissionError("private-marker")
        original_unlink(path, missing_ok=missing_ok)

    def fail_publish(src: Path, dst: Path) -> None:
        raise OSError("private-marker")

    monkeypatch.setattr(Path, "unlink", fail_unlink)
    monkeypatch.setattr(worker().os, "replace", fail_publish)
    with pytest.raises(OSError):
        worker().write_orders_dataset(source, output)
    assert {p.name for p in output.iterdir()} == {"fatal.json", "lineage.jsonl"}
    assert b"private-marker" not in (output / "fatal.json").read_bytes()


def test_failed_artifact_write_cannot_publish_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, output = tmp_path / "clean", tmp_path / "orders"
    clean_dataset(source, [("I", "A", "P", 1, "1")])
    original_open = Path.open

    def fail_lineage(path: Path, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        if path == output / "lineage.jsonl":
            raise OSError("injected artifact write failure")
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_lineage)
    with pytest.raises(OSError):
        worker().write_orders_dataset(source, output)
    assert not (output / "manifest.json").exists()
    assert json.loads((output / "fatal.json").read_bytes())["stage"] == "write_artifacts"
    assert {path.name for path in output.iterdir()} == {"fatal.json"}


def test_snapshot_cleanup_failure_cannot_publish_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, output = tmp_path / "clean", tmp_path / "orders"
    clean_dataset(source, [])
    original_remove = shutil.rmtree

    def fail_cleanup(path: Any, *args: Any, **kwargs: Any) -> None:
        if Path(path).parent == output and Path(path).name.startswith(".clean-"):
            raise OSError("injected snapshot cleanup failure")
        original_remove(path, *args, **kwargs)

    monkeypatch.setattr(shutil, "rmtree", fail_cleanup)
    with pytest.raises(OSError):
        worker().write_orders_dataset(source, output)
    assert not (output / "manifest.json").exists()
    assert json.loads((output / "fatal.json").read_bytes())["stage"] == "cleanup_snapshot"
    assert {path.name for path in output.iterdir()} == {"fatal.json"}


def test_run_diagnostic_failure_cannot_publish_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, output = tmp_path / "clean", tmp_path / "orders"
    clean_dataset(source, [])
    original_open = Path.open

    def fail_run(path: Path, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        if path == output / "run.json":
            raise OSError("injected execution diagnostic failure")
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_run)
    with pytest.raises(OSError):
        worker().write_orders_dataset(source, output)
    assert not (output / "manifest.json").exists()
    assert json.loads((output / "fatal.json").read_bytes())["stage"] == "write_run"
    assert {path.name for path in output.iterdir()} == {"fatal.json"}


def test_unsupported_python_runtime_fails_before_reserving_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = worker()
    monkeypatch.setattr(module.sys, "version_info", (3, 11, 0))
    with pytest.raises(RuntimeError, match="Python 3.12"):
        module.write_orders_dataset(tmp_path / "clean", tmp_path / "orders")
    assert not (tmp_path / "orders").exists()


def test_missing_clean_cannot_be_reserved_as_worker_output(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    with pytest.raises(ValueError, match="overlaps"):
        worker().write_orders_dataset(missing, missing)
    assert not missing.exists()


def test_repeated_runs_and_unicode_ordering_are_deterministic(tmp_path: Path) -> None:
    source = tmp_path / "clean"
    rows: list[tuple[str, str | None, str, int | str, str]] = [
        ("\U0001f600", "A", "P", 1, "1"),
        ("\ue000", None, "P", 1, "1"),
        ("Å", None, "SERVICE", 1, "99"),
        ("Å", None, "SERVICE", 1, "99"),
    ]
    clean_dataset(source, rows)
    first, second = tmp_path / "first", tmp_path / "second"
    module = worker()
    module.write_orders_dataset(source, first)
    module.write_orders_dataset(source, second)
    for name in (
        "manifest.json",
        "audit.json",
        "orders.jsonl",
        "lineage.jsonl",
        "duplicates.jsonl",
    ):
        assert (first / name).read_bytes() == (second / name).read_bytes()
    assert [order["invoice_id"] for order in records(first / "orders.jsonl")] == [
        "\ue000",
        "\U0001f600",
    ]
    assert len(records(first / "duplicates.jsonl")) == 1
    reordered = tmp_path / "reordered"
    clean_dataset(reordered, list(reversed(rows)))
    module.write_orders_dataset(reordered, tmp_path / "third")
    assert (first / "orders.jsonl").read_bytes() == (
        tmp_path / "third" / "orders.jsonl"
    ).read_bytes()
