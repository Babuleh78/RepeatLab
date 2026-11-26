"""Persist exact invoice orders from a verified clean dataset."""

import argparse
import json
import os
import shutil
import sys
from collections.abc import Iterable, Iterator
from contextlib import contextmanager, suppress
from decimal import Decimal
from hashlib import file_digest, sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, BinaryIO, NoReturn

from repeatlab._artifacts import open_artifact
from repeatlab.aggregation import AggregationResult, OrderAmountLimitError
from repeatlab.cleaning import CleanDataset, read_clean_dataset

_ENVELOPE = {"schema": "repeatlab-orders", "schema_version": 1}
_CLEAN_FILES = ("manifest.json", "clean_events.jsonl", "dispositions.jsonl", "audit.json")


def _failure_message(error: BaseException) -> str:
    """Describe a worker failure without echoing source data or local paths."""
    if isinstance(error, OrderAmountLimitError):
        return str(error)
    if isinstance(error, FileNotFoundError):
        return "Required input or directory is missing"
    if isinstance(error, FileExistsError):
        return "Output path already exists"
    if isinstance(error, PermissionError):
        return "Access to a required file was denied"
    if isinstance(error, OSError):
        return "Could not read or write the required files"
    if isinstance(error, KeyboardInterrupt):
        return "Run interrupted"
    return "Input validation or order calculation failed"


def _json_text(value: Any) -> str:
    """Encode the exchange's JSON types without the int-to-string digit limit."""
    if value is None or isinstance(value, (str, bool)):
        return json.dumps(value, ensure_ascii=False, allow_nan=False)
    if isinstance(value, int):
        return format(Decimal(value), "f")
    if isinstance(value, dict):
        return (
            "{"
            + ",".join(_json_text(key) + ":" + _json_text(value[key]) for key in sorted(value))
            + "}"
        )
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_json_text(item) for item in value) + "]"
    raise TypeError("unsupported orders JSON value")


class _CreatedFiles:
    def __init__(self) -> None:
        self.files: dict[Path, os.stat_result] = {}

    @contextmanager
    def create(self, path: Path) -> Iterator[BinaryIO]:
        with path.open("xb") as stream:
            self.files[path] = os.fstat(stream.fileno())
            yield stream

    def write_json(self, path: Path, value: dict[str, Any]) -> None:
        with self.create(path) as stream:
            stream.write((_json_text(value) + "\n").encode("utf-8"))

    def write_records(self, path: Path, records: Iterable[dict[str, Any]]) -> None:
        with self.create(path) as stream:
            for record in records:
                stream.write((_json_text({**_ENVELOPE, **record}) + "\n").encode("utf-8"))

    def discard(self) -> None:
        for path, created in self.files.items():
            with suppress(OSError):
                if os.path.samestat(path.lstat(), created):
                    path.unlink()


def _file_sha256(path: Path) -> str:
    with open_artifact(path) as stream:
        return file_digest(stream, "sha256").hexdigest()


def _write_artifacts(
    output: Path, dataset: CleanDataset, result: AggregationResult, created: _CreatedFiles
) -> dict[str, int]:
    created.write_records(
        output / "orders.jsonl",
        (
            {
                "order_key": order.invoice_id,
                "invoice_id": order.invoice_id,
                "customer_id": order.customer_id,
                "event_time": order.event_time.isoformat(),
                "kind": order.kind,
                "line_count": order.line_count,
                "quantity": order.quantity,
                "stock_code_count": order.stock_code_count,
                "total_amount": str(order.total_amount),
            }
            for order in result.orders
        ),
    )
    excluded = frozenset(result.excluded_service_event_ids)
    created.write_records(
        output / "lineage.jsonl",
        (
            {
                "event_id": event.event_id,
                "invoice_id": event.invoice_id,
                "order_key": None if event.event_id in excluded else event.invoice_id,
                "disposition": (
                    "excluded_service" if event.event_id in excluded else "included_product"
                ),
            }
            for event in sorted(dataset.events, key=lambda event: event.event_id)
        ),
    )
    created.write_records(
        output / "duplicates.jsonl",
        ({"event_ids": group} for group in result.possible_duplicate_event_ids),
    )
    counts = {
        "orders": len(result.orders),
        "sale_orders": sum(order.kind == "sale" for order in result.orders),
        "cancellation_orders": sum(order.kind == "cancellation" for order in result.orders),
        "missing_customer_orders": sum(order.customer_id is None for order in result.orders),
        "included_product_rows": len(dataset.events) - len(excluded),
        "excluded_service_rows": len(excluded),
        "duplicate_groups": len(result.possible_duplicate_event_ids),
    }
    created.write_json(
        output / "audit.json",
        {**_ENVELOPE, "counts": counts, "source_clean_counts": dict(dataset.counts)},
    )
    return counts


def write_orders_dataset(input_path: Path, output_path: Path) -> str:
    """Write a new completed orders exchange, returning inherited data quality.

    Fixed clean artifacts are copied to a private snapshot before verification.
    Every artifact, diagnostic and snapshot cleanup precedes atomic publication
    of the completed manifest. An existing output path is always preserved.
    """
    if sys.version_info[:2] != (3, 12):
        raise RuntimeError("the orders worker requires Python 3.12")
    source, target = input_path.resolve(), output_path.resolve()
    if target.is_relative_to(source) or source.is_relative_to(target):
        raise ValueError("output directory overlaps input clean directory")
    output_path.mkdir(mode=0o700, parents=True)
    created = _CreatedFiles()
    snapshot: Path | None = None
    snapshot_stat: os.stat_result | None = None
    pending_manifest = output_path / ".manifest.json.tmp"
    final_manifest = output_path / "manifest.json"
    stage = "snapshot_clean"
    try:
        with TemporaryDirectory(prefix=".clean-", dir=output_path) as temporary:
            snapshot = Path(temporary)
            snapshot_stat = snapshot.stat()
            for name in _CLEAN_FILES:
                with (
                    open_artifact(input_path / name) as source_file,
                    created.create(snapshot / name) as dest,
                ):
                    shutil.copyfileobj(source_file, dest, 1024 * 1024)
            with open_artifact(snapshot / "manifest.json") as manifest_file:
                manifest_bytes = manifest_file.read()
            stage = "verify_clean"
            dataset = read_clean_dataset(snapshot)
            # The reader has validated this exact private manifest and its artifacts.
            clean_manifest = json.loads(manifest_bytes)
            stage = "aggregate"
            result = dataset.aggregate()
            if result.quarantined_invoices:
                raise ValueError("verified clean input produced inconsistent invoices")
            stage = "write_artifacts"
            counts = _write_artifacts(output_path, dataset, result, created)
            manifest = {
                **_ENVELOPE,
                "policy_version": "orders-v1",
                "worker_protocol_version": 1,
                "status": "completed",
                "data_status": dataset.data_status,
                "input": {
                    "clean_manifest_sha256": sha256(manifest_bytes).hexdigest(),
                    "clean_artifacts": clean_manifest["artifacts"],
                    "config_sha256": dataset.config_sha256,
                },
                "counts": counts,
                "source_clean_counts": dict(dataset.counts),
                "artifacts": {
                    name: _file_sha256(output_path / name)
                    for name in ("orders.jsonl", "lineage.jsonl", "duplicates.jsonl", "audit.json")
                },
            }
            stage = "cleanup_snapshot"
        stage = "write_run"
        created.write_json(
            output_path / "run.json",
            {**_ENVELOPE, "status": "completed", "data_status": dataset.data_status},
        )
        stage = "publish_manifest"
        created.write_json(pending_manifest, manifest)
        if os.path.lexists(final_manifest):
            raise FileExistsError("Output manifest already exists")
        os.replace(pending_manifest, final_manifest)
        return dataset.data_status
    except BaseException as error:
        pending = created.files.get(pending_manifest)
        if pending is not None:
            # An interruption can arrive just after the final atomic rename.
            try:
                committed = os.path.samestat(final_manifest.lstat(), pending)
            except OSError:
                committed = False
            if committed:
                raise
        created.discard()
        if snapshot is not None and snapshot_stat is not None:
            with suppress(OSError):
                if os.path.samestat(snapshot.lstat(), snapshot_stat):
                    snapshot.rmdir()
        try:
            created.write_json(
                output_path / "fatal.json",
                {
                    **_ENVELOPE,
                    "status": "failed",
                    "stage": stage,
                    "error_type": type(error).__name__,
                    "message": _failure_message(error),
                },
            )
        except Exception:
            pass
        raise


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        self.print_usage(sys.stderr)
        self.exit(64, "Invalid worker arguments\n")


def main(arguments: list[str] | None = None) -> int:
    """Run the installed orders worker with the dataset exit-code protocol."""
    parser = _ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    options = parser.parse_args(arguments)
    try:
        status = write_orders_dataset(options.input, options.output)
    except (Exception, KeyboardInterrupt) as error:
        print(f"orders failed: {_failure_message(error)}", file=sys.stderr)
        return 1
    return 2 if status == "quarantined" else 0


if __name__ == "__main__":
    sys.exit(main())
