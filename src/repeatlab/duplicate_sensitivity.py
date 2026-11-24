"""Write paired per-order facts for a verified, private clean snapshot."""

import argparse
import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from repeatlab.aggregation import AggregatedOrder, aggregate_orders
from repeatlab.cleaning import read_clean_dataset


def _identity(order: AggregatedOrder) -> tuple[str, str | None, datetime, str, int]:
    return order.invoice_id, order.customer_id, order.event_time, order.kind, order.stock_code_count


def _measures(order: AggregatedOrder) -> dict[str, int | str]:
    return {
        "lines": order.line_count,
        # Decimal formats arbitrary integers without changing Python's global digit limit.
        "units": format(Decimal(order.quantity), "f"),
        "amount": str(order.total_amount),
    }


def write_facts(clean_path: Path, output_path: Path) -> str:
    """Compare all accepted rows with one deterministic representative per content group."""
    if sys.version_info[:2] != (3, 12):
        raise RuntimeError("the sensitivity worker requires Python 3.12")
    dataset = read_clean_dataset(clean_path)
    retained = dataset.aggregate()
    # The order builder emits sorted, disjoint groups of equal business contents.
    omitted = {
        event_id for group in retained.possible_duplicate_event_ids for event_id in group[1:]
    }
    collapsed = (
        aggregate_orders(
            (event for event in dataset.events if event.event_id not in omitted),
            service_codes=dataset.service_codes,
        )
        if omitted
        else retained
    )
    if retained.quarantined_invoices or collapsed.quarantined_invoices:
        raise ValueError("verified clean input produced inconsistent invoices")

    if len(retained.orders) != len(collapsed.orders):
        raise ValueError("scenario order counts differ")
    if any(
        _identity(before) != _identity(after)
        for before, after in zip(retained.orders, collapsed.orders, strict=True)
    ):
        raise ValueError("scenario order identities differ")

    stream = output_path.open("x", encoding="utf-8", newline="\n")
    try:
        with stream:
            for before, after in zip(retained.orders, collapsed.orders, strict=True):
                fact = {
                    "order_key": before.invoice_id,
                    "event_time": before.event_time.isoformat(),
                    "kind": before.kind,
                    "customer_known": before.customer_id is not None,
                    "retained": _measures(before),
                    "collapsed": _measures(after),
                }
                stream.write(json.dumps(fact, ensure_ascii=False, separators=(",", ":")) + "\n")
    except BaseException:
        output_path.unlink(missing_ok=True)
        raise
    return dataset.data_status


def main(arguments: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    try:
        options = parser.parse_args(arguments)
    except SystemExit as error:
        return 0 if error.code == 0 else 64
    try:
        status = write_facts(options.input, options.output)
    except (Exception, KeyboardInterrupt) as error:
        print(f"duplicate sensitivity failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    return 2 if status == "quarantined" else 0


if __name__ == "__main__":
    sys.exit(main())
