"""Aggregation guards against row multiplication and temporal distortions."""

from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from itertools import permutations
from typing import Any, cast

import pytest

from repeatlab.aggregation import CleanEvent, aggregate_orders
from repeatlab.temporal import build_snapshot

SNAPSHOT = datetime(2011, 1, 1)
OBSERVED_UNTIL = datetime(2011, 2, 1)


def event(event_id: str = "e1", **changes: object) -> CleanEvent:
    return replace(
        CleanEvent(
            event_id=event_id,
            invoice_id="invoice-1",
            customer_id="A",
            event_time=datetime(2010, 12, 10),
            kind="sale",
            stock_code="product-1",
            quantity=1,
            unit_price=Decimal("0.335"),
        ),
        **cast(Any, changes),
    )


def test_synthetic_join_row_multiplication_is_rejected_at_aggregation_boundary() -> None:
    events = (event("source-row"),)
    product_reference = (("product-1", "first"), ("product-1", "duplicate-key"))
    joined = tuple(
        source_event
        for source_event in events
        for stock_code, _ in product_reference
        if source_event.stock_code == stock_code
    )

    assert len(joined) == 2
    with pytest.raises(ValueError, match="event_id.*source-row"):
        aggregate_orders(joined)


def test_all_results_and_diagnostics_are_invariant_to_input_permutations() -> None:
    events = (
        event("b"),
        event("a"),
        event("c", invoice_id="service-only", stock_code="SERVICE"),
        event("e", invoice_id="bad", customer_id="B"),
        event("d", invoice_id="bad", customer_id=None),
        event("f", invoice_id="another", customer_id=None),
    )
    expected = aggregate_orders(events, service_codes={"SERVICE"})
    assert [order.invoice_id for order in expected.orders] == ["another", "invoice-1"]
    assert expected.possible_duplicate_event_ids == (("a", "b"),)
    assert expected.excluded_service_event_ids == ("c",)
    assert expected.quarantined_invoices[0].conflicting_fields == ("customer_id",)
    assert [row.event_id for row in expected.quarantined_invoices[0].events] == ["d", "e"]

    for permuted in permutations(events):
        assert aggregate_orders(permuted, service_codes={"SERVICE"}) == expected


def test_multiple_product_lines_increase_snapshot_frequency_by_one() -> None:
    result = aggregate_orders((event("a"), event("b", stock_code="product-2")))

    rows = build_snapshot(
        (order.to_temporal_order() for order in result.orders),
        snapshot_time=SNAPSHOT,
        observed_until=OBSERVED_UNTIL,
    )

    assert [(row.customer_id, row.history_order_count, row.label) for row in rows] == [("A", 1, 0)]


def test_unknown_customer_is_preserved_by_adapter_and_excluded_from_snapshot() -> None:
    result = aggregate_orders((event(customer_id=None),))
    order = result.orders[0].to_temporal_order()

    assert order.customer_id is None
    assert build_snapshot((order,), snapshot_time=SNAPSHOT, observed_until=OBSERVED_UNTIL) == ()


def test_future_cancellation_remains_separate_and_does_not_rewrite_history() -> None:
    history = (event("a", quantity=3),)
    cancellation = event(
        "return",
        invoice_id="cancellation-1",
        event_time=datetime(2011, 1, 5),
        kind="cancellation",
        quantity=-3,
    )
    before = aggregate_orders(history)
    after = aggregate_orders((*history, cancellation))

    assert [(order.invoice_id, order.total_amount) for order in after.orders] == [
        ("cancellation-1", Decimal("-1.005")),
        ("invoice-1", Decimal("1.005")),
    ]
    snapshots = [
        build_snapshot(
            (order.to_temporal_order() for order in result.orders),
            snapshot_time=SNAPSHOT,
            observed_until=OBSERVED_UNTIL,
        )
        for result in (before, after)
    ]
    assert snapshots[0] == snapshots[1]
    assert [(row.history_order_count, row.label) for row in snapshots[1]] == [(1, 0)]
