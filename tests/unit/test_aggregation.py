"""Hand-checked contracts for aggregating classified invoice lines."""

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal, localcontext
from itertools import permutations
from typing import Any, cast

import pytest

from repeatlab.aggregation import CleanEvent, aggregate_orders

HISTORY_TIME = datetime(2010, 12, 10, 10)


def event(event_id: str = "e1", **changes: object) -> CleanEvent:
    return replace(
        CleanEvent(
            event_id=event_id,
            invoice_id="invoice-1",
            customer_id="A",
            event_time=HISTORY_TIME,
            kind="sale",
            stock_code="product-1",
            quantity=1,
            unit_price=Decimal("1"),
        ),
        # Tests deliberately pass invalid field types across this boundary.
        **cast(Any, changes),
    )


def test_product_lines_make_one_order_with_exact_totals() -> None:
    result = aggregate_orders(
        (
            event("e1", quantity=2, unit_price=Decimal("1.25")),
            event("e2", stock_code="product-2", unit_price=Decimal("0.335")),
        )
    )

    assert len(result.orders) == 1
    order = result.orders[0]
    assert (
        order.invoice_id,
        order.customer_id,
        order.event_time,
        order.kind,
        order.line_count,
        order.quantity,
        order.stock_code_count,
        order.total_amount,
    ) == ("invoice-1", "A", HISTORY_TIME, "sale", 2, 3, 2, Decimal("2.835"))
    assert result.excluded_service_event_ids == ()
    assert result.possible_duplicate_event_ids == ()
    assert result.quarantined_invoices == ()


def test_service_codes_are_explicit_and_exclusions_are_auditable() -> None:
    events = (
        event("product"),
        event("delivery", stock_code="DELIVERY", quantity=5, unit_price=Decimal("2")),
        event("service-only", invoice_id="invoice-2", stock_code="DELIVERY"),
    )

    unfiltered = aggregate_orders(events)
    filtered = aggregate_orders(events, service_codes={"DELIVERY"})

    assert [(order.invoice_id, order.total_amount) for order in unfiltered.orders] == [
        ("invoice-1", Decimal("11")),
        ("invoice-2", Decimal("1")),
    ]
    assert len(filtered.orders) == 1
    order = filtered.orders[0]
    assert (order.line_count, order.quantity, order.stock_code_count, order.total_amount) == (
        1,
        1,
        1,
        Decimal("1"),
    )
    assert filtered.excluded_service_event_ids == ("delivery", "service-only")


def test_equal_contents_with_distinct_ids_are_counted_and_flagged() -> None:
    result = aggregate_orders((event("z"), event("a"), event("m")))

    assert result.orders[0].line_count == 3
    assert result.orders[0].quantity == 3
    assert result.orders[0].stock_code_count == 1
    assert result.orders[0].total_amount == Decimal("3")
    assert result.possible_duplicate_event_ids == (("a", "m", "z"),)


@pytest.mark.parametrize(
    "changes",
    [
        {"invoice_id": "invoice-2"},
        {"customer_id": "B"},
        {"event_time": datetime(2010, 12, 11)},
        {"kind": "cancellation", "quantity": -1},
        {"stock_code": "product-2"},
        {"quantity": 2},
        {"unit_price": Decimal("2")},
    ],
)
def test_possible_duplicates_require_all_business_fields_to_match(
    changes: dict[str, object],
) -> None:
    assert aggregate_orders((event("a"), event("b", **changes))).possible_duplicate_event_ids == ()


@pytest.mark.parametrize("stock_code", ["product-1", "SERVICE"])
def test_repeated_event_id_is_rejected_before_exclusion(stock_code: str) -> None:
    repeated = event("repeated-id", stock_code=stock_code)
    with pytest.raises(ValueError, match="event_id.*repeated-id"):
        aggregate_orders((repeated, repeated), service_codes={"SERVICE"})


def test_repeated_event_id_with_different_contents_is_rejected() -> None:
    with pytest.raises(ValueError, match="event_id.*same-id"):
        aggregate_orders((event("same-id"), event("same-id", invoice_id="invoice-2")))


@pytest.mark.parametrize(
    "changes,conflicting_fields",
    [
        ({"customer_id": "B"}, ("customer_id",)),
        ({"customer_id": None}, ("customer_id",)),
        ({"event_time": datetime(2010, 12, 11)}, ("event_time",)),
        ({"kind": "cancellation", "quantity": -1}, ("kind",)),
        (
            {
                "customer_id": None,
                "event_time": datetime(2010, 12, 11),
                "kind": "cancellation",
                "quantity": -1,
            },
            ("customer_id", "event_time", "kind"),
        ),
    ],
)
def test_conflicting_invoice_is_quarantined_while_valid_invoice_is_emitted(
    changes: dict[str, object], conflicting_fields: tuple[str, ...]
) -> None:
    original = event("a")
    conflicting = event("b", **changes)
    valid = event("valid", invoice_id="valid-invoice")

    result = aggregate_orders((conflicting, valid, original))

    assert [order.invoice_id for order in result.orders] == ["valid-invoice"]
    assert len(result.quarantined_invoices) == 1
    quarantine = result.quarantined_invoices[0]
    assert quarantine.invoice_id == "invoice-1"
    assert quarantine.conflicting_fields == conflicting_fields
    assert quarantine.events == (original, conflicting)


def test_service_filter_does_not_hide_an_invoice_conflict() -> None:
    product = event("a")
    service = event("b", stock_code="SERVICE", customer_id=None)

    result = aggregate_orders((service, product), service_codes={"SERVICE"})

    assert result.orders == ()
    assert result.quarantined_invoices[0].conflicting_fields == ("customer_id",)
    assert result.quarantined_invoices[0].events == (product, service)
    assert result.excluded_service_event_ids == ()


def test_service_and_quarantine_exclusions_preserve_all_original_events() -> None:
    events = (
        event("product"),
        event("service", stock_code="SERVICE"),
        event("bad-product", invoice_id="bad"),
        event("bad-service", invoice_id="bad", stock_code="SERVICE", customer_id=None),
    )

    result = aggregate_orders(events, service_codes={"SERVICE"})

    assert result.orders[0].line_count == 1
    assert result.excluded_service_event_ids == ("service",)
    assert tuple(row.event_id for row in result.quarantined_invoices[0].events) == (
        "bad-product",
        "bad-service",
    )
    assert (
        sum(order.line_count for order in result.orders)
        + len(result.excluded_service_event_ids)
        + sum(len(invoice.events) for invoice in result.quarantined_invoices)
    ) == len(events)


def test_unknown_customer_invoice_is_preserved_without_filling_an_id() -> None:
    result = aggregate_orders((event("a", customer_id=None), event("b", customer_id=None)))

    assert result.orders[0].customer_id is None
    assert result.orders[0].line_count == 2
    assert result.orders[0].total_amount == Decimal("2")
    assert result.quarantined_invoices == ()


@pytest.mark.parametrize(
    "quantities,prices,kind,expected",
    [
        ((3,), ("0.335",), "sale", "1.005"),
        (
            (3, 2),
            ("10000000000000000000000000000.0000000000000000001", "0.0000000000000000001"),
            "sale",
            "30000000000000000000000000000.0000000000000000005",
        ),
        ((3, 2), ("1E-50", "2E-50"), "sale", "7E-50"),
        ((-3, -1), ("0.335", "0.1"), "cancellation", "-1.105"),
        ((10**40,), ("1.25",), "sale", "1.25E+40"),
    ],
)
def test_money_is_exact_under_restrictive_caller_decimal_context(
    quantities: tuple[int, ...], prices: tuple[str, ...], kind: str, expected: str
) -> None:
    events = tuple(
        event(str(index), quantity=quantity, unit_price=Decimal(price), kind=kind)
        for index, (quantity, price) in enumerate(zip(quantities, prices, strict=True))
    )

    with localcontext() as context:
        context.prec = 2
        context.Emax = 9
        context.Emin = -9
        for signal in context.traps:
            context.traps[signal] = True
        context.clear_flags()
        for permuted in permutations(events):
            order = aggregate_orders(permuted).orders[0]
            assert order.total_amount == Decimal(expected)
            assert order.quantity == sum(quantities)
        assert not any(context.flags.values())
        assert context.prec == 2


@pytest.mark.parametrize(
    "changes",
    [
        {"event_id": None},
        {"invoice_id": 7},
        {"customer_id": 123},
        {"event_time": "2010-12-10"},
        {"event_time": HISTORY_TIME.replace(tzinfo=UTC)},
        {"kind": "return"},
        {"stock_code": 123},
        {"quantity": True},
        {"quantity": 1.0},
        {"quantity": 0},
        {"quantity": -1},
        {"kind": "cancellation", "quantity": 1},
        {"unit_price": 0.335},
        {"unit_price": "0.335"},
        {"unit_price": Decimal("0")},
        {"unit_price": Decimal("-1")},
        {"unit_price": Decimal("NaN")},
        {"unit_price": Decimal("sNaN")},
        {"unit_price": Decimal("Infinity")},
        {"unit_price": Decimal("-Infinity")},
    ],
)
def test_invalid_event_fields_are_rejected_without_coercion(changes: dict[str, object]) -> None:
    with pytest.raises((TypeError, ValueError)):
        replace(event(), **cast(Any, changes))


def test_empty_input_returns_empty_diagnostics_and_orders() -> None:
    result = aggregate_orders(iter(()))

    assert result.orders == ()
    assert result.excluded_service_event_ids == ()
    assert result.possible_duplicate_event_ids == ()
    assert result.quarantined_invoices == ()


def test_generator_input_is_consumed_once() -> None:
    result = aggregate_orders(event(str(index)) for index in range(3))

    assert result.orders[0].line_count == 3
    assert result.orders[0].total_amount == Decimal("3")
