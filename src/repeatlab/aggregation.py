"""Aggregate classified invoice lines without changing their event times."""

from collections.abc import Collection, Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import MAX_PREC, Context, Decimal, InvalidOperation, Overflow, localcontext
from os import environ

from repeatlab.temporal import Order, OrderKind


@dataclass(frozen=True, slots=True)
class CleanEvent:
    """One classified source line with a stable, unique event identifier."""

    event_id: str
    invoice_id: str
    customer_id: str | None
    event_time: datetime
    kind: OrderKind
    stock_code: str
    quantity: int
    unit_price: Decimal

    def __post_init__(self) -> None:
        for name in ("event_id", "invoice_id", "stock_code"):
            if not isinstance(getattr(self, name), str):
                raise TypeError(f"{name} must be a string")
        if self.customer_id is not None and not isinstance(self.customer_id, str):
            raise TypeError("customer_id must be a string or None")
        if not isinstance(self.event_time, datetime):
            raise TypeError("event_time must be a datetime")
        if self.event_time.tzinfo is not None:
            raise ValueError("event_time must be naive, in the source log's time scale")
        if self.kind not in ("sale", "cancellation"):
            raise ValueError("kind must be sale or cancellation")
        if not isinstance(self.quantity, int) or isinstance(self.quantity, bool):
            raise TypeError("quantity must be an integer, not bool")
        if (self.kind == "sale" and self.quantity <= 0) or (
            self.kind == "cancellation" and self.quantity >= 0
        ):
            raise ValueError("quantity must be positive for sale and negative for cancellation")
        if not isinstance(self.unit_price, Decimal):
            raise TypeError("unit_price must be Decimal")
        if not self.unit_price.is_finite() or self.unit_price <= 0:
            raise ValueError("unit_price must be finite and positive")


@dataclass(frozen=True, slots=True)
class AggregatedOrder:
    """One consistent invoice after configured service lines are excluded."""

    invoice_id: str
    customer_id: str | None
    event_time: datetime
    kind: OrderKind
    line_count: int
    quantity: int
    stock_code_count: int
    total_amount: Decimal

    def to_temporal_order(self) -> Order:
        """Expose one invoice as one event to the existing snapshot builder."""
        return Order(self.customer_id, self.event_time, self.kind)


@dataclass(frozen=True, slots=True)
class QuarantinedInvoice:
    """An inconsistent invoice, retaining every original event for audit."""

    invoice_id: str
    conflicting_fields: tuple[str, ...]
    events: tuple[CleanEvent, ...]


@dataclass(frozen=True, slots=True)
class AggregationResult:
    """Orders and deterministic diagnostics for every excluded source line."""

    orders: tuple[AggregatedOrder, ...]
    excluded_service_event_ids: tuple[str, ...]
    possible_duplicate_event_ids: tuple[tuple[str, ...], ...]
    quarantined_invoices: tuple[QuarantinedInvoice, ...]


_AMOUNT_LIMIT = "REPEATLAB_MAX_ORDER_AMOUNT_DIGITS"


class OrderAmountLimitError(ValueError):
    """The configured limit cannot accommodate this exact order coefficient."""


def _amount_digit_limit() -> int:
    value = environ.get(_AMOUNT_LIMIT, "100000")
    if not value.isascii() or not value.isdecimal() or len(value) > len(str(MAX_PREC)):
        raise OrderAmountLimitError(f"{_AMOUNT_LIMIT} must be a positive decimal precision")
    digits = int(value)
    if not 1 <= digits <= MAX_PREC:
        raise OrderAmountLimitError(f"{_AMOUNT_LIMIT} must be a positive decimal precision")
    return digits


def _exact_total(events: list[CleanEvent], arithmetic: Context) -> Decimal:
    """Sum same-sign products at their preferred exponent, within the output digit budget."""
    # Relative exponents keep compact values at either end of Decimal's exponent range.
    exponent: int | None = None
    total = Decimal(0)
    for event in events:
        price = event.unit_price.as_tuple()
        assert isinstance(price.exponent, int)
        if exponent is None:
            exponent = price.exponent
        elif price.exponent < exponent:
            shift = exponent - price.exponent
            if total.adjusted() + shift >= arithmetic.prec:
                raise OrderAmountLimitError(f"Exact order total exceeds {_AMOUNT_LIMIT}")
            total = arithmetic.scaleb(total, shift)
            exponent = price.exponent
        shift = price.exponent - exponent
        if (
            len(price.digits) + shift > arithmetic.prec
            or event.quantity.bit_length() > 4 * arithmetic.prec
        ):
            raise OrderAmountLimitError(f"Exact order total exceeds {_AMOUNT_LIMIT}")
        coefficient = Decimal((0, price.digits, shift))
        try:
            product = arithmetic.multiply(coefficient, Decimal(event.quantity))
            total = arithmetic.add(total, product)
        except Overflow as error:
            raise OrderAmountLimitError(f"Exact order total exceeds {_AMOUNT_LIMIT}") from error
    final = total.as_tuple()
    assert exponent is not None
    with localcontext() as context:
        context.traps[InvalidOperation] = True
        try:
            return Decimal((final.sign, final.digits, exponent))
        except InvalidOperation as error:
            raise ValueError("exact order total is not representable as Decimal") from error


def _duplicate_groups(invoice_events: list[CleanEvent]) -> list[tuple[str, ...]]:
    if len(invoice_events) == 1:
        return []
    contents: dict[tuple[object, ...], list[str]] = {}
    for event in invoice_events:
        content = (
            event.customer_id,
            event.event_time,
            event.kind,
            event.stock_code,
            event.quantity,
            event.unit_price,
        )
        contents.setdefault(content, []).append(event.event_id)
    return [tuple(sorted(ids)) for ids in contents.values() if len(ids) > 1]


def aggregate_orders(
    events: Iterable[CleanEvent], *, service_codes: Collection[str] = ()
) -> AggregationResult:
    """Aggregate invoices, preserving every conflict and service exclusion.

    Duplicate event identifiers reject the entire input, including excluded
    rows. Consistency is checked before service filtering: a conflicting
    invoice is quarantined in full, while other invoices are still processed.
    Equal business contents with different identifiers are retained and flagged
    across all input events. Money is exact and independent of Decimal context.
    An unrepresentable total raises ValueError without changing caller context.
    """
    digits = _amount_digit_limit()
    arithmetic = Context(
        prec=digits, Emin=0, Emax=digits - 1, traps=[InvalidOperation, Overflow], clamp=0
    )
    invoices: dict[str, list[CleanEvent]] = {}
    seen_ids: set[str] = set()
    repeated_ids: set[str] = set()
    for event in events:
        if event.event_id in seen_ids:
            repeated_ids.add(event.event_id)
        seen_ids.add(event.event_id)
        invoices.setdefault(event.invoice_id, []).append(event)
    if repeated_ids:
        raise ValueError(f"duplicate event_id values: {sorted(repeated_ids)!r}")
    seen_ids.clear()

    excluded_codes = frozenset(service_codes)
    orders: list[AggregatedOrder] = []
    service_event_ids: list[str] = []
    quarantines: list[QuarantinedInvoice] = []
    duplicate_groups: list[tuple[str, ...]] = []
    for invoice_id in sorted(invoices):
        invoice_events = invoices.pop(invoice_id)
        duplicate_groups.extend(_duplicate_groups(invoice_events))
        conflicting_fields = tuple(
            name
            for name in ("customer_id", "event_time", "kind")
            if len({getattr(event, name) for event in invoice_events}) != 1
        )
        if conflicting_fields:
            invoice_events.sort(key=lambda event: event.event_id)
            quarantines.append(
                QuarantinedInvoice(invoice_id, conflicting_fields, tuple(invoice_events))
            )
            continue

        product_events: list[CleanEvent] = []
        for event in invoice_events:
            if event.stock_code in excluded_codes:
                service_event_ids.append(event.event_id)
            else:
                product_events.append(event)
        if not product_events:
            continue
        first = product_events[0]
        orders.append(
            AggregatedOrder(
                invoice_id=invoice_id,
                customer_id=first.customer_id,
                event_time=first.event_time,
                kind=first.kind,
                line_count=len(product_events),
                quantity=sum(event.quantity for event in product_events),
                stock_code_count=len({event.stock_code for event in product_events}),
                total_amount=_exact_total(product_events, arithmetic),
            )
        )

    return AggregationResult(
        orders=tuple(orders),
        excluded_service_event_ids=tuple(sorted(service_event_ids)),
        possible_duplicate_event_ids=tuple(sorted(duplicate_groups)),
        quarantined_invoices=tuple(quarantines),
    )
