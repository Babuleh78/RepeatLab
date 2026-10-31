"""Aggregate classified invoice lines without changing their event times."""

from collections.abc import Collection, Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, localcontext

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


def _exact_total(events: list[CleanEvent]) -> Decimal:
    """Sum integer coefficients at a shared exponent, bypassing Decimal context."""
    terms: list[tuple[int, int]] = []
    for event in events:
        price = event.unit_price.as_tuple()
        # Validated prices are finite and positive, so their exponent is an int.
        assert isinstance(price.exponent, int)
        coefficient = int(Decimal((0, price.digits, 0)))
        terms.append((coefficient * event.quantity, price.exponent))
    exponent = min(exponent for _, exponent in terms)
    total = sum(coefficient * 10 ** (scale - exponent) for coefficient, scale in terms)
    with localcontext() as context:
        context.traps[InvalidOperation] = True
        try:
            return Decimal((int(total < 0), Decimal(abs(total)).as_tuple().digits, exponent))
        except InvalidOperation as error:
            raise ValueError("exact order total is not representable as Decimal") from error


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
    invoices: dict[str, list[CleanEvent]] = {}
    contents: dict[tuple[object, ...], list[str]] = {}
    seen_ids: set[str] = set()
    repeated_ids: set[str] = set()
    for event in events:
        if event.event_id in seen_ids:
            repeated_ids.add(event.event_id)
        seen_ids.add(event.event_id)
        invoices.setdefault(event.invoice_id, []).append(event)
        content = (
            event.invoice_id,
            event.customer_id,
            event.event_time,
            event.kind,
            event.stock_code,
            event.quantity,
            event.unit_price,
        )
        contents.setdefault(content, []).append(event.event_id)
    if repeated_ids:
        raise ValueError(f"duplicate event_id values: {sorted(repeated_ids)!r}")

    excluded_codes = frozenset(service_codes)
    orders: list[AggregatedOrder] = []
    service_event_ids: list[str] = []
    quarantines: list[QuarantinedInvoice] = []
    for invoice_id, invoice_events in sorted(invoices.items()):
        invoice_events.sort(key=lambda event: event.event_id)
        conflicting_fields = tuple(
            name
            for name in ("customer_id", "event_time", "kind")
            if len({getattr(event, name) for event in invoice_events}) != 1
        )
        if conflicting_fields:
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
                total_amount=_exact_total(product_events),
            )
        )

    return AggregationResult(
        orders=tuple(orders),
        excluded_service_event_ids=tuple(sorted(service_event_ids)),
        possible_duplicate_event_ids=tuple(
            sorted(tuple(sorted(ids)) for ids in contents.values() if len(ids) > 1)
        ),
        quarantined_invoices=tuple(quarantines),
    )
