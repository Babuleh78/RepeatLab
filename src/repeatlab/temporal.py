"""Minimal point-in-time contract for already validated orders."""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

type OrderKind = Literal["sale", "cancellation"]


def _require_naive(value: datetime, name: str) -> None:
    if value.tzinfo is not None:
        raise ValueError(f"{name} must be naive, in the source log's time scale")


@dataclass(frozen=True, slots=True)
class Order:
    """One validated order or cancellation, not an individual product line."""

    customer_id: str | None
    event_time: datetime
    kind: OrderKind

    def __post_init__(self) -> None:
        _require_naive(self.event_time, "event_time")
        if self.kind not in ("sale", "cancellation"):
            raise ValueError("kind must be sale or cancellation")


@dataclass(frozen=True, slots=True)
class CustomerSnapshot:
    """An eligible customer; censored rows are excluded from binary modelling."""

    customer_id: str
    snapshot_time: datetime
    label_end: datetime
    history_order_count: int
    label: Literal[0, 1] | None
    status: Literal["complete", "censored"]


def build_snapshot(
    orders: Iterable[Order],
    *,
    snapshot_time: datetime,
    observed_until: datetime,
    horizon_days: int = 30,
    lookback_days: int = 180,
) -> tuple[CustomerSnapshot, ...]:
    """Build history counts and labels with explicit half-open time windows.

    Only known customers with a sale in [t - lookback_days, t) are eligible.
    The target window is [t, t + horizon_days). ``observed_until`` is an
    exclusive observation boundary supplied by the caller, never inferred
    from the last order. Every incomplete horizon gets a censored status and
    no binary label, even when a positive order has already been observed.
    """
    _require_naive(snapshot_time, "snapshot_time")
    _require_naive(observed_until, "observed_until")
    if horizon_days <= 0 or lookback_days <= 0:
        raise ValueError("horizon_days and lookback_days must be positive")
    if observed_until < snapshot_time:
        raise ValueError("observed_until must not precede snapshot_time")

    history_start = snapshot_time - timedelta(days=lookback_days)
    label_end = snapshot_time + timedelta(days=horizon_days)
    complete = observed_until >= label_end
    history_counts: dict[str, int] = {}
    target_customers: set[str] = set()

    for order in orders:
        customer_id = order.customer_id
        if customer_id is None or order.kind != "sale" or order.event_time >= observed_until:
            continue
        if history_start <= order.event_time < snapshot_time:
            history_counts[customer_id] = history_counts.get(customer_id, 0) + 1
        elif snapshot_time <= order.event_time < label_end:
            target_customers.add(customer_id)

    return tuple(
        CustomerSnapshot(
            customer_id=customer_id,
            snapshot_time=snapshot_time,
            label_end=label_end,
            history_order_count=history_counts[customer_id],
            label=(1 if customer_id in target_customers else 0) if complete else None,
            status="complete" if complete else "censored",
        )
        for customer_id in sorted(history_counts)
    )
