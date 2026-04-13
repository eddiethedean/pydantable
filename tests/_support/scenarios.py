"""Domain-shaped columnar payloads and Python oracles for integration-style tests."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from pydantable import DataFrameModel


class LineItem(DataFrameModel):
    line_id: int
    customer_id: int | None
    sku: str
    qty: int
    unit_price_cents: int
    status: str
    discount_bps: int | None  # basis points; 100 = 1%


class Customer(DataFrameModel):
    customer_id: int
    country: str
    email: str | None


class PromoHit(DataFrameModel):
    customer_id: int
    campaign: str


def line_items_retail_payload() -> dict[str, list[Any]]:
    """~26 rows: duplicate customers, null FK, unicode SKUs, mixed order statuses."""
    return {
        "line_id": list(range(1, 27)),
        "customer_id": [
            101,
            101,
            102,
            103,
            None,
            101,
            104,
            102,
            105,
            101,
            103,
            104,
            106,
            102,
            105,
            101,
            107,
            None,
            103,
            106,
            104,
            105,
            102,
            107,
            101,
            108,
        ],
        "sku": [
            "SKU-alpha-1",
            "  trim-me  ",
            "WIDGET-BASIC",
            "ギフト-箱",
            "ORPHAN-1",
            "SKU-alpha-1",
            "HEAVY-CRATE",
            "WIDGET-BASIC",
            "DISC-10",
            "SKU-alpha-1",
            "ギフト-箱",
            "HEAVY-CRATE",
            "NEW-ARRIVAL",
            "WIDGET-BASIC",
            "DISC-10",
            "SKU-alpha-1",
            "LIMITED-ED",
            "ORPHAN-2",
            "ギフト-箱",
            "NEW-ARRIVAL",
            "HEAVY-CRATE",
            "DISC-10",
            "WIDGET-BASIC",
            "LIMITED-ED",
            "SKU-alpha-1",
            "CLEARANCE",
        ],
        "qty": [
            1, 2, 3, 1, 1, 4, 1, 2, 5, 1, 2, 1, 1, 1, 2, 3, 1, 1, 1, 2, 1, 1, 4, 1, 2, 6
        ],
        "unit_price_cents": [
            1000,
            2500,
            500,
            12000,
            999,
            1000,
            45000,
            500,
            200,
            1000,
            12000,
            45000,
            3000,
            500,
            200,
            1000,
            8000,
            500,
            12000,
            3000,
            45000,
            200,
            500,
            8000,
            1000,
            150,
        ],
        "status": [
            "paid",
            "paid",
            "shipped",
            "paid",
            "cancelled",
            "pending",
            "paid",
            "shipped",
            "paid",
            "paid",
            "returned",
            "paid",
            "paid",
            "shipped",
            "paid",
            "paid",
            "paid",
            "cancelled",
            "paid",
            "shipped",
            "paid",
            "paid",
            "shipped",
            "paid",
            "paid",
            "paid",
        ],
        "discount_bps": [
            None,
            0,
            500,
            None,
            None,
            None,
            None,
            None,
            1000,
            None,
            None,
            None,
            None,
            None,
            1000,
            None,
            2000,
            None,
            None,
            None,
            None,
            1000,
            None,
            None,
            None,
            None,
        ],
    }


def customers_retail_payload() -> dict[str, list[Any]]:
    return {
        "customer_id": [101, 102, 103, 104, 105, 106, 107, 108],
        "country": ["US", "CA", "JP", "US", "DE", "US", "FR", "UK"],
        "email": [
            "ada@example.com",
            None,
            "tanaka@例.jp",
            "bob@example.com",
            "greta@example.de",
            "chip@example.com",
            "léa@example.fr",
            "  noreply@shop.test  ",
        ],
    }


def promo_hits_payload() -> dict[str, list[Any]]:
    """Multiple rows per customer_id to exercise non-unique join keys."""
    return {
        "customer_id": [101, 101, 102, 103, 103, 104, 105, 106],
        "campaign": [
            "SPRING",
            "EMAIL",
            "SPRING",
            "LOYALTY",
            "LOYALTY",
            "WINBACK",
            "SPRING",
            "NEW",
        ],
    }


def _line_total_cents_row(
    qty: int,
    unit_price_cents: int,
    discount_bps: int | None,
) -> int:
    d = 0 if discount_bps is None else int(discount_bps)
    # Match SQL-style: apply discount in basis points to subtotal.
    subtotal = qty * unit_price_cents
    return (subtotal * (10_000 - d)) // 10_000


def golden_revenue_by_customer_paid_shipped(
    payload: dict[str, list[Any]],
) -> dict[int, int]:
    """Sum line totals for rows with status in paid/shipped and non-null customer_id."""
    totals: dict[int, int] = defaultdict(int)
    n = len(payload["line_id"])
    for i in range(n):
        st = payload["status"][i]
        if st not in ("paid", "shipped"):
            continue
        cid = payload["customer_id"][i]
        if cid is None:
            continue
        lt = _line_total_cents_row(
            int(payload["qty"][i]),
            int(payload["unit_price_cents"][i]),
            payload["discount_bps"][i],
        )
        totals[int(cid)] += lt
    return dict(totals)


def golden_left_join_line_to_promo_rowcount(
    line_payload: dict[str, list[Any]],
    promo_payload: dict[str, list[Any]],
) -> int:
    """Expected row count for left join on customer_id.

    For each left row: if key is null, one row; else if the right has ``k`` matches,
    ``k`` rows; if ``k == 0``, one row (unmatched, null-filled right).
    """
    promo_by_cid: dict[int, int] = defaultdict(int)
    for cid in promo_payload["customer_id"]:
        promo_by_cid[int(cid)] += 1

    total = 0
    n = len(line_payload["line_id"])
    for i in range(n):
        cid = line_payload["customer_id"][i]
        if cid is None:
            total += 1
            continue
        k = promo_by_cid.get(int(cid), 0)
        total += k if k > 0 else 1
    return total


def golden_inner_join_customers_rowcount(
    line_payload: dict[str, list[Any]],
    customer_payload: dict[str, list[Any]],
) -> int:
    cust_ids = set(int(x) for x in customer_payload["customer_id"])
    n = 0
    for i in range(len(line_payload["line_id"])):
        cid = line_payload["customer_id"][i]
        if cid is not None and int(cid) in cust_ids:
            n += 1
    return n


def golden_top5_line_totals_desc(
    payload: dict[str, list[Any]],
) -> list[int]:
    """Top 5 line_total_cents values among all rows (any status), descending."""
    totals: list[int] = []
    n = len(payload["line_id"])
    for i in range(n):
        totals.append(
            _line_total_cents_row(
                int(payload["qty"][i]),
                int(payload["unit_price_cents"][i]),
                payload["discount_bps"][i],
            )
        )
    return sorted(totals, reverse=True)[:5]
