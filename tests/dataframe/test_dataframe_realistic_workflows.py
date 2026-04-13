"""Multi-step pipelines on domain-shaped tables (see ``tests._support.scenarios``)."""

from __future__ import annotations

from typing import Any

from pydantable.expressions import Literal, coalesce

from tests._support.scenarios import (
    Customer,
    LineItem,
    PromoHit,
    customers_retail_payload,
    golden_inner_join_customers_rowcount,
    golden_left_join_line_to_promo_rowcount,
    golden_revenue_by_customer_paid_shipped,
    golden_top5_line_totals_desc,
    line_items_retail_payload,
    promo_hits_payload,
)
from tests._support.tables import assert_table_eq_sorted


def _line_total_expr(df: Any) -> Any:
    bps = coalesce(df.discount_bps, Literal(value=0))
    sub = df.qty * df.unit_price_cents
    numer = sub * (Literal(value=10000) - bps)
    return (numer / Literal(value=10000)).floor()


def test_revenue_by_customer_filter_with_columns_groupby() -> None:
    payload = line_items_retail_payload()
    df = LineItem(payload)
    gold = golden_revenue_by_customer_paid_shipped(payload)

    paid_or_shipped = (df.status == "paid") | (df.status == "shipped")
    filtered = df.filter(paid_or_shipped & df.customer_id.is_not_null())
    enriched = filtered.with_columns(line_total_cents=_line_total_expr(filtered))
    out = enriched.group_by("customer_id").agg(
        revenue=("sum", "line_total_cents"),
    ).collect(as_lists=True)

    expected = {
        "customer_id": list(gold.keys()),
        "revenue": [gold[k] for k in gold],
    }
    assert_table_eq_sorted(out, expected, keys=["customer_id"])


def test_left_join_duplicate_promo_keys_expands_rowcount() -> None:
    lines = LineItem(line_items_retail_payload())
    promos = PromoHit(promo_hits_payload())
    joined = lines.join(promos, on="customer_id", how="left")
    out = joined.collect(as_lists=True)
    want = golden_left_join_line_to_promo_rowcount(
        line_items_retail_payload(),
        promo_hits_payload(),
    )
    assert len(out["line_id"]) == want


def test_inner_join_customers_matches_expected_matches() -> None:
    lines = LineItem(line_items_retail_payload())
    cust = Customer(customers_retail_payload())
    joined = lines.join(cust, on="customer_id", how="inner")
    out = joined.collect(as_lists=True)
    want = golden_inner_join_customers_rowcount(
        line_items_retail_payload(),
        customers_retail_payload(),
    )
    assert len(out["line_id"]) == want


def test_top_line_totals_sort_head() -> None:
    payload = line_items_retail_payload()
    df = LineItem(payload)
    want = golden_top5_line_totals_desc(payload)

    out = (
        df.with_columns(line_total_cents=_line_total_expr(df))
        .sort("line_total_cents", descending=True)
        .head(5)
        .collect(as_lists=True)
    )
    got_sorted = sorted(out["line_total_cents"], reverse=True)
    assert got_sorted == want
