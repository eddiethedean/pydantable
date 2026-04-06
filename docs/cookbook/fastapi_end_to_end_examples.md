# FastAPI end-to-end examples

These examples are longer, **copy/paste-friendly** patterns for realistic services.
For the shortest runnable app, start with {doc}`/GOLDEN_PATH_FASTAPI`. For reference
tables and routing patterns, see {doc}`/FASTAPI`.

## Example 1: Router + multi-table body — revenue by country

Real services often receive **more than one related table** (partner feed, staged
upload, or denormalized batch). Validate each as `list[...RowModel]`, build two
`DataFrameModel` instances, then **join → fill nulls → aggregate**. Return
**`collect()`**; FastAPI applies **`response_model`** to validate and serialize
the response (no need to wrap rows in `model_validate` yourself).

```python
from fastapi import APIRouter, FastAPI
from pydantic import BaseModel

from pydantable import DataFrameModel

router = APIRouter(prefix="/api/v1/reports", tags=["reports"])


class OrderLineDF(DataFrameModel):
    order_id: int
    user_id: int
    amount: float | None


class UserDimDF(DataFrameModel):
    user_id: int
    country: str


class OrderUserDF(OrderLineDF):
    country: str | None


class CountryRevenueDF(DataFrameModel):
    country: str | None
    total: float | None
    n_orders: int


class SalesByCountryBody(BaseModel):
    """Two datasets in one JSON payload (common for bulk ingest APIs)."""

    orders: list[OrderLineDF.RowModel]
    users: list[UserDimDF.RowModel]


class CountryRevenueRow(BaseModel):
    country: str
    total: float
    n_orders: int


app = FastAPI()
app.include_router(router)


@router.post("/sales-by-country", response_model=list[CountryRevenueRow])
def sales_by_country(body: SalesByCountryBody):
    orders = OrderLineDF(body.orders)
    users = UserDimDF(body.users)
    joined = orders.join_as(
        other=users,
        model=OrderUserDF,
        on=[orders.col.user_id],
        how="left",
    )
    filled = joined.fill_null(0.0, subset=["amount"])
    rolled = filled.group_by_agg_as(
        CountryRevenueDF,
        keys=[filled.col.country],
        total=("sum", filled.col.amount),
        n_orders=("count", filled.col.order_id),
    ).sort("country")
    return rolled.collect()
```

Example request (abbreviated):

```json
{
  "orders": [
    {"order_id": 1, "user_id": 10, "amount": 50.0},
    {"order_id": 2, "user_id": 10, "amount": null},
    {"order_id": 3, "user_id": 20, "amount": 20.0}
  ],
  "users": [
    {"user_id": 10, "country": "US"},
    {"user_id": 20, "country": "CA"}
  ]
}
```

Example response (sorted for readability):

```json
[
  {"country": "CA", "total": 20.0, "n_orders": 1},
  {"country": "US", "total": 50.0, "n_orders": 2}
]
```

## Example 2: Query parameters + `collect()` — ranked adults with a cap

Use **`Query`** for bounds that belong on the URL (versioning, caching, client
SDKs). Chain **`filter` → `sort` → `head`** then **`collect()`** for the
response body.

```python
from typing import Annotated

from fastapi import FastAPI, Query
from pydantic import BaseModel

from pydantable import DataFrameModel


class UserDF(DataFrameModel):
    id: int
    age: int | None


class AdultRow(BaseModel):
    id: int
    age: int | None


app = FastAPI()


@app.post("/users/adults", response_model=list[AdultRow])
def adults(
    rows: list[UserDF.RowModel],
    min_age: Annotated[int, Query(ge=0, le=120)] = 18,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
):
    df = UserDF(rows)
    ranked = df.filter(df.col.age >= min_age).sort("age", descending=True).head(limit)
    return ranked.collect()
```

With body `[{"id": 1, "age": 22}, {"id": 2, "age": null}, {"id": 3, "age": 15}, {"id": 4, "age": 30}]`
and default query params, the handler returns adults sorted by `age` descending, at most
`limit` rows — for example `[{"id": 4, "age": 30}, {"id": 1, "age": 22}]` when `limit=2`.

## Example 3: Derived column + filter — top lines by computed total

`with_columns` adds **`line_total`**; subsequent **`filter`**, **`sort`**, and
**`head`** use the **derived** dataframe (`df2`, `df3`, …) so column references
match the migrated schema.

```python
from typing import Annotated

from fastapi import FastAPI, Query
from pydantic import BaseModel

from pydantable import DataFrameModel


class LineItemDF(DataFrameModel):
    sku: str
    qty: int
    unit_price: float


class LineItemWithTotalDF(LineItemDF):
    line_total: float


class LineTotalDF(DataFrameModel):
    sku: str
    qty: int
    line_total: float


class LineTotalRow(BaseModel):
    sku: str
    qty: int
    line_total: float


app = FastAPI()


@app.post("/procurement/top-lines", response_model=list[LineTotalRow])
def top_lines(
    rows: list[LineItemDF.RowModel],
    min_line_total: Annotated[float, Query(ge=0.0)] = 0.0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
):
    df = LineItemDF(rows)
    df2 = df.with_columns_as(LineItemWithTotalDF, line_total=df.col.qty * df.col.unit_price)
    df3 = df2.filter(df2.col.line_total >= min_line_total).sort(
        "line_total", descending=True
    )
    out = df3.head(limit).select_as(LineTotalDF, df3.col.sku, df3.col.qty, df3.col.line_total)
    return out.collect()
```

For `[{"sku": "A", "qty": 2, "unit_price": 10.0}, {"sku": "B", "qty": 1, "unit_price": 5.0}]`
with `min_line_total=10` and `limit=1`, **`collect()`** yields one row: **`A`** with
**`line_total` 20.0**.

