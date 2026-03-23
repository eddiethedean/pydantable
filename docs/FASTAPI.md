# FastAPI Integration Guide

This guide shows FastAPI-oriented patterns using `DataFrameModel` as the
primary API: validated request bodies, typed transforms, and **`collect()`** to
materialize **row lists** for JSON responses.

## Why this matters

For FastAPI services, `pydantable` gives you:

- Pydantic validation at API boundaries (`RowModel` per `DataFrameModel`)
- typed dataframe transformations in handlers or services
- **`collect()`** — `list` of Pydantic models for the **current** projection (ideal for `response_model=list[YourRow]`)
- **`to_dict()`** — `dict[str, list]` when the response is **column-shaped** JSON

**Materialization is synchronous:** `collect()`, `to_dict()`, `collect(as_lists=True)`, and optional `to_polars()` run **blocking** work (Rust + Polars inside the extension). For ASGI stacks, plan on **`asyncio.to_thread`** or similar until **async I/O** lands in **0.15.0** (see [`EXECUTION.md`](EXECUTION.md) and [`ROADMAP.md`](ROADMAP.md)).

## Install

From PyPI (prebuilt wheels include the native extension on supported platforms):

```bash
pip install pydantable
```

From a git checkout, build the extension (for example with [Maturin](https://www.maturin.rs/)):

```bash
pip install .
```

## Trusted ingest (`trusted_mode` and `validate_data`)

For **`DataFrameModel(...)`** and **`DataFrame[Schema](...)`**, ingestion defaults to full per-cell validation (`trusted_mode="off"`). For **trusted** bulk paths (pre-validated upstream data, internal services), use:

| Mode | Meaning |
|------|---------|
| **`trusted_mode="off"`** | Default: full Pydantic validation per cell (same as omitting the argument). |
| **`trusted_mode="shape_only"`** | Skip element validation; still checks column names and row counts. Replaces legacy **`validate_data=False`**. |
| **`trusted_mode="strict"`** | Trusted bulk input plus dtype / nested-shape checks against the schema (including Polars columns). |

**`validate_data=True` / `False`** remains a **compatibility alias** mapped onto those modes; prefer **`trusted_mode`** in new code. Details, nested rules, and runtime payloads: [`DATAFRAMEMODEL.md`](DATAFRAMEMODEL.md), [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md).

```python
from pydantable import DataFrameModel


class UserDF(DataFrameModel):
    id: int
    age: int | None


# Trusted path: caller guarantees rows already match RowModel (e.g. validated earlier in the pipeline).
df = UserDF(
    [{"id": 1, "age": 20}, {"id": 2, "age": None}],
    trusted_mode="shape_only",
)
```

## Column-shaped JSON request bodies

Row lists are natural for OpenAPI (`list[YourRowModel]`). Some clients send **columnar** JSON (`parallel arrays`). Model that with a Pydantic body whose fields are lists, then pass a **`dict[str, list]`** into **`DataFrameModel`**:

```python
from pydantic import BaseModel

from pydantable import DataFrameModel


class UserDF(DataFrameModel):
    id: int
    age: int | None


class UsersColumnarBody(BaseModel):
    """OpenAPI-friendly columnar payload: keys match dataframe columns."""

    id: list[int]
    age: list[int | None]


# In a route: UsersColumnarBody validated by FastAPI, then:
body = UsersColumnarBody(id=[1, 2], age=[20, None])
df = UserDF({"id": body.id, "age": body.age})
```

Same schema rules apply as for columnar constructors in [`DATAFRAMEMODEL.md`](DATAFRAMEMODEL.md) (equal-length columns, types per field).

## Large tables, Polars, Arrow, and trust boundaries

**Default (`trusted_mode="off"`)** is the right choice for **public** or **untrusted** HTTP bodies: every cell is validated against your `RowModel` types before any Rust work runs. Use it when clients can send arbitrary JSON.

**When `trusted_mode` is appropriate in routes**

| Situation | Suggested mode | Notes |
|-----------|----------------|--------|
| Internal service-to-service batch (same org, authN/Z at gateway) | `shape_only` or `strict` | You still enforce column names and row counts; `strict` adds dtype / nested-shape checks for Polars and columnar buffers. |
| Upstream already validated rows (e.g. warehouse export, replay from your own DB) | `shape_only` | Fastest path; assumes wire format matches schema. |
| Polars `DataFrame` or NumPy / **PyArrow** columns built **inside** your stack | `strict` | Checks Polars dtypes (and Python column buffers where implemented) against annotations; see [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md) (“Runtime column payloads”). |

**Who may skip full `RowModel` validation**

- Only code paths where **mis-typed data cannot reach the constructor** without a deliberate privilege break (private workers, ETL you own, or payloads already validated by Pydantic at an earlier hop).
- **Do not** attach `trusted_mode="shape_only"` / `strict` directly to a public upload endpoint that accepts raw user JSON unless another layer has already validated every cell.

**Polars and Arrow in handlers**

- Passing a **Polars `DataFrame`** requires trusted mode (`shape_only` or `strict`); see [`DATAFRAMEMODEL.md`](DATAFRAMEMODEL.md).
- **`strict`** rejects Polars columns whose dtypes do not match the schema (including nested list / struct / map shapes). Prefer **`strict`** when the frame comes from Arrow/Parquet/IPC and you want a safety net without per-cell Pydantic.
- For performance characteristics (validation vs ingest vs `collect()`), see [`PERFORMANCE.md`](PERFORMANCE.md) and [`EXECUTION.md`](EXECUTION.md).

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
    rolled = (
        orders.join(users, on="user_id", how="left")
        .fill_null(0.0, subset=["amount"])
        .group_by("country")
        .agg(total=("sum", "amount"), n_orders=("count", "order_id"))
        .sort("country")
    )
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
    ranked = df.filter(df.age >= min_age).sort("age", descending=True).head(limit)
    return ranked.collect()
```

With body `[{"id": 1, "age": 22}, {"id": 2, "age": null}, {"id": 3, "age": 15}, {"id": 4, "age": 30}]` and default query params, the handler returns adults sorted by `age` descending, at most `limit` rows — here `[{"id": 4, "age": 30}, {"id": 1, "age": 22}]` when `limit=2`.

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
    df2 = df.with_columns(line_total=df.qty * df.unit_price)
    df3 = df2.filter(df2.line_total >= min_line_total).sort(
        "line_total", descending=True
    )
    out = df3.head(limit).select("sku", "qty", "line_total")
    return out.collect()
```

For `[{"sku": "A", "qty": 2, "unit_price": 10.0}, {"sku": "B", "qty": 1, "unit_price": 5.0}]` with `min_line_total=10` and `limit=1`, **`collect()`** yields one row: **`A`** with **`line_total` 20.0**.

## Columnar vs row-shaped responses

- **`to_dict()`** — `dict[str, list]`; one JSON object with parallel arrays.
- **`collect()`** — `list` of Pydantic models for the **current** schema; return it from the handler and let **`response_model`** define OpenAPI and validate the serialized response.
- **`to_dicts()`** — `list[dict]` from row models when you want plain dicts without a separate DTO class.

## Error timing and API safety

In the current Rust-first design:

- invalid expression type combinations fail while building the expression AST
- invalid `filter()` condition types fail before execution
- invalid `select()` projections (for example, empty projections) fail from Rust
  logical-plan validation before execution

That keeps handlers predictable: many errors surface before **`collect()`** runs.

## Practical pattern for larger apps

- **Routes**: Pydantic request/response models; **`collect()`** for row-list responses.
- **Services**: `DataFrameModel` transforms (reusable across HTTP, CLI, workers).
- **Adapters**: load/save column dicts or row lists from databases, queues, or object storage.

This keeps schema and transformation contracts in one typed layer.
