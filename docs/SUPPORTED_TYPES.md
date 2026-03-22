# Supported data types

This page is the **authoritative list** of scalar types pydantable accepts on
`DataFrameModel` / `DataFrame[Schema]` fields, uses in **expression typing**
(Rust AST), and maps to **Rust schema descriptors** (`{"base": ..., "nullable": ...}`).

For behavior contracts (nulls, joins, ordering), see `INTERFACE_CONTRACT.md`.

## Scalar base types (columns + expressions)

Each column in your schema is one **scalar** Python type from this set:

| Python type | Import | Descriptor `base` (Rust) |
|-------------|--------|---------------------------|
| `int` | built-in | `int` |
| `float` | built-in | `float` |
| `bool` | built-in | `bool` |
| `str` | built-in | `str` |
| `datetime` | `from datetime import datetime` | `datetime` |
| `date` | `from datetime import date` | `date` |
| `timedelta` | `from datetime import timedelta` | `duration` |

Use these types in **Pydantic field annotations** on `DataFrameModel` subclasses (or
`Schema` models for `DataFrame[Schema]`). Runtime cell values must be instances
compatible with that annotation (validated by Pydantic when `validate_data=True`).

## Nullability

Nullable columns use the same base types with **`None`** as a cell value:

- `Optional[T]`, `T | None`, or `Union[T, None]` with `T` from the table above.

Expression results and filter conditions follow SQL-like null rules; see
`INTERFACE_CONTRACT.md`.

## Expression typing (Rust)

The native core builds a typed expression tree for `Expr` (column references,
literals, arithmetic, comparisons, etc.). **Invalid combinations fail when the
expression is built**, not only at execution time. Base types match the table
above (including temporal types).

## Not supported as schema column types

These are **out of scope** for the current schema system:

- Nested Pydantic models, `list[...]`, `dict[...]`, or arbitrary objects as **per-cell** values
- `explode` / `unnest` on non-scalar columns (see `INTERFACE_CONTRACT.md`; list/struct columns are not modeled yet)

## Future / planned types (roadmap direction)

The following are **not implemented today**; they are the intended direction for
richer schemas and APIs. Ordering and timing follow project priorities (see
`ROADMAP.md`); this list is **not** a commitment to ship every item.

| Planned category | Examples | Notes |
|------------------|----------|--------|
| **Nested Pydantic models** | `Address`, `Money(amount: Decimal, currency: str)` as a field type on `DataFrameModel` | Struct/object columns: one **nested model instance** per row per column; JSON round-trip, validation, and Rust descriptors for nested shapes. |
| **Homogeneous list columns** | `list[int]`, `list[str]`, `list[float]` | Enables **`explode`**, list-aware **`unnest`**, and element-wise ops where defined. |
| **Maps / dict-like cells** | `dict[str, T]` with a fixed value type `T`, or a dedicated **map** dtype | Semi-structured columns; stricter than arbitrary JSON `dict`. |
| **Enums** | `enum.Enum` or `Literal[...]`-backed fields | Discrete categoricals with stable string/value mapping. |
| **Decimal / money** | `Decimal` | Exact numeric; important for billing and financial APIs. |
| **Binary** | `bytes` | Opaque blobs; execution surface may stay limited (pass-through, equality, length). |
| **UUID** | `UUID` | Common in APIs; often maps to Polars/Utf8 or dedicated extension dtype. |
| **Time-of-day** | `time` | Distinct from `datetime` and `timedelta`; useful for schedules. |
| **Geospatial / extension dtypes** | e.g. WKB, GeoJSON-backed types | Only if there is a clear Polars/Arrow story and API surface. |

**Nested models** are the anchor feature: once struct columns exist, list columns of structs and controlled `explode`/`unnest` paths become realistic. Broader **expression typing** for these types will follow the same pattern as scalars: invalid operations fail at AST build time where possible.

## Runtime column payloads (Python)

For **default** construction, columns are typically `dict[str, list]` with one
Python value per row per column. Lists may be plain `list`, `tuple`, or
`numpy.ndarray` (see `schema.validate_columns_strict`).

With **`validate_data=False`**, trusted bulk paths may pass **NumPy**, **PyArrow**,
or a **Polars `DataFrame`** as documented in `EXECUTION.md` and `PERFORMANCE.md`;
scalar dtypes must still match the schema.

## See also

- `DATAFRAMEMODEL.md` — `DataFrameModel` and row vs column inputs
- `INTERFACE_CONTRACT.md` — null semantics, joins, reshape constraints
- `pydantable-core/src/dtype.rs` — mapping from Python annotations to internal dtypes
