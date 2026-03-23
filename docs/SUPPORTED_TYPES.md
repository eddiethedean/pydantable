# Supported data types

This page is the **authoritative list** of column types pydantable accepts on
`DataFrameModel` / `DataFrame[Schema]` fields, uses in **expression typing**
(Rust AST), and maps to **Rust schema descriptors** (scalar:
`{"base": ..., "nullable": ...}`, nested struct:
`{"kind": "struct", "nullable": ..., "fields": [...]}`, or homogeneous list:
`{"kind": "list", "nullable": ..., "inner": <descriptor>}`).

For behavior contracts (nulls, joins, ordering), see `INTERFACE_CONTRACT.md`.

## Scalar base types (columns + expressions)

Each column in your schema is one **scalar** Python type from this set:

| Python type | Import | Descriptor `base` (Rust) |
|-------------|--------|---------------------------|
| `int` | built-in | `int` |
| `float` | built-in | `float` |
| `bool` | built-in | `bool` |
| `str` | built-in | `str` |
| `UUID` | `from uuid import UUID` | `uuid` (stored as Polars **Utf8**; cells round-trip as `uuid.UUID`) |
| `Decimal` | `from decimal import Decimal` | `decimal` (Polars **`Decimal(38, 9)`**; scale fixed at 9) |
| `enum.Enum` subclass | `import enum` | `enum` (Polars **Utf8**; wire value is the member’s `.value` when it is a string, otherwise `str(member)`) |
| `datetime` | `from datetime import datetime` | `datetime` |
| `date` | `from datetime import date` | `date` |
| `time` | `from datetime import time` | `time` (Polars **Time**; wall clock, distinct from `datetime` / `timedelta`) |
| `timedelta` | `from datetime import timedelta` | `duration` |
| `bytes` | built-in | `binary` (Polars **Binary**; limited `Expr` surface) |

Use these types in **Pydantic field annotations** on `DataFrameModel` subclasses (or
`Schema` models for `DataFrame[Schema]`). Runtime cell values must be instances
compatible with that annotation (validated by Pydantic when `validate_data=True`).

## Nullability

Nullable columns use the same base types with **`None`** as a cell value:

- `Optional[T]`, `T | None`, or `Union[T, None]` with `T` from the table above.

Expression results and filter conditions follow SQL-like null rules; see
`INTERFACE_CONTRACT.md`.

## Nested Pydantic models (struct columns)

A column may use a **`Schema` / `BaseModel` subclass** whose fields are themselves
supported column types (scalars or further nested models). Each cell is one nested
model instance: in columnar Python input, use a **list of dicts** (or objects that
validate as the nested model).

Rust maps these to **struct** dtypes and Polars **struct** columns. **Expression
support for struct columns is intentionally limited** (for example, no arithmetic
on whole structs; equality is allowed only when struct shapes match). Prefer
scalar fields or `Expr.struct_field(...)` for field projection.

## Map-like columns (`dict[str, T]`)

String-keyed maps are supported as **`dict[str, T]`** where **`T`** can be scalar,
list, map, struct, or unions with `None` (JSON-like payloads). Cells
are Python `dict` values; the engine stores a logical map as Polars
**`List(Struct{key: str, value: T})`**. Expression support is intentionally small:
**`map_len()`**, **`map_get(key)`**, **`map_contains_key(key)`**, **`map_keys()`**,
**`map_values()`**, and **`map_entries()`** (see *Expression typing* below); not all
Polars map ops are exposed.

**`map_from_entries()`** builds map cells from a list of `{key, value}` entry structs.
If the entry list contains **duplicate string keys**, the **last** entry for that key
wins (Polars map semantics); do not rely on raising an error for duplicates.

## Homogeneous list columns (`list[T]` / `List[T]`)

Use **`list[T]`** (or `typing.List[T]`) where **`T`** is any supported column type
(scalar, nested struct, or another `list[...]`). Each cell is a Python `list` of
values matching `T`. Rust uses `DTypeDesc::List` and Polars **list** columns.

- **`explode(...)`** is supported for list-typed columns: it lowers to Polars
  `explode` and sets the column’s dtype to the inner `T` (**nullable**, even when
  the list column was non-nullable). Multi-column explode requires **equal list
  lengths** on each row; empty list cells produce **no** output rows for that row.
- **`unnest(...)`** is supported for **struct** columns (nested models): fields
  become top-level columns named **`{struct_column}_{field}`** (separator `_`), with
  dtypes and nullability derived from the nested schema.
- **Expressions** on list columns (see below): indexing, membership, length, and
  numeric reductions—**not** element-wise arithmetic between two list columns.
  Use **`explode`** when you need one row per list element.

### Descriptor shape (list)

```json
{
  "kind": "list",
  "nullable": false,
  "inner": {"base": "int", "nullable": false}
}
```

### Descriptor shape (Rust ↔ Python)

Scalars keep the existing flat form for compatibility:

```json
{"base": "int", "nullable": false}
```

Nested models use `kind: "struct"` with ordered fields (each field has a `name`
and recursive `dtype`):

```json
{
  "kind": "struct",
  "nullable": false,
  "fields": [
    {"name": "street", "dtype": {"base": "str", "nullable": false}},
    {"name": "zip", "dtype": {"base": "int", "nullable": true}}
  ]
}
```

When the logical plan changes shape, pydantable rebuilds field types from Rust
descriptors. If a column **name** was already in the current schema and the new
descriptor **matches** that annotation (via
`descriptor_matches_column_annotation` in `python/pydantable/schema.py`), the
**original** Python type is kept—including your nested `Schema` classes. New or
renamed columns, or columns whose dtype changed (for example after `with_columns`
or `fill_null`), still use anonymous `create_model` types where no prior
annotation applies.

## Expression typing (Rust)

The native core builds a typed expression tree for `Expr` (column references,
literals, arithmetic, comparisons, etc.). **Invalid combinations fail when the
expression is built**, not only at execution time. Scalar base types match the table
above (including temporal types); struct columns follow the conservative rules
described above.

### Type-specific `Expr` methods (common operations)

Beyond generic arithmetic and comparisons, the following are supported (see
`Expr` in the Python API):

- **Numeric:** `abs()`, `round(decimals=...)`, `floor()`, `ceil()` on `int` / `float` columns.
- **String:** `strip()`, `upper()`, `lower()`, `str_replace(old, new)` (literal substrings), `strip_prefix`, `strip_suffix`, `strip_chars`, plus `substr`, `char_length`, `concat`.
- **Boolean:** `&`, `|`, `~` for combining boolean-typed expressions.
- **Datetime / date / time:** `dt_year()` … `dt_day()` on **`date`** or **`datetime`**; `dt_hour()` … **`dt_nanosecond()`** on **`datetime`** or **`time`**; **`dt_date()`** on **`datetime`** (calendar `date`). **`strptime(format, to_datetime=...)`** parses **`str`** → **`date`** or **`datetime`**. **`unix_timestamp(unit=...)`** returns epoch **`int`** from **`date`** / **`datetime`**. **`datetime ± timedelta`** and **`date ± timedelta`** use typed binary ops (see Rust `infer_arith_dtype`).
- **Homogeneous lists:** `list_len()`, **`list_get(index)`** (int index; OOB → null), **`list_contains(value)`**, **`list_min()`** / **`list_max()`** / **`list_sum()`** on `list[int]` or `list[float]` (min/max/sum are numeric lists only).
- **Maps (`dict[str, T]`):** **`map_len()`** (number of entries), **`map_get(key)`** (value or null), **`map_contains_key(key)`** (boolean), **`map_keys()`** (list of keys), **`map_values()`** (list of values), **`map_entries()`** (list of `{key, value}` structs); physical encoding is `List(Struct{key, value})`.
- **Binary (`bytes`):** **`binary_len()`** (per-row byte length).
- **Cast:** `cast(T)` supports the usual primitive conversions plus `datetime` → `date` / `str` and `date` → `str`, and **`str` → `date` / `datetime`** using Polars’ string parsing (ISO-8601-shaped strings; behavior follows Polars). For a **fixed format**, use **`strptime(format, ...)`** instead of `cast`.

Temporal part extraction and `dt_date()` on timezone-aware `datetime` values follow Polars’ interpretation of the stored dtype.

## Not supported as schema column types

These are **out of scope** for the current schema system:

- `dict` types with **non-string keys** (`dict[int, ...]`, etc.)
- Arbitrary objects as **per-cell** values (except nested **`BaseModel`** columns,
  homogeneous **`list[T]`**, and **`dict[str, T]`** maps as documented above)

## When unsupported field types fail

- **`DataFrameModel` subclasses**: each field annotation is validated **when the class is defined** (in `__init_subclass__`). Unsupported types (for example bare `list` without an inner type, `dict[int, str]` (non-string keys), `int | str`, or `typing.Any`) raise **`TypeError`** immediately, before `RowModel` is generated. The message lists supported dtypes and points to this page.
- **`DataFrame[Schema]`** with a hand-written **`Schema`** subclass: there is **no**
  class-time check on the `Schema` model (unlike `DataFrameModel`). Unsupported
  annotations surface when you **first construct** `DataFrame[YourSchema](...)`
  (native plan build from `schema_fields()`), or from Pydantic during validation.
  Nested `BaseModel` fields are supported when annotations match what the Rust
  dtype layer accepts (nested scalars or further nested models).

## Future / planned types (roadmap direction)

The following are **not implemented today**; they are the intended direction for
richer schemas and APIs. Ordering and timing follow project priorities (see
`ROADMAP.md`); this list is **not** a commitment to ship every item.

| Planned category | Examples | Notes |
|------------------|----------|--------|
| **Literal / typing-only categoricals** | `Literal["a","b"]` as a distinct dtype (today: use a concrete **`enum.Enum`** subclass). | Narrower validation story than free-form `enum`. |
| **Geospatial / extension dtypes** | e.g. WKB, GeoJSON-backed types | Only if there is a clear Polars/Arrow story and API surface. |

**Already shipped (scalar columns):** **`uuid.UUID`**, **`decimal.Decimal`**, concrete **`enum.Enum`**, **`datetime`**, **`date`**, **`time`**, **`timedelta`**, **`bytes`**, plus homogeneous **`dict[str, T]`** map columns. **Homogeneous `list[T]`** columns, **`explode()`**, list **`Expr`** helpers, and **`unnest()`** on **struct** columns (see `ROADMAP.md`).

## Runtime column payloads (Python)

For **default** construction, columns are typically `dict[str, list]` with one
Python value per row per column; **struct** columns use a list of **dicts** (or
compatible row objects). Lists may be plain `list`, `tuple`, or
`numpy.ndarray` (see `schema.validate_columns_strict`).

With **`validate_data=False`**, trusted bulk paths may pass **NumPy**, **PyArrow**,
or a **Polars `DataFrame`** as documented in {doc}`EXECUTION` and {doc}`PERFORMANCE`;
scalar dtypes must still match the schema. Use **`trusted_mode`** on **`DataFrame` /
`DataFrameModel`** for explicit **`shape_only`** vs **`strict`** checks (introduced in
**0.11.0**; **0.12.0** extends **`strict`** to nested list / dict / struct shapes on
Polars and columnar Python paths; see `schema.validate_columns_strict`); **`validate_data`**
remains a compatibility alias mapped onto those modes.

**Migration (recommended):** replace `validate_data=False` with `trusted_mode="shape_only"`
(or `trusted_mode="strict"` when you want Polars dtype-shape checks on nested columns).
Replace `validate_data=True` with `trusted_mode="off"` or omit the argument. See
{doc}`DATAFRAMEMODEL` (“Trusted ingest”).

## See also

- {doc}`DATAFRAMEMODEL` — `DataFrameModel` and row vs column inputs
- {doc}`INTERFACE_CONTRACT` — null semantics, joins, reshape constraints
- `pydantable-core/src/dtype.rs` — mapping from Python annotations to internal dtypes
