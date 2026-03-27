# Supported data types

This page is the **authoritative list** of column types pydantable accepts on
`DataFrameModel` / `DataFrame[Schema]` fields, uses in **expression typing**
(Rust AST), and maps to **Rust schema descriptors** (scalar:
`{"base": ..., "nullable": ...}` plus optional homogeneous **`"literals": [...]`** for
`typing.Literal[...]` columns, nested struct:
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
| `typing.Literal[...]` | `from typing import Literal` | Same `base` as the homogeneous value kind (`str` / `int` / `bool`); descriptor includes **`literals`** list (all-`str`, all-`int`, or all-`bool` parameters only; **no mixing**). Stored like plain `str` / `int` / `bool`. **`filter(col == "x")`** is rejected at **expression build time** if `"x"` is not in the `Literal` set. |
| `IPv4Address` | `from ipaddress import IPv4Address` | `ipv4` (Polars **Utf8**; canonical IPv4 string) |
| `IPv6Address` | `from ipaddress import IPv6Address` | `ipv6` (Polars **Utf8**; canonical IPv6 string) |
| `WKB` | `from pydantable.types import WKB` | `wkb` (Polars **Binary**; `bytes` subclass with Pydantic validation; same limited `Expr` surface as **`bytes`**) |

Use these types in **Pydantic field annotations** on `DataFrameModel` subclasses (or
`Schema` models for `DataFrame[Schema]`). Runtime cell values must be instances
compatible with that annotation (validated by Pydantic under default ingest, i.e. `trusted_mode="off"` or omitted).

## Nullability

Nullable columns use the same base types with **`None`** as a cell value:

- `Optional[T]`, `T | None`, or `Union[T, None]` with `T` from the table above.

Expression results and filter conditions follow SQL-like null rules; see
`INTERFACE_CONTRACT.md`.

## Typed strings (`Annotated[str, ...]`)

**`Annotated[str, ...]`** is accepted as a column annotation: metadata is stripped for
the Rust dtype (logical **`str`** / Polars **Utf8**), while **Pydantic** on
`collect()` / `RowModel` still applies your constraints (for example
**`Annotated[str, pydantic.HttpUrl]`**). Match other projects’ “newtype string” patterns
without a separate Rust `base` type.

### Practical notes (1.2.0 scalars)

**`typing.Literal[...]`**

- Parameters must be **all `str`**, **all `int`**, or **all `bool`** (no mixing).
- **`filter(col == literal)`** is checked when the expression is built: the constant must
  appear in the column’s `Literal` set (same idea for `!=`).
- Nullable columns use **`Literal[...] | None`** (or `Optional[...]`); `None` is not a
  `Literal` member for those checks.

**IP addresses (`IPv4Address`, `IPv6Address`)**

- Column input may be **strings**; pydantable normalizes to **`ipaddress`** instances
  under default validation.
- In **`Expr` comparisons**, wrap addresses on the RHS with **`IPv4Address(...)`** /
  **`IPv6Address(...)`**. The Python expression builder types the RHS literal as **`str`**
  otherwise, which does not satisfy the IP column dtype in **`compare_op`** (even though
  the Rust core allows some IP/string combinations in other paths).

**`WKB`**

- `pydantable.types.WKB` is a **`bytes`** subclass with Pydantic integration for row models.
- Use **`WKB(b"...")`** (or equal `WKB` cells) on the RHS of **`==`** / **`!=`** for
  reliable typing. **`Expr.binary_len()`** is implemented for **`bytes`** columns; for
  **`WKB`**, use **`df.col.cast(bytes).binary_len()`** today.

**`Annotated[str, ...]`**

- For **`collect()`** / **`RowModel`**, Pydantic enforces your metadata (length, URL,
  regex, etc.). Invalid cells may only surface at **materialization** time unless the
  constructor path validates early—see tests in **`tests/test_extended_scalar_dtypes_v12.py`**.

```python
from __future__ import annotations

import ipaddress
from typing import Annotated, Literal

from pydantic import Field, HttpUrl

from pydantable import DataFrameModel
from pydantable.types import WKB


class Row(DataFrameModel):
    env: Literal["dev", "prod"]
    host: ipaddress.IPv4Address
    geom: WKB | None
    url: Annotated[str, HttpUrl]


df = Row(
    {
        "env": ["dev"],
        "host": ["192.168.0.1"],
        "geom": [WKB(b"\x01\x02")],
        "url": ["https://example.com"],
    }
)
needle = ipaddress.IPv4Address("192.168.0.1")
subset = df.filter(df.env == "dev").filter(df.host == needle)
```

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

**Arrow-native `map` columns (0.15.0+):** You may pass a PyArrow **`Array`** or **`ChunkedArray`** typed as **`map<string|large_string, V>`** for a **`dict[str, T]`** field. Each row is converted to a Python **`dict`** (or **`None`** if the map cell is null). **Non-string** Arrow map keys are rejected. With **`trusted_mode='strict'`**, scalar **`T`** is checked against the Arrow **value** type; nested **`T`** (list, struct, map) uses best-effort acceptance—prefer **`trusted_mode='off'`** when you need full Pydantic validation of nested cells. **Heterogeneous keys** (e.g. **`dict[int, T]`**) are not supported.

**0.17.0 — Map expressions after Arrow ingest:** After ingest, the column is a normal **`dict[str, T]`** map for planning and **`Expr`**: **`map_get(key)`** yields **null** when the key is absent (or the whole map cell is null); **`map_contains_key(key)`** is boolean. Same rules apply to maps built from Python dict cells. See **`tests/test_pyarrow_map_ingest.py`** (`test_arrow_map_ingest_then_map_get_and_contains`).

**0.18.0 / 0.19.0 / 0.20.0 — Non-string map keys:** **`dict[int, T]`**, other non-string Python keys, and Arrow **`map`** types whose keys are not **`string` / `large_string`** remain **unsupported**. That work is explicitly deferred; see {doc}`ROADMAP` **Later**. **0.20.0** does not change map dtypes or ingest.

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

The following are **not implemented today**; follow project priorities in `ROADMAP.md`.

| Planned category | Examples | Notes |
|------------------|----------|--------|
| **Richer geospatial** | e.g. GeoJSON column type, CRS metadata | **`WKB`** covers opaque binary geometry today; heavier GIS scope is deferred. |

**Already shipped (scalar columns):** primitives in the table above, including
**`typing.Literal[...]`** (homogeneous str/int/bool), **`ipaddress.IPv4Address`** /
**`IPv6Address`**, **`pydantable.types.WKB`**, **`uuid.UUID`**, **`decimal.Decimal`**, concrete
**`enum.Enum`**, **`datetime`**, **`date`**, **`time`**, **`timedelta`**, **`bytes`**, plus
homogeneous **`dict[str, T]`** map columns, **Homogeneous `list[T]`** columns, **`explode()`**,
list **`Expr`** helpers, and **`unnest()`** on **struct** columns (see `ROADMAP.md`).

## Runtime column payloads (Python)

For **default** construction, columns are typically `dict[str, list]` with one
Python value per row per column; **struct** columns use a list of **dicts** (or
compatible row objects). Lists may be plain `list`, `tuple`, or
`numpy.ndarray` (see `schema.validate_columns_strict`).

**0.16.0 — PyArrow `Table` / `RecordBatch`:** When **`pyarrow`** is installed, you may pass a **`pa.Table`** or **`RecordBatch`** as **`DataFrame` / `DataFrameModel`** input. It is converted to **`dict[str, list]`** via **`to_pylist()`** per column (copies), then the usual validation runs. **`pydantable.materialize_parquet`** and **`materialize_ipc`** ( **`pydantable.io`**) produce the same shape for file/bytes sources. **`DataFrame.to_arrow()`** goes the other way: execute the plan as for **`to_dict()`**, then **`pyarrow.Table.from_pydict`**. Supported cell types are those that already round-trip through list materialization (scalars, **JSON-friendly** nested shapes per engine limits); exotic Arrow extension types may not map cleanly—validate with **`trusted_mode='off'`** when in doubt.

With **`trusted_mode="shape_only"`** or **`"strict"`**, trusted bulk paths may pass **NumPy**, **PyArrow**,
or a **Polars `DataFrame`** as documented in {doc}`EXECUTION` and {doc}`PERFORMANCE`;
scalar dtypes must still match the schema. **`trusted_mode`** on **`DataFrame` /
`DataFrameModel`** selects **`shape_only`** vs **`strict`** checks (**0.11.0**; **0.12.0** extends **`strict`** to nested list / dict / struct shapes on
Polars and columnar Python paths; **0.13.0** adds **`strict`** dtype checks for **PyArrow**
`Array` / `ChunkedArray` columns and accepts concrete Arrow array classes as trusted
buffers). See **`schema.validate_columns_strict`** for the low-level API (**`validate_elements`** remains a bridge for direct callers).

See {doc}`DATAFRAMEMODEL` (“Trusted ingest”). The legacy **`validate_data`** constructor argument was removed in **0.15.0**.

**0.14.0 — `shape_only` dtype drift:** when **`trusted_mode="shape_only"`**, pydantable
may emit **`pydantable.DtypeDriftWarning`** if a column would be **rejected under
`strict`** (e.g. string cells for an **`int`** field). Set environment variable
**`PYDANTABLE_SUPPRESS_SHAPE_ONLY_DRIFT_WARNINGS=1`** to silence these warnings in
noisy pipelines.

## See also

- {doc}`DATAFRAMEMODEL` — `DataFrameModel` and row vs column inputs
- {doc}`INTERFACE_CONTRACT` — null semantics, joins, reshape constraints
- `pydantable-core/src/dtype.rs` — mapping from Python annotations to internal dtypes
