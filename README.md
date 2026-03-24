# PydanTable

[![CI](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml?query=branch%3Amain)
[![Documentation](https://readthedocs.org/projects/pydantable/badge/?version=latest)](https://pydantable.readthedocs.io/en/latest/)
[![PyPI version](https://img.shields.io/pypi/v/pydantable)](https://pypi.org/project/pydantable/)
[![Python versions](https://img.shields.io/pypi/pyversions/pydantable)](https://pypi.org/project/pydantable/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Typed dataframe transformations for FastAPI and Pydantic services, backed by a Rust execution core (Polars inside the native extension).**

**Current release: 0.19.0** · Python **3.10+**

---

## At a glance

- **Schemas first:** Pydantic field annotations define column types, nullability (`T | None`), and which expressions are legal. Many mistakes are caught when you build the `Expr`, not only when you run the query.
- **Two entry styles:** `DataFrameModel` (SQLModel-like whole-table class with a generated row model) or `DataFrame[YourSchema](data)` with any Pydantic `BaseModel` schema.
- **Polars-shaped API:** `select`, `with_columns`, `filter`, `join`, `group_by`, windows, reshape helpers — semantics are documented in the [interface contract](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html), not guaranteed identical to Polars on every edge case.
- **Optional extras:** `pydantable[polars]` for `to_polars()`; `pydantable[arrow]` for `read_parquet` / `read_ipc`, `to_arrow` / `ato_arrow`, and `pa.Table` / `RecordBatch` constructors.
- **Optional façades:** `pydantable.pandas` and `pydantable.pyspark` swap naming/imports; execution stays the same in-process core (not a real Spark or pandas backend).
- **Service-ready:** Sync and async materialization (`collect`, `to_dict`, `acollect`, `ato_dict`, …), [FastAPI](https://pydantable.readthedocs.io/en/latest/FASTAPI.html) patterns, and trusted ingest modes for bulk JSON or Arrow.
- **REPL / logging:** `repr(df)` on **`DataFrame`** and **`DataFrameModel`** shows the parameterized class, schema type, and column dtypes (wide tables truncate with `… and N more`). Row counts are omitted—logical plans can change length without materializing; use **`collect()`** / **`to_dict()`** when you need data. **Jupyter / VS Code notebooks:** **`_repr_html_()`** renders a bounded **HTML table** preview (no **`polars`** required). Details: [Execution](https://pydantable.readthedocs.io/en/latest/EXECUTION.html).

---

## Documentation

The **canonical manual** is on Read the Docs: **[https://pydantable.readthedocs.io/en/latest/](https://pydantable.readthedocs.io/en/latest/)**

| Topic | Read the Docs |
|--------|----------------|
| **Home / overview** | [Documentation home](https://pydantable.readthedocs.io/en/latest/index.html) |
| **Changelog & versions** | [Changelog](https://pydantable.readthedocs.io/en/latest/changelog.html) · [Versioning (0.x)](https://pydantable.readthedocs.io/en/latest/VERSIONING.html) |
| **`DataFrameModel`** (inputs, transforms, collisions, materialization) | [DataFrameModel](https://pydantable.readthedocs.io/en/latest/DATAFRAMEMODEL.html) |
| **Column types** (scalars, structs, `list[T]`, maps, trusted ingest) | [Supported data types](https://pydantable.readthedocs.io/en/latest/SUPPORTED_TYPES.html) |
| **FastAPI** (routers, bodies, async, multipart) | [FastAPI integration](https://pydantable.readthedocs.io/en/latest/FASTAPI.html) |
| **Execution** (`collect`, `to_dict`, `to_polars`, `to_arrow`, async, `repr`) | [Execution](https://pydantable.readthedocs.io/en/latest/EXECUTION.html) |
| **Semantics** (nulls, joins, windows, reshape) | [Interface contract](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html) |
| **Roadmap** (shipped **0.19.0**, **Planned 0.20.0** UX + notebooks, path to **v1.0.0**) | [Roadmap](https://pydantable.readthedocs.io/en/latest/ROADMAP.html) |
| **Why not Polars alone?** | [Why not just use Polars?](https://pydantable.readthedocs.io/en/latest/WHY_NOT_POLARS.html) |
| **Pandas-style API** (`pydantable.pandas`) | [Pandas UI](https://pydantable.readthedocs.io/en/latest/PANDAS_UI.html) |
| **PySpark-style API** (`pydantable.pyspark`) | [PySpark UI](https://pydantable.readthedocs.io/en/latest/PYSPARK_UI.html) · [Parity matrix](https://pydantable.readthedocs.io/en/latest/PYSPARK_PARITY.html) |
| **Polars parity** | [Scorecard](https://pydantable.readthedocs.io/en/latest/PARITY_SCORECARD.html) · [Workflows](https://pydantable.readthedocs.io/en/latest/POLARS_WORKFLOWS.html) · [Transformation roadmap](https://pydantable.readthedocs.io/en/latest/POLARS_TRANSFORMATIONS_ROADMAP.html) |
| **Contributors** | [Developer guide](https://pydantable.readthedocs.io/en/latest/DEVELOPER.html) |
| **Architecture plan** | [Plan document](https://pydantable.readthedocs.io/en/latest/pydantable_plan.html) |
| **Python API (autodoc)** | [API reference](https://pydantable.readthedocs.io/en/latest/api/index.html) |

---

## Install

```bash
pip install pydantable
```

**Optional dependencies** (same package, feature extras):

```bash
pip install 'pydantable[polars]'   # to_polars()
pip install 'pydantable[arrow]'  # read_parquet/read_ipc, to_arrow, Table/RecordBatch constructors
```

**From a git checkout** you need a Rust toolchain and a build of the extension (e.g. [Maturin](https://www.maturin.rs/)):

```bash
pip install .
# editable: maturin develop --manifest-path pydantable-core/Cargo.toml
```

Full setup, `make check-full`, and release notes: [Developer guide](https://pydantable.readthedocs.io/en/latest/DEVELOPER.html).

---

## Quick start

```python
from pydantable import DataFrameModel

class User(DataFrameModel):
    id: int
    age: int | None

df = User({"id": [1, 2], "age": [20, None]})
df2 = df.with_columns(age2=df.age * 2)
df3 = df2.select("id", "age2")
df4 = df3.filter(df3.age2 > 10)

# Columnar dict (good for JSON APIs)
print(df4.to_dict())
# {'age2': [40], 'id': [1]}

# List of Pydantic row models (default collect)
for row in df4.collect():
    print(row.id, row.age2)
```

**Materialization:** [`collect()`](https://pydantable.readthedocs.io/en/latest/DATAFRAMEMODEL.html) → `list` of row models; [`to_dict()`](https://pydantable.readthedocs.io/en/latest/EXECUTION.html) / `collect(as_lists=True)` → `dict[str, list]`; `to_polars()` / `to_arrow()` when the matching extra is installed. **Async:** `acollect`, `ato_dict`, `ato_polars`, `ato_arrow` offload blocking work from the event loop ([Execution](https://pydantable.readthedocs.io/en/latest/EXECUTION.html), [FastAPI](https://pydantable.readthedocs.io/en/latest/FASTAPI.html)).

**Alternate import styles** (same engine):

```python
from pydantable.pandas import DataFrameModel as PandasDataFrameModel
from pydantable.pyspark import DataFrameModel as PySparkDataFrameModel
from pydantable import DataFrameModel as DefaultDataFrameModel
```

More examples: [FastAPI](https://pydantable.readthedocs.io/en/latest/FASTAPI.html), [Polars-style workflows](https://pydantable.readthedocs.io/en/latest/POLARS_WORKFLOWS.html).

**Validation policy:** Constructors validate strictly by default. For messy row lists, `ignore_errors=True` plus `on_validation_errors=callback` receives failed rows (`row_index`, `row`, Pydantic `errors`). Trusted bulk paths use `trusted_mode` (`off` / `shape_only` / `strict`). Details: [DataFrameModel](https://pydantable.readthedocs.io/en/latest/DATAFRAMEMODEL.html), [Supported types](https://pydantable.readthedocs.io/en/latest/SUPPORTED_TYPES.html).

---

## Expression & API surface

Typed **`Expr`** builds a Rust AST. Highlights:

- **Globals in `select`:** `global_sum`, `global_mean`, `global_count`, `global_min`, `global_max`, `global_row_count()` (row count). PySpark façade: `F.count()` with no argument = row count.
- **Windows:** `row_number`, `rank`, `dense_rank`, `window_sum`, `window_mean`, `window_min`, `window_max`, `lag`, `lead` with `Window.partitionBy(...).orderBy(..., nulls_last=...)`; framed `rowsBetween` / `rangeBetween` where supported ([window semantics](https://pydantable.readthedocs.io/en/latest/WINDOW_SQL_SEMANTICS.html)).
- **Temporal & strings:** `strptime`, `unix_timestamp`, `cast` to `date`/`datetime`, `dt_*` parts, `strip` / `lower` / `upper`, `str_replace`, `strip_prefix` / `suffix` / `chars`, list helpers (`list_len`, `list_get`, …).
- **Maps (string keys):** `map_len`, `map_get`, `map_contains_key`, `map_keys`, `map_values`, `map_entries`, `map_from_entries`, `element_at`; `binary_len` for `bytes` columns.

PySpark-named wrappers: `pydantable.pyspark.sql.functions` mirrors much of the above ([parity table](https://pydantable.readthedocs.io/en/latest/PYSPARK_PARITY.html)).

---

## Recent releases

**0.19.0** — Pre-1.0 **documentation consolidation**: [Versioning (0.x)](https://pydantable.readthedocs.io/en/latest/VERSIONING.html), [interface contract](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html) cross-links, parity/README/index refresh for the **0.19 → 1.0** path, [PERFORMANCE](https://pydantable.readthedocs.io/en/latest/PERFORMANCE.html) benchmark spot-check note, release-hygiene alignment with CI; **group_by** tests sort output where row order is not guaranteed (stable **`pytest-xdist`**). No new **`Expr`** or PySpark façade methods.

**0.18.0** — Clearer **Polars** error context for **`group_by().agg()`**; explicit deferral of non-string map keys ([Supported types](https://pydantable.readthedocs.io/en/latest/SUPPORTED_TYPES.html), [Roadmap](https://pydantable.readthedocs.io/en/latest/ROADMAP.html)); parity/roadmap doc refresh (no new façade APIs); Hypothesis smoke for **join** / **group_by**.

**0.17.0** — Tighter docs and tests for **`map_get` / `map_contains_key`** after PyArrow **`map<utf8, …>`** ingest; more **`pyspark.sql.functions`** thin wrappers (`str_replace`, `regexp_replace`, `strip_*`, `strptime`, `binary_len`, `list_*`). Non-string map keys (`dict[int, T]`, etc.) remain future work ([Roadmap](https://pydantable.readthedocs.io/en/latest/ROADMAP.html) **Later**).

**0.16.x** — Arrow interchange (`read_parquet` / `read_ipc`, `to_arrow` / `ato_arrow`, Table/RecordBatch constructors), FastAPI multipart and deployment docs, map-column arithmetic `TypeError` fix, `DataFrame[Schema](pa.Table)` constructor fix.

Older highlights: **0.15.0** async materialization and Arrow map ingest; **0.14.0** window null ordering and FastAPI `TestClient` coverage. Full history: [Changelog](https://pydantable.readthedocs.io/en/latest/changelog.html).

---

## Development

From a clone with `.venv` and `pip install -e ".[dev]"` plus a built extension:

```bash
make check-full              # Ruff, mypy, Rust fmt / clippy / tests
PYTHONPATH=python pytest -q  # integration tests (see DEVELOPER.md)
```

Rust tests need the Makefile `PYO3_PYTHON` / `PYTHONPATH` wiring: `make rust-test`. Details: [Developer guide](https://pydantable.readthedocs.io/en/latest/DEVELOPER.html).

---

## License

MIT
