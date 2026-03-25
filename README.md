# PydanTable

[![CI](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml?query=branch%3Amain)
[![Documentation](https://readthedocs.org/projects/pydantable/badge/?version=latest)](https://pydantable.readthedocs.io/en/latest/)
[![PyPI version](https://img.shields.io/pypi/v/pydantable)](https://pypi.org/project/pydantable/)
[![Python versions](https://img.shields.io/pypi/pyversions/pydantable)](https://pypi.org/project/pydantable/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Typed dataframe transformations for FastAPI and Pydantic services, backed by a Rust execution core (Polars inside the native extension).**

**Current release: 0.23.0** · Python **3.10–3.13**

---

## At a glance

- **Schemas first:** Pydantic field annotations define column types, nullability (`T | None`), and which expressions are legal. Many mistakes are caught when you build the `Expr`, not only when you run the query.
- **Two entry styles:** `DataFrameModel` (SQLModel-like whole-table class with a generated row model) or `DataFrame[YourSchema](data)` with any Pydantic `BaseModel` schema.
- **Polars-shaped API:** `select`, `with_columns`, `filter`, `join`, `group_by`, windows, reshape helpers — semantics are documented in the [interface contract](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html), not guaranteed identical to Polars on every edge case.
- **I/O:** **Primary** — **`DataFrame` / `DataFrameModel`** — lazy **`read_*` / `write_*`**, eager **`materialize_*` / `fetch_sql`**, Polars options via **`scan_kwargs`** / **`write_kwargs`**. **Secondary** — **`pydantable.io`** — lazy **`ScanFileRoot`**, raw column dicts, **`export_*`**, **`write_sql`**, URL/object-store helpers when you do not construct a typed frame first. [I/O overview](https://pydantable.readthedocs.io/en/latest/IO_OVERVIEW.html) (per-format guides), [Execution](https://pydantable.readthedocs.io/en/latest/EXECUTION.html) (lazy vs collect).
- **Optional extras:** `pydantable[polars]` for `to_polars()` and **`export_*`** (**`dict[str, list]` → file**); `pydantable[arrow]` for **buffer/streaming** Parquet/IPC, `to_arrow` / `ato_arrow`, and `pa.Table` / `RecordBatch` constructors; `pydantable[io]` bundles **arrow + polars** for full **I/O**; **`[sql]`** for `fetch_sql` / `write_sql` (**SQLAlchemy**; add **psycopg**, **pymysql**, etc. for your database URLs); **`[cloud]`**, **`[excel]`**, **`[kafka]`**, **`[bq]`**, **`[snowflake]`**, **`[rap]`** for other bridges in **`docs/DATA_IO_SOURCES.md`**.
- **Optional façades:** `pydantable.pandas` and `pydantable.pyspark` swap naming/imports; execution stays the same in-process core (not a real Spark or pandas backend).
- **Service-ready:** Sync and async materialization (`collect`, `to_dict`, `acollect`, `ato_dict`, …), [FastAPI](https://pydantable.readthedocs.io/en/latest/FASTAPI.html) patterns, and trusted ingest modes for bulk JSON or Arrow.
- **REPL / discovery:** `repr(df)` on **`DataFrame`** and **`DataFrameModel`** shows the parameterized class, schema type, and column dtypes (wide tables truncate with `… and N more`). **`columns`**, **`shape`**, **`empty`**, **`dtypes`**, **`info()`**, and **`describe()`** (numeric, bool, str) are on the core API (see [Interface contract](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html) for **`shape`** vs materialized rows). **`Expr`** and **`WhenChain`** have readable **`repr`** for debugging pipelines. Row counts in **`repr`** are omitted—use **`collect()`** / **`to_dict()`** when you need data. **Jupyter / VS Code notebooks:** **`_repr_html_()`** renders a bounded **HTML table** preview (no **`polars`** required); tune via **`pydantable.display`** or **`PYDANTABLE_REPR_HTML_*`**. Details: [Execution](https://pydantable.readthedocs.io/en/latest/EXECUTION.html).

---

## Upgrading

- From **0.22.x → 0.23.0**: **breaking I/O renames** — eager file reads into **`dict[str, list]`** are **`materialize_*` / `amaterialize_*`** (not the old **`read_*` / `aread_*`** names); **lazy** local files use **`read_*` / `aread_*`** ( **`ScanFileRoot`** ); lazy plan output uses **`DataFrame.write_*`**; eager **`dict[str, list]` → file** uses **`export_*` / `aexport_*`**; **`read_sql` / `aread_sql`** → **`fetch_sql` / `afetch_sql`**; HTTP column readers **`read_*_url`** → **`fetch_*_url`**; lazy HTTP Parquet temp-file entry is **`read_parquet_url` / `aread_parquet_url`**. See [Changelog](https://pydantable.readthedocs.io/en/latest/changelog.html) and [Execution](https://pydantable.readthedocs.io/en/latest/EXECUTION.html).
- From **0.21.x → 0.22.0**: no intended breaking changes (see changelog for **0.22.0** I/O additions).

## Documentation

The **canonical manual** is on Read the Docs: **[https://pydantable.readthedocs.io/en/latest/](https://pydantable.readthedocs.io/en/latest/)**

| Topic | Read the Docs |
|--------|----------------|
| **Home / overview** | [Documentation home](https://pydantable.readthedocs.io/en/latest/index.html) |
| **Five-minute tour** | [Quickstart](https://pydantable.readthedocs.io/en/latest/QUICKSTART.html) |
| **Changelog & versions** | [Changelog](https://pydantable.readthedocs.io/en/latest/changelog.html) · [Versioning (0.x)](https://pydantable.readthedocs.io/en/latest/VERSIONING.html) |
| **`DataFrameModel`** (inputs, transforms, collisions, materialization) | [DataFrameModel](https://pydantable.readthedocs.io/en/latest/DATAFRAMEMODEL.html) |
| **Column types** (scalars, structs, `list[T]`, maps, trusted ingest) | [Supported data types](https://pydantable.readthedocs.io/en/latest/SUPPORTED_TYPES.html) |
| **FastAPI** (routers, bodies, async, multipart) | [FastAPI integration](https://pydantable.readthedocs.io/en/latest/FASTAPI.html) |
| **Execution** (`collect`, `to_dict`, `to_polars`, `to_arrow`, async, `repr`) | [Execution](https://pydantable.readthedocs.io/en/latest/EXECUTION.html) |
| **Data I/O** (primary: `DataFrame` / `DataFrameModel`; secondary: `pydantable.io`) | [I/O overview](https://pydantable.readthedocs.io/en/latest/IO_OVERVIEW.html) |
| **Semantics** (nulls, joins, windows, reshape) | [Interface contract](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html) |
| **Roadmap** (shipped **0.20.0** UX / discovery, path to **v1.0.0**) | [Roadmap](https://pydantable.readthedocs.io/en/latest/ROADMAP.html) |
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
pip install 'pydantable[polars]'   # to_polars(); write_parquet/write_* from dict (IPC hop)
pip install 'pydantable[arrow]'    # materialize_parquet/ipc from bytes, to_arrow, Table/RecordBatch
pip install 'pydantable[io]'       # arrow + polars (recommended for mixed file I/O)
pip install 'pydantable[sql]'      # fetch_sql / write_sql (SQLAlchemy + your DB driver)
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

**0.21.0** — **Streamlit ergonomics:** `DataFrame` / `DataFrameModel` implement the **dataframe interchange protocol** (`__dataframe__`) via PyArrow so **`st.dataframe(df)`** can render typed frames directly when `pyarrow` is installed (`pip install 'pydantable[arrow]'`). See [Execution](https://pydantable.readthedocs.io/en/latest/EXECUTION.html) (**interchange**) and [Streamlit integration](https://pydantable.readthedocs.io/en/latest/STREAMLIT.html) for editing fallbacks (`st.data_editor(df.to_arrow())` / `to_polars()`), costs, and limitations.

**0.20.0** — **UX, discovery, docs, and display:** [Quickstart](https://pydantable.readthedocs.io/en/latest/QUICKSTART.html), [Execution](https://pydantable.readthedocs.io/en/latest/EXECUTION.html) (materialization costs, import styles, copy-as / interchange); core **`columns`**, **`shape`**, **`info()`**, **`describe()`** (int/float/bool/str), **`value_counts`**, **`set_display_options`** / **`PYDANTABLE_REPR_HTML_*`**, **`_repr_mimebundle_`**, optional **`PYDANTABLE_VERBOSE_ERRORS`**; **`Expr`** / **`WhenChain`** **`repr`**; PySpark **`show()`** / **`summary()`**; multi-line **`DataFrame`** **`repr`** and **`_repr_html_`**. [Changelog](https://pydantable.readthedocs.io/en/latest/changelog.html), [Interface contract](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html) **Introspection**.

**0.19.0** — Pre-1.0 **documentation consolidation**: [Versioning (0.x)](https://pydantable.readthedocs.io/en/latest/VERSIONING.html), [interface contract](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html) cross-links, parity/README/index refresh for the **0.19 → 1.0** path, [PERFORMANCE](https://pydantable.readthedocs.io/en/latest/PERFORMANCE.html) benchmark spot-check note, release-hygiene alignment with CI; **group_by** tests sort output where row order is not guaranteed (stable **`pytest-xdist`**). No new **`Expr`** or PySpark façade methods.

**0.18.0** — Clearer **Polars** error context for **`group_by().agg()`**; explicit deferral of non-string map keys ([Supported types](https://pydantable.readthedocs.io/en/latest/SUPPORTED_TYPES.html), [Roadmap](https://pydantable.readthedocs.io/en/latest/ROADMAP.html)); parity/roadmap doc refresh (no new façade APIs); Hypothesis smoke for **join** / **group_by**.

**0.17.0** — Tighter docs and tests for **`map_get` / `map_contains_key`** after PyArrow **`map<utf8, …>`** ingest; more **`pyspark.sql.functions`** thin wrappers (`str_replace`, `regexp_replace`, `strip_*`, `strptime`, `binary_len`, `list_*`). Non-string map keys (`dict[int, T]`, etc.) remain future work ([Roadmap](https://pydantable.readthedocs.io/en/latest/ROADMAP.html) **Later**).

**0.16.x** — Arrow interchange (eager Parquet/IPC readers, `to_arrow` / `ato_arrow`, Table/RecordBatch constructors), FastAPI multipart and deployment docs, map-column arithmetic `TypeError` fix, `DataFrame[Schema](pa.Table)` constructor fix.

**0.23.0** — Out-of-core lazy file roots (**`read_*` / `aread_*`**), **`DataFrame` / `DataFrameModel`** **`write_*`** pipeline output, **`export_*`** for eager dict→file, **`materialize_*` / `fetch_sql`** naming; **`pydantable.io`** remains the module-level mirror; see [I/O overview](https://pydantable.readthedocs.io/en/latest/IO_OVERVIEW.html), [Changelog](https://pydantable.readthedocs.io/en/latest/changelog.html).

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
