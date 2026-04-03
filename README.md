# PydanTable

[![CI](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml?query=branch%3Amain)
[![Documentation](https://readthedocs.org/projects/pydantable/badge/?version=latest)](https://pydantable.readthedocs.io/en/latest/)
[![PyPI version](https://img.shields.io/pypi/v/pydantable)](https://pypi.org/project/pydantable/)
[![Python versions](https://img.shields.io/pypi/pyversions/pydantable)](https://pypi.org/project/pydantable/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Strongly typed DataFrames for Python, powered by Rust** — Pydantic schemas, Polars-backed execution in the native extension, and an API built for services (including optional FastAPI integration).

**Current release: 1.14.1** — highlights in the [changelog](https://pydantable.readthedocs.io/en/latest/CHANGELOG.html).

## Why PydanTable

- **One schema, many surfaces:** define columns with Pydantic models; use `DataFrameModel` (SQLModel-style) or `DataFrame[YourSchema]`.
- **Typed expressions:** `Expr` and transform chains are validated and lowered in Rust; many errors fail fast at build/plan time.
- **Familiar operations:** `select`, `filter`, `join`, `group_by`, windows, melt/pivot, and pandas-flavored helpers where they help.
- **Flexible materialization:** row models via `collect()` / `rows()`, columnar `dict[str, list]`, or Polars/PyArrow with the right extras.
- **I/O:** lazy `read_*` / `aread_*`, streaming writes, NDJSON/JSON Lines, Parquet, CSV, IPC, HTTP, SQL (SQLModel-first `fetch_sqlmodel` / `write_sqlmodel`, explicit string SQL `fetch_sql_raw` / `write_sql_raw`, or deprecated unprefixed names) — [I/O overview](https://pydantable.readthedocs.io/en/latest/IO_OVERVIEW.html), [IO_SQL](https://pydantable.readthedocs.io/en/latest/IO_SQL.html), [SQLModel roadmap](https://pydantable.readthedocs.io/en/latest/SQLMODEL_SQL_ROADMAP.html), and [decision tree](https://pydantable.readthedocs.io/en/latest/IO_DECISION_TREE.html).
- **JSON & struct columns:** struct expressions, JSON encode/decode helpers, unnest/nested models — [IO_JSON](https://pydantable.readthedocs.io/en/latest/IO_JSON.html), [SELECTORS](https://pydantable.readthedocs.io/en/latest/SELECTORS.html).
- **FastAPI (optional):** shared executor lifespan, NDJSON streaming from `astream()`, OpenAPI-friendly columnar bodies, `register_exception_handlers` (**503** / **400** / **422**). Start with the [golden path](https://pydantable.readthedocs.io/en/latest/GOLDEN_PATH_FASTAPI.html) and [FastAPI guide](https://pydantable.readthedocs.io/en/latest/FASTAPI.html).

## Install

```bash
pip install pydantable
```

Common extras:

```bash
pip install "pydantable[polars]"   # to_polars
pip install "pydantable[arrow]"    # to_arrow / Arrow constructors
pip install "pydantable[io]"       # full file I/O convenience (arrow + polars)
pip install "pydantable[sql]"      # SQLModel + SQLAlchemy: fetch_sqlmodel, write_sqlmodel, *_raw, …
pip install "pydantable[pandas]"   # pandas-flavored façade (pandas UI doc)
pip install "pydantable[fastapi]"  # FastAPI integration (pydantable.fastapi)
```

## Quick start

```python
from pydantable import DataFrameModel

class User(DataFrameModel):
    id: int
    age: int | None

df = User({"id": [1, 2], "age": [20, None]})
result = (
    df.with_columns(age2=df.age * 2)
    .filter(df.age > 10)
    .select("id", "age2")
)

print(result.to_dict())
print([r.model_dump() for r in result.collect()])
```

Output (exact values depend on filtering; this matches `scripts/verify_doc_examples.py`):

```text
{'id': [1], 'age2': [40]}
[{'id': 1, 'age2': 40}]
```

## Core concepts

| Piece | Role |
| ----- | ---- |
| `DataFrameModel` | Table class with annotated columns (`class Orders(DataFrameModel): ...`). |
| `DataFrame[Schema]` | Generic API over your own Pydantic `BaseModel`. |
| `Expr` | Typed expressions in `with_columns`, `filter`, etc. |
| **Errors** | Ingest issues such as column length mismatch raise `ColumnLengthMismatchError` (`ValueError` subclass) from `pydantable.errors` — map to HTTP **400** in FastAPI via `register_exception_handlers`. |

**Static typing**

- **mypy:** schema-evolving return types for many chains via the bundled [mypy plugin](https://github.com/eddiethedean/pydantable/blob/main/python/pydantable/mypy_plugin.py) (`plugins` in `pyproject.toml`).
- **Pyright / Pylance:** use committed stubs under `typings/`; for explicit targets, `as_model(...)` / `try_as_model(...)` / `assert_model(...)`. See [TYPING](https://pydantable.readthedocs.io/en/latest/TYPING.html).

**Rich column types** (`Literal`, `ipaddress`, `WKB`, `Annotated`, …) are covered in [SUPPORTED_TYPES](https://pydantable.readthedocs.io/en/latest/SUPPORTED_TYPES.html).

**Materialization:** `collect()` / `rows()` → row models; `to_dict()` → `dict[str, list]`; `to_polars()` / `to_arrow()` with matching extras.

## I/O at a glance

- **`DataFrameModel` / `DataFrame[Schema]`:** lazy `read_*` / `aread_*`, `export_*`, `write_*`, SQLModel I/O (`fetch_sqlmodel`, `write_sqlmodel`, …); eager `materialize_*` and SQL `fetch_*` / `iter_*` patterns live on **`pydantable.io`** — pass `dict[str, list]` into constructors for typed frames.
- **Scripts:** raw helpers (`ScanFileRoot`, iterators) on **`pydantable.io`** for glue code.
- **SQL details:** [IO_SQL](https://pydantable.readthedocs.io/en/latest/IO_SQL.html) (recommended APIs, `*_raw`, deprecations) and [SQLMODEL_SQL_ROADMAP](https://pydantable.readthedocs.io/en/latest/SQLMODEL_SQL_ROADMAP.html) (phased migration).
- Large files & NDJSON patterns: [IO_JSON](https://pydantable.readthedocs.io/en/latest/IO_JSON.html), [IO_NDJSON](https://pydantable.readthedocs.io/en/latest/IO_NDJSON.html), [EXECUTION](https://pydantable.readthedocs.io/en/latest/EXECUTION.html).

## Validation controls

- Strict by default on constructors.
- Optional ingest controls: `trusted_mode`, `ignore_errors`, `on_validation_errors`.
- Missing optional fields: `fill_missing_optional` (default `True`).
- Validation presets: `validation_profile=...` (or `__pydantable__ = {"validation_profile": "..."}`).
- Per-column and nested strictness: {doc}`STRICTNESS` (field policies + profile defaults).

## Documentation

| Topic | Link |
| ----- | ---- |
| Docs home | [pydantable.readthedocs.io](https://pydantable.readthedocs.io/en/latest/) |
| Map of all pages | [DOCS_MAP](https://pydantable.readthedocs.io/en/latest/DOCS_MAP.html) |
| Quickstart | [QUICKSTART](https://pydantable.readthedocs.io/en/latest/QUICKSTART.html) |
| `DataFrameModel` | [DATAFRAMEMODEL](https://pydantable.readthedocs.io/en/latest/DATAFRAMEMODEL.html) |
| Typing (mypy vs Pyright) | [TYPING](https://pydantable.readthedocs.io/en/latest/TYPING.html) |
| I/O overview | [IO_OVERVIEW](https://pydantable.readthedocs.io/en/latest/IO_OVERVIEW.html) |
| SQL (SQLModel, raw string SQL) | [IO_SQL](https://pydantable.readthedocs.io/en/latest/IO_SQL.html) · [SQLMODEL_SQL_ROADMAP](https://pydantable.readthedocs.io/en/latest/SQLMODEL_SQL_ROADMAP.html) |
| Pandas-like API | [PANDAS_UI](https://pydantable.readthedocs.io/en/latest/PANDAS_UI.html) |
| FastAPI path | [GOLDEN_PATH_FASTAPI](https://pydantable.readthedocs.io/en/latest/GOLDEN_PATH_FASTAPI.html) → [FASTAPI](https://pydantable.readthedocs.io/en/latest/FASTAPI.html) → [FASTAPI_ENHANCEMENTS](https://pydantable.readthedocs.io/en/latest/FASTAPI_ENHANCEMENTS.html) |
| Service ergonomics (OpenAPI, aliases, redaction) | [SERVICE_ERGONOMICS](https://pydantable.readthedocs.io/en/latest/SERVICE_ERGONOMICS.html) |
| Custom dtypes | [CUSTOM_DTYPES](https://pydantable.readthedocs.io/en/latest/CUSTOM_DTYPES.html) |
| Strictness | [STRICTNESS](https://pydantable.readthedocs.io/en/latest/STRICTNESS.html) |
| Cookbooks | [Cookbook index](https://pydantable.readthedocs.io/en/latest/cookbook/index.html) (FastAPI, lazy pipelines, JSON logs, …) |
| Example multi-router app | `docs/examples/fastapi/service_layout/` in this repo |
| Test helpers | `pydantable.testing.fastapi` — see [FASTAPI](https://pydantable.readthedocs.io/en/latest/FASTAPI.html) |
| Execution & async | [EXECUTION](https://pydantable.readthedocs.io/en/latest/EXECUTION.html) · [MATERIALIZATION](https://pydantable.readthedocs.io/en/latest/MATERIALIZATION.html) |
| Behavioral contract | [INTERFACE_CONTRACT](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html) |
| Troubleshooting | [TROUBLESHOOTING](https://pydantable.readthedocs.io/en/latest/TROUBLESHOOTING.html) |
| Versioning | [VERSIONING](https://pydantable.readthedocs.io/en/latest/VERSIONING.html) |
| Changelog | [CHANGELOG](https://pydantable.readthedocs.io/en/latest/CHANGELOG.html) |

## Development

Use a virtual environment at **`.venv`** in the repo root (the `Makefile` defaults to `.venv/bin/python`). Full contributor setup, Maturin/Rust builds, and release notes: [DEVELOPER](https://pydantable.readthedocs.io/en/latest/DEVELOPER.html).

```bash
make check-full      # ruff, ty, pyright, typing snippet tests, Sphinx, Rust
```

## License

MIT
