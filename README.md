# PydanTable

[![CI](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml?query=branch%3Amain)
[![Documentation](https://readthedocs.org/projects/pydantable/badge/?version=latest)](https://pydantable.readthedocs.io/en/latest/)
[![PyPI version](https://img.shields.io/pypi/v/pydantable)](https://pypi.org/project/pydantable/)
[![Python versions](https://img.shields.io/pypi/pyversions/pydantable)](https://pypi.org/project/pydantable/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Typed DataFrame workflows for Python services, with Pydantic schemas and a Rust execution core.

## Why PydanTable

- Define table shape once using Pydantic types.
- Catch many errors early with typed expressions.
- Use familiar DataFrame operations (`select`, `filter`, `join`, `group_by`, windows).
- Materialize as row models or `dict[str, list]`, depending on API needs.
- **FastAPI:** optional `pydantable.fastapi` helpers — shared executor lifespan, NDJSON streaming from `astream()`, OpenAPI-friendly **columnar** bodies (`columnar_dependency` / `rows_dependency`), and `register_exception_handlers` (**503** / **400** / **422** for common failures). See the [FastAPI guide](https://pydantable.readthedocs.io/en/latest/FASTAPI.html) and [golden path](https://pydantable.readthedocs.io/en/latest/GOLDEN_PATH_FASTAPI.html).

## Install

```bash
pip install pydantable
```

Common extras:

```bash
pip install "pydantable[polars]"  # to_polars
pip install "pydantable[arrow]"   # to_arrow / Arrow constructors
pip install "pydantable[io]"      # full file I/O convenience (arrow + polars)
pip install "pydantable[sql]"     # fetch_sql / write_sql helpers
pip install "pydantable[fastapi]" # FastAPI integration helpers (pydantable.fastapi)
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

print(result.to_dict())   # {'id': [1], 'age2': [40]}
print(result.collect())   # list of Pydantic row models
```

## Core concepts

- `DataFrameModel`: SQLModel-like table class (`class Orders(DataFrameModel): ...`).
- `DataFrame[Schema]`: generic API over your own Pydantic `BaseModel`.
- `Expr`: typed expressions used in transforms.
- **Errors:** predictable ingest failures such as column length mismatch raise `ColumnLengthMismatchError` (subclass of `ValueError`) from `pydantable.errors`; map to HTTP **400** in FastAPI via `register_exception_handlers`.
- Static typing:
  - **mypy** can infer schema-evolving return types for many transform chains (via the mypy plugin).
  - **pyright/Pylance** relies on shipped stubs; use `as_model(...)` / `try_as_model(...)` / `assert_model(...)` when you want an explicit after-schema model.
- **1.2.0 column types** (`typing.Literal[...]`, `ipaddress` IPv4/IPv6, `WKB`, `Annotated[str, ...]`) are documented in [SUPPORTED_TYPES](https://pydantable.readthedocs.io/en/latest/SUPPORTED_TYPES.html) (including `Expr` comparison notes).
- Materialization:
  - `collect()` -> list of row models
  - `to_dict()` -> `dict[str, list]`
  - `to_polars()` / `to_arrow()` with matching extras installed

## I/O at a glance

- Default: **`DataFrameModel`** / **`DataFrame[Schema]`** — lazy `read_*` / `aread_*`, `export_*`, `write_sql` / `awrite_sql`, …; eager `materialize_*` / `fetch_sql` / `iter_sql` live on **`pydantable.io`** → pass `dict[str, list]` into constructors for typed frames.
- Lazy file pipelines: `MyModel.read_*` / `await MyModel.aread_*` → transform → `write_*`
- The **`pydantable.io`** package exposes **raw** helpers (`dict[str, list]`, `ScanFileRoot`) for scripts and glue code — see **IO_OVERVIEW** in the docs.

## Validation controls

- Strict by default on constructors.
- Optional ingest controls: `trusted_mode`, `ignore_errors`, `on_validation_errors`.
- Missing optional fields are controlled by `fill_missing_optional` (default `True`).

## Documentation

- Docs home: [pydantable.readthedocs.io](https://pydantable.readthedocs.io/en/latest/)
- **Where to read what:** [DOCS_MAP](https://pydantable.readthedocs.io/en/latest/DOCS_MAP.html)
- Quickstart: [QUICKSTART](https://pydantable.readthedocs.io/en/latest/QUICKSTART.html)
- DataFrameModel guide: [DATAFRAMEMODEL](https://pydantable.readthedocs.io/en/latest/DATAFRAMEMODEL.html)
- I/O overview: [IO_OVERVIEW](https://pydantable.readthedocs.io/en/latest/IO_OVERVIEW.html)
- **FastAPI:** [GOLDEN_PATH_FASTAPI](https://pydantable.readthedocs.io/en/latest/GOLDEN_PATH_FASTAPI.html) → [FASTAPI](https://pydantable.readthedocs.io/en/latest/FASTAPI.html) → [FASTAPI_ENHANCEMENTS](https://pydantable.readthedocs.io/en/latest/FASTAPI_ENHANCEMENTS.html) (roadmap, troubleshooting)
- **Cookbooks (FastAPI):** columnar bodies · async materialization · [observability (request IDs + `observe`)](https://pydantable.readthedocs.io/en/latest/cookbook/fastapi_observability.html) · [background `submit`](https://pydantable.readthedocs.io/en/latest/cookbook/fastapi_background_tasks.html) · [lazy async pipeline](https://pydantable.readthedocs.io/en/latest/cookbook/async_lazy_pipeline.html) — index: [Cookbook](https://pydantable.readthedocs.io/en/latest/cookbook/index.html)
- Example **multi-router** app (copy from repo): `docs/examples/fastapi/service_layout/`
- **Tests:** `pydantable.testing.fastapi` (`fastapi_test_client`, `fastapi_app_with_executor`) — see [FASTAPI](https://pydantable.readthedocs.io/en/latest/FASTAPI.html)
- Execution & materialization: [EXECUTION](https://pydantable.readthedocs.io/en/latest/EXECUTION.html) · [MATERIALIZATION](https://pydantable.readthedocs.io/en/latest/MATERIALIZATION.html)
- Behavioral contract: [INTERFACE_CONTRACT](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html)
- Troubleshooting: [TROUBLESHOOTING](https://pydantable.readthedocs.io/en/latest/TROUBLESHOOTING.html)
- Versioning policy: [VERSIONING](https://pydantable.readthedocs.io/en/latest/VERSIONING.html)
- Changelog: [changelog](https://pydantable.readthedocs.io/en/latest/changelog.html)

## Development

Create and use a virtual environment at **`.venv`** in the repo root (the `Makefile` defaults to `.venv/bin/python`). Contributor setup, Maturin/Rust builds, and release workflow: [DEVELOPER](https://pydantable.readthedocs.io/en/latest/DEVELOPER.html).

```bash
make check-full
```

## License

MIT
