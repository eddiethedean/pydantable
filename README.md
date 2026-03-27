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
- Fit cleanly into FastAPI request/response flows.

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
- Static typing:
  - **mypy** can infer schema-evolving return types for many transform chains (via the mypy plugin).
  - **pyright/Pylance** relies on shipped stubs; use `as_model(...)` / `try_as_model(...)` / `assert_model(...)` when you want an explicit after-schema model.
- Materialization:
  - `collect()` -> list of row models
  - `to_dict()` -> `dict[str, list]`
  - `to_polars()` / `to_arrow()` with matching extras installed

## I/O at a glance

- Lazy file pipelines: `read_*` / `aread_*` -> transform -> `write_*`
- Eager reads: `materialize_*`, `fetch_sql`, `fetch_*_url`
- Eager writes: `export_*`, `write_sql`
- Full I/O API is in `pydantable.io`

## Validation controls

- Strict by default on constructors.
- Optional ingest controls: `trusted_mode`, `ignore_errors`, `on_validation_errors`.
- Missing optional fields are controlled by `fill_missing_optional` (default `True`).

## Documentation

- Docs home: [pydantable.readthedocs.io](https://pydantable.readthedocs.io/en/latest/)
- Quickstart: [QUICKSTART](https://pydantable.readthedocs.io/en/latest/QUICKSTART.html)
- DataFrameModel guide: [DATAFRAMEMODEL](https://pydantable.readthedocs.io/en/latest/DATAFRAMEMODEL.html)
- I/O overview: [IO_OVERVIEW](https://pydantable.readthedocs.io/en/latest/IO_OVERVIEW.html)
- FastAPI patterns: [FASTAPI](https://pydantable.readthedocs.io/en/latest/FASTAPI.html)
- Behavioral contract: [INTERFACE_CONTRACT](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html)
- Versioning policy: [VERSIONING](https://pydantable.readthedocs.io/en/latest/VERSIONING.html)
- Changelog: [changelog](https://pydantable.readthedocs.io/en/latest/changelog.html)

## Development

```bash
make check-full
```

Contributor setup and release workflow: [DEVELOPER](https://pydantable.readthedocs.io/en/latest/DEVELOPER.html)

## License

MIT
