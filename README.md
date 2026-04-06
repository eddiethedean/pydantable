# PydanTable

[![CI](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml?query=branch%3Amain)
[![Documentation](https://readthedocs.org/projects/pydantable/badge/?version=latest)](https://pydantable.readthedocs.io/en/latest/)
[![PyPI version](https://img.shields.io/pypi/v/pydantable)](https://pypi.org/project/pydantable/)
[![Python versions](https://img.shields.io/pypi/pyversions/pydantable)](https://pypi.org/project/pydantable/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Strictly-typed DataFrames for Python, powered by Rust** — define a schema (Pydantic), and get an API that **cannot** “accidentally” change schemas at runtime.

**Current release: 2.0.0** — highlights in the [changelog](https://pydantable.readthedocs.io/en/latest/CHANGELOG.html).

## Core contract

- **Column identity is typed**: use `df.col.<field>` / `ColumnRef` (no stringly-typed schema evolution).
- **Schema evolution is explicit**: schema-changing transforms require `*_as(AfterSchema/AfterModel, ...)` and are runtime-validated.

The contract is defined by the strict typed dataframe spec:
[typed_dataframe_spec](https://pydantable.readthedocs.io/en/latest/typed_dataframe_spec.html).

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
pip install "pydantable[fastapi]"  # FastAPI integration (pydantable.fastapi)
pip install "pydantable[moltres]"   # SqlDataFrame / SqlDataFrameModel (sqlalchemy engine)
```

## Quick start

```python
from pydantable import DataFrameModel

class User(DataFrameModel):
    id: int
    age: int | None

df = User({"id": [1, 2], "age": [20, None]})

class WithAge2(User):
    age2: int | None

class Result(DataFrameModel):
    id: int
    age2: int | None

df2 = df.with_columns_as(WithAge2, age2=df.col.age * 2)
out = df2.filter(df2.col.age > 10).drop_as(Result, df2.col.age)

print(out.to_dict())
print([r.model_dump() for r in out.collect()])
```

Output:

```text
{'id': [1], 'age2': [40]}
[{'id': 1, 'age2': 40}]
```

## Core concepts (one screen)

| Piece | Role |
| ----- | ---- |
| `DataFrameModel` | Table class with annotated columns (`class Orders(DataFrameModel): ...`). |
| `DataFrame[Schema]` | Generic API over your own Pydantic `BaseModel`. |
| `Expr` / `ColumnRef` | Expressions and typed column tokens used in transforms. |
| `collect()` / `to_dict()` | Materialize as row models or `dict[str, list]` (and Polars/Arrow with extras). |

## I/O, SQL, and engines

- **I/O overview**: <https://pydantable.readthedocs.io/en/latest/IO_OVERVIEW.html>
- **SQL** (`*_sqlmodel`, `*_raw`): <https://pydantable.readthedocs.io/en/latest/IO_SQL.html>
- **SQLModel roadmap**: <https://pydantable.readthedocs.io/en/latest/SQLMODEL_SQL_ROADMAP.html>
- **Custom engines** (`ExecutionEngine`): <https://pydantable.readthedocs.io/en/latest/CUSTOM_ENGINE_PACKAGE.html>
- **Moltres SQL engine (optional)**: <https://pydantable.readthedocs.io/en/latest/MOLTRES_SQL.html>

## Documentation

Start here: <https://pydantable.readthedocs.io/en/latest/>

Transforms cheat sheet (**`with_columns_as`**, **`join_as`** with **`schema=`** / **`model=`** keywords, etc.): <https://pydantable.readthedocs.io/en/latest/TRANSFORMS_QUICK_REF.html>

## Development

Use a virtual environment at **`.venv`** in the repo root (the `Makefile` defaults to `.venv/bin/python`). Full contributor setup, Maturin/Rust builds, and release notes: [DEVELOPER](https://pydantable.readthedocs.io/en/latest/DEVELOPER.html).

Before **`pip install -e .`** on a checkout (when **`pydantable-native`** matching your **`pyproject.toml`** pin is not on PyPI yet), install **protocol** then build native: **`pip install -e ./pydantable-protocol`**, then **`make native-develop`**, then **`pip install -e ".[dev,docs]"`** (see DEVELOPER).

```bash
make check-full      # ruff, ty, pyright, typing snippet tests, Sphinx, Rust
```

## License

MIT
