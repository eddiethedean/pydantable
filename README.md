# PydanTable

[![CI](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml?query=branch%3Amain)
[![Documentation](https://readthedocs.org/projects/pydantable/badge/?version=latest)](https://pydantable.readthedocs.io/en/latest/)
[![PyPI version](https://img.shields.io/pypi/v/pydantable)](https://pypi.org/project/pydantable/)
[![Python versions](https://img.shields.io/pypi/pyversions/pydantable)](https://pypi.org/project/pydantable/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Strongly typed DataFrames for Python, powered by Rust** — Pydantic schemas, Polars-backed execution in the native extension, and an API built for services (including optional FastAPI integration).

**Current release: 1.18.0** — highlights in the [changelog](https://pydantable.readthedocs.io/en/latest/CHANGELOG.html).

## What it is

- **Typed tables**: define columns with Pydantic models (`DataFrameModel` or `DataFrame[Schema]`). See [DATAFRAMEMODEL](https://pydantable.readthedocs.io/en/latest/DATAFRAMEMODEL.html) and [QUICKSTART](https://pydantable.readthedocs.io/en/latest/QUICKSTART.html).
- **Typed expressions + lazy plans**: transforms build a plan that’s validated/lowered in Rust. See [EXECUTION](https://pydantable.readthedocs.io/en/latest/EXECUTION.html) and [INTERFACE_CONTRACT](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html).
- **Materialize when you choose**: `collect()` (row models) or `to_dict()` (columnar), plus Arrow/Polars with extras. See [MATERIALIZATION](https://pydantable.readthedocs.io/en/latest/MATERIALIZATION.html).
- **I/O + services**: file/HTTP/SQL I/O, and optional FastAPI integration patterns. Start at [IO_OVERVIEW](https://pydantable.readthedocs.io/en/latest/IO_OVERVIEW.html) and [GOLDEN_PATH_FASTAPI](https://pydantable.readthedocs.io/en/latest/GOLDEN_PATH_FASTAPI.html).

## Install

```bash
pip install pydantable
```

Common extras:

```bash
pip install "pydantable[polars]"   # to_polars
pip install "pydantable[arrow]"    # to_arrow / Arrow constructors
pip install "pydantable[io]"       # full file I/O convenience (arrow + polars)
pip install "pydantable[sql]"      # SQLModel + SQLAlchemy + moltres-core lazy SqlDataFrame; add a DB-API driver for your URL
pip install "pydantable[pandas]"   # pandas-flavored façade (pandas UI doc)
pip install "pydantable[fastapi]"  # FastAPI integration (pydantable.fastapi)
pip install "pydantable[mongo]"     # pymongo + Beanie + Mongo plan stack (lazy MongoDataFrame + I/O + from_beanie)
pip install "pydantable[spark]"    # SparkDataFrame / SparkDataFrameModel (raikou-core + pyspark + sparkdantic)
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

## Where to go next (ReadTheDocs)

- **Start**: [QUICKSTART](https://pydantable.readthedocs.io/en/latest/QUICKSTART.html) · [DATAFRAMEMODEL](https://pydantable.readthedocs.io/en/latest/DATAFRAMEMODEL.html)
- **Typing**: [TYPING](https://pydantable.readthedocs.io/en/latest/TYPING.html) (mypy plugin vs stubs / Pyright / ty)
- **Execution & semantics**: [EXECUTION](https://pydantable.readthedocs.io/en/latest/EXECUTION.html) · [MATERIALIZATION](https://pydantable.readthedocs.io/en/latest/MATERIALIZATION.html) · [INTERFACE_CONTRACT](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html)
- **I/O**: [IO_DECISION_TREE](https://pydantable.readthedocs.io/en/latest/IO_DECISION_TREE.html) → [IO_OVERVIEW](https://pydantable.readthedocs.io/en/latest/IO_OVERVIEW.html) (CSV/Parquet/NDJSON/JSON/IPC/HTTP/SQL)
- **FastAPI**: [GOLDEN_PATH_FASTAPI](https://pydantable.readthedocs.io/en/latest/GOLDEN_PATH_FASTAPI.html) → [FASTAPI](https://pydantable.readthedocs.io/en/latest/FASTAPI.html)
- **Optional engines**: [SQL_ENGINE](https://pydantable.readthedocs.io/en/latest/SQL_ENGINE.html) (SQL) · [MONGO_ENGINE](https://pydantable.readthedocs.io/en/latest/MONGO_ENGINE.html) (Mongo) · [SPARK_ENGINE](https://pydantable.readthedocs.io/en/latest/SPARK_ENGINE.html) (Spark)
- **Everything**: [DOCS_MAP](https://pydantable.readthedocs.io/en/latest/DOCS_MAP.html)

## Development

Use a virtual environment at **`.venv`** in the repo root (the `Makefile` defaults to `.venv/bin/python`). Full contributor setup, Maturin/Rust builds, and release notes: [DEVELOPER](https://pydantable.readthedocs.io/en/latest/DEVELOPER.html).

```bash
make check-full      # ruff, ty, pyright, typing snippet tests, MkDocs, Rust
```

## License

MIT
