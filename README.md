# PydanTable

[![CI](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml?query=branch%3Amain)
[![Documentation](https://readthedocs.org/projects/pydantable/badge/?version=latest)](https://pydantable.readthedocs.io/en/latest/)
[![PyPI version](https://img.shields.io/pypi/v/pydantable)](https://pypi.org/project/pydantable/)
[![Python versions](https://img.shields.io/pypi/pyversions/pydantable)](https://pypi.org/project/pydantable/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Strongly typed DataFrames for Python, powered by Rust.**

PydanTable combines **Pydantic schemas** with a **Polars-backed Rust execution engine** to provide a typed, service-friendly DataFrame API (with optional integrations for FastAPI, SQL, MongoDB, Spark, and more).

**Current release:** 1.19.0 — highlights in the [changelog](https://pydantable.readthedocs.io/en/latest/project/changelog/).

## Documentation

- **Docs (latest):** [pydantable.readthedocs.io](https://pydantable.readthedocs.io/en/latest/)
- **Quickstart:** [Getting started → Quickstart](https://pydantable.readthedocs.io/en/latest/getting-started/quickstart/)
- **Docs map:** [Getting started → Docs map](https://pydantable.readthedocs.io/en/latest/getting-started/docs-map/)

## What you get

- **Typed tables** via Pydantic models: `DataFrameModel` or `DataFrame[Schema]`
- **Typed expressions + lazy plans** validated/lowered in Rust
- **Explicit materialization**: `collect()` (rows) or `to_dict()` (columns), plus optional Arrow/Polars exports
- **File / HTTP / SQL I/O** helpers and integration patterns for services

Key references:

- **DataFrameModel:** [User guide → DataFrameModel](https://pydantable.readthedocs.io/en/latest/user-guide/dataframemodel/)
- **Execution:** [User guide → Execution](https://pydantable.readthedocs.io/en/latest/user-guide/execution/)
- **Materialization:** [User guide → Materialization](https://pydantable.readthedocs.io/en/latest/user-guide/materialization/)
- **Interface contract:** [Semantics → Interface contract](https://pydantable.readthedocs.io/en/latest/semantics/interface-contract/)
- **I/O overview:** [I/O → Overview](https://pydantable.readthedocs.io/en/latest/io/overview/)

## Install

```bash
pip install pydantable
```

Optional extras:

```bash
pip install "pydantable[polars]"   # to_polars
pip install "pydantable[arrow]"    # to_arrow / Arrow constructors
pip install "pydantable[io]"       # full file I/O convenience (arrow + polars)
pip install "pydantable[sql]"      # SQLModel + SQLAlchemy + moltres-core lazy SqlDataFrame; add a DB-API driver for your URL
pip install "pydantable[pandas]"   # pandas-flavored façade (pandas UI doc)
pip install "pydantable[fastapi]"  # FastAPI integration (pydantable.fastapi)
pip install "pydantable[mongo]"    # pymongo + Beanie + Mongo plan stack (lazy MongoDataFrame + I/O + from_beanie)
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

Output (one run):

```text
{'id': [1], 'age2': [40]}
[{'id': 1, 'age2': 40}]
```

## Next steps

- **Start here:** [Quickstart](https://pydantable.readthedocs.io/en/latest/getting-started/quickstart/)
- **Typing guide:** [User guide → Typing](https://pydantable.readthedocs.io/en/latest/user-guide/typing/)
- **I/O decision tree:** [I/O → Decision tree](https://pydantable.readthedocs.io/en/latest/io/decision-tree/)
- **FastAPI golden path:** [Integrations → FastAPI → Golden path](https://pydantable.readthedocs.io/en/latest/integrations/fastapi/golden-path/)
- **Engines:** [SQL](https://pydantable.readthedocs.io/en/latest/integrations/engines/sql/) · [Mongo](https://pydantable.readthedocs.io/en/latest/integrations/engines/mongo/) · [Spark](https://pydantable.readthedocs.io/en/latest/integrations/engines/spark/)

## Development

Use a virtual environment at **`.venv`** in the repo root (the `Makefile` defaults to `.venv/bin/python`). Full contributor setup, native builds, and contributor notes: [Project → Developer](https://pydantable.readthedocs.io/en/latest/project/developer/).

```bash
make check-full      # ruff, ty, pyright, typing snippet tests, MkDocs, Rust
```

## License

MIT
