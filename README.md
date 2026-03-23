# PydanTable

[![CI](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml?query=branch%3Amain)
[![Documentation](https://readthedocs.org/projects/pydantable/badge/?version=latest)](https://pydantable.readthedocs.io/en/latest/)
[![PyPI version](https://img.shields.io/pypi/v/pydantable)](https://pypi.org/project/pydantable/)
[![Python versions](https://img.shields.io/pypi/pyversions/pydantable)](https://pypi.org/project/pydantable/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Typed dataframe transformations for FastAPI and Pydantic services, backed by a Rust execution core.**

**Current release: 0.7.0** · Python **3.10+**

---

## Documentation

**The full manual lives on Read the Docs:**

### **[https://pydantable.readthedocs.io/en/latest/](https://pydantable.readthedocs.io/en/latest/)**

That site is the supported entry point for concepts, contracts, API notes, and examples. The sections below point to the same pages so you can jump straight from GitHub.

| Topic | Read the Docs |
|--------|----------------|
| **Home / overview** | [Documentation home](https://pydantable.readthedocs.io/en/latest/index.html) |
| **`DataFrameModel` contract** (inputs, transforms, collisions, materialization) | [DataFrameModel (SQLModel-like)](https://pydantable.readthedocs.io/en/latest/DATAFRAMEMODEL.html) |
| **Column types** (scalars, structs, `list[T]`, nullability, unsupported cases) | [Supported data types](https://pydantable.readthedocs.io/en/latest/SUPPORTED_TYPES.html) |
| **FastAPI** (routers, bodies, `collect`, responses) | [FastAPI integration](https://pydantable.readthedocs.io/en/latest/FASTAPI.html) |
| **Execution model** (`collect`, `to_dict`, `to_polars`, optional Python Polars, UI modules) | [Execution (Rust engine)](https://pydantable.readthedocs.io/en/latest/EXECUTION.html) |
| **Semantics** (nulls, joins, ordering, reshaping, windows — Polars-style contract) | [Interface contract](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html) |
| **Roadmap** (0.5.0–0.7.0 shipped, path to v1.0.0) | [Roadmap](https://pydantable.readthedocs.io/en/latest/ROADMAP.html) |
| **Why not use Polars directly?** | [Why not just use Polars?](https://pydantable.readthedocs.io/en/latest/WHY_NOT_POLARS.html) |
| **Pandas-style imports** (`pydantable.pandas`) | [Pandas UI](https://pydantable.readthedocs.io/en/latest/PANDAS_UI.html) |
| **PySpark-style imports** (`pydantable.pyspark`) | [PySpark UI](https://pydantable.readthedocs.io/en/latest/PYSPARK_UI.html) |
| **PySpark helpers & parity** | [PySpark interface](https://pydantable.readthedocs.io/en/latest/PYSPARK_INTERFACE.html) · [PySpark API parity](https://pydantable.readthedocs.io/en/latest/PYSPARK_PARITY.html) |
| **Polars parity** (scorecard, workflows, transformation roadmap) | [Parity scorecard](https://pydantable.readthedocs.io/en/latest/PARITY_SCORECARD.html) · [Polars-style workflows](https://pydantable.readthedocs.io/en/latest/POLARS_WORKFLOWS.html) · [Transformation parity roadmap](https://pydantable.readthedocs.io/en/latest/POLARS_TRANSFORMATIONS_ROADMAP.html) |
| **Contributors** (build, test, benchmarks, releases) | [Developer guide](https://pydantable.readthedocs.io/en/latest/DEVELOPER.html) |
| **Plan / vision** (architecture phasing) | [Plan document](https://pydantable.readthedocs.io/en/latest/pydantable_plan.html) |
| **Python API reference** (autodoc) | [API reference](https://pydantable.readthedocs.io/en/latest/api/index.html) |

For copy-paste convenience, the site base URL is:

`https://pydantable.readthedocs.io/en/latest/`

---

## What PydanTable does

PydanTable keeps **Pydantic models** as the source of truth for:

- column types and nullability (`Optional[T]` / `T | None`)
- **typed expressions** — invalid combinations fail when the expression is built (Rust AST), not only at runtime
- **schema evolution** — chained transforms produce new model types with stable rules (e.g. `with_columns` name collisions)

The default API feels **Polars-like**; optional **`pydantable.pandas`** and **`pydantable.pyspark`** modules only change naming and imports — execution is always the native core. Details: [Execution](https://pydantable.readthedocs.io/en/latest/EXECUTION.html), [Interface contract](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html).

**0.7.0+ expression surface (Rust-typed `Expr`):** whole-frame **`global_*`** aggregates (`sum`, `mean`, `count`, `min`, `max`), **window** `lag` / `lead` (with `Window.partitionBy(...).orderBy(...)`), **temporal** helpers (`strptime`, `unix_timestamp`, `dt_nanosecond`), **map** entry count (`map_len`), and **binary** byte length (`binary_len`). PySpark-named wrappers live under `pydantable.pyspark.sql.functions`. See [CHANGELOG](https://pydantable.readthedocs.io/en/latest/changelog.html).

---

## Install

```bash
pip install pydantable
```

Optional Python **Polars** (for `to_polars()` only):

```bash
pip install 'pydantable[polars]'
```

**From a git checkout**, the Rust extension must be built (e.g. with [maturin](https://www.maturin.rs/)):

```bash
pip install .
```

See [Developer guide — local setup](https://pydantable.readthedocs.io/en/latest/DEVELOPER.html) for `maturin develop`, release builds, and CI parity.

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

print(df4.to_dict())
```

Example output:

```text
{'age2': [40], 'id': [1]}
```

- **Materialization:** [`collect()`](https://pydantable.readthedocs.io/en/latest/DATAFRAMEMODEL.html) returns a list of Pydantic row models; [`to_dict()`](https://pydantable.readthedocs.io/en/latest/EXECUTION.html) returns columnar `dict[str, list]`.
- **Alternate UIs:**

  ```python
  from pydantable.pandas import DataFrameModel as PandasDataFrameModel
  from pydantable.pyspark import DataFrameModel as PySparkDataFrameModel
  from pydantable import DataFrameModel as DefaultDataFrameModel
  ```

More examples: [FastAPI integration](https://pydantable.readthedocs.io/en/latest/FASTAPI.html), [Polars-style workflows](https://pydantable.readthedocs.io/en/latest/POLARS_WORKFLOWS.html).

---

## Development

```bash
make check-full   # Ruff, mypy, Rust fmt/clippy/tests (see Makefile for `rust-test` env)
pytest -q         # or: pytest -n auto  with the [dev] extra
```

Rust + Python: see [Developer guide](https://pydantable.readthedocs.io/en/latest/DEVELOPER.html) (formatting, `maturin`, `make rust-test` for `cargo test` with the venv `PYTHONPATH`, benchmarks, contribution workflow).

---

## License

MIT
