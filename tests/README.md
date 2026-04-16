# Test layout

How to run the suite, markers, coverage, and CI alignment are documented in [Testing](../docs/TESTING.md).

## Directory map

| Directory | Focus |
|-----------|--------|
| `tests/io/` | Scans, lazy reads, JSON/Parquet/CSV/IPC, HTTP transports, doc example scripts tied to I/O |
| `tests/dataframe/` | Core `DataFrame` behavior, expressions, materialization, engine contracts, version checks, Hypothesis, phase/feature tests not covered below |
| `tests/dataframe_model/` | `DataFrameModel` parity, constructors, variants |
| `tests/sql/` | SQLModel I/O, string-SQL deprecation, Moltres / `SqlDataFrame` |
| `tests/fastapi/` | FastAPI integration and columnar helpers |
| `tests/typing/` | mypy / Pyright subprocess contracts, typing runtime checks |
| `tests/third_party/` | Pandas UI, PySpark façade, SparkDantic bridge, engine contracts, Streamlit, Polars workflow examples |
| `tests/spark/` | JVM `SparkSession` + `SparkDataFrame` / raikou-core (`@pytest.mark.spark`) |

Shared helpers live in `tests/_support/`. Pytest plugins and fixtures are in `tests/conftest.py`.

## File prefix cheat sheet

| Prefix | Typical area |
|--------|----------------|
| `test_io_*` | I/O, URLs, formats (see `tests/io/`) |
| `test_lazy_read_*` | Lazy scan / validation |
| `test_sqlmodel_*` / `test_sql_*` | SQL / SQLModel / Moltres |
| `test_fastapi_*` / `test_pydantable_fastapi_*` | FastAPI |
| `test_mypy_*` / `test_pyright_*` | Static typing contract tests |
| `test_pandas_*` / `test_pyspark_*` | Optional UI facades |
| `test_dataframe_*` | DataFrame and model-focused tests |
| `test_async_*` | Async materialization / cancellation |
| `test_v0*` / `test_phase*` | Historical feature or phased rollout tests |

New files should prefer `test_<area>_<behavior>.py` rather than version-only names (`test_v018_*`) unless you are extending an existing version-scoped module.
