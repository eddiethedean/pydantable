# Pydantable

PydanTable is a strongly-typed DataFrame layer for FastAPI + Pydantic services,
with a Rust-powered execution core (Polars-backed inside the native extension).

**Materialization (0.6+):** `collect()` returns a **list of Pydantic row models** for the current schema. Use **`to_dict()`** (or **`collect(as_lists=True)`**) for columnar **`dict[str, list]`** responses. **`to_polars()`** is available when the optional Python **`polars`** package is installed (`pip install 'pydantable[polars]'`). Details: `EXECUTION.md`, `DATAFRAMEMODEL.md`.

**Scalar dtypes:** `int`, `float`, `bool`, `str`, `datetime`, `date`, `timedelta`, each nullable via `Optional` / `| None`. Unsupported `DataFrameModel` field annotations fail at **class definition** time. Authoritative list and error timing: `SUPPORTED_TYPES.md`.

```{toctree}
:maxdepth: 2

DATAFRAMEMODEL
SUPPORTED_TYPES
FASTAPI
ROADMAP
DEVELOPER
WHY_NOT_POLARS
pydantable_plan
EXECUTION
PANDAS_UI
PYSPARK_UI
PYSPARK_PARITY
PYSPARK_INTERFACE
INTERFACE_CONTRACT
PARITY_SCORECARD
POLARS_WORKFLOWS
POLARS_TRANSFORMATIONS_ROADMAP

api/index
```

