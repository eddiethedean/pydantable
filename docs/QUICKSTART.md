# Five-minute tour

This page is the **RTD-friendly** version of the optional notebook in the repository
at `notebooks/five_minute_tour.ipynb` (same steps).
It uses the same steps: build a typed `DataFrame`, inspect it, summarize, filter, and materialize.

!!! note
    Requires a working `pydantable` install with the Rust extension (`pip install .` or wheels).


## 1. Schema and data

```python
from pydantic import BaseModel

from pydantable import DataFrame


class Row(BaseModel):
    id: int
    score: float
    label: str


df = DataFrame[Row]({
        "id": [1, 2, 3],
        "score": [10.0, 20.5, 7.0],
        "label": ["a", "b", "a"],
    })
```

## 2. String repr and HTML (Jupyter)

In a terminal, `repr(df)` shows the schema and column dtypes (no row count—plans may be lazy).

In **Jupyter** / **VS Code**, the last expression in a cell can render as HTML via `_repr_html_()` (bounded preview; same cost class as `head()` + `to_dict()` for the slice). See [EXECUTION](/EXECUTION.md) **Jupyter / HTML** and **Display options**.

## 3. Discovery helpers

```python
df.columns
df.shape  # root-buffer semantics after lazy transforms—see [INTERFACE_CONTRACT](/INTERFACE_CONTRACT.md)
df.info()
print(df.describe())
```

## 4. Filter and materialize

```python
filtered = df.filter(df.score > 8.0)
rows = filtered.collect()  # list[Pydantic row models]
cols = filtered.to_dict()  # dict[str, list]
```

Use `to_polars()` / `to_arrow()` when the optional extras are installed ([EXECUTION](/EXECUTION.md) **Copy as / interchange**).

## Where to read next

- [DATAFRAMEMODEL](/DATAFRAMEMODEL.md) — `DataFrameModel`, validation, transforms
- [PANDAS_UI](/PANDAS_UI.md) — optional **`pydantable.pandas`** import (`assign`, `merge`, cleaning helpers)
- [EXECUTION](/EXECUTION.md) — materialization cost, async, display limits
- [INTERFACE_CONTRACT](/INTERFACE_CONTRACT.md) — semantics (joins, nulls, `shape` vs executed rows)
- [IO_DECISION_TREE](/IO_DECISION_TREE.md) — pick lazy vs eager I/O; prefer **`DataFrameModel`** / **`DataFrame[Schema]`** classmethods over raw **`pydantable.io`**
- [IO_OVERVIEW](/IO_OVERVIEW.md) — per-format tables (Parquet, CSV, NDJSON, JSON, IPC, HTTP, SQL)
- [MONGO_ENGINE](/MONGO_ENGINE.md) / [BEANIE](/BEANIE.md) — optional **`pydantable[mongo]`** (lazy **`MongoDataFrame`**, eager **`fetch_mongo`** / **`afetch_mongo`**, Beanie ODM helpers)
