# Five-minute tour

This page is the **RTD-friendly** version of the optional notebook in the repository
at `notebooks/five_minute_tour.ipynb` (same steps).
It uses the same steps: build a typed `DataFrame`, inspect it, summarize, filter, and materialize.

```{note}
Requires a working `pydantable` install with the Rust extension (`pip install .` or wheels).
```

## 1. Schema and data

```python
from pydantic import BaseModel

from pydantable import DataFrame


class Row(BaseModel):
    id: int
    score: float
    label: str


df = DataFrame[Row](
    {
        "id": [1, 2, 3],
        "score": [10.0, 20.5, 7.0],
        "label": ["a", "b", "a"],
    }
)
```

## 2. String repr and HTML (Jupyter)

In a terminal, `repr(df)` shows the schema and column dtypes (no row count—plans may be lazy).

In **Jupyter** / **VS Code**, the last expression in a cell can render as HTML via `_repr_html_()` (bounded preview; same cost class as `head()` + `to_dict()` for the slice). See {doc}`EXECUTION` **Jupyter / HTML** and **Display options**.

## 3. Discovery helpers

```python
df.columns
df.shape  # root-buffer semantics after lazy transforms—see {doc}`INTERFACE_CONTRACT`
df.info()
print(df.describe())
```

## 4. Filter and materialize

```python
filtered = df.filter(df.score > 8.0)
rows = filtered.collect()  # list[Pydantic row models]
cols = filtered.to_dict()  # dict[str, list]
```

Use `to_polars()` / `to_arrow()` when the optional extras are installed ({doc}`EXECUTION` **Copy as / interchange**).

## Where to read next

- {doc}`DATAFRAMEMODEL` — `DataFrameModel`, validation, transforms
- {doc}`PANDAS_UI` — **deprecated** **`pydantable.pandas`** import (`assign`, `merge`, cleaning helpers)
- {doc}`EXECUTION` — materialization cost, async, display limits
- {doc}`INTERFACE_CONTRACT` — semantics (joins, nulls, `shape` vs executed rows)
- {doc}`IO_DECISION_TREE` — pick lazy vs eager I/O; prefer **`DataFrameModel`** / **`DataFrame[Schema]`** classmethods over raw **`pydantable.io`**
- {doc}`IO_OVERVIEW` — per-format tables (Parquet, CSV, NDJSON, JSON, IPC, HTTP, SQL)
