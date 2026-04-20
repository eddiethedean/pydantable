# Five-minute tour

This page is the **RTD-friendly** version of the optional notebook in the repository
at `notebooks/five_minute_tour.ipynb` (same steps).
It uses the same steps: build a typed `DataFrame`, inspect it, summarize, filter, and materialize.

!!! note
    Requires a working `pydantable` install with the Rust extension (`pip install .` or wheels).

!!! tip "If you’re deciding between tools"
    If you’re choosing between pydantable and a general-purpose DataFrame library, start here:

    - [Why pydantable?](../positioning/why-pydantable.md)
    - [Comparisons (Polars + pandas)](../positioning/comparisons/index.md)
    - [Mental model](../concepts/mental-model.md)

## 1. Schema and data

```python
from datetime import datetime, timezone

from pydantable import DataFrame, Schema


class UserEvent(Schema):
    event_id: str
    user_id: int
    event_name: str
    occurred_at: datetime
    latency_ms: int | None


df = DataFrame[UserEvent](
    {
        "event_id": ["evt_001", "evt_002", "evt_003"],
        "user_id": [101, 101, 202],
        "event_name": ["page_view", "purchase", "page_view"],
        "occurred_at": [
            datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 20, 12, 5, tzinfo=timezone.utc),
            datetime(2026, 4, 20, 12, 7, tzinfo=timezone.utc),
        ],
        "latency_ms": [12, 220, None],
    }
)
```

## 2. String repr and HTML (Jupyter)

In a terminal, `repr(df)` shows the schema and column dtypes (no row count—plans may be lazy).

In **Jupyter** / **VS Code**, the last expression in a cell can render as HTML via `_repr_html_()` (bounded preview; same cost class as `head()` + `to_dict()` for the slice). See [EXECUTION](../user-guide/execution.md) **Jupyter / HTML** and **Display options**.

## 3. Discovery helpers

```python
df.columns
df.shape  # root-buffer semantics after lazy transforms—see [INTERFACE_CONTRACT](../semantics/interface-contract/)
df.info()
print(df.describe())
```

Runnable script:

```bash
python docs/examples/getting_started/quickstart_discovery_helpers.py
```

--8<-- "examples/getting_started/quickstart_discovery_helpers.py"

### Output

```text
--8<-- "examples/getting_started/quickstart_discovery_helpers.py.out.txt"
```

## 4. Filter and materialize

```python
filtered = df.filter((df.event_name == "page_view") & (df.latency_ms.is_not_null()))
rows = filtered.collect()  # list[Pydantic row models]
cols = filtered.to_dict()  # dict[str, list]
```

Use `to_polars()` / `to_arrow()` when the optional extras are installed ([EXECUTION](../user-guide/execution.md) **Copy as / interchange**).

## Where to read next

- [DATAFRAMEMODEL](../user-guide/dataframemodel.md) — `DataFrameModel`, validation, transforms
- [PANDAS_UI](../integrations/alternate-surfaces/pandas-ui.md) — optional **`pydantable.pandas`** import (`assign`, `merge`, cleaning helpers)
- [EXECUTION](../user-guide/execution.md) — materialization cost, async, display limits
- [INTERFACE_CONTRACT](../semantics/interface-contract.md) — semantics (joins, nulls, `shape` vs executed rows)
- [IO_DECISION_TREE](../io/decision-tree.md) — pick lazy vs eager I/O; prefer **`DataFrameModel`** / **`DataFrame[Schema]`** classmethods over raw **`pydantable.io`**
- [IO_OVERVIEW](../io/overview.md) — per-format tables (Parquet, CSV, NDJSON, JSON, IPC, HTTP, SQL)
- [MONGO_ENGINE](../integrations/engines/mongo.md) / [BEANIE](../integrations/engines/beanie.md) — optional **`pydantable[mongo]`** (lazy **`MongoDataFrame`**, eager **`fetch_mongo`** / **`afetch_mongo`**, Beanie ODM helpers)
