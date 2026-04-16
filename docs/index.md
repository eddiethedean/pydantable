# PydanTable

**Strongly-typed DataFrame layer for FastAPI and Pydantic services**, with a Rust execution core (Polars-backed inside the native extension). This site is the **full manual**; the repository **README** is the short entrypoint for install one-liners.

!!! abstract "See also"
    Execution builds on [Polars](https://docs.pola.rs/) (see their [user guide](https://docs.pola.rs/user-guide/)) and ships as a native Rust extension. PydanTable adds **Pydantic-first** schemas, **SQLModel**-style tabular models, and **service / data** I/O patterns on top.


!!! note
    **Current release:** see [CHANGELOG](/CHANGELOG.md) — stable **1.x** under [VERSIONING](/VERSIONING.md). Roadmap: [ROADMAP](/ROADMAP.md); SQLModel milestones: [SQLMODEL_SQL_ROADMAP](/SQLMODEL_SQL_ROADMAP.md).


## Minimal example

```python
from pydantable import DataFrame, Schema


class Row(Schema):
    id: int
    score: float


df = DataFrame[Row]({"id": [1, 2], "score": [10.0, 20.5]})
```

## Choose your path

<div class="grid cards" markdown="1">

-   __Services (FastAPI)__

    ---

    Shortest runnable path, HTTP patterns, **`register_exception_handlers`** (**503** / **400** / **422**), and cookbooks. Tests: **`pydantable.testing.fastapi`**. Troubleshooting: [TROUBLESHOOTING](/TROUBLESHOOTING.md).

    [Golden path →](/GOLDEN_PATH_FASTAPI.md)

-   __Data & I/O__

    ---

    `DataFrameModel`, lazy vs eager I/O, [IO_OVERVIEW](/IO_OVERVIEW.md), and format guides (Parquet, CSV, SQL, …). Pandas-like helpers: [PANDAS_UI](/PANDAS_UI.md).

    [I/O decision tree →](/IO_DECISION_TREE.md)

-   __Library & contracts__

    ---

    Public API guarantees, [VERSIONING](/VERSIONING.md), plans/plugins ([PLAN_AND_PLUGINS](/PLAN_AND_PLUGINS.md)), and custom engine packages ([CUSTOM_ENGINE_PACKAGE](/CUSTOM_ENGINE_PACKAGE.md)).

    [Interface contract →](/INTERFACE_CONTRACT.md)

-   __Optional engines__

    ---

    **SQL** ([SQL_ENGINE](/SQL_ENGINE.md), **`pydantable[sql]`**), **Mongo** ([MONGO_ENGINE](/MONGO_ENGINE.md), **`pydantable[mongo]`**), **Spark** ([SPARK_ENGINE](/SPARK_ENGINE.md), **`pydantable[spark]`**). Beanie: [BEANIE](/BEANIE.md). PySpark façade: [PYSPARK_UI](/PYSPARK_UI.md).

    [Spark engine →](/SPARK_ENGINE.md)

</div>

## Guide map

- **Services:** [GOLDEN_PATH_FASTAPI](/GOLDEN_PATH_FASTAPI.md) → [FASTAPI](/FASTAPI.md) → [FASTAPI_ADVANCED](/FASTAPI_ADVANCED.md) → [FASTAPI_ENHANCEMENTS](/FASTAPI_ENHANCEMENTS.md) → [DATAFRAMEMODEL](/DATAFRAMEMODEL.md) → [EXECUTION](/EXECUTION.md). Cookbooks: [fastapi_columnar_bodies](/cookbook/fastapi_columnar_bodies.md), [fastapi_observability](/cookbook/fastapi_observability.md), [fastapi_background_tasks](/cookbook/fastapi_background_tasks.md), [async_lazy_pipeline](/cookbook/async_lazy_pipeline.md). Layout: `docs/examples/fastapi/service_layout/`.
- **Data workflows:** [DATAFRAMEMODEL](/DATAFRAMEMODEL.md) → [IO_DECISION_TREE](/IO_DECISION_TREE.md) → [IO_OVERVIEW](/IO_OVERVIEW.md).
- **Typing:** [TYPING](/TYPING.md) — mypy plugin vs **Pyright** / **Pylance** / Astral **`ty`**, and **`as_model(...)`** on transform chains.
- **Third-party execution:** [CUSTOM_ENGINE_PACKAGE](/CUSTOM_ENGINE_PACKAGE.md). **Mongo** helpers and **Beanie** reads/writes: [IO_DECISION_TREE](/IO_DECISION_TREE.md) and [BEANIE](/BEANIE.md).
- **Not sure where something lives?** [DOCS_MAP](/DOCS_MAP.md).

