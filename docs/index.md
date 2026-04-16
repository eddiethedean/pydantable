# PydanTable

**Strongly-typed DataFrame layer for FastAPI and Pydantic services**, with a Rust execution core (Polars-backed inside the native extension). This site is the **full manual**; the repository **README** is the short entrypoint for install one-liners.

!!! abstract "See also"
    Execution builds on [Polars](https://docs.pola.rs/) (see their [user guide](https://docs.pola.rs/user-guide/)) and ships as a native Rust extension. PydanTable adds **Pydantic-first** schemas, **SQLModel**-style tabular models, and **service / data** I/O patterns on top.


!!! note
    **Current release:** see [CHANGELOG](project/changelog.md) — stable **1.x** under [VERSIONING](semantics/versioning.md). Roadmap: [ROADMAP](project/roadmap.md); SQLModel milestones: [SQLMODEL_SQL_ROADMAP](project/sqlmodel-sql-roadmap.md).


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

    Shortest runnable path, HTTP patterns, **`register_exception_handlers`** (**503** / **400** / **422**), and cookbooks. Tests: **`pydantable.testing.fastapi`**. Troubleshooting: [TROUBLESHOOTING](getting-started/troubleshooting.md).

    [Golden path →](integrations/fastapi/golden-path.md)

-   __Data & I/O__

    ---

    `DataFrameModel`, lazy vs eager I/O, [IO_OVERVIEW](io/overview.md), and format guides (Parquet, CSV, SQL, …). Pandas-like helpers: [PANDAS_UI](integrations/alternate-surfaces/pandas-ui.md).

    [I/O decision tree →](io/decision-tree.md)

-   __Library & contracts__

    ---

    Public API guarantees, [VERSIONING](semantics/versioning.md), plans/plugins ([PLAN_AND_PLUGINS](user-guide/plan-and-plugins.md)), and custom engine packages ([CUSTOM_ENGINE_PACKAGE](integrations/engines/custom-engine-package.md)).

    [Interface contract →](semantics/interface-contract.md)

-   __Optional engines__

    ---

    **SQL** ([SQL_ENGINE](integrations/engines/sql.md), **`pydantable[sql]`**), **Mongo** ([MONGO_ENGINE](integrations/engines/mongo.md), **`pydantable[mongo]`**), **Spark** ([SPARK_ENGINE](integrations/engines/spark.md), **`pydantable[spark]`**). Beanie: [BEANIE](integrations/engines/beanie.md). PySpark façade: [PYSPARK_UI](integrations/alternate-surfaces/pyspark-ui.md).

    [Spark engine →](integrations/engines/spark.md)

</div>

## Guide map

- **Services:** [GOLDEN_PATH_FASTAPI](integrations/fastapi/golden-path.md) → [FASTAPI](integrations/fastapi/fastapi.md) → [FASTAPI_ADVANCED](integrations/fastapi/advanced.md) → [FASTAPI_ENHANCEMENTS](integrations/fastapi/enhancements.md) → [DATAFRAMEMODEL](user-guide/dataframemodel.md) → [EXECUTION](user-guide/execution.md). Cookbooks: [fastapi_columnar_bodies](cookbook/fastapi_columnar_bodies.md), [fastapi_observability](cookbook/fastapi_observability.md), [fastapi_background_tasks](cookbook/fastapi_background_tasks.md), [async_lazy_pipeline](cookbook/async_lazy_pipeline.md). Layout: `docs/examples/fastapi/service_layout/`.
- **Data workflows:** [DATAFRAMEMODEL](user-guide/dataframemodel.md) → [IO_DECISION_TREE](io/decision-tree.md) → [IO_OVERVIEW](io/overview.md).
- **Typing:** [TYPING](user-guide/typing.md) — mypy plugin vs **Pyright** / **Pylance** / Astral **`ty`**, and **`as_model(...)`** on transform chains.
- **Third-party execution:** [CUSTOM_ENGINE_PACKAGE](integrations/engines/custom-engine-package.md). **Mongo** helpers and **Beanie** reads/writes: [IO_DECISION_TREE](io/decision-tree.md) and [BEANIE](integrations/engines/beanie.md).
- **Not sure where something lives?** [DOCS_MAP](getting-started/docs-map.md).

