# Docs map

Find documentation by **what you want to do**.

## Build typed tables and transforms

| I want to… | Start here |
|------------|------------|
| Install and verify the native extension | [Installation](installation.md) |
| Run my first pipeline in 5 minutes | [Quickstart](quickstart.md) |
| Define schemas and validation rules | [DataFrameModel](../user-guide/dataframemodel.md) |
| Understand plans, lazy vs eager, and costs | [Execution](../user-guide/execution.md), [Materialization](../user-guide/materialization.md) |
| Pick dtypes and edge cases | [Supported types](../user-guide/supported-types.md) |
| Fix typing in my editor | [Typing](../user-guide/typing.md) |

## Ship a FastAPI service

| I want to… | Start here |
|------------|------------|
| Shortest runnable async app | [Golden path](../integrations/fastapi/golden-path.md) |
| Request/response patterns and errors | [FastAPI guide](../integrations/fastapi/fastapi.md) |
| Columnar JSON bodies | [fastapi_columnar_bodies](../cookbook/fastapi_columnar_bodies.md) |
| Production settings and observability | [fastapi_settings](../cookbook/fastapi_settings.md), [fastapi_observability](../cookbook/fastapi_observability.md) |

## Read and write data

| I want to… | Start here |
|------------|------------|
| Choose lazy vs eager I/O | [I/O decision tree](../io/decision-tree.md) |
| Format-specific guides (Parquet, CSV, SQL, …) | [I/O overview](../io/overview.md) |
| SQLModel database round-trips | [SQL I/O](../io/sql.md), [SQL engine](../integrations/engines/sql.md) |
| MongoDB / Beanie | [Mongo engine](../integrations/engines/mongo.md), [Beanie](../integrations/engines/beanie.md) |

## Optional engines and facades

| I want to… | Start here |
|------------|------------|
| Lazy SQL transforms | [SQL engine](../integrations/engines/sql.md) |
| Lazy Mongo transforms | [Mongo engine](../integrations/engines/mongo.md) |
| Spark / PySpark compatibility | [Spark engine](../integrations/engines/spark.md), [PySpark UI](../integrations/alternate-surfaces/pyspark-ui.md) |
| Pandas-shaped method names | [Pandas UI](../integrations/alternate-surfaces/pandas-ui.md) |
| Custom execution backend | [Custom engine package](../integrations/engines/custom-engine-package.md) |

## Guarantees, upgrades, and help

| I want to… | Start here |
|------------|------------|
| Know what behavior is guaranteed | [Interface contract](../semantics/interface-contract.md) |
| Upgrade or migrate from pandas/Polars | [Migration](migration.md) |
| Understand semver | [Versioning](../semantics/versioning.md) |
| Fix errors | [Troubleshooting](troubleshooting.md) |
| Look up a term | [Glossary](../concepts/glossary.md) |

## Decide whether to adopt

| I want to… | Start here |
|------------|------------|
| See where pydantable fits | [Why pydantable?](../positioning/why-pydantable.md), [Mental model](../concepts/mental-model.md) |
| Compare to Polars or pandas | [Comparisons](../positioning/comparisons/index.md) |
| Review security for services | [Security considerations](../project/security.md) |

## Contribute or extend

| I want to… | Start here |
|------------|------------|
| Set up a dev environment | [Developer guide](../project/developer.md) |
| Run tests like CI | [Testing](../project/testing.md) |
| Edit documentation | [Docs style guide](../project/contributing/docs-style-guide.md) |
| See what ships next | [Roadmap](../project/roadmap.md) |
