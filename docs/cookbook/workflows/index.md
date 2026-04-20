# Workflows (by intent)

This page is a routing layer: **start from what you’re trying to do** and jump to the most relevant guide/recipe.

If you’re new to the core concepts first, read:

- [Mental model](../../concepts/mental-model.md)

## Transform workflows

- **Joins + groupby**: [Join + groupby recipe](../transforms_join_groupby.md)
- **Windows**:
  - [Windows framing primer](../windows_framing_primer.md)
  - [Window semantics (SQL-style)](../../semantics/window-sql-semantics.md)
- **Selectors / column sets**: [Selectors](../../user-guide/selectors.md)

## I/O workflows

- **Pick an I/O entrypoint**: [I/O decision tree](../../io/decision-tree.md)
- **Format overview and per-format guides**: [I/O overview](../../io/overview.md)
- **SQL read/write**: [SQL I/O](../../io/sql.md)
- **HTTP + Parquet pattern**: [HTTP parquet ctx recipe](../io_http_parquet_ctx.md)
- **Write a lazy pipeline**: [Lazy pipeline write recipe](../io_lazy_pipeline_write.md)

## Service workflows (FastAPI)

- **Start-to-finish service path**: [Golden path (FastAPI)](../../integrations/fastapi/golden-path.md)
- **Columnar request/response bodies**: [FastAPI columnar bodies](../fastapi_columnar_bodies.md)
- **Async materialization patterns**: [FastAPI async materialization](../fastapi_async_materialization.md)
- **End-to-end examples**: [FastAPI end-to-end examples](../fastapi_end_to_end_examples.md)
- **Background tasks**: [FastAPI background tasks](../fastapi_background_tasks.md)
- **Observability**: [FastAPI observability](../fastapi_observability.md)
- **Settings**: [FastAPI settings](../fastapi_settings.md)

## Execution workflows

- **Understand cost / display / interchange**: [Execution](../../user-guide/execution.md)
- **Choose a materialization mode**: [Materialization](../../user-guide/materialization.md)

## Typing workflows

- **How typing works (mypy/pyright/ty)**: [Typing](../../user-guide/typing.md)
- **Common issues / sharp edges**: [Troubleshooting](../../getting-started/troubleshooting.md)

## “Where does pydantable fit?”

- [Why pydantable?](../../positioning/why-pydantable.md)
- [Comparisons](../../positioning/comparisons/index.md)

