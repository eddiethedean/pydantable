# Cookbook

Opinionated, end-to-end recipes built for production usage. Every recipe aims to be:

- **copy/paste runnable**
- explicit about **validation** and **materialization** costs
- clear about **pitfalls** (ordering, null semantics, async cancellation)

```{toctree}
:titlesonly:
:maxdepth: 2

async_lazy_pipeline
fastapi_columnar_bodies
fastapi_async_materialization
io_lazy_pipeline_write
io_http_parquet_ctx
transforms_join_groupby
windows_framing_primer
```

