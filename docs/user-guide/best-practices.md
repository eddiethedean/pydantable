# Best practices

Opinionated patterns for production services and typed pipelines.

## Choose the right table type

| Pattern | Use when |
|---------|----------|
| **`DataFrameModel`** | FastAPI routes, reusable table definitions, **`RowModel`** for OpenAPI |
| **`DataFrame[Schema]`** | Libraries, generic utilities, minimal class-body sugar |
| Raw **`pydantic.BaseModel`** | Rare — prefer **`Schema`** or **`DataFrameModel`** |

## Validation and `trusted_mode`

```text
Untrusted client data  →  trusted_mode="off" (default)
Pre-validated internal →  shape_only or strict (document the trust boundary)
```

Pair **`register_exception_handlers`** in FastAPI with explicit **`response_model`** types.

## I/O entrypoints

- **Large files:** **`read_*`** → transforms → **`write_*`** (lazy pipeline)
- **Small/medium or tests:** **`materialize_*`** → **`MyModel(cols)`**
- **Unsure:** [I/O decision tree](../io/decision-tree.md)

## Testing

- Sort by **identity keys** before comparing rows ([Interface contract](../semantics/interface-contract.md))
- Use **`pydantable.testing.fastapi`** for lifespan-aware **`TestClient`**
- Patch **`engine=`** or use stub engines for unit tests — see [Developer guide](../project/developer.md)

## Schema design

- Prefer explicit **`int | None`** over bare optional defaults for service contracts
- Use nested **`class Row(Schema):`** on **`DataFrameModel`** for row-level validators
- Document column aliases in [Service ergonomics](service-ergonomics.md) when ingesting external JSON

## Performance

- **`to_polars()`** / **`to_arrow()`** materialize through Python dicts — not zero-copy
- **`trusted_mode="shape_only"`** on large trusted ingests skips per-cell Pydantic
- See [Execution](execution.md) and [Performance](../project/performance.md)

## Related

- [Mental model](../concepts/mental-model.md)
- [Troubleshooting](../getting-started/troubleshooting.md)
- [Security considerations](../project/security.md)
