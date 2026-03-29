# Pydantable API Specification

**Status:** Sketch. **Canonical API:** [EXECUTION.md](../EXECUTION.md), package **`DataFrame`** / **`DataFrameModel`**.

## Execution Modes

### Sync
df.collect()

### Async
await df.acollect()

### Background
handle = df.submit()

## Streaming API

async for chunk in df.astream():
    process(chunk)

## FastAPI Integration

```python
@app.get("/pipeline")
async def pipeline():
    df = build_pipeline()
    return await df.acollect()
```

## Design Goals

- Zero user thread management
- Native async compatibility
- High-performance Rust execution
- Clean API symmetry
