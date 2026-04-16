# Pydantable Production-Grade Execution & Async Architecture Spec

**Status:** Architecture sketch. **Shipped subset:** [EXECUTION.md](../user-guide/execution.md) (**`acollect`**, **`submit`**, **`astream`**, Rust **`async_execute_plan`**). **Not shipped here:** distributed execution, Arrow Flight, adaptive planning (see [ROADMAP.md](../project/roadmap.md)).

## 1. Vision

Pydantable is a Rust-backed DataFrame engine with first-class async
orchestration in Python, enabling seamless integration with modern async
systems (e.g., FastAPI) while leveraging high-performance parallel
execution in Rust.

------------------------------------------------------------------------

## 2. Core Principles

-   Async is for orchestration, not computation
-   Rust handles all heavy computation using parallel execution (Rayon)
-   Python remains non-blocking and event-loop friendly
-   Zero user-managed threading

------------------------------------------------------------------------

## 3. Execution Model

### Phases

1.  Logical Plan Construction (Python)
2.  Physical Plan Optimization (Rust)
3.  Execution (Rust, parallel)
4.  Materialization (Rust → Python)

------------------------------------------------------------------------

## 4. API Surface

### Sync

``` python
df.collect()
```

### Async

``` python
await df.acollect()
```

### Background Jobs

``` python
handle = df.submit()
result = await handle.result()
```

### Streaming

``` python
async for batch in df.astream():
    process(batch)
```

------------------------------------------------------------------------

## 5. Execution Handle Design

``` python
class ExecutionHandle:
    async def result(self): ...
    def cancel(self): ...
    def done(self): ...
```

Features: - Awaitable result - Cancellation support - Status polling

------------------------------------------------------------------------

## 6. Rust Execution Layer

-   Uses Rayon thread pool
-   Query plan executed in parallel
-   Memory handled via Arrow-compatible structures

------------------------------------------------------------------------

## 7. Rust ↔ Python Async Bridge

### Technology

-   PyO3
-   **`pyo3-async-runtimes`** (Tokio runtime; supersedes the older **`pyo3-asyncio`** naming in docs)

### Flow

1.  Python calls `acollect`
2.  Rust spawns execution task
3.  Returns Future to Python
4.  Python awaits result

------------------------------------------------------------------------

## 8. Streaming Architecture

-   Output produced in Arrow record batches
-   Batches pushed through async channel
-   Python consumes via async iterator

Backpressure handled via bounded channels.

------------------------------------------------------------------------

## 9. FastAPI Integration

``` python
@app.get("/pipeline")
async def pipeline():
    df = build_pipeline()
    return await df.acollect()
```

------------------------------------------------------------------------

## 10. Scheduler & Task Integration

Supports: - APScheduler - Celery (future) - Native task submission via
`submit()`

------------------------------------------------------------------------

## 11. Error Handling

-   Rust errors mapped to Python exceptions
-   Async-safe propagation
-   Partial failure handling (streaming mode)

------------------------------------------------------------------------

## 12. Future Roadmap

-   Distributed execution
-   Arrow Flight integration
-   Query caching
-   Adaptive execution planning

------------------------------------------------------------------------

## 13. Competitive Positioning

  Feature                      Pandas   Polars   Pydantable
  ---------------------------- -------- -------- ------------
  Async API                    ❌       ❌       ✅
  Rust backend                 ❌       ✅       ✅
  Native async orchestration   ❌       ❌       ✅
  Streaming API                ❌       ⚠️       ✅

------------------------------------------------------------------------

## 14. Summary

Pydantable bridges the gap between async Python systems and
high-performance data processing by combining:

-   Async-native APIs
-   Rust-powered execution
-   Clean, unified pipeline model
