# Four materialization modes (lazy plan → Python)

A **`DataFrame`** / **`DataFrameModel`** holds a **lazy logical plan**. Turning it into rows, column dicts, Polars, Arrow, or chunked dicts is **materialization**. PydanTable exposes **four** ways to schedule and consume that terminal work. They all use the **same** Rust engine and semantics; they differ in **threading / async integration** and in **whether** you consume one blob or many **`dict[str, list]`** chunks.

Canonical enum: **`PlanMaterialization`** and **`plan_materialization_summary()`** (import from **`pydantable`**). Cost notes: [EXECUTION](../user-guide/execution.md). **FastAPI** route examples for all four modes: [FASTAPI](../integrations/fastapi/fastapi.md) (**Four materialization modes**).

| Mode | Enum | Primary APIs | Typical use |
|------|------|--------------|-------------|
| **Blocking (sync)** | `PlanMaterialization.BLOCKING` (`"blocking"`) | `collect`, `to_dict`, `to_polars`, `to_arrow`, `collect_batches`, lazy `write_*`, … | Scripts, CLIs, **sync** `def` route handlers |
| **Async (await)** | `PlanMaterialization.ASYNC` (`"async"`) | `await acollect`, `await ato_dict`, `await ato_polars`, `await ato_arrow`, `arows`, `ato_dicts`, … | **`async def`** ASGI handlers; keep the event loop responsive (see [EXECUTION](../user-guide/execution.md)) |
| **Deferred (background)** | `PlanMaterialization.DEFERRED` (`"deferred"`) | `submit()` → **`ExecutionHandle`**; `await handle.result()` | Overlap work with other awaits; optional custom `executor=` |
| **Chunked (dict batches)** | `PlanMaterialization.CHUNKED` (`"chunked"`) | `for b in df.stream(...)`, `async for b in df.astream(...)` | **Streaming HTTP** bodies (e.g. NDJSON lines per batch); still **one** full collect before slicing (see [EXECUTION](../user-guide/execution.md)) |

## Blocking (`BLOCKING`)

Default path: **blocking** Rust + Polars work on the **current thread**. Use **`collect()`** for validated row models, **`to_dict()`** / **`collect(as_lists=True)`** for column dicts, optional **`to_polars()`** / **`to_arrow()`** when those extras are installed.

**`collect_batches()`** is still **blocking** and still **one** engine collect; it returns **Polars** chunks. Only **`stream`** / **`astream`** fall under the **chunked** mode below.

## Async (`ASYNC`)

Same logical work as blocking, but invoked with **`await`**. When the wheel exposes **`async_execute_plan`**, the engine call is awaited as a Rust coroutine; otherwise work runs in **`asyncio.to_thread`** or your **`executor=`** ([EXECUTION](../user-guide/execution.md)).

## Deferred (`DEFERRED`)

**`submit()`** starts **`collect()`** (same kwargs) on a **daemon thread** or a **`ThreadPoolExecutor`** you pass. **`await handle.result()`** is async and matches the result of **`collect()`**. **`handle.cancel()`** only cancels the wait if work has not started—it does **not** abort in-flight engine work.

## Chunked (`CHUNKED`)

**`stream()`** and **`astream()`** yield **`dict[str, list]`** batches after **one** full materialization, using the same slicing strategy as **`collect_batches`**. This is **chunked replay** for serialization-friendly HTTP streaming, **not** Polars out-of-core streaming and **not** a promise of bounded memory for huge scans. Requires **`pydantable[polars]`** for chunk conversion.

## Related

- **Eager file I/O** on **`DataFrameModel`** (**`materialize_*`**, **`fetch_sql`**, …) is **not** a fifth plan mode: it loads external data into a typed frame (backed by **`dict[str, list]`**), then you run transforms and terminal materialization as usual ([IO_OVERVIEW](../io/overview.md)).
- **Engine streaming** (**`streaming=True`** / **`Engine::Streaming`**) is a **Polars collect** option, orthogonal to the four modes above ([EXECUTION](../user-guide/execution.md)).
