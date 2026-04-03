# ADR: Modular execution engines

## Status

Accepted — documents the Python-side refactor introducing `pydantable.engine`.

## Context

PydanTable executes typed `DataFrame` operations via the PyO3 extension `pydantable._core` (Polars-backed). Call sites previously imported `pydantable.rust_engine` helpers and `_require_rust_core()` from many modules, which made an alternate backend (for example SQL generation) hard to isolate.

## Decision

1. **`NativePolarsEngine`** (`python/pydantable/engine/native.py`) owns all calls into `pydantable._core` for plan execution, eager ops (`execute_*`), and sinks (`write_*` to Parquet/CSV/IPC/NDJSON).
2. **`get_default_engine()`** returns a process-wide default engine typed as **`ExecutionEngine`**; **`set_default_engine(None)`** resets it to a lazily constructed **`NativePolarsEngine`** (primarily for tests).
3. **`DataFrame`** holds **`_engine`** and routes plan transforms and materialization through **`self._engine`** only (no direct `rust_core` access from the DataFrame implementation). **`_rust_plan`** remains the opaque logical plan handle (unchanged public shape).
4. **`ExecutionEngine`** (`python/pydantable/engine/protocols.py`) is the single structural protocol for a drop-in backend: **`PlanExecutor`** + **`SinkWriter`** + plan helpers (`make_plan`, `plan_*`, …) + **`capabilities`**. Implementations raise **`UnsupportedEngineOperationError`** for unsupported operations.
5. **`EngineCapabilities`** includes **`backend: Literal["native", "stub", "custom"]`** plus feature flags derived from the native module when applicable.
6. **`rust_engine.py`** remains a **compatibility shim**: free functions delegate to **`get_default_engine()`** so existing imports keep working.
7. **Expressions:** **`get_expression_runtime()`** supplies the object used to build `Expr` trees when the default engine is **`NativePolarsEngine`**; otherwise callers must use **`set_expression_runtime(...)`** or expression APIs will raise **`UnsupportedEngineOperationError`**.
8. **Protocols** (`PlanExecutor`, `SinkWriter`, `ExecutionEngine`) describe the intended seam; they do not require inheritance.
9. **`StubExecutionEngine`** (`python/pydantable/engine/stub.py`) is an in-tree reference for registry and typing tests.

## Track B (optional, not scheduled)

A portable Python expression IR shared by multiple backends would require a separate roadmap (Rust bridge, typing, and parity tests).

## Consequences

- New engine work should implement **`ExecutionEngine`** where possible and document gaps via **`EngineCapabilities`** and **`UnsupportedEngineOperationError`**.
- Tests that monkeypatched **`pydantable.dataframe._impl.execute_plan`** should patch **`NativePolarsEngine.execute_plan`** (or the frame’s engine class) so **`DataFrame`** dispatch stays consistent.
- Tests that monkeypatched **`rust_engine._RUST_CORE`** should patch **`pydantable.engine._binding._RUST_CORE`** instead so `NativePolarsEngine` sees the fake.
