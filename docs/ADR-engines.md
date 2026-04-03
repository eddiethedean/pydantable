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
10. **Plan helpers** that build lazy plans (including **`plan_rolling_agg`**) live on **`ExecutionEngine`** so façades use **`DataFrame._engine`** instead of **`get_default_engine().rust_core`**.
11. **Lazy `ScanFileRoot` construction** in **`pydantable.io`** uses **`pydantable.engine._binding.require_rust_core()`** rather than importing **`pydantable._core`** directly, keeping extension loading in one place.

## Consequences

- New engine work should implement **`ExecutionEngine`** where possible and document gaps via **`EngineCapabilities`** and **`UnsupportedEngineOperationError`**.
- Tests that monkeypatched **`pydantable.dataframe._impl.execute_plan`** should patch **`NativePolarsEngine.execute_plan`** (or the frame’s engine class) so **`DataFrame`** dispatch stays consistent.
- Tests that monkeypatched **`rust_engine._RUST_CORE`** should patch **`pydantable.engine._binding._RUST_CORE`** instead so `NativePolarsEngine` sees the fake.
- **`scripts/check_engine_bypass.py`** (run via **`make engine-bypass-check`** and CI) fails if new code under **`python/pydantable/`** imports **`pydantable._core`**, uses **`get_default_engine().rust_core`**, or similar bypasses outside the allowlist: the whole **`python/pydantable/engine/`** tree, **`python/pydantable/io/_core_io.py`**, **`python/pydantable/_extension.py`**, and **`python/pydantable/rust_engine.py`**.

### Extension checklist (custom backend)

1. Implement **`ExecutionEngine`** (see **`python/pydantable/engine/protocols.py`**) — mirror **`NativePolarsEngine`** for operations you support.
2. Return accurate **`capabilities`** (set **`backend`** to **`"custom"`** when not native/stub).
3. For unsupported calls, raise **`UnsupportedEngineOperationError`** with a clear message.
4. If users should build **`Expr`** against your default engine, register **`set_expression_runtime(...)`** (native does this implicitly via **`get_expression_runtime()`**).
5. Keep **`StubExecutionEngine`** and **`tests/test_engine_contract.py`** in sync when **`ExecutionEngine`** gains new members (contract tests use **`typing_extensions.get_protocol_members`**).

## Track B (optional, not scheduled)

A portable Python expression IR shared by multiple backends would require a separate roadmap (Rust bridge, typing, and parity tests). **Eager file I/O** helpers that still call **`pydantable._core`** directly remain in **`io/_core_io.py`** by design; routing them through **`ExecutionEngine`** would be a separate phase if a single choke point for all native entry points is desired.
