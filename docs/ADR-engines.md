# ADR: Modular execution engines

## Status

Accepted — documents the Python-side refactor introducing `pydantable.engine`.

## Context

PydanTable executes typed `DataFrame` operations via the PyO3 extension shipped in `pydantable-native` (`pydantable_native._core`, Polars-backed). Call sites previously imported `pydantable.rust_engine` helpers and `_require_rust_core()` from many modules, which made an alternate backend (for example SQL generation) hard to isolate.

## Decision

1. **`NativePolarsEngine`** (in the `pydantable-native` distribution) owns all calls into `pydantable_native._core` for plan execution, eager ops (`execute_*`), and sinks (`write_*` to Parquet/CSV/IPC/NDJSON).
2. **`get_default_engine()`** returns a process-wide default engine typed as **`ExecutionEngine`**; **`set_default_engine(None)`** resets it to a lazily constructed **`NativePolarsEngine`** (primarily for tests).
3. **`DataFrame`** holds **`_engine`** and routes plan transforms and materialization through **`self._engine`** only (no direct `rust_core` access from the DataFrame implementation). **`_rust_plan`** remains the opaque logical plan handle (unchanged public shape).
4. **`ExecutionEngine`** is defined in the zero-dependency **`pydantable-protocol`** distribution (re-exported from `python/pydantable/engine/protocols.py` for convenience). It is the single structural protocol for a drop-in backend: **`PlanExecutor`** + **`SinkWriter`** + plan helpers (`make_plan`, `plan_*`, …) + **`capabilities`**. Third-party engine packages can depend only on **`pydantable-protocol`** for typing and **`pydantable_protocol.UnsupportedEngineOperationError`**; they do not need to install **`pydantable`**. Implementations raise that error (or a subclass) for unsupported operations. **`MissingRustExtensionError`** also lives in **`pydantable-protocol`** so **`pydantable-native`** can load without importing **`pydantable`**.
5. **`pydantable-native`** depends only on **`pydantable-protocol`** (not **`pydantable`**). **`pydantable`** declares **`pydantable-native`** as a required dependency so **`pip install pydantable`** installs the full stack.
6. **`EngineCapabilities`** includes **`backend: Literal["native", "stub", "custom"]`** plus feature flags derived from the native module when applicable.
7. **`rust_engine.py`** remains a **compatibility shim**: free functions delegate to **`get_default_engine()`** so existing imports keep working.
8. **Expressions:** **`get_expression_runtime()`** supplies the object used to build `Expr` trees when the default engine is **`NativePolarsEngine`**; otherwise callers must use **`set_expression_runtime(...)`** or expression APIs will raise **`UnsupportedEngineOperationError`**.
9. **Protocols** (`PlanExecutor`, `SinkWriter`, `ExecutionEngine`) describe the intended seam; they do not require inheritance.
10. **`StubExecutionEngine`** (`python/pydantable/engine/stub.py`) is an in-tree reference for registry and typing tests.
11. **Plan helpers** that build lazy plans (including **`plan_rolling_agg`**) live on **`ExecutionEngine`** so façades use **`DataFrame._engine`** instead of **`get_default_engine().rust_core`**.
12. **Lazy `ScanFileRoot` construction** in **`pydantable.io`** uses **`pydantable_native.require_rust_core()`** rather than importing the extension directly, keeping extension loading in one place.

## Consequences

- New engine work should implement **`ExecutionEngine`** where possible and document gaps via **`EngineCapabilities`** and **`UnsupportedEngineOperationError`**.
- Tests that monkeypatched **`pydantable.dataframe._impl.execute_plan`** should patch **`NativePolarsEngine.execute_plan`** (or the frame’s engine class) so **`DataFrame`** dispatch stays consistent.
- Tests that monkeypatched **`rust_engine._RUST_CORE`** should patch **`pydantable_native._binding._RUST_CORE`** instead so **`NativePolarsEngine`** sees the fake.
- **`scripts/check_engine_bypass.py`** (run via **`make engine-bypass-check`** and CI) fails if new code under **`python/pydantable/`** imports the native extension directly, uses **`get_default_engine().rust_core`**, or similar bypasses outside the allowlist: the whole **`python/pydantable/engine/`** tree, **`python/pydantable/_extension.py`**, and **`python/pydantable/rust_engine.py`**.

### Extension checklist (custom backend)

End-to-end guide for shipping a third-party engine on PyPI: [CUSTOM_ENGINE_PACKAGE](/CUSTOM_ENGINE_PACKAGE.md). In-repo optional integrations: [MOLTRES_SQL](/MOLTRES_SQL.md) (lazy-SQL stack), [MONGO_ENGINE](/MONGO_ENGINE.md) (**`MongoPydantableEngine`** in **pydantable**, **`MongoRoot`** from the Mongo plan stack for **`MongoDataFrame`**). Eager Mongo column-dict helpers (**`fetch_mongo`** / **`write_mongo`**, **`afetch_mongo`** / **`awrite_mongo`**, … — **PyMongo**) are normal I/O, not a third-party **`ExecutionEngine`** package.

1. Implement **`ExecutionEngine`** (see **`pydantable_protocol`**, re-exported under **`pydantable.engine.protocols`**) — mirror **`NativePolarsEngine`** for operations you support.
2. Return accurate **`capabilities`** (set **`backend`** to **`"custom"`** when not native/stub).
3. For unsupported calls, raise **`UnsupportedEngineOperationError`** with a clear message.
4. If users should build **`Expr`** against your default engine, register **`set_expression_runtime(...)`** (native does this implicitly via **`get_expression_runtime()`**).
5. Keep **`StubExecutionEngine`** and **`tests/test_engine_contract.py`** in sync when **`ExecutionEngine`** gains new members (contract tests use **`typing_extensions.get_protocol_members`**).

## Track B (optional, not scheduled)

A portable Python expression IR shared by multiple backends would require a separate roadmap (Rust bridge, typing, and parity tests). **Eager file I/O** helpers are provided by `pydantable-native` alongside the extension today; routing them through **`ExecutionEngine`** would be a separate phase if a single choke point for all native entry points is desired.
