# Troubleshooting / FAQ

## `MissingRustExtensionError`

**Symptom:** you see `MissingRustExtensionError` (a `NotImplementedError` subclass).

**Meaning:** the native extension (`pydantable-native`, module `pydantable_native._core`) is missing or does not export a required symbol.

**Common causes**

- You are in a source checkout and have not built the extension yet.
- You installed from source without building wheels (e.g. editable install without `maturin develop`).
- You are running on a platform/Python where a wheel is not available and build failed.

**Fix**

- In a repo checkout:

```bash
maturin develop --manifest-path pydantable-core/Cargo.toml
```

- Or install a published wheel:

```bash
pip install pydantable
```

See [DEVELOPER](/DEVELOPER.md) for contributor setup.

## Pandas UI: `ImportError` / `ModuleNotFoundError` for **`pandas`**

**Symptom:** calling **`get_dummies`**, **`cut`**, **`qcut`**, **`factorize_column`**, or **`ewm().mean()`** on **`pydantable.pandas.DataFrame`** fails because **`pandas`** is not installed.

**Meaning:** those helpers are **eager** and delegate to **pandas** (or NumPy via the same stack) for binning / factorize / exponential weighted mean after one materialization.

**Fix:** `pip install pandas` (not required for the default Polars-style export). Duplicate detection (**`duplicated`**, **`drop_duplicates(keep=False)`**) uses the Rust plan and does **not** need pandas. See [PANDAS_UI](/PANDAS_UI.md).

## Why does `df.shape[0]` not match the number of materialized rows?

`shape` follows **root-buffer semantics** and can be out of sync after lazy transforms (e.g. `filter`).

- Use `to_dict()` / `collect()` to get the executed row count.
- See [INTERFACE_CONTRACT](/INTERFACE_CONTRACT.md) (Introspection) for the contract.

## Why did my test fail because rows are in a different order?

Row order is **not** a stable API guarantee for many operations.

- Compare results by sorting on identity keys (join keys, group keys).
- The test suite uses this pattern intentionally.
- See [INTERFACE_CONTRACT](/INTERFACE_CONTRACT.md) (Ordering).

## Async: why doesn’t cancellation stop execution?

Async APIs (`acollect`, `ato_dict`, …) run blocking native work in a worker thread (or a native async path when available). Cancelling the awaiting task does **not** cancel in-flight Rust/Polars execution—the background work can still run to completion.

If you need strict cancellation semantics, design your service route around **timeouts** and **request limits** (reverse proxies, worker kill policies) rather than expecting the engine call to stop mid-flight. For **FastAPI** patterns that combine async materialization with **`BackgroundTasks`**, see [fastapi_background_tasks](/cookbook/fastapi_background_tasks.md) and [EXECUTION](/EXECUTION.md).

## I/O: why didn’t I get a Rust error under `engine="auto"`?

For **`materialize_*`** (and the **`PYDANTABLE_IO_ENGINE`** default **`auto`**), pydantable **prefers Rust** when the path applies, but on **failure** it may **fall back** to PyArrow or stdlib without re-raising the Rust error. That keeps pipelines resilient but can hide native-only failures during development.

- To **force** the Rust path and see exceptions: **`engine="rust"`** or **`PYDANTABLE_IO_ENGINE=rust`** (when the format supports it).
- Details: [IO_OVERVIEW](/IO_OVERVIEW.md) (Engine matrix), [IO_DECISION_TREE](/IO_DECISION_TREE.md) (Engine selection).

## Performance: why is `to_arrow()` / `to_polars()` slower than expected?

These APIs **materialize** the plan to a Python `dict[str, list]` first, then build Arrow/Polars objects in Python.

- `to_arrow()` is not a zero-copy export of engine buffers.
- Prefer lazy file workflows (`read_*` → transforms → `write_*`) for large data.
- See [EXECUTION](/EXECUTION.md) (Materialization costs).

## I/O: when should I use `read_*` vs `materialize_*`?

- Use **`read_*`** for large local files and pipelines where you will transform then write.
- Use **`materialize_*`** when you want a Python column dict in memory (small/medium data, tests).

Use [IO_DECISION_TREE](/IO_DECISION_TREE.md) to pick the right entrypoint.

## Typing: why doesn’t my editor infer the after-schema type?

Some type checkers cannot automatically infer schema-evolving return types from transformation chains. That includes **Pyright**, **Pylance**, and **Astral `ty`**, and any setup **without** the pydantable **mypy** plugin ( **`ty` does not load mypy plugins** ).

- For Pyright, Pylance, **`ty`**, and similar, use `DataFrameModel.as_model(AfterModel)` to state the after-model explicitly.
- If you prefer not to raise on mismatch, use `try_as_model(AfterModel) -> AfterModel | None`.
- If you want a richer mismatch explanation, use `assert_model(AfterModel)` (raises with a schema diff).
- For **mypy** with `pydantable.mypy_plugin`, transform chains can often be typed automatically; see [TYPING](/TYPING.md).

## FastAPI: `MissingRustExtensionError` on import

**Symptom:** `import pydantable.fastapi` raises **`MissingRustExtensionError`** (same as importing the core package without a built wheel).

**Fix:** build or install the native extension as in the **`MissingRustExtensionError`** section at the top of this page. The FastAPI helpers require the compiled package.

## FastAPI: column length mismatch returns **400** but I expected **500**

If you call **`validate_columns_strict`** or construct a frame from unequal-length columns and **`register_exception_handlers`** is installed, **`ColumnLengthMismatchError`** is mapped to **HTTP 400** (bad request), not **500**. Disable the handler or catch the exception in the route if you need different behavior.

## FastAPI: tests hang or executor is missing in `TestClient`

Use **`pydantable.testing.fastapi.fastapi_app_with_executor`** / **`fastapi_test_client`** so the app’s lifespan runs and **`RequestContext.executor`** is bound. See [FASTAPI](/FASTAPI.md) (Testing helpers).

