# Troubleshooting / FAQ

## `MissingRustExtensionError`

**Symptom:** you see `MissingRustExtensionError` (a `NotImplementedError` subclass).

**Meaning:** `pydantable._core` (the native extension) is missing or does not export a required symbol.

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

See {doc}`DEVELOPER` for contributor setup.

## Why does `df.shape[0]` not match the number of materialized rows?

`shape` follows **root-buffer semantics** and can be out of sync after lazy transforms (e.g. `filter`).

- Use `to_dict()` / `collect()` to get the executed row count.
- See {doc}`INTERFACE_CONTRACT` (Introspection) for the contract.

## Why did my test fail because rows are in a different order?

Row order is **not** a stable API guarantee for many operations.

- Compare results by sorting on identity keys (join keys, group keys).\n+- The test suite uses this pattern intentionally.\n+- See {doc}`INTERFACE_CONTRACT` (Ordering).

## Async: why doesn’t cancellation stop execution?

Async APIs (`acollect`, `ato_dict`, …) run blocking native work in a worker thread. Cancelling the awaiting task does **not** cancel in-flight Rust/Polars execution.

If you need strict cancellation semantics, design your service route around timeouts and request limits rather than expecting the engine call to stop mid-flight.

## Performance: why is `to_arrow()` / `to_polars()` slower than expected?

These APIs **materialize** the plan to a Python `dict[str, list]` first, then build Arrow/Polars objects in Python.

- `to_arrow()` is not a zero-copy export of engine buffers.\n+- Prefer lazy file workflows (`read_*` → transforms → `write_*`) for large data.\n+- See {doc}`EXECUTION` (Materialization costs).

## I/O: when should I use `read_*` vs `materialize_*`?

- Use **`read_*`** for large local files and pipelines where you will transform then write.\n+- Use **`materialize_*`** when you want a Python column dict in memory (small/medium data, tests).\n+\n+Use {doc}`IO_DECISION_TREE` to pick the right entrypoint.

