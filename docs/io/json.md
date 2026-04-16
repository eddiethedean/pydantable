# JSON (array and JSON Lines)

How JSON values map to typed columns (including nested objects and maps) is summarized in [SUPPORTED_TYPES](../user-guide/supported-types.md) (**JSON (RFC 8259) vs column types**).

## Naming: `read_json` vs `read_ndjson` vs `materialize_json`

| API | Role | Layout |
|-----|------|--------|
| :meth:`~pydantable.dataframe_model.DataFrameModel.read_json` / :meth:`~pydantable.dataframe_model.DataFrameModel.read_ndjson` | Typed lazy :class:`~pydantable.dataframe.DataFrame` / :class:`~pydantable.dataframe_model.DataFrameModel` (same Polars path). | **JSON Lines** only — ``read_json`` is an alias of ``read_ndjson``. |
| :func:`materialize_json` | Eager **``dict[str, list]``** read (**``from pydantable import materialize_json``**). | Detects **array** ``[{...}, ...]`` **or** JSON Lines in one file. |

**Internals:** untyped lazy scans return a raw :class:`~pydantable_native._core.ScanFileRoot`; that layer lives in the **`pydantable.io`** implementation package—**application code** should use **`DataFrameModel`** / **`DataFrame`** classmethods or import helpers from **`pydantable`**.

There is **no** lazy scan for a single JSON **array** file in pydantable today — use :func:`materialize_json` (or :func:`~pydantable.io.iter_json_array` for batched array processing after a full parse; **``iter_json_array``** is not on the root package—prefer lazy NDJSON or **`materialize_json`**).

**Multi-file / directory:** lazy **`read_json`** / **`read_ndjson`** follow the same **JSON Lines** path semantics (directories, **`*.jsonl`** globs, **`scan_kwargs`** including **`glob=True`** for API parity with **`read_csv`** / **`read_parquet`**). Polars uses **`LazyJsonLineReader`**; details and **`glob=False`** behavior: [IO_NDJSON](ndjson.md). **JSON array** files and **array** datasets in directories are **not** lazily scanned—use per-file **`materialize_json`** / **`iter_json_array`**, or convert to NDJSON (see [Polars 0.53 vs pydantable scan audit](data-io-sources.md#audit-polars-053x-vs-pydantable-1110-phase-a)).

## Large files, memory, and entrypoint choice

**Prefer lazy JSON Lines for big logs:** :meth:`~pydantable.dataframe_model.DataFrameModel.read_ndjson` / ``read_json`` (or :class:`~pydantable.dataframe.DataFrame` classmethods) keep work on a Polars :class:`~polars.LazyFrame` until you :meth:`~pydantable.dataframe.DataFrame.collect`, :meth:`~pydantable.dataframe.DataFrame.to_dict`, or :meth:`~pydantable.dataframe.DataFrame.write_parquet` / ``write_ndjson`` / etc. The full file is **not** loaded as a Python column dict first.

**Terminal materialization:** :meth:`~pydantable.dataframe.DataFrame.collect` runs the plan and returns rows (or use ``collect(as_lists=True)`` / :meth:`~pydantable.dataframe.DataFrame.to_dict` for columnar dicts). For very large results, consider :meth:`~pydantable.dataframe.DataFrame.head` / :meth:`~pydantable.dataframe.DataFrame.slice` **before** collect, or write to disk with a lazy ``write_*`` sink instead of pulling everything into Python.

**Polars streaming engine:** on terminal APIs you can pass ``streaming=True`` (or set ``PYDANTABLE_ENGINE_STREAMING``) so the Rust engine requests Polars **streaming** collect where supported — **best-effort**; some plans fall back to in-memory behavior. See [EXECUTION](../user-guide/execution.md) (**Streaming / engine collect**).

**Chunked Python-side reads:** :func:`iter_ndjson` / :func:`aiter_ndjson` (**``from pydantable import iter_ndjson, aiter_ndjson``**) yield ``dict[str, list]`` **batches** of a fixed row count — useful when you want bounded Python memory **without** building a lazy ``DataFrame`` plan (e.g. simple ETL scripts). See [IO_OVERVIEW](../io/overview.md) (**Batched column dict I/O**).

**JSON array files** (``[{...}, {...}]``): use :func:`materialize_json` / :func:`amaterialize_json` for a full eager column dict, or :func:`~pydantable.io.iter_json_array` / :func:`~pydantable.io.aiter_json_array` — those paths **load the entire JSON** first, then chunk in Python (advanced; **``iter_json_array``** is not re-exported on **`pydantable`**).

Runnable example (lazy filter + ``collect``):

```bash
python docs/examples/io/large_ndjson_patterns.py
```


--8<-- "examples/io/large_ndjson_patterns.py"

### Output

```text
--8<-- "examples/io/large_ndjson_patterns.py.out.txt"
```

## NDJSON scan kwargs (presets)

Typed and untyped lazy reads accept **``**scan_kwargs``** forwarded to Polars (see [DATA_IO_SOURCES](../io/data-io-sources.md)). **``read_json``** uses the same allowlist as **``read_ndjson``**. For **NDJSON / JSON Lines**, allowed keys include:

- **``infer_schema_length``** — how many lines Polars uses to infer dtypes (or ``None`` for engine default). Increase if early lines are not representative of the full file (e.g. sparse optional fields appear later).
- **``n_rows``** — cap rows read from the file (sampling / debugging).
- **``ignore_errors``** — skip malformed lines where the engine allows it; **can drop data silently** — use only for dirty logs when you accept lossy ingest.
- **``low_memory``**, **``rechunk``** — Polars memory / chunking hints.

Omitted keys use **Polars** defaults for ``LazyJsonLineReader`` (pydantable pins Polars **0.53**; see upstream Polars docs for default numeric values).

## Eager column dict

**Default:** :func:`materialize_json` / :func:`amaterialize_json` return **`dict[str, list]`** (detects array vs JSON Lines); wrap with **`MyModel(cols, ...)`** for a typed frame.

**Utilities:** :meth:`~pydantable.dataframe_model.DataFrameModel.export_json` / :meth:`~pydantable.dataframe_model.DataFrameModel.aexport_json` write one JSON **array** of row objects (or call the same shapes via **`pydantable.io`** helpers if you are extending the library).

## Eager ``export_json`` serialization

:meth:`~pydantable.dataframe_model.DataFrameModel.export_json` uses the standard library ``json.dump`` with ``default=str`` (same behavior as the **`pydantable.io`** implementation). Nested Python ``dict`` and ``list`` cells serialize as JSON objects and arrays. Values that are not JSON-native (for example ``datetime``, ``Decimal``, ``UUID``) are written as ``str(value)``, which is **not** necessarily ISO-8601 or another stable wire format. For API-stable JSON, build rows with :meth:`~pydantable.dataframe_model.DataFrameModel.to_dicts` / Pydantic ``model_dump(mode="json")`` after materialization, or normalize scalars before calling ``export_json``.

**Lazy NDJSON** and **eager** ``materialize_json`` can infer nested shapes differently; map-like JSON objects may round-trip more reliably through **eager** ``materialize_json`` + a typed constructor than through a **lazy** scan—see tests in ``tests/test_json_io_phase_a.py``.

To emit JSON **text from nested model columns inside a frame**, use :meth:`~pydantable.expressions.Expr.struct_json_encode` on the struct-typed :class:`~pydantable.expressions.Expr` (Polars-backed); see [SUPPORTED_TYPES](../user-guide/supported-types.md).

To **parse JSON strings in-column** back into structs or maps (without a separate file read), use :meth:`~pydantable.expressions.Expr.str_json_decode` after you have a **`str`** column of JSON text (e.g. from **`struct_json_encode`**, logging pipelines, or `materialize_json` on a string field).

See also: [IO_NDJSON](../io/ndjson.md), [IO_DECISION_TREE](../io/decision-tree.md).
