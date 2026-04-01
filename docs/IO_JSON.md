# JSON (array and JSON Lines)

How JSON values map to typed columns (including nested objects and maps) is summarized in {doc}`SUPPORTED_TYPES` (**JSON (RFC 8259) vs column types**).

## Lazy local file (`read_json`)

**Default:** :meth:`~pydantable.dataframe_model.DataFrameModel.read_json` / :meth:`~pydantable.dataframe_model.DataFrameModel.aread_json` (lazy **JSON Lines** scan — same Polars path as :func:`~pydantable.io.read_ndjson`). Module :func:`pydantable.io.read_json` returns a raw :class:`~pydantable._core.ScanFileRoot` for :class:`~pydantable.dataframe.DataFrame` / :class:`~pydantable.dataframe_model.DataFrameModel` without a typed wrapper.

For a single JSON **array** of objects (``[{...}, {...}]``), use :func:`pydantable.io.materialize_json` / :func:`~pydantable.io.amaterialize_json` and pass the result to **`MyModel(...)`** — there is no out-of-core lazy scan for that layout in pydantable today. For **batched** Python-side processing of an array file, :func:`~pydantable.io.iter_json_array` / :func:`~pydantable.io.aiter_json_array` still **load the entire JSON** first, then yield **`dict[str, list]`** chunks (**batch_size**).

## Eager column dict

**Default:** :func:`pydantable.io.materialize_json` / :func:`~pydantable.io.amaterialize_json` return **`dict[str, list]`** (detects array vs JSON Lines); wrap with **`MyModel(cols, ...)`** for a typed frame.

**Utilities:** :func:`pydantable.io.export_json` / :func:`~pydantable.io.aexport_json` write one JSON **array** of row objects.

## Eager ``export_json`` serialization

:func:`~pydantable.io.export_json` uses the standard library ``json.dump`` with ``default=str``. Nested Python ``dict`` and ``list`` cells serialize as JSON objects and arrays. Values that are not JSON-native (for example ``datetime``, ``Decimal``, ``UUID``) are written as ``str(value)``, which is **not** necessarily ISO-8601 or another stable wire format. For API-stable JSON, build rows with :meth:`~pydantable.dataframe_model.DataFrameModel.to_dicts` / Pydantic ``model_dump(mode="json")`` after materialization, or normalize scalars before calling ``export_json``.

**Lazy NDJSON** and **eager** ``materialize_json`` can infer nested shapes differently; map-like JSON objects may round-trip more reliably through **eager** ``materialize_json`` + a typed constructor than through a **lazy** scan—see tests in ``tests/test_json_io_phase_a.py``.

To emit JSON **text from nested model columns inside a frame**, use :meth:`~pydantable.expressions.Expr.struct_json_encode` on the struct-typed :class:`~pydantable.expressions.Expr` (Polars-backed); see {doc}`SUPPORTED_TYPES`.

See also: {doc}`IO_NDJSON`, {doc}`IO_DECISION_TREE`.
