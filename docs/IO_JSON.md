# JSON (array and JSON Lines)

## Lazy local file (`read_json`)

**Default:** :meth:`~pydantable.dataframe_model.DataFrameModel.read_json` / :meth:`~pydantable.dataframe_model.DataFrameModel.aread_json` (lazy **JSON Lines** scan — same Polars path as :func:`~pydantable.io.read_ndjson`). Module :func:`pydantable.io.read_json` returns a raw :class:`~pydantable._core.ScanFileRoot` for :class:`~pydantable.dataframe.DataFrame` / :class:`~pydantable.dataframe_model.DataFrameModel` without a typed wrapper.

For a single JSON **array** of objects (``[{...}, {...}]``), use :func:`pydantable.io.materialize_json` / :func:`~pydantable.io.amaterialize_json` and pass the result to **`MyModel(...)`** — there is no out-of-core lazy scan for that layout in pydantable today. For **batched** Python-side processing of an array file, :func:`~pydantable.io.iter_json_array` / :func:`~pydantable.io.aiter_json_array` still **load the entire JSON** first, then yield **`dict[str, list]`** chunks (**batch_size**).

## Eager column dict

**Default:** :func:`pydantable.io.materialize_json` / :func:`~pydantable.io.amaterialize_json` return **`dict[str, list]`** (detects array vs JSON Lines); wrap with **`MyModel(cols, ...)`** for a typed frame.

**Utilities:** :func:`pydantable.io.export_json` / :func:`~pydantable.io.aexport_json` write one JSON **array** of row objects.

See also: {doc}`IO_NDJSON`, {doc}`IO_DECISION_TREE`.
