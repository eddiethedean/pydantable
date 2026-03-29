# JSON (array and JSON Lines)

## Lazy local file (`read_json`)

:func:`pydantable.io.read_json` is a **JSON Lines** lazy scan — the same Polars path as :func:`~pydantable.io.read_ndjson` (newline-delimited objects). It returns a :class:`~pydantable._core.ScanFileRoot` for :class:`~pydantable.dataframe.DataFrame` / :class:`~pydantable.dataframe_model.DataFrameModel`.

For a single JSON **array** of objects (``[{...}, {...}]``), use eager :func:`~pydantable.io.materialize_json` and wrap the column dict in your model — there is no out-of-core lazy scan for that layout in pydantable today. For **batched** Python-side processing of an array file, :func:`~pydantable.io.iter_json_array` / :func:`~pydantable.io.aiter_json_array` still **load the entire JSON** first, then yield **`dict[str, list]`** chunks (**batch_size**).

## Eager column dict

* :func:`pydantable.io.materialize_json` — detects array vs JSON Lines (see implementation).
* :func:`pydantable.io.export_json` — writes one JSON **array** of row objects.

Async mirrors: :func:`~pydantable.io.amaterialize_json`, :func:`~pydantable.io.aexport_json`.

## `DataFrameModel`

Classmethods :meth:`~pydantable.dataframe_model.DataFrameModel.read_json`, :meth:`~pydantable.dataframe_model.DataFrameModel.materialize_json`, and :meth:`~pydantable.dataframe_model.DataFrameModel.export_json` delegate to :mod:`pydantable.io` (same patterns as other formats).

See also: {doc}`IO_NDJSON`, {doc}`IO_DECISION_TREE`.
