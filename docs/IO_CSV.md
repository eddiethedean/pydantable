# CSV I/O

**Primary:** **`DataFrame[Schema].read_csv`**, **`write_csv`**, and **`DataFrameModel`** classmethods / instance methods. **Secondary:** **`pydantable.io`** — **`ScanFileRoot`**, **`materialize_csv`**, **`export_csv`**, **`fetch_csv_url`**.

## Read (sources)

### `DataFrame[Schema]` and `DataFrameModel`

- **`DataFrame[Schema].read_csv(path, *, columns=None, **scan_kwargs)`**
- **`MyModel.read_csv(...)`**, **`await MyModel.aread_csv(..., executor=None)`**
- **`MyModel.materialize_csv`**, **`await MyModel.amaterialize_csv`**

### `pydantable.io`

- **`read_csv`**, **`aread_csv`** — lazy **`ScanFileRoot`**
- **`materialize_csv`**, **`amaterialize_csv`** — eager **`dict[str, list]`** (**`engine`**, **`use_rap`** on sync path)
- **`fetch_csv_url`** — HTTP(S) → temp file → read; temp removed after read
- **`iter_csv`**, **`aiter_csv`**, **`write_csv_batches`** — stdlib **`csv`** batching over paths or text streams; cell values are **strings** (or **`None`** for short rows). See {doc}`IO_OVERVIEW` (**Batched column dict I/O**).

**`scan_kwargs`:** for example **`separator`**, **`has_header`**, **`skip_rows`**, **`skip_lines`**, **`n_rows`**, **`infer_schema_length`**, **`ignore_errors`**, **`low_memory`**, **`rechunk`**, **`glob`**, **`cache`**, **`quote_char`**, **`eol_char`**. Unknown keys raise **`ValueError`**. See {doc}`DATA_IO_SOURCES`.

**`use_rap=True`** ( **`materialize_csv`** only): uses **`aread_csv_rap`** when **no** event loop; in async code **`await aread_csv_rap(path)`** from **`pydantable.io.rap_support`**.

## Write (targets)

### `DataFrame[Schema]` and `DataFrameModel`

- **`df.write_csv(path, *, separator=..., compression=..., write_kwargs=..., streaming=...)`**
- **`model.write_csv(...)`** — same.

**`write_kwargs`:** **`include_header`**, **`include_bom`**. See {doc}`DATA_IO_SOURCES`.

### `pydantable.io`

- **`export_csv`**, **`aexport_csv`** — eager column dict → file.
- **`write_csv_batches`** — append many rectangular batches to one CSV (**`mode="w"`** / **`"a"`**, **`write_header`**).

## Runnable example

Run conventions: {doc}`IO_OVERVIEW` (**Runnable example**).

```bash
python docs/examples/io/csv_lazy_roundtrip.py
```

```{literalinclude} examples/io/csv_lazy_roundtrip.py
:language: python
```

## See also

{doc}`IO_OVERVIEW` · {doc}`IO_HTTP` · {doc}`EXECUTION`
