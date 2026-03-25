# Arrow IPC / Feather file I/O

**Module:** `pydantable.io` · **Typed frames:** `DataFrame[Schema].read_ipc`, `write_ipc` (and `DataFrameModel` equivalents).

This covers **Arrow IPC file** (`.arrow` / `.feather`-style single file), not arbitrary **streaming IPC** on a socket unless you materialize through PyArrow yourself.

## Read (sources)

### Lazy local file → `ScanFileRoot`

- **`read_ipc(path, *, columns=None, **scan_kwargs)`**
- **`aread_ipc(...)`**

**`scan_kwargs`:** **`record_batch_statistics`** is supported. Unknown keys raise **`ValueError`**. See {doc}`DATA_IO_SOURCES`.

### Eager `dict[str, list]`

- **`materialize_ipc(source, *, as_stream=False, engine=None)`**
- **`amaterialize_ipc(...)`**

**`as_stream=False`** (default): local file paths can use Rust; otherwise PyArrow. **`as_stream=True`** uses PyArrow stream decoding. Install **`pydantable[arrow]`** when the path goes through PyArrow.

## Write (targets)

### Lazy plan → file

- **`DataFrame[Schema].write_ipc(path, *, compression=None)`**

IPC sink options are intentionally narrow: use top-level **`compression=`**. **Non-empty `write_kwargs`** is rejected (so scan-style keys do not silently apply to the wrong API).

### Eager column dict → file

- **`export_ipc(path, data, *, engine=None)`**
- **`aexport_ipc(...)`**

## Runnable example

Run conventions: {doc}`IO_OVERVIEW` (**Runnable example**).

```bash
python docs/examples/io/ipc_roundtrip.py
```

```{literalinclude} examples/io/ipc_roundtrip.py
:language: python
```

## See also

{doc}`IO_OVERVIEW` · {doc}`EXECUTION` · {doc}`SUPPORTED_TYPES` (Arrow interchange)
