# HTTP(S) and object-store reads

**Primary:** use **`DataFrame[Schema].read_parquet_url`** for lazy Parquet over HTTP, or pass column dicts from downloads into **`DataFrame` / `DataFrameModel`**. **Secondary:** **`pydantable.io`** — **`fetch_bytes`**, **`fetch_*_url`**, **`read_from_object_store`** (re-exported from **`pydantable.io.http`**).

## Experimental gate

By default, URL and cloud-style helpers require **`experimental=True`** on each call **or** environment variable **`PYDANTABLE_IO_EXPERIMENTAL=1`**. This matches **`fetch_bytes`** and **`read_from_object_store`**.

## `DataFrame` / `DataFrameModel`

**Lazy Parquet URL**

- **`DataFrame[Schema].read_parquet_url(url, *, experimental=True, columns=None, **kwargs)`**
- **`MyModel.read_parquet_url(...)`** — **`kwargs`** for **`fetch_bytes`** only (not **`scan_kwargs`**).
- **`DataFrameModel`** has **no** **`aread_parquet_url`**; use **`pydantable.io.aread_parquet_url`** and construct **`DataFrame[Schema]`** if you need async.

The temp **`.parquet`** is **not** auto-deleted; see {doc}`IO_PARQUET` and {doc}`DATA_IO_SOURCES`.

**Eager URL helpers (column dict)**

Pass **`fetch_parquet_url`**, **`fetch_csv_url`**, or **`fetch_ndjson_url`** results to **`MyModel(cols)`** or **`DataFrame[Schema](cols)`** — there are **no** **`DataFrameModel.fetch_*_url`** classmethods.

**Object stores**

Pass **`read_from_object_store(...)`** output to **`MyModel(cols)`** or **`DataFrame[Schema](cols)`** — there is no **`DataFrameModel.read_from_object_store`**.

## `pydantable.io`

### Raw bytes

- **`fetch_bytes(url, *, experimental=True, headers=None, timeout=60.0)`** — **HTTP/HTTPS** only (stdlib **`urllib`**).

### Eager format helpers (`dict[str, list]`)

| Function | Notes |
|----------|--------|
| **`fetch_parquet_url`** | PyArrow on bytes; optional **`columns=`**. |
| **`fetch_csv_url`** | Temp CSV file; Rust read with stdlib fallback. |
| **`fetch_ndjson_url`** | Temp NDJSON file; Rust read. |

**`kwargs`** beyond each function’s explicit parameters are passed through to **`fetch_bytes`** (e.g. **`headers`**, **`timeout`**).

### Lazy Parquet URL

- **`read_parquet_url`**, **`aread_parquet_url`** — return **`ScanFileRoot`**; same temp-file lifecycle as above.

### Object-store URIs (`s3://`, `gs://`, `az://`, …)

- **`read_from_object_store(uri, *, experimental=True, format="parquet", **kwargs)`**

Requires **`fsspec`** and a backend (e.g. **`s3fs`**). Install **`pydantable[cloud]`** or add dependencies manually. **`kwargs`** are forwarded to **`fsspec.open`**. **`format`** is **`"parquet"`** (default), **`"csv"`**, or **`"ndjson"`** / **`"jsonl"`** (object is read fully into memory, then decoded).

## Runnable example

Spawns a **local** **`http.server`** on **127.0.0.1** (no external network). Run conventions: {doc}`IO_OVERVIEW` (**Runnable example**).

```bash
python docs/examples/io/http_local_fetch.py
```

The script uses **`read_parquet_url`** from **`pydantable.io`** so it can **`os.unlink(root.path)`** after **`collect()`**; **`DataFrame.read_parquet_url`** performs the same download but you must arrange cleanup yourself if you need to delete the temp file immediately.

```{literalinclude} examples/io/http_local_fetch.py
:language: python
```

## See also

{doc}`IO_OVERVIEW` · {doc}`IO_PARQUET` · {doc}`IO_CSV` · {doc}`IO_NDJSON` · {doc}`FASTAPI` (thread-pool patterns for I/O)
