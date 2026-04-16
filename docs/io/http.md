# HTTP(S) and object-store reads

**Primary:** use **`DataFrame[Schema].read_parquet_url`** for lazy Parquet over HTTP, or pass column dicts from downloads into **`DataFrame` / `DataFrameModel`**. **Secondary:** **`pydantable.io`** — **`fetch_bytes`**, **`fetch_*_url`**, **`read_from_object_store`** (re-exported from **`pydantable.io.http`**).

## Experimental gate

By default, URL and cloud-style helpers require **`experimental=True`** on each call **or** environment variable **`PYDANTABLE_IO_EXPERIMENTAL=1`**. This matches **`fetch_bytes`** and **`read_from_object_store`**.

## `DataFrame` / `DataFrameModel`

**Lazy Parquet URL**

- **`DataFrame[Schema].read_parquet_url(url, *, experimental=True, columns=None, **kwargs)`**
- **`MyModel.read_parquet_url(...)`** — **`kwargs`** for **`fetch_bytes`** only (not **`scan_kwargs`**).
- **`pydantable.io.read_parquet_url_ctx`**, **`aread_parquet_url_ctx`**, and **`DataFrameModel.read_parquet_url_ctx`**, **`aread_parquet_url_ctx`** delete the temp file when the context block exits (preferred when you do not need the path yourself).

The temp **`.parquet`** from the non-context **`read_parquet_url`** is **not** auto-deleted; see [IO_PARQUET](../io/parquet.md) and [DATA_IO_SOURCES](../io/data-io-sources.md).

**Eager URL helpers (column dict)**

Pass **`fetch_parquet_url`**, **`fetch_csv_url`**, or **`fetch_ndjson_url`** results to **`MyModel(cols)`** or **`DataFrame[Schema](cols)`** — there are **no** **`DataFrameModel.fetch_*_url`** classmethods.

**Object stores**

Pass **`read_from_object_store(...)`** output to **`MyModel(cols)`** or **`DataFrame[Schema](cols)`** — there is no **`DataFrameModel.read_from_object_store`**.

## `pydantable.io`

### Raw bytes

- **`fetch_bytes(url, *, experimental=True, headers=None, timeout=60.0, max_bytes=None)`** — **HTTP/HTTPS** only (stdlib **`urllib`**). Set **`max_bytes`** to cap download size (**`ValueError`** if exceeded).

### Eager format helpers (`dict[str, list]`)

| Function | Notes |
|----------|--------|
| **`fetch_parquet_url`** | PyArrow on bytes; optional **`columns=`**. |
| **`fetch_csv_url`** | Temp CSV file; Rust read with stdlib fallback. |
| **`fetch_ndjson_url`** | Temp NDJSON file; Rust read. |

**`kwargs`** beyond each function’s explicit parameters are passed through to **`fetch_bytes`** (e.g. **`headers`**, **`timeout`**, **`max_bytes`**).

### Lazy Parquet URL

- **`read_parquet_url`**, **`aread_parquet_url`** — return **`ScanFileRoot`**; temp-file lifecycle as above unless you use a context manager.
- **`read_parquet_url_ctx(dataframe_cls, url, ...)`**, **`aread_parquet_url_ctx`** — yield **`DataFrame[Schema]`** and unlink the temp path in **`finally`**.

### Object-store URIs (`s3://`, `gs://`, `az://`, …)

- **`read_from_object_store(uri, *, experimental=True, format="parquet", max_bytes=None)`**

Requires **`fsspec`** and a backend (e.g. **`s3fs`**). Install **`pydantable[cloud]`** or add dependencies manually. **`format`** is **`"parquet"`** (default), **`"csv"`**, or **`"ndjson"`** / **`"jsonl"`** (object is read into memory in chunks until complete or **`max_bytes`**). **`max_bytes`** limits how many bytes are buffered (**`ValueError`** if exceeded).

## Runnable example

Spawns a **local** **`http.server`** on **127.0.0.1** (no external network). Run conventions: [IO_OVERVIEW](../io/overview.md) (**Runnable example**).

```bash
python docs/examples/io/http_local_fetch.py
```

The script uses **`read_parquet_url`** from **`pydantable.io`** so it can **`os.unlink(root.path)`** after **`collect()`**; **`DataFrame.read_parquet_url`** performs the same download but you must arrange cleanup yourself if you need to delete the temp file immediately.


--8<-- "examples/io/http_local_fetch.py"

### Output

```text
--8<-- "examples/io/http_local_fetch.py.out.txt"
```

## See also

[IO_OVERVIEW](../io/overview.md) · [IO_PARQUET](../io/parquet.md) · [IO_CSV](../io/csv.md) · [IO_NDJSON](../io/ndjson.md) · [FASTAPI](../integrations/fastapi/fastapi.md) (thread-pool patterns for I/O)
