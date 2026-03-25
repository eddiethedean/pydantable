# HTTP(S) and object-store reads

**Modules:** `pydantable.io.http`, re-exported from `pydantable.io`. Most URL helpers are **experimental** unless you opt in globally.

## Experimental gate

By default, URL and cloud-style helpers require **`experimental=True`** on each call **or** environment variable **`PYDANTABLE_IO_EXPERIMENTAL=1`**. This matches **`fetch_bytes`** and **`read_from_object_store`**.

## Raw bytes

- **`fetch_bytes(url, *, experimental=True, headers=None, timeout=60.0)`** — **HTTP/HTTPS** only (stdlib **`urllib`**).

Use this when you already know how to parse the body, or as the low-level primitive for other helpers.

## Eager format helpers (return `dict[str, list]`)

These download the full response, then decode (temp file for CSV/NDJSON):

| Function | Notes |
|----------|--------|
| **`fetch_parquet_url`** | PyArrow on bytes; optional **`columns=`**. |
| **`fetch_csv_url`** | Temp CSV file; Rust read with stdlib fallback. |
| **`fetch_ndjson_url`** | Temp NDJSON file; Rust read. |

**`kwargs`** beyond each function’s explicit parameters are passed through to **`fetch_bytes`** (e.g. **`headers`**, **`timeout`**).

## Lazy Parquet over HTTP

**`read_parquet_url`** / **`aread_parquet_url`** download to a **named temp** **`.parquet`** and return **`ScanFileRoot`** for lazy plans. That temp file is **not** auto-deleted; see {doc}`IO_PARQUET` and {doc}`DATA_IO_SOURCES`.

## Object-store URIs (`s3://`, `gs://`, `az://`, …)

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
