# Optional formats and bridges (`pydantable.io.extras`)

**Primary:** pass returned **`dict[str, list]`** to **`DataFrame[Schema](cols)`** or **`MyModel(cols)`** (see {doc}`DATAFRAMEMODEL`). **Secondary:** **`pydantable.io.extras`** helpers (also re-exported on **`pydantable.io`** where applicable). Many are **experimental**; pass **`experimental=True`** or set **`PYDANTABLE_IO_EXPERIMENTAL=1`**.

There are **no** **`DataFrameModel.read_excel`** (or similar) classmethods — extras always return a column dict first.

## `DataFrame` / `DataFrameModel`

After **`read_excel`**, **`read_delta`**, **`read_bigquery`**, …:

- **`MyModel(cols)`** or **`DataFrame[Schema](cols)`**

**Stdin:** **`read_csv_stdin`** → **`MyModel(cols)`** the same way.

## `pydantable.io.extras`

### Spreadsheets

- **`read_excel(path, *, sheet_name=0, experimental=True)`** — **`pydantable[excel]`** (**openpyxl**).

### Lake / columnar files

- **`read_delta(path, *, experimental=True)`** — Delta via PyArrow dataset (**`pydantable[arrow]`**).
- **`read_avro(path, *, experimental=True)`** — PyArrow Avro (**`pydantable[arrow]`**).
- **`read_orc(path, *, experimental=True)`** — PyArrow ORC (**`pydantable[arrow]`**).

### Cloud warehouses (SDK bridges)

- **`read_bigquery(query, *, project=None, experimental=True, **kwargs)`** — **`pydantable[bq]`**; **`kwargs`** → **`bigquery.Client`**.
- **`read_snowflake(sql, *, experimental=True, **connect_kwargs)`** — **`pydantable[snowflake]`**.

### Streaming / messaging

- **`read_kafka_json_batch(topic, *, bootstrap_servers, max_messages=100, experimental=True, **consumer_config)`** — **`pydantable[kafka]`**.
- **Batch iterators (1.5.0+):** when a backend supports chunked reads, **`iter_excel`**, **`iter_delta`**, **`iter_avro`**, **`iter_orc`**, **`iter_bigquery`**, **`iter_snowflake`**, and **`iter_kafka_json`** yield the same **`dict[str, list]`** shape as **`iter_csv`** / **`iter_parquet`** (see {doc}`IO_OVERVIEW`). Some sources still buffer internally (e.g. full JSON-array loads); check docstrings and optional extras.

### Stdin / stdout

- **`read_csv_stdin(stream=None, *, engine="auto")`**
- **`write_csv_stdout(data, stream=None, *, engine="auto")`** — uses **`export_csv`** internally; for **`DataFrame`**, prefer **`to_dict()`** + **`export_csv`** or **`write_csv`** from a lazy frame.

### Async CSV (RAP)

**`pydantable.io.rap_support.aread_csv_rap`** — see {doc}`IO_CSV`.

## Runnable examples

### Stdin / stdout (no optional extras)

```bash
python docs/examples/io/extras_stdin_stdout.py
```

```{literalinclude} examples/io/extras_stdin_stdout.py
:language: python
```

### Optional: Excel (`pydantable[excel]`)

```bash
python docs/examples/io/extras_read_excel_optional.py
```

```{literalinclude} examples/io/extras_read_excel_optional.py
:language: python
```

Other helpers follow the same pattern: install the matching extra, then call the function and wrap with **`DataFrame` / `DataFrameModel`**.

## See also

{doc}`IO_OVERVIEW` · {doc}`IO_CSV` · {doc}`DATA_IO_SOURCES` (tiering) · **`pyproject.toml`** optional dependency groups
