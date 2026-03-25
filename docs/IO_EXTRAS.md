# Optional formats and bridges (`pydantable.io.extras`)

**Module:** `pydantable.io.extras` (also re-exported on **`pydantable.io`** where applicable). These paths return **`dict[str, list]`** for **`DataFrameModel`** / **`DataFrame`** unless noted otherwise. Many helpers are **experimental**; pass **`experimental=True`** or set **`PYDANTABLE_IO_EXPERIMENTAL=1`**.

## Spreadsheets

- **`read_excel(path, *, sheet_name=0, experimental=True)`** — requires **`pydantable[excel]`** (**openpyxl**).

## Lake / columnar files

- **`read_delta(path, *, experimental=True)`** — Delta table directory via PyArrow dataset (**`pydantable[arrow]`**).
- **`read_avro(path, *, experimental=True)`** — PyArrow Avro (**`pydantable[arrow]`**).
- **`read_orc(path, *, experimental=True)`** — PyArrow ORC (**`pydantable[arrow]`**).

## Cloud warehouses (SDK bridges)

- **`read_bigquery(query, *, project=None, experimental=True, **kwargs)`** — **google-cloud-bigquery** (**`pydantable[bq]`**); extra **`kwargs`** go to **`bigquery.Client`**.
- **`read_snowflake(sql, *, experimental=True, **connect_kwargs)`** — **snowflake-connector-python** (**`pydantable[snowflake]`**); **`connect_kwargs`** are passed to **`snowflake.connector.connect`**.

## Streaming / messaging

- **`read_kafka_json_batch(topic, *, bootstrap_servers, max_messages=100, experimental=True, **consumer_config)`** — **kafka-python** (**`pydantable[kafka]`**); polls JSON object payloads and merges keys into columns (plus **`key`**, **`partition`**, **`offset`**).

## Stdin / stdout

- **`read_csv_stdin(stream=None, *, engine="auto")`** — reads **`sys.stdin`** (or **`stream`**) via a temp file and **`materialize_csv`** (no separate experimental flag).
- **`write_csv_stdout(data, stream=None, *, engine="auto")`** — writes CSV to **`sys.stdout`** (or **`stream`**) via a temp file and **`export_csv`**.

## Async CSV (RAP)

**`pydantable.io.rap_support.aread_csv_rap`** — optional high-throughput CSV path; see {doc}`IO_CSV` and the module docstring for event-loop constraints.

## Runnable examples

### Stdin / stdout (no optional extras)

Uses **`io.StringIO`** instead of real stdin/stdout. Run conventions: {doc}`IO_OVERVIEW` (**Runnable example**).

```bash
python docs/examples/io/extras_stdin_stdout.py
```

```{literalinclude} examples/io/extras_stdin_stdout.py
:language: python
```

### Optional: Excel (`pydantable[excel]`)

Exits successfully even when **openpyxl** is missing (prints a skip message).

```bash
python docs/examples/io/extras_read_excel_optional.py
```

```{literalinclude} examples/io/extras_read_excel_optional.py
:language: python
```

Other helpers (**Delta**, **Avro**, **BigQuery**, **Kafka**, …) follow the same pattern: install the matching extra, then call the function on a small fixture in a **`if __name__ == "__main__"`** block.

## See also

{doc}`IO_OVERVIEW` · {doc}`IO_CSV` · {doc}`DATA_IO_SOURCES` (tiering) · **`pyproject.toml`** optional dependency groups
