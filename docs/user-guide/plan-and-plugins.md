# Plan introspection, observability, and plugins

This page documents three **additive** 1.x surfaces:

- **`explain()`**: inspect a `DataFrame`/`DataFrameModel` logical plan without running it.
- **`pydantable.observe`**: opt-in lightweight instrumentation hooks.
- **`pydantable.plugins`**: a small registry for I/O readers/writers (extension point).

## `explain()` — plan introspection

`explain()` does **not** materialize data. It returns either a human-readable plan
summary or a JSON-serializable dict.

```python
from pydantable import DataFrame, Schema


class Row(Schema):
    x: int
    y: int | None


df = DataFrame[Row]({"x": [1, 2, 3], "y": [None, 2, 3]})
print(df.with_columns(z=df.x + 1).filter(df.y.is_not_null()).explain())
```

Machine form:

```python
j = df.select("x").explain(format="json")
assert j["version"] == 1
assert isinstance(j["steps"], list)
```

## `pydantable.observe` — observability hooks

Set a global observer callback to receive event dictionaries for execution and I/O
boundaries. This is **stdlib-only** and intentionally lightweight.

```python
from pydantable import DataFrame, Schema
from pydantable.observe import set_observer


class Row(Schema):
    x: int


events = []
set_observer(events.append)

df = DataFrame[Row]({"x": [1, 2, 3]})
_ = df.to_dict()

set_observer(None)
print(events[-1])
```

For a minimal default, set `PYDANTABLE_TRACE=1` to emit trace events to stderr when
no observer is configured.

**Schema drift across Parquet files:** the lazy **`read_parquet`** path does not emit a dedicated “schemas differ” event; Polars handles union and **`allow_missing_columns`** at scan/collect time (see [IO_PARQUET](/io/parquet.md)). For custom checks—e.g. comparing PyArrow **`schema`** per file before building a plan—run that logic in application code and optionally **`set_observer`** on your own steps so events stay explicit and testable.

## `pydantable.plugins` — registry for I/O extension

`pydantable` registers built-in I/O functions as readers/writers. This provides a
single place for discovery and for optional third-party extensions.

```python
from pydantable.plugins import list_readers, get_reader

print([p.name for p in list_readers()])
read_parquet = get_reader("read_parquet")
```

