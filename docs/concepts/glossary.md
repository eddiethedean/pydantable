# Glossary

Short definitions for terms used across the manual.

| Term | Meaning |
|------|---------|
| **`DataFrameModel`** | SQLModel-like table class: schema on the class body, generated **`RowModel`**, typed transforms |
| **`DataFrame[Schema]`** | Generic typed frame over a Pydantic **`Schema`** or compatible model |
| **`Schema`** | Pydantic row shape base type used with **`DataFrame[T]`** |
| **`Expr`** | Typed column expression (filters, **`with_columns`**, aggregations) |
| **`Plan`** | Lazy logical transform graph executed in Rust/Polars at materialization |
| **`ScanFileRoot`** | Lazy file scan handle from **`read_*`** / **`aread_*`** — not a materialized table yet |
| **`materialize_*`** | Eager read into **`dict[str, list]`** (full file in Python memory) |
| **`read_*`** | Lazy scan root for pipeline transforms before **`collect`** or **`write_*`** |
| **`export_*`** | Eager write from **`dict[str, list]`** to a file |
| **`write_*`** (on DataFrame) | Lazy pipeline sink — executes in Rust without a giant Python dict |
| **`collect()`** | Run the plan; return **`list`** of Pydantic row models (default) |
| **`to_dict()`** | Run the plan; return **`dict[str, list]`** |
| **`trusted_mode`** | **`off`** (full validation), **`shape_only`**, or **`strict`** (dtype/shape checks) |
| **`Materialization mode`** | Blocking sync, async **`await`**, deferred **`submit`**, or chunked **`stream`** / **`astream`** |
| **`ExecutionEngine`** | Protocol for where plans run (default: native Polars-backed extension) |
| **`pydantable-native`** | PyPI package containing **`pydantable_native._core`** (Rust extension) |
| **`pydantable-protocol`** | Zero-dependency engine protocols for third-party backends |
| **`MissingRustExtensionError`** | Native extension not built or not loadable |
| **`RowModel`** | Per-row Pydantic model generated from a **`DataFrameModel`** (e.g. **`UserDF.RowModel`**) |

See also: [Mental model](mental-model.md), [I/O decision tree](../io/decision-tree.md).
