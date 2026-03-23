# Window `RANGE` semantics (multi-key `orderBy`)

pydantable does **not** guarantee identical window results across PostgreSQL, Spark, Oracle, SQL Server, and other engines. NULL ordering, peer groups for `CURRENT ROW`, and dialect-specific rules can differ.

## Normative reference

**PostgreSQL-style** behavior for **`RANGE`** frames with **multiple** `ORDER BY` columns:

1. **Sort order:** Rows in each partition are ordered **lexicographically** by all `orderBy` columns, respecting each column’s `ascending` flag. Ties use a **stable** row index tie-break.
2. **Range axis:** `rangeBetween(start, end)` compares **only the first** `orderBy` column’s values. `start` and `end` are **inclusive** offsets in that column’s native unit (same as single-key `rangeBetween`: integers for integral types; `datetime` / `duration` use microseconds; `date` uses day offset; floats use `f64` deltas).
3. **Tie-breaker columns:** Additional `orderBy` columns affect **sort position** only. Two rows with the same first key can both lie inside a range window for a given current row if their first keys differ from the current row’s first key by at most the frame bounds.

This matches the common SQL rule that **offset** bounds in `RANGE` apply to the **first** sort key when multiple keys are listed.

## Null ordering in window `orderBy`

pydantable does **not** expose **`NULLS FIRST` / `NULLS LAST`** on `Window.partitionBy(...).orderBy(...)` today. Window sort order follows **Polars** defaults for the underlying `over` / framed-window path: nulls are ordered relative to non-null values according to Polars’ rules for the expression dtypes (and may differ from PostgreSQL or Spark for the same SQL-shaped intent).

**Contract:** treat **null ordering in window `orderBy`** as **implementation-defined** unless you pin a library version and validate for your dtypes. For deterministic tests, avoid nulls in `orderBy` keys or sort explicit columns first with `sort()` and then use windows only when the contract you need is covered by tests.

## Peer rows, `CURRENT ROW`, and framed windows

For **`rowsBetween`** and **`rangeBetween`**, the engine evaluates frames over rows in **partition sort order** (all `orderBy` keys, with stable tie-breaking as described above for multi-key `RANGE`).

- **`CURRENT ROW`** in SQL terms corresponds to the row being evaluated; peer rows share the same **sort key vector** (for unframed ranking functions, ties get the same rank where `rank` / `dense_rank` semantics apply).
- **Framed** aggregates include or exclude peers based on the frame bounds; exact inclusion for ties at frame edges follows Polars’ window implementation. Do not assume identical edge behavior vs Spark or ANSI SQL without testing your partition/`orderBy`/frame combination.

See [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md) for the supported window API surface.

## Spark / PySpark notes

Apache Spark’s `rangeBetween` also uses the **ordering expression** for frame bounds; with multiple `orderBy` columns, behavior is engine-specific. pydantable’s contract is the rule above—do not assume Spark parity for every edge case without testing.

## Related

- [`INTERFACE_CONTRACT.md`](INTERFACE_CONTRACT.md) — supported window API surface.
- [`PYSPARK_PARITY.md`](PYSPARK_PARITY.md) — façade coverage vs Spark names.
- [`DATAFRAMEMODEL.md`](DATAFRAMEMODEL.md) / [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md) — trusted ingest (**`strict`**) and nested column rules when building dataframes used in windowed queries.
- [`ROADMAP.md`](ROADMAP.md) — release train; async I/O is **not** part of window semantics today.
