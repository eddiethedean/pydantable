# Spark engine (raikou-core) and SparkDantic

This page covers **two related ideas**:

1. **Optional PySpark execution** (`raikou-core`) — run the same typed `DataFrame` API on a real
   `pyspark.sql.DataFrame` (`SparkDataFrame`, `SparkDataFrameModel`).
2. **SparkDantic** — derive JVM `StructType`, JSON, or DDL from Pydantic models (including
   pydantable `Schema` / `DataFrameModel` row types).

The **PySpark-shaped façade** (`pydantable.pyspark` — `withColumn`, `F.col`, …) is **not** a Spark
cluster client; it uses the in-process Rust engine. See {doc}`PYSPARK_UI` for that surface. This
page is about **real PySpark** and **schema interchange**.

## Install

```bash
pip install "pydantable[spark]"
```

Pulls in:

- **raikou-core** — Spark `ExecutionEngine` for `SparkDataFrame`
- **pyspark** — pinned to `<4` for broad Java compatibility
- **sparkdantic** — Pydantic → Spark schemas ([SparkDantic](https://github.com/mitchelllisle/sparkdantic))

## Usage: `SparkDataFrame` on a PySpark `DataFrame`

```python
from pydantable import Schema, SparkDataFrame


class Row(Schema):
    x: int
    y: str


# spark_df is a pyspark.sql.DataFrame
df = SparkDataFrame[Row].from_spark_dataframe(spark_df)

out = df.filter(df.spark_col("x") > 1).select("y").to_dict()
```

**Rules:**

- Pass **PySpark `Column`** expressions to `filter` / `with_columns` (`df.spark_col("x") > 1`,
  `F.lit(...)`, …). Native pydantable `Expr` objects are **rejected** (they target the Polars-backed
  core, not the Spark engine).
- Parameterize the class: `SparkDataFrame[YourSchema].from_spark_dataframe(...)` — calling
  `SparkDataFrame.from_spark_dataframe` on the raw class raises `TypeError`.

## SparkDantic (schemas from Pydantic)

Import from **`pydantable.pyspark.sparkdantic`** (re-exports and thin wrappers around SparkDantic).

| SparkDantic feature | In PydanTable |
|---------------------|---------------|
| `create_spark_schema` / `create_json_spark_schema` | Re-exported; wrappers `to_pyspark_struct_type`, `to_spark_json_schema` |
| DDL string | `to_spark_ddl_schema` → `create_ddl_spark_schema` (needs PySpark) |
| `SparkField`, `SparkModel` | Re-exported |
| `TypeConversionError`, `SparkdanticImportError` | Re-exported |

**Wrapper keyword arguments** (forwarded on all `to_*` / `dataframe_model_to_*` helpers):
`safe_casting`, `by_alias`, `mode` (`"validation"` \| `"serialization"`), `exclude_fields`
(use `Field(exclude=True)` on columns to omit when `exclude_fields=True`).

```python
from pydantic import Field

from pydantable import Schema
from pydantable.pyspark.sparkdantic import (
    SparkField,
    SparkModel,
    to_pyspark_struct_type,
    to_spark_ddl_schema,
    to_spark_json_schema,
)


class Row(Schema):
    x: int
    name: str | None
    meta: str = Field(exclude=True)


st = to_pyspark_struct_type(Row, exclude_fields=True)
js = to_spark_json_schema(Row, exclude_fields=True)
ddl = to_spark_ddl_schema(Row, exclude_fields=True)
```

**`DataFrameModel`:** `dataframe_model_to_pyspark_struct_type(M)`,
`dataframe_model_to_spark_json_schema(M)`, `dataframe_model_to_spark_ddl_schema(M)`, or pass
`M.RowModel` to the `to_*` functions.

**`SparkModel`:** subclass `SparkModel` and use `MyModel.model_spark_schema()` /
`model_json_spark_schema()` / `model_ddl_spark_schema()` (same options as upstream SparkDantic).

### Example: Spark session + schema from pydantable

Use SparkDantic output as input to `SparkSession.createDataFrame` when you want Spark to enforce
the same shape as your pydantable `Schema`:

```python
from pydantable import Schema
from pydantable.pyspark.sparkdantic import to_pyspark_struct_type


class Row(Schema):
    id: int
    label: str | None


def rows_to_spark_df(spark, rows: list[dict]):
    st = to_pyspark_struct_type(Row)
    return spark.createDataFrame(rows, schema=st)
```

## Troubleshooting

- **Java:** PySpark needs a **JVM** compatible with the installed Spark version. If `SparkSession`
  fails to start, check `JAVA_HOME` and Spark’s Java requirements.
- **Windows / CI:** Local `SparkSession` tests are fragile on some Windows setups (Hadoop shims).
  PydanTable’s own CI runs JVM-backed `spark`-marked tests on Linux and macOS; use
  `pydantable.pyspark.sparkdantic` JSON helpers without PySpark when you only need schema dicts.
- **Missing optional stack:** If `raikou-core` is not installed, `SparkDataFrame.from_spark_dataframe`
  raises `ImportError` with an install hint — the module is optional by design.

## Notes

- The Spark engine stack is **lazy-imported**; `import pydantable` does not load PySpark or
  raikou-core.
- For behaviour guarantees (joins, nulls, windows) on the **default** engine, see {doc}`INTERFACE_CONTRACT`.
  Spark-backed frames follow the **Spark / raikou-core** execution path documented in raikou-core.
