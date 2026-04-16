# Spark engine (raikou-core)

PydanTable can execute lazy typed `DataFrame` plans on **PySpark** via the optional
`raikou-core` engine.

## Install

```bash
pip install "pydantable[spark]"
```

This installs:

- `raikou-core` (Spark `ExecutionEngine` implementation)
- `pyspark` (pinned to `<4` for broad Java compatibility)
- [`sparkdantic`](https://github.com/mitchelllisle/sparkdantic) (Pydantic → JVM `StructType` / JSON schema; see below)

## Usage

```python
from pydantable import Schema, SparkDataFrame


class Row(Schema):
    x: int
    y: str


# spark_df is a pyspark.sql.DataFrame
df = SparkDataFrame[Row].from_spark_dataframe(spark_df)

out = df.filter(df.spark_col("x") > 1).select("y").to_dict()
```

## SparkDantic (full SparkDantic API + helpers)

[SparkDantic](https://github.com/mitchelllisle/sparkdantic) maps Pydantic models to Spark
schemas (JVM `StructType`, JSON, or DDL). Import everything from
`pydantable.pyspark.sparkdantic`:

| SparkDantic feature | In PydanTable |
|---------------------|---------------|
| `create_spark_schema` / `create_json_spark_schema` | Re-exported; also wrapped (see below) |
| `SparkField`, `SparkModel` | Re-exported — use `SparkField(spark_type=...)` or subclass `SparkModel` for `model_spark_schema()` / `model_json_spark_schema()` / `model_ddl_spark_schema()` |
| `TypeConversionError`, `SparkdanticImportError` | Re-exported |
| `create_ddl_spark_schema` | Wrapped as `to_spark_ddl_schema` (needs PySpark) |

**Wrappers** pass through SparkDantic’s keyword arguments on every path:
`safe_casting`, `by_alias`, `mode` (`"validation"` \| `"serialization"`), `exclude_fields`
(with `Field(exclude=True)` on columns to drop).

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
    meta: str = Field(exclude=True)  # omit from Spark schema with exclude_fields=True


# JVM StructType (requires pyspark)
st = to_pyspark_struct_type(Row, exclude_fields=True)

# JSON schema dict (sparkdantic only; no JVM)
js = to_spark_json_schema(Row, exclude_fields=True)

# DDL string (requires pyspark)
ddl = to_spark_ddl_schema(Row, exclude_fields=True)
```

**`DataFrameModel`:** use `dataframe_model_to_pyspark_struct_type(M)`,
`dataframe_model_to_spark_json_schema(M)`, or `dataframe_model_to_spark_ddl_schema(M)`, or pass
`M.RowModel` to the `to_*` functions.

**`SparkModel`:** for models that only need Spark schema generation, subclass `SparkModel` and call
`MyModel.model_spark_schema()` (same options as upstream SparkDantic).

## Notes

- The Spark engine is **optional** and lazily imported; `pydantable` does not
  import Spark at import time.
- You need a working Java runtime compatible with the installed Spark version.

