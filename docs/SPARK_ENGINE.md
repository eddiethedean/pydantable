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

## Notes

- The Spark engine is **optional** and lazily imported; `pydantable` does not
  import Spark at import time.
- You need a working Java runtime compatible with the installed Spark version.

