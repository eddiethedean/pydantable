---
orphan: true
---

# PySpark API parity (pydantable.pyspark)

The `pydantable.pyspark` facade is **removed in strict pydantable 2.0**, so parity tracking
against Apache Spark is **out of scope** for the strict 2.0 documentation set.

- **Strict 2.0**: use the core API (`DataFrameModel`, `DataFrame[Schema]`) with **typed column tokens** (`df.col.<field>`) and explicit schema evolution via **`*_as(AfterModel/AfterSchema, ...)`**.
- **Migrating from 1.x**: see {doc}`MIGRATION_1_to_2`.

