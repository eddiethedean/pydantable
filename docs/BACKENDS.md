# Execution Backends

Pydantable exposes a single typed DataFrame API, but execution is dispatched
through a selectable backend boundary.

## Default (Polars-style)

Use the default exports:

```python
from pydantable import DataFrameModel
```

This default interface is backed by the existing Rust/Polars execution core.

## Optional interface modules

Pydantable also provides import-based interface modules:

```python
from pydantable.pandas import DataFrameModel  # pandas interface
from pydantable.pyspark import DataFrameModel  # pyspark interface
```

These interfaces keep the same typed API and contracts, while selecting a
different backend name in the Python dispatch layer.

### PySpark interface selection

You can select the PySpark interface in two equivalent ways:

```python
from pydantable.pyspark import DataFrameModel
```

```python
import os
os.environ["PYDANTABLE_BACKEND"] = "pyspark"
from pydantable import DataFrameModel
```

The PySpark interface supports the same currently implemented operation
families as default exports:

- core transforms (`select`, `with_columns`, `filter`, `sort`, `unique`, slicing)
- null/type transforms (`fill_null`, `drop_nulls`, `cast`, null predicates)
- joins and group-by aggregations
- reshape (`melt`/`unpivot`, `pivot`)
- rolling and dynamic window operations
- temporal columns/literals (`datetime`, `date`, `duration`, including nullable)
- PySpark-style select wrappers (`withColumn`, `withColumns`, `withColumnRenamed`,
  `withColumnsRenamed`, `toDF`, `transform`, `select_typed`)

`selectExpr` SQL-string projection is intentionally out of scope for the typed
interface. Use typed expressions with `select_typed(...)` instead.

## Execution model for `pandas` / `pyspark` modules

The `pydantable.pandas` and `pydantable.pyspark` modules are **naming/import
variants** of the same typed API. They set a backend tag for Python dispatch
and tests. The ``pyspark`` backend uses the Rust core for execution. The
``pandas`` backend runs ``execute_plan`` via the optional pandas runtime; joins,
reshape, rolling windows, and other operations still use the Rust core. Pydantable
does not run Apache Spark for the ``pyspark`` import path.

See also `docs/PANDAS_UI.md` and `docs/PYSPARK_UI.md`.

Semantics are defined by `docs/INTERFACE_CONTRACT.md`, independent of selected
interface module.

