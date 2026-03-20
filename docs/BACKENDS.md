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

## Current status

In this skeleton/refactor stage, `pandas`/`pyspark` interfaces currently use the
existing Rust/Polars execution core as a fallback executor. The backend
boundary is in place so real pandas/pyspark lowering can be added
incrementally.

