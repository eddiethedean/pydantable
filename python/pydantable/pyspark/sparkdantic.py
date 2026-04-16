"""Interop with `SparkDantic <https://github.com/mitchelllisle/sparkdantic>`__ for JVM
PySpark schemas.

:class:`~pydantable.schema.Schema` and
:class:`~pydantable.dataframe_model.DataFrameModel` row types are Pydantic models, so
`sparkdantic` can derive ``pyspark.sql.types.StructType``, JSON schemas, or DDL strings.

**Features exposed here**

- **Re-exports:** :class:`SparkModel`, :func:`SparkField`, and sparkdantic exceptions —
  use :func:`SparkField` for per-column ``spark_type`` overrides (string, PySpark
  ``DataType``, etc.), or subclass :class:`SparkModel` for
  ``model_spark_schema`` / ``model_json_spark_schema`` / ``model_ddl_spark_schema``.
- **Wrappers** around ``create_spark_schema``, ``create_json_spark_schema``, and
  ``create_ddl_spark_schema`` with the same keyword arguments (``safe_casting``,
  ``by_alias``, ``mode``, ``exclude_fields``).
- **DataFrameModel helpers** that resolve
  :attr:`~pydantable.dataframe_model.DataFrameModel.RowModel`.

Requires ``pip install "pydantable[spark]"`` (``sparkdantic``, ``pyspark``,
``raikou-core``).
JVM ``StructType`` and DDL helpers call into PySpark; JSON-schema helpers need only
``sparkdantic``.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel  # noqa: TC002
from pydantic.json_schema import JsonSchemaMode  # noqa: TC002

# Public re-exports (sparkdantic does not require PySpark at import time).
from sparkdantic import (
    SparkField,
    SparkModel,
    create_json_spark_schema,
    create_spark_schema,
)
from sparkdantic.exceptions import SparkdanticImportError, TypeConversionError


def to_pyspark_struct_type(
    model_cls: type[BaseModel],
    *,
    safe_casting: bool = False,
    by_alias: bool = True,
    mode: JsonSchemaMode = "validation",
    exclude_fields: bool = False,
) -> Any:
    """JVM ``pyspark.sql.types.StructType`` for *model_cls* (requires PySpark).

    Wraps ``create_spark_schema`` — same parameters as SparkDantic.
    """
    return create_spark_schema(
        model_cls,
        safe_casting,
        by_alias,
        mode,
        exclude_fields,
    )


def to_spark_json_schema(
    model_cls: type[BaseModel],
    *,
    safe_casting: bool = False,
    by_alias: bool = True,
    mode: JsonSchemaMode = "validation",
    exclude_fields: bool = False,
) -> dict[str, Any]:
    """Spark-compatible JSON schema dict (no JVM ``StructType``).

    Wraps ``create_json_spark_schema``.
    """
    return create_json_spark_schema(
        model_cls,
        safe_casting,
        by_alias,
        mode,
        exclude_fields,
    )


def to_spark_ddl_schema(
    model_cls: type[BaseModel],
    *,
    safe_casting: bool = False,
    by_alias: bool = True,
    mode: JsonSchemaMode = "validation",
    exclude_fields: bool = False,
) -> str:
    """PySpark schema DDL string (requires PySpark — same as SparkDantic)."""
    from sparkdantic.model import create_ddl_spark_schema

    return create_ddl_spark_schema(
        model_cls,
        safe_casting,
        by_alias,
        mode,
        exclude_fields,
    )


def _row_model_from_dataframe_model(dataframe_model_cls: Any) -> type[BaseModel]:
    row = getattr(dataframe_model_cls, "RowModel", None)
    if row is None:
        raise TypeError(
            "Expected a DataFrameModel subclass with RowModel; got "
            f"{dataframe_model_cls!r}"
        )
    return row


def dataframe_model_to_pyspark_struct_type(
    dataframe_model_cls: Any,
    *,
    safe_casting: bool = False,
    by_alias: bool = True,
    mode: JsonSchemaMode = "validation",
    exclude_fields: bool = False,
) -> Any:
    """Like :func:`to_pyspark_struct_type` for a DataFrameModel subclass."""
    return to_pyspark_struct_type(
        _row_model_from_dataframe_model(dataframe_model_cls),
        safe_casting=safe_casting,
        by_alias=by_alias,
        mode=mode,
        exclude_fields=exclude_fields,
    )


def dataframe_model_to_spark_json_schema(
    dataframe_model_cls: Any,
    *,
    safe_casting: bool = False,
    by_alias: bool = True,
    mode: JsonSchemaMode = "validation",
    exclude_fields: bool = False,
) -> dict[str, Any]:
    """Like :func:`to_spark_json_schema` for a DataFrameModel subclass."""
    return to_spark_json_schema(
        _row_model_from_dataframe_model(dataframe_model_cls),
        safe_casting=safe_casting,
        by_alias=by_alias,
        mode=mode,
        exclude_fields=exclude_fields,
    )


def dataframe_model_to_spark_ddl_schema(
    dataframe_model_cls: Any,
    *,
    safe_casting: bool = False,
    by_alias: bool = True,
    mode: JsonSchemaMode = "validation",
    exclude_fields: bool = False,
) -> str:
    """Like :func:`to_spark_ddl_schema` for a DataFrameModel subclass."""
    return to_spark_ddl_schema(
        _row_model_from_dataframe_model(dataframe_model_cls),
        safe_casting=safe_casting,
        by_alias=by_alias,
        mode=mode,
        exclude_fields=exclude_fields,
    )


__all__ = [
    "SparkField",
    "SparkModel",
    "SparkdanticImportError",
    "TypeConversionError",
    "create_json_spark_schema",
    "create_spark_schema",
    "dataframe_model_to_pyspark_struct_type",
    "dataframe_model_to_spark_ddl_schema",
    "dataframe_model_to_spark_json_schema",
    "to_pyspark_struct_type",
    "to_spark_ddl_schema",
    "to_spark_json_schema",
]
