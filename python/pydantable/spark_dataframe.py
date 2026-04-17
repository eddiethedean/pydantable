"""Spark-backed lazy :class:`~pydantable.dataframe.DataFrame` (PySpark).

Install the optional extra: ``pip install "pydantable[spark]"``.

This facade mirrors :mod:`pydantable.sql_dataframe` and
:mod:`pydantable.mongo_dataframe`: it wires a custom execution engine (from
``raikou-core``) and supplies root constructors.
"""

from __future__ import annotations

from typing import Any, cast

from .dataframe import DataFrame
from .dataframe_model import DataFrameModel
from .expressions import Expr
from .schema import field_types_for_rust, schema_field_types


def _import_spark_engine_types() -> tuple[Any, Any]:
    try:
        from raikou_core.engine import SparkExecutionEngine
        from raikou_core.roots import SparkRoot
    except ImportError as exc:
        raise ImportError(
            "SparkDataFrame / SparkDataFrameModel require the optional Spark stack. "
            'Install with: pip install "pydantable[spark]"'
        ) from exc
    return SparkExecutionEngine, SparkRoot


class SparkDataFrame(DataFrame):
    """Typed dataframe using a PySpark-backed execution engine (raikou-core)."""

    def pyspark_ui(self) -> Any:
        """Return a PySpark-shaped wrapper over this Spark-backed frame."""
        from pydantable.pyspark.spark_dataframe import (
            SparkDataFrame as PySparkSparkDataFrame,
        )

        return PySparkSparkDataFrame._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=self._rust_plan,
            engine=self._engine,
        )

    def pandas_ui(self) -> Any:
        """Return a pandas-shaped wrapper over this Spark-backed frame."""
        from pydantable.pandas_spark_dataframe import (
            SparkDataFrame as PandasSparkDataFrame,
        )

        return PandasSparkDataFrame._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=self._rust_plan,
            engine=self._engine,
        )

    def spark_col(self, name: str) -> Any:
        """Return a Spark Column for use with this engine."""
        from pyspark.sql import functions as F

        return F.col(name)

    def where_native(self, condition: Any) -> Any:
        """Engine-native filter (alias for :meth:`filter`)."""
        return self.filter(condition)

    def select_native(self, *cols: Any) -> Any:
        """Engine-native projection by Spark Columns (simple column refs only)."""
        if not cols:
            raise ValueError("select_native(...) requires at least one column.")
        names: list[str] = []
        for c in cols:
            if isinstance(c, str):
                names.append(c)
                continue
            jc = getattr(c, "_jc", None)
            if jc is None:
                raise TypeError(
                    "select_native(...) expects pyspark Columns or strings."
                )
            s = jc.toString()
            if not isinstance(s, str):
                raise TypeError(
                    "select_native(...) expects a Spark Column with a name."
                )
            s2 = s.strip("`")
            if not s2.isidentifier():
                raise TypeError(
                    "select_native(...) only supports simple column references."
                )
            names.append(s2)
        return self.select(*names)

    @classmethod
    def from_spark_dataframe(cls, df: Any, *, engine: Any | None = None) -> Any:
        """Lazy frame over an existing ``pyspark.sql.DataFrame`` root.

        Call on a concrete parametrized class, e.g.
        ``SparkDataFrame[MySchema].from_spark_dataframe(spark_df)``.
        """
        if getattr(cls, "_schema_type", None) is None:
            raise TypeError(
                "Use SparkDataFrame[Schema].from_spark_dataframe(df) with a schema."
            )
        SparkExecutionEngine, SparkRoot = _import_spark_engine_types()
        eng = engine if engine is not None else SparkExecutionEngine()
        st = cls._schema_type
        assert st is not None
        fts = schema_field_types(st)
        plan = eng.make_plan(field_types_for_rust(fts))
        root = SparkRoot(df)
        return cls._from_plan(
            root_data=root,
            root_schema_type=st,
            current_schema_type=st,
            rust_plan=plan,
            engine=eng,
        )

    def filter(self, condition: Any) -> Any:  # type: ignore[override]
        """Spark engine filter.

        For Spark execution, pass a ``pyspark.sql.Column`` (or a raikou-core SparkExpr),
        not a native pydantable ``Expr``.
        """
        if isinstance(condition, Expr):
            raise TypeError(
                "SparkDataFrame.filter expects a pyspark Column "
                "(use df.spark_col('x') > 1), not a native pydantable Expr."
            )
        rust_plan = self._engine.plan_filter(self._rust_plan, condition)
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )

    def with_columns(self, **columns: Any) -> Any:  # type: ignore[override]
        """Spark engine computed columns.

        Values should be ``pyspark.sql.Column`` (or raikou-core SparkExpr).
        """
        rust_plan = self._engine.plan_with_columns(self._rust_plan, columns)
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust_plan,
            engine=self._engine,
        )


class SparkDataFrameModel(DataFrameModel):
    """``DataFrameModel`` bound to the optional Spark execution engine."""

    _dataframe_cls = SparkDataFrame

    def pyspark_ui(self) -> Any:
        """Return a PySpark-shaped wrapper over this Spark-backed model."""
        return self._wrap_inner_df(self._df.pyspark_ui())

    def pandas_ui(self) -> Any:
        """Return a pandas-shaped wrapper over this Spark-backed model."""
        return self._wrap_inner_df(self._df.pandas_ui())

    @classmethod
    def from_spark_dataframe(cls, df: Any, *, engine: Any | None = None) -> Any:
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].from_spark_dataframe(df, engine=engine)
        return cls._wrap_inner_df(inner)


__all__ = ["SparkDataFrame", "SparkDataFrameModel"]
