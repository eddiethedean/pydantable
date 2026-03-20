from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

from .dataframe import DataFrame as _BaseDataFrame
from .dataframe_model import DataFrameModel as _BaseDataFrameModel
from .expressions import Expr
from .schema import Schema


class DataFrame(_BaseDataFrame):
    """PySpark-flavored interface class; execution uses the shared Rust core."""

    _backend = "pyspark"

    @staticmethod
    def _as_pyspark_df(df: _BaseDataFrame) -> DataFrame:
        return cast(
            "DataFrame",
            DataFrame._from_plan(
                root_data=df._root_data,
                root_schema_type=df._root_schema_type,
                current_schema_type=df._current_schema_type,
                rust_plan=df._rust_plan,
            ),
        )

    def withColumn(self, colName: str, col: Expr | Any) -> DataFrame:
        return self._as_pyspark_df(self.with_columns(**{colName: col}))

    def withColumns(self, colsMap: Mapping[str, Expr | Any]) -> DataFrame:
        return self._as_pyspark_df(self.with_columns(**dict(colsMap)))

    def withColumnRenamed(self, existing: str, new: str) -> DataFrame:
        return self._as_pyspark_df(self.rename({existing: new}))

    def withColumnsRenamed(self, colsMap: Mapping[str, str]) -> DataFrame:
        return self._as_pyspark_df(self.rename(dict(colsMap)))

    def toDF(self, *cols: str) -> DataFrame:
        current = list(self.schema_fields().keys())
        if len(cols) != len(current):
            raise ValueError(f"toDF() expects {len(current)} names, got {len(cols)}.")
        mapping = dict(zip(current, cols, strict=True))
        return self._as_pyspark_df(self.rename(mapping))

    def transform(
        self,
        func: Callable[..., _BaseDataFrame],
        *args: Any,
        **kwargs: Any,
    ) -> DataFrame:
        out = func(self, *args, **kwargs)
        if not isinstance(out, _BaseDataFrame):
            raise TypeError("transform(func, ...) expects func to return a DataFrame.")
        return self._as_pyspark_df(out)

    def select_typed(self, *cols: str, **named_exprs: Expr | Any) -> DataFrame:
        # Typed-safe projection for computed columns without SQL-string expressions.
        if not cols and not named_exprs:
            raise ValueError(
                "select_typed() requires at least one column or expression."
            )
        projected = self.with_columns(**named_exprs) if named_exprs else self
        select_cols = [*cols, *named_exprs.keys()]
        return self._as_pyspark_df(projected.select(*select_cols))


class DataFrameModel(_BaseDataFrameModel):
    """PySpark-flavored typed model wrapper."""

    _dataframe_cls = DataFrame

    def withColumn(self, colName: str, col: Expr | Any) -> DataFrameModel:
        return cast(
            "DataFrameModel", self._from_dataframe(self._df.withColumn(colName, col))
        )

    def withColumns(self, colsMap: Mapping[str, Expr | Any]) -> DataFrameModel:
        return cast(
            "DataFrameModel", self._from_dataframe(self._df.withColumns(colsMap))
        )

    def withColumnRenamed(self, existing: str, new: str) -> DataFrameModel:
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.withColumnRenamed(existing, new)),
        )

    def withColumnsRenamed(self, colsMap: Mapping[str, str]) -> DataFrameModel:
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.withColumnsRenamed(colsMap)),
        )

    def toDF(self, *cols: str) -> DataFrameModel:
        return cast("DataFrameModel", self._from_dataframe(self._df.toDF(*cols)))

    def transform(
        self,
        func: Callable[..., _BaseDataFrameModel],
        *args: Any,
        **kwargs: Any,
    ) -> DataFrameModel:
        out = func(self, *args, **kwargs)
        if not isinstance(out, _BaseDataFrameModel):
            raise TypeError(
                "transform(func, ...) expects func to return a DataFrameModel."
            )
        return cast("DataFrameModel", self._from_dataframe(out._df))

    def select_typed(self, *cols: str, **named_exprs: Expr | Any) -> DataFrameModel:
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.select_typed(*cols, **named_exprs)),
        )


__all__ = ["DataFrame", "DataFrameModel", "Expr", "Schema"]
