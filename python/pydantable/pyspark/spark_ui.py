from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from pydantable.dataframe import DataFrame as CoreDataFrame
from pydantable.dataframe_model import DataFrameModel as CoreDataFrameModel

from .sql.types import StructField, StructType, annotation_to_data_type

if TYPE_CHECKING:
    from pydantable.expressions import Expr


class DataFrame(CoreDataFrame):
    """
    PySpark-flavored method names on the typed logical DataFrame.

    This is not an Apache Spark DataFrame; execution uses the pydantable backend
    (Polars/Rust by default for the pyspark backend name).
    """

    _backend = "pyspark"

    def withColumn(self, name: str, col: Expr | Any) -> DataFrame:
        """Add or replace a column (Spark ``withColumn``)."""
        return self.with_columns(**{name: col})

    def where(self, condition: Expr) -> DataFrame:
        """Filter rows (Spark ``where`` / ``filter``)."""
        return self.filter(condition)

    def filter(self, condition: Expr) -> DataFrame:
        """Filter rows (Spark ``filter``)."""
        return cast("DataFrame", super().filter(condition))

    def select(self, *cols: str | Any) -> DataFrame:
        """Project columns (Spark ``select``)."""
        return cast("DataFrame", super().select(*cols))

    def orderBy(
        self,
        *columns: str,
        ascending: bool | list[bool] | None = None,
    ) -> DataFrame:
        """Sort rows (Spark ``orderBy``)."""
        return cast(
            "DataFrame", super().order_by(*columns, ascending=ascending)
        )

    def sort(
        self,
        *columns: str,
        ascending: bool | list[bool] | None = None,
    ) -> DataFrame:
        """Alias of :meth:`orderBy`."""
        return self.orderBy(*columns, ascending=ascending)

    def limit(self, num: int) -> DataFrame:
        """Take the first ``num`` rows (Spark ``limit``)."""
        return cast("DataFrame", super().limit(num))

    def drop(self, *cols: str) -> DataFrame:
        """Drop columns by name (Spark ``drop``)."""
        return cast("DataFrame", super().drop(*cols))

    def distinct(self) -> DataFrame:
        """Distinct rows across all columns (Spark ``distinct``)."""
        return cast("DataFrame", super().distinct())

    def withColumnRenamed(self, existing: str, new: str) -> DataFrame:
        """Rename one column (Spark ``withColumnRenamed``)."""
        return cast(
            "DataFrame", super().with_column_renamed(existing, new)
        )

    def dropDuplicates(self, subset: list[str] | None = None) -> DataFrame:
        """Spark ``dropDuplicates``; subset-based dedup is not implemented."""
        if subset is not None:
            raise NotImplementedError(
                "dropDuplicates(subset=...) is not implemented; use distinct() "
                "for all-column deduplication."
            )
        return self.distinct()

    def union(self, other: DataFrame) -> DataFrame:
        return cast("DataFrame", super().union(other))

    def unionAll(self, other: DataFrame) -> DataFrame:
        return self.union(other)

    @property
    def columns(self) -> list[str]:
        return list(self._current_field_types.keys())

    @property
    def schema(self) -> StructType:
        fields = [
            StructField(n, annotation_to_data_type(t))
            for n, t in self._current_field_types.items()
        ]
        return StructType(fields)

    def __getitem__(self, key: str | list[str]) -> Any:
        if isinstance(key, str):
            return self.col(key)
        if isinstance(key, list):
            if not key:
                raise ValueError("Column list must be non-empty.")
            return self.select(*key)
        raise TypeError(
            "DataFrame indexing supports a single column name (str) or list[str]."
        )


class DataFrameModel(CoreDataFrameModel):
    _dataframe_cls = DataFrame

    def withColumn(self, name: str, col: Expr | Any) -> DataFrameModel:
        return type(self)._from_dataframe(self._df.withColumn(name, col))

    def where(self, condition: Expr) -> DataFrameModel:
        return type(self)._from_dataframe(self._df.where(condition))

    def filter(self, condition: Expr) -> DataFrameModel:
        return type(self)._from_dataframe(self._df.filter(condition))

    def select(self, *cols: str | Any) -> DataFrameModel:
        return type(self)._from_dataframe(self._df.select(*cols))

    def orderBy(
        self,
        *columns: str,
        ascending: bool | list[bool] | None = None,
    ) -> DataFrameModel:
        return type(self)._from_dataframe(
            self._df.order_by(*columns, ascending=ascending)
        )

    def sort(
        self,
        *columns: str,
        ascending: bool | list[bool] | None = None,
    ) -> DataFrameModel:
        return self.orderBy(*columns, ascending=ascending)

    def limit(self, num: int) -> DataFrameModel:
        return type(self)._from_dataframe(self._df.limit(num))

    def drop(self, *cols: str) -> DataFrameModel:
        return type(self)._from_dataframe(self._df.drop(*cols))

    def distinct(self) -> DataFrameModel:
        return type(self)._from_dataframe(self._df.distinct())

    def withColumnRenamed(self, existing: str, new: str) -> DataFrameModel:
        return type(self)._from_dataframe(
            self._df.with_column_renamed(existing, new)
        )

    def dropDuplicates(self, subset: list[str] | None = None) -> DataFrameModel:
        if subset is not None:
            raise NotImplementedError(
                "dropDuplicates(subset=...) is not implemented; use distinct()."
            )
        return self.distinct()

    def union(self, other: DataFrameModel | DataFrame) -> DataFrameModel:
        od = other._df if isinstance(other, DataFrameModel) else other
        return type(self)._from_dataframe(self._df.union(od))

    def unionAll(self, other: DataFrame) -> DataFrameModel:
        return self.union(other)

    @property
    def columns(self) -> list[str]:
        return list(self._df.columns)

    @property
    def schema(self) -> StructType:
        return self._df.schema

    def __getitem__(self, key: str | list[str]) -> Any:
        return self._df[key]
