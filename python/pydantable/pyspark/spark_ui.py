from __future__ import annotations

from collections.abc import Sequence  # noqa: TC003
from typing import TYPE_CHECKING, Any, cast

from pydantable.dataframe import DataFrame as CoreDataFrame
from pydantable.dataframe_model import DataFrameModel as CoreDataFrameModel
from pydantable.expressions import ColumnRef  # noqa: TC001
from pydantable.schema import make_derived_schema_type

from .sql.types import StructField, StructType, annotation_to_data_type

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


class DataFrame(CoreDataFrame):
    """
    PySpark-flavored method names on the typed logical DataFrame.

    This is not an Apache Spark DataFrame; execution uses the pydantable Rust
    core (Polars engine) for the ``pyspark`` backend name.
    """

    _backend = "pyspark"

    @staticmethod
    def _as_pyspark_df(df: CoreDataFrame) -> DataFrame:
        return cast(
            "DataFrame",
            DataFrame._from_plan(
                root_data=df._root_data,
                root_schema_type=df._root_schema_type,
                current_schema_type=df._current_schema_type,
                rust_plan=df._rust_plan,
            ),
        )

    def withColumn(self, name: str, col: Any) -> DataFrame:
        """Add or replace a column (Spark ``withColumn``)."""
        return self._as_pyspark_df(self.with_columns(**{name: col}))

    def withColumns(self, colsMap: Mapping[str, Any]) -> DataFrame:
        """Add or replace multiple columns (Spark ``withColumns``)."""
        return self._as_pyspark_df(self.with_columns(**dict(colsMap)))

    def withColumnRenamed(self, existing: str, new: str) -> DataFrame:
        """Rename one column (Spark ``withColumnRenamed``)."""
        return self._as_pyspark_df(self.rename({existing: new}))

    def withColumnsRenamed(self, colsMap: Mapping[str, str]) -> DataFrame:
        """Rename multiple columns (Spark ``withColumnsRenamed``)."""
        return self._as_pyspark_df(self.rename(dict(colsMap)))

    def toDF(self, *cols: str) -> DataFrame:
        """Rename all columns in order (Spark ``toDF``)."""
        current = list(self.schema_fields().keys())
        if len(cols) != len(current):
            raise ValueError(f"toDF() expects {len(current)} names, got {len(cols)}.")
        mapping = dict(zip(current, cols, strict=True))
        renamed = self.rename(mapping)
        ordered_types = {name: renamed._current_field_types[name] for name in cols}
        ordered_schema = make_derived_schema_type(
            renamed._root_schema_type, ordered_types
        )
        return self._as_pyspark_df(
            type(self)._from_plan(
                root_data=renamed._root_data,
                root_schema_type=renamed._root_schema_type,
                current_schema_type=ordered_schema,
                rust_plan=renamed._rust_plan,
            )
        )

    def transform(
        self,
        func: Callable[..., CoreDataFrame],
        *args: Any,
        **kwargs: Any,
    ) -> DataFrame:
        out = func(self, *args, **kwargs)
        if not isinstance(out, CoreDataFrame):
            raise TypeError("transform(func, ...) expects func to return a DataFrame.")
        return self._as_pyspark_df(out)

    def select_typed(self, *cols: str, **named_exprs: Any) -> DataFrame:
        """Typed projection with computed columns (no SQL-string ``selectExpr``)."""
        if not cols and not named_exprs:
            raise ValueError(
                "select_typed() requires at least one column or expression."
            )
        projected = self.with_columns(**named_exprs) if named_exprs else self
        select_cols = [*cols, *named_exprs.keys()]
        return self._as_pyspark_df(projected.select(*select_cols))

    def where(self, condition: Any) -> DataFrame:
        """Filter rows (Spark ``where`` / ``filter``)."""
        return self.filter(condition)

    def filter(self, condition: Any) -> DataFrame:
        """Filter rows (Spark ``filter``)."""
        return cast("DataFrame", super().filter(condition))

    def select(self, *cols: Any) -> DataFrame:
        """Project columns (Spark ``select``)."""
        return cast("DataFrame", super().select(*cols))

    def orderBy(
        self,
        *columns: str,
        ascending: bool | list[bool] | None = None,
    ) -> DataFrame:
        """Sort rows (Spark ``orderBy``)."""
        if not columns:
            raise ValueError("orderBy() requires at least one column name.")
        if ascending is None:
            descending: bool | list[bool] = False
        elif isinstance(ascending, bool):
            descending = [not ascending] * len(columns)
        else:
            if len(ascending) != len(columns):
                raise ValueError("ascending length must match columns length.")
            descending = [not x for x in ascending]
        return cast("DataFrame", super().sort(*columns, descending=descending))

    def limit(self, num: int) -> DataFrame:
        """Take the first ``num`` rows (Spark ``limit``)."""
        if num < 0:
            raise ValueError("limit(n) expects n >= 0.")
        return cast("DataFrame", super().head(num))

    def drop(self, *columns: str | ColumnRef) -> DataFrame:
        """Drop columns by name (Spark ``drop``)."""
        return cast("DataFrame", super().drop(*columns))

    def distinct(
        self,
        subset: Sequence[str] | None = None,
        *,
        keep: str = "first",
    ) -> DataFrame:
        """Distinct rows (Spark ``distinct`` / Polars ``unique``)."""
        return cast("DataFrame", super().distinct(subset=subset, keep=keep))

    def dropDuplicates(self, subset: list[str] | None = None) -> DataFrame:
        """Spark ``dropDuplicates``; subset-based dedup is not implemented."""
        if subset is not None:
            raise NotImplementedError(
                "dropDuplicates(subset=...) is not implemented; use distinct() "
                "for all-column deduplication."
            )
        return self.distinct()

    def union(self, other: DataFrame) -> DataFrame:
        return cast(
            "DataFrame",
            CoreDataFrame.concat([self, other], how="vertical"),
        )

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

    def withColumn(self, name: str, col: Any) -> DataFrameModel:
        return cast(
            "DataFrameModel", self._from_dataframe(self._df.withColumn(name, col))
        )

    def withColumns(self, colsMap: Mapping[str, Any]) -> DataFrameModel:
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
        func: Callable[..., CoreDataFrameModel],
        *args: Any,
        **kwargs: Any,
    ) -> DataFrameModel:
        out = func(self, *args, **kwargs)
        if not isinstance(out, CoreDataFrameModel):
            raise TypeError(
                "transform(func, ...) expects func to return a DataFrameModel."
            )
        return cast("DataFrameModel", self._from_dataframe(out._df))

    def select_typed(self, *cols: str, **named_exprs: Any) -> DataFrameModel:
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.select_typed(*cols, **named_exprs)),
        )

    def where(self, condition: Any) -> DataFrameModel:
        return cast("DataFrameModel", self._from_dataframe(self._df.where(condition)))

    def filter(self, condition: Any) -> DataFrameModel:
        return cast("DataFrameModel", self._from_dataframe(self._df.filter(condition)))

    def select(self, *cols: Any) -> DataFrameModel:
        return cast("DataFrameModel", self._from_dataframe(self._df.select(*cols)))

    def orderBy(
        self,
        *columns: str,
        ascending: bool | list[bool] | None = None,
    ) -> DataFrameModel:
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.orderBy(*columns, ascending=ascending)),
        )

    def limit(self, num: int) -> DataFrameModel:
        return cast("DataFrameModel", self._from_dataframe(self._df.limit(num)))

    def drop(self, *cols: str | ColumnRef) -> DataFrameModel:
        return cast("DataFrameModel", self._from_dataframe(self._df.drop(*cols)))

    def distinct(
        self,
        subset: Sequence[str] | None = None,
        *,
        keep: str = "first",
    ) -> DataFrameModel:
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.distinct(subset=subset, keep=keep)),
        )

    def dropDuplicates(self, subset: list[str] | None = None) -> DataFrameModel:
        if subset is not None:
            raise NotImplementedError(
                "dropDuplicates(subset=...) is not implemented; use distinct()."
            )
        return self.distinct()

    def union(self, other: DataFrameModel | DataFrame) -> DataFrameModel:
        od = other._df if isinstance(other, DataFrameModel) else other
        return cast("DataFrameModel", self._from_dataframe(self._df.union(od)))

    def unionAll(self, other: DataFrame) -> DataFrameModel:
        return self.union(other)

    @property
    def columns(self) -> list[str]:
        return list(self._df.columns)

    @property
    def schema(self) -> StructType:
        return self._df.schema

    def __getitem__(self, key: str | list[str]) -> Any:
        return self._df[key]  # type: ignore[index]
