"""Spark-style :class:`DataFrame` and :class:`DataFrameModel` (core API underneath)."""

from __future__ import annotations

from collections.abc import Sequence  # noqa: TC003
from typing import TYPE_CHECKING, Any, cast
from typing import Literal as TypingLiteral

from pydantable.dataframe import DataFrame as CoreDataFrame
from pydantable.dataframe import GroupedDataFrame as CoreGroupedDataFrame
from pydantable.dataframe_model import DataFrameModel as CoreDataFrameModel
from pydantable.expressions import ColumnRef, Expr, Literal, global_row_count
from pydantable.schema import make_derived_schema_type

from .sql.types import (
    ArrayType,
    DataType,
    StructField,
    StructType,
    annotation_to_data_type,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

_MAX_SHOW_CELL = 48


def _format_schema_type_lines(dt: DataType, prefix: str) -> list[str]:
    lines: list[str] = []
    if isinstance(dt, StructType):
        for sf in dt.fields:
            child = sf.dataType
            nul = str(child.nullable).lower()
            if isinstance(child, StructType):
                lines.append(f"{prefix} |-- {sf.name}: struct (nullable = {nul})")
                lines.extend(_format_schema_type_lines(child, prefix + "    "))
            elif isinstance(child, ArrayType):
                lines.append(f"{prefix} |-- {sf.name}: array (nullable = {nul})")
            else:
                tn = child.typeName
                lines.append(f"{prefix} |-- {sf.name}: {tn} (nullable = {nul})")
    return lines


class PySparkGroupedDataFrame(CoreGroupedDataFrame):
    """Spark ``groupBy`` result; aggregations return :class:`DataFrame`."""

    def agg(
        self,
        *,
        streaming: bool | None = None,
        **aggregations: tuple[str, str] | tuple[str, Expr],
    ) -> DataFrame:
        out = super().agg(streaming=streaming, **aggregations)
        return DataFrame._as_pyspark_df(out)

    def sum(self, *columns: str, streaming: bool | None = None) -> DataFrame:
        return DataFrame._as_pyspark_df(super().sum(*columns, streaming=streaming))

    def mean(self, *columns: str, streaming: bool | None = None) -> DataFrame:
        return DataFrame._as_pyspark_df(super().mean(*columns, streaming=streaming))

    def min(self, *columns: str, streaming: bool | None = None) -> DataFrame:
        return DataFrame._as_pyspark_df(super().min(*columns, streaming=streaming))

    def max(self, *columns: str, streaming: bool | None = None) -> DataFrame:
        return DataFrame._as_pyspark_df(super().max(*columns, streaming=streaming))

    def count(self, *columns: str, streaming: bool | None = None) -> DataFrame:
        """Spark ``count()`` with no args = rows per group (uses core :meth:`len`)."""
        if not columns:
            return DataFrame._as_pyspark_df(super().len(streaming=streaming))
        return DataFrame._as_pyspark_df(super().count(*columns, streaming=streaming))

    def len(self, *, streaming: bool | None = None) -> DataFrame:
        return DataFrame._as_pyspark_df(super().len(streaming=streaming))

    def pivot(
        self,
        pivot_col: str | ColumnRef,
        *,
        values: list[Any] | None = None,
    ) -> PySparkPivotedGroupedDataFrame:
        """Spark-style grouped pivot handle: ``groupBy(...).pivot(...).agg(...)``."""
        if isinstance(pivot_col, str):
            pivot_name = pivot_col
        elif isinstance(pivot_col, Expr):
            referenced = pivot_col.referenced_columns()
            if len(referenced) != 1:
                raise TypeError(
                    "pivot(pivot_col=...) expects a column name or single-column "
                    f"ColumnRef; referenced_columns={sorted(referenced)!r}"
                )
            pivot_name = next(iter(referenced))
        else:
            raise TypeError("pivot(pivot_col=...) expects a column name or ColumnRef.")
        if values is not None and not isinstance(values, list):
            raise TypeError("pivot(values=...) expects a list or None.")
        return PySparkPivotedGroupedDataFrame(
            df=self._df,
            keys=self._keys,
            pivot_col=pivot_name,
            pivot_values=values,
            maintain_order=self._maintain_order,
            drop_nulls=self._drop_nulls,
        )


class PySparkGroupedDataFrameModel:
    """Model-level grouped handle after :meth:`DataFrameModel.groupBy`."""

    def __init__(
        self,
        grouped: PySparkGroupedDataFrame,
        model_type: type[CoreDataFrameModel],
    ) -> None:
        self._grouped = grouped
        self._model_type = model_type

    def __repr__(self) -> str:
        inner = "\n".join(f"  {line}" for line in repr(self._grouped).split("\n"))
        return f"PySparkGroupedDataFrameModel({self._model_type.__name__})\n{inner}"

    def agg(self, **aggregations: Any) -> DataFrameModel:
        return self._model_type._from_dataframe(self._grouped.agg(**aggregations))

    def sum(self, *columns: str, streaming: bool | None = None) -> DataFrameModel:
        return self._model_type._from_dataframe(
            self._grouped.sum(*columns, streaming=streaming)
        )

    def mean(self, *columns: str, streaming: bool | None = None) -> DataFrameModel:
        return self._model_type._from_dataframe(
            self._grouped.mean(*columns, streaming=streaming)
        )

    def min(self, *columns: str, streaming: bool | None = None) -> DataFrameModel:
        return self._model_type._from_dataframe(
            self._grouped.min(*columns, streaming=streaming)
        )

    def max(self, *columns: str, streaming: bool | None = None) -> DataFrameModel:
        return self._model_type._from_dataframe(
            self._grouped.max(*columns, streaming=streaming)
        )

    def count(self, *columns: str, streaming: bool | None = None) -> DataFrameModel:
        return self._model_type._from_dataframe(
            self._grouped.count(*columns, streaming=streaming)
        )

    def len(self, *, streaming: bool | None = None) -> DataFrameModel:
        return self._model_type._from_dataframe(self._grouped.len(streaming=streaming))

    def pivot(
        self,
        pivot_col: str | ColumnRef,
        *,
        values: list[Any] | None = None,
    ) -> PySparkPivotedGroupedDataFrameModel:
        return PySparkPivotedGroupedDataFrameModel(
            grouped=self._grouped.pivot(pivot_col, values=values),
        )


class PySparkPivotedGroupedDataFrame:
    """Handle for Spark-style ``groupBy(...).pivot(...).agg(...)``."""

    __slots__ = (
        "_df",
        "_drop_nulls",
        "_keys",
        "_maintain_order",
        "_pivot_col",
        "_pivot_values",
    )

    def __init__(
        self,
        *,
        df: CoreDataFrame,
        keys: Sequence[str],
        pivot_col: str,
        pivot_values: list[Any] | None,
        maintain_order: bool,
        drop_nulls: bool,
    ) -> None:
        self._df = df
        self._keys = list(keys)
        self._pivot_col = str(pivot_col)
        self._pivot_values = pivot_values
        self._maintain_order = bool(maintain_order)
        self._drop_nulls = bool(drop_nulls)

    def __repr__(self) -> str:
        return (
            "PySparkPivotedGroupedDataFrame("
            f"by={self._keys!r}, pivot={self._pivot_col!r}, "
            f"values={self._pivot_values!r})"
        )

    def agg(
        self,
        *,
        streaming: bool | None = None,
        **aggregations: tuple[str, str] | tuple[str, Expr],
    ) -> DataFrame:
        if not aggregations:
            raise TypeError("agg() requires at least one aggregation spec.")

        # Use an internal separator so we can reliably strip the pivot executor suffix.
        internal_sep = "__"

        out_names: list[str] = []
        specs: dict[str, tuple[str, str]] = {}
        for out_name, spec in aggregations.items():
            if not isinstance(out_name, str) or not out_name:
                raise TypeError("agg() output names must be non-empty strings.")
            if internal_sep in out_name:
                raise ValueError(
                    f"agg() output name {out_name!r} cannot contain {internal_sep!r}."
                )
            if not isinstance(spec, tuple) or len(spec) != 2:
                raise TypeError(
                    "agg() expects specs like "
                    "output_name=('count'|'sum'|'mean'|'min'|'max'|'median'|"
                    "'std'|'var'|'first'|'last'|'n_unique', column)."
                )
            op, col_spec = spec
            if not isinstance(op, str) or not op:
                raise TypeError("Aggregation operator must be a non-empty string.")
            if isinstance(col_spec, str):
                in_col = col_spec
            elif isinstance(col_spec, Expr):
                referenced = col_spec.referenced_columns()
                if len(referenced) != 1:
                    raise TypeError(
                        "Aggregation column must reference exactly one column."
                    )
                in_col = next(iter(referenced))
            else:
                raise TypeError(
                    "Aggregation column must be a column name or Expr "
                    "referencing one column."
                )
            out_names.append(out_name)
            specs[out_name] = (op, in_col)

        # Aggregate at (keys + pivot_col) grain, then pivot those aggregated outputs.
        use_streaming = streaming
        grouped = self._df.group_by(
            *self._keys,
            self._pivot_col,
            maintain_order=self._maintain_order,
            drop_nulls=self._drop_nulls,
        )
        aggregated = grouped.agg(streaming=use_streaming, **specs)
        pivoted = aggregated.pivot(
            index=self._keys,
            columns=self._pivot_col,
            values=out_names,
            aggregate_function="first",
            separator=internal_sep,
            pivot_values=self._pivot_values,
            streaming=use_streaming,
        )

        # Rename core pivot outputs to Spark-ish names:
        # - multi-value: `<pivot_value>__<out_name>__first` -> `<pivot_value>_<out_name>`
        # - single-value: `<pivot_value>__first` -> `<pivot_value>_<out_name>`
        rename_map: dict[str, str] = {}
        for c in pivoted.columns:
            if c in self._keys:
                continue
            parts = c.rsplit(internal_sep, 2)
            if len(parts) == 3 and parts[2] == "first" and parts[1] in specs:
                pivot_value, out_name, _ = parts
                rename_map[c] = f"{pivot_value}_{out_name}"
            elif len(parts) == 2 and parts[1] == "first" and len(out_names) == 1:
                pivot_value, _ = parts
                rename_map[c] = f"{pivot_value}_{out_names[0]}"
        if rename_map:
            pivoted = pivoted.rename(rename_map)
        return DataFrame._as_pyspark_df(pivoted)


class PySparkPivotedGroupedDataFrameModel:
    """Model-level wrapper for grouped pivot; output is a :class:`DataFrame`."""

    __slots__ = ("_grouped",)

    def __init__(self, *, grouped: PySparkPivotedGroupedDataFrame) -> None:
        self._grouped = grouped

    def __repr__(self) -> str:
        return f"PySparkPivotedGroupedDataFrameModel(\n  {self._grouped!r}\n)"

    def agg(self, **aggregations: Any) -> DataFrame:
        return self._grouped.agg(**aggregations)


class DataFrameNaFunctions:
    """Spark ``df.na``-style missing-value helpers (delegates to core)."""

    __slots__ = ("_df",)

    def __init__(self, df: DataFrame) -> None:
        self._df = df

    def drop(
        self,
        how: str = "any",
        thresh: int | None = None,
        subset: str | list[str] | None = None,
    ) -> DataFrame:
        if thresh is not None:
            return self._df.dropna(how=how, thresh=thresh, subset=subset)
        return self._df.dropna(how=how, subset=subset)

    def fill(
        self,
        value: Any,
        subset: str | list[str] | None = None,
    ) -> DataFrame:
        return self._df.fillna(value, subset=subset)


class DataFrameModelNaFunctions:
    """Spark ``df.na`` for :class:`DataFrameModel`."""

    __slots__ = ("_m",)

    def __init__(self, m: DataFrameModel) -> None:
        self._m = m

    def drop(
        self,
        how: str = "any",
        thresh: int | None = None,
        subset: str | list[str] | None = None,
    ) -> DataFrameModel:
        mt = type(self._m)
        if thresh is not None:
            return cast(
                "DataFrameModel",
                mt._from_dataframe(
                    self._m._df.dropna(how=how, thresh=thresh, subset=subset),
                ),
            )
        return cast(
            "DataFrameModel",
            mt._from_dataframe(self._m._df.dropna(how=how, subset=subset)),
        )

    def fill(
        self,
        value: Any,
        subset: str | list[str] | None = None,
    ) -> DataFrameModel:
        mt = type(self._m)
        return cast(
            "DataFrameModel",
            mt._from_dataframe(self._m._df.fillna(value, subset=subset)),
        )


def _text_show_table(
    data: dict[str, list[Any]],
    *,
    truncate: bool,
) -> str:
    cols = list(data.keys())
    if not cols:
        return "(empty)"
    nrows = len(next(iter(data.values())))
    lines: list[str] = [
        " | ".join(cols),
        "-+-".join("-" * min(len(c), 14) for c in cols),
    ]
    for i in range(nrows):
        row_cells: list[str] = []
        for c in cols:
            cell = repr(data[c][i])
            if truncate and len(cell) > _MAX_SHOW_CELL:
                cell = f"{cell[: _MAX_SHOW_CELL - 1]}…"
            row_cells.append(cell)
        lines.append(" | ".join(row_cells))
    return "\n".join(lines)


class DataFrame(CoreDataFrame):
    """Typed table with PySpark method names; runs in-process via the Rust core."""

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
        """Apply ``func(self, ...)``; must return a :class:`DataFrame`."""
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

    def select(
        self,
        *cols: Any,
        exclude: Any = None,
        **named: Any,
    ) -> DataFrame:
        """Project columns (Spark ``select``)."""
        return cast("DataFrame", super().select(*cols, exclude=exclude, **named))

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

    def limit(self, num: int = 0) -> DataFrame:
        """Take the first ``num`` rows (Spark ``limit``)."""
        if num < 0:
            raise ValueError("limit(n) expects n >= 0.")
        return cast("DataFrame", super().head(num))

    def drop(self, *columns: Any, strict: bool = True) -> DataFrame:
        """Drop columns by name (Spark ``drop``)."""
        return cast("DataFrame", super().drop(*columns, strict=strict))

    def distinct(
        self,
        subset: Sequence[str] | None = None,
        *,
        keep: str = "first",
    ) -> DataFrame:
        """Distinct rows (Spark ``distinct`` / Polars ``unique``)."""
        return cast("DataFrame", super().distinct(subset=subset, keep=keep))

    def dropDuplicates(self, subset: list[str] | None = None) -> DataFrame:
        """Spark ``dropDuplicates``; delegates to :meth:`distinct`."""
        return self.distinct(subset=subset) if subset is not None else self.distinct()

    def union(self, other: DataFrame) -> DataFrame:
        """Append rows from ``other`` (vertical concat; schemas must align)."""
        return self._as_pyspark_df(
            CoreDataFrame.concat([self, other], how="vertical"),
        )

    def unionAll(self, other: DataFrame) -> DataFrame:
        """Alias of :meth:`union` (Spark naming)."""
        return self.union(other)

    def unionByName(
        self,
        other: DataFrame,
        *,
        allowMissingColumns: bool = False,
    ) -> DataFrame:
        """Union rows aligning columns by name (reorders ``other`` to match ``self``).

        Column sets must match unless ``allowMissingColumns=True`` (then missing
        fields are filled with nulls; requires compatible nullable dtypes).
        """
        left = list(self._current_field_types.keys())
        right = list(other._current_field_types.keys())
        if set(left) != set(right):
            if not allowMissingColumns:
                raise ValueError(
                    "unionByName requires identical column names on both sides "
                    "unless allowMissingColumns=True."
                )
            return self._union_by_name_allow_missing(other)
        return self.union(self._as_pyspark_df(other.select(*left)))

    def _union_by_name_allow_missing(self, other: DataFrame) -> DataFrame:
        left_names = list(self._current_field_types.keys())
        right_names = list(other._current_field_types.keys())
        all_names = list(dict.fromkeys([*left_names, *right_names]))

        def _pad(df: CoreDataFrame, *, peer: CoreDataFrame) -> CoreDataFrame:
            out = df
            have = set(df._current_field_types.keys())
            for name in all_names:
                if name not in have and name in peer._current_field_types:
                    ann = peer._current_field_types[name]
                    out = out.with_columns(**{name: Literal(value=None).cast(ann)})
            return out.select(*all_names)

        left_padded = _pad(self, peer=other)
        right_padded = _pad(other, peer=self)
        return self._as_pyspark_df(
            CoreDataFrame.concat([left_padded, right_padded], how="vertical"),
        )

    def intersect(self, other: DataFrame) -> DataFrame:
        """Rows present in both frames (distinct row keys; same schema required)."""
        keys = list(self._current_field_types.keys())
        if keys != list(other._current_field_types.keys()):
            raise ValueError("intersect() requires identical schemas.")
        inner = super().join(other, on=keys, how="inner")
        return self._as_pyspark_df(inner.distinct())

    def subtract(self, other: DataFrame) -> DataFrame:
        """Anti join on all columns (schemas must match)."""
        keys = list(self._current_field_types.keys())
        if keys != list(other._current_field_types.keys()):
            raise ValueError("subtract() requires identical schemas.")
        return self._as_pyspark_df(super().join(other, on=keys, how="anti"))

    def exceptAll(self, other: DataFrame) -> DataFrame:
        """Alias of :meth:`subtract` (not Spark multiset ``EXCEPT ALL``)."""
        return self.subtract(other)

    def join(self, other: CoreDataFrame, **kwargs: Any) -> DataFrame:
        out = super().join(other, **kwargs)
        return self._as_pyspark_df(out)

    def group_by(
        self,
        *keys: str | ColumnRef,
        maintain_order: bool = False,
        drop_nulls: bool = True,
    ) -> PySparkGroupedDataFrame:
        inner = super().group_by(
            *keys, maintain_order=maintain_order, drop_nulls=drop_nulls
        )
        return PySparkGroupedDataFrame(
            inner._df,
            inner._keys,
            maintain_order=inner._maintain_order,
            drop_nulls=inner._drop_nulls,
        )

    def groupBy(
        self,
        *keys: str | ColumnRef,
        maintain_order: bool = False,
        drop_nulls: bool = True,
    ) -> PySparkGroupedDataFrame:
        """Spark alias of :meth:`group_by`."""
        return self.group_by(
            *keys, maintain_order=maintain_order, drop_nulls=drop_nulls
        )

    def sort(
        self,
        *columns: str,
        ascending: bool | list[bool] | None = None,
    ) -> DataFrame:
        """Sort by column names (Spark ``sort`` / ``orderBy``)."""
        return self.orderBy(*columns, ascending=ascending)

    def crossJoin(self, other: DataFrame) -> DataFrame:
        """Cross join (Spark ``crossJoin``)."""
        return self._as_pyspark_df(super().join(other, how="cross"))

    def count(self) -> int:
        """Row count via ``global_row_count()`` (Spark-style action)."""
        d = self.select(global_row_count()).to_dict()
        col = next(iter(d.keys()))
        return int(d[col][0])

    def fillna(
        self,
        value: Any = None,
        *,
        subset: str | list[str] | None = None,
    ) -> DataFrame:
        """Spark name for :meth:`fill_null`."""
        if isinstance(subset, str):
            sub: str | list[str] | None = subset
        elif subset is None:
            sub = None
        else:
            sub = list(subset)
        return self._as_pyspark_df(self.fill_null(value, subset=sub))

    def dropna(
        self,
        how: str = "any",
        *,
        thresh: int | None = None,
        subset: str | list[str] | None = None,
    ) -> DataFrame:
        """Spark name for :meth:`drop_nulls` (``thresh`` maps to ``threshold``)."""
        if isinstance(subset, str):
            sub: str | list[str] | None = subset
        elif subset is None:
            sub = None
        else:
            sub = list(subset)
        return self._as_pyspark_df(
            self.drop_nulls(sub, how=how, threshold=thresh),
        )

    @property
    def na(self) -> DataFrameNaFunctions:
        return DataFrameNaFunctions(self)

    def printSchema(self, level: int | None = None) -> None:
        """Print schema tree (Spark ``printSchema``; ``level`` accepted for parity)."""
        _ = level
        print("root")
        for line in _format_schema_type_lines(self.schema, ""):
            print(line)

    def explain(
        self,
        extended: bool | None = None,
        mode: str | None = None,
        *,
        format: TypingLiteral["text", "json"] = "text",
    ) -> None:
        """Print logical plan (Spark ``explain``; ``extended`` / ``mode`` reserved)."""
        _ = extended
        _ = mode
        out = super().explain(format=format)
        if isinstance(out, dict):
            import json

            print(json.dumps(out, indent=2, sort_keys=True))
        else:
            print(out)

    def describe(self) -> str:
        """Same string as core :meth:`~pydantable.dataframe.DataFrame.describe`."""
        return super().describe()

    def toPandas(self):
        """Materialize to a ``pandas.DataFrame`` (requires ``pandas`` installed)."""
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError("toPandas() requires pandas (pip install pandas).") from e
        return pd.DataFrame(self.to_dict())

    def show(
        self,
        n: int = 20,
        truncate: bool = True,
        vertical: bool = False,
    ) -> None:
        """Print up to ``n`` rows (via :meth:`head` + :meth:`to_dict`).

        **Cost:** bounded materialization. Not a distributed Spark runtime.
        """
        h = self.head(int(n))
        data = h.to_dict()
        if vertical:
            cols = list(data.keys())
            if not cols:
                print("(empty)")
                return
            nrows = len(next(iter(data.values())))
            for i in range(nrows):
                print(f"- record {i}")
                for c in cols:
                    print(f"  {c}: {data[c][i]!r}")
            return
        print(_text_show_table(data, truncate=truncate))

    def summary(self) -> str:
        """Spark-style name for :meth:`describe` (numeric columns; materializes)."""
        return self.describe()

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
    """Class-based container using :class:`DataFrame` for Spark-shaped methods."""

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

    def select(self, *cols: str | ColumnRef | Expr, **named: Any) -> DataFrameModel:
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.select(*cols, **named)),
        )

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
        return self.distinct(subset=subset) if subset is not None else self.distinct()

    def union(self, other: DataFrameModel | DataFrame) -> DataFrameModel:
        od = other._df if isinstance(other, DataFrameModel) else other
        return cast("DataFrameModel", self._from_dataframe(self._df.union(od)))

    def unionAll(self, other: DataFrameModel | DataFrame) -> DataFrameModel:
        return self.union(other)

    def unionByName(
        self,
        other: DataFrameModel | DataFrame,
        *,
        allowMissingColumns: bool = False,
    ) -> DataFrameModel:
        od = other._df if isinstance(other, DataFrameModel) else other
        return cast(
            "DataFrameModel",
            self._from_dataframe(
                self._df.unionByName(od, allowMissingColumns=allowMissingColumns)
            ),
        )

    def intersect(self, other: DataFrameModel | DataFrame) -> DataFrameModel:
        od = other._df if isinstance(other, DataFrameModel) else other
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.intersect(od)),
        )

    def subtract(self, other: DataFrameModel | DataFrame) -> DataFrameModel:
        od = other._df if isinstance(other, DataFrameModel) else other
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.subtract(od)),
        )

    def exceptAll(self, other: DataFrameModel | DataFrame) -> DataFrameModel:
        od = other._df if isinstance(other, DataFrameModel) else other
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.exceptAll(od)),
        )

    def group_by(
        self,
        *keys: Any,
        maintain_order: bool = False,
        drop_nulls: bool = True,
    ) -> PySparkGroupedDataFrameModel:
        inner = self._df.group_by(
            *keys, maintain_order=maintain_order, drop_nulls=drop_nulls
        )
        if not isinstance(inner, PySparkGroupedDataFrame):
            raise TypeError("group_by did not return PySparkGroupedDataFrame.")
        return PySparkGroupedDataFrameModel(inner, type(self))

    def groupBy(
        self,
        *keys: Any,
        maintain_order: bool = False,
        drop_nulls: bool = True,
    ) -> PySparkGroupedDataFrameModel:
        return self.group_by(
            *keys, maintain_order=maintain_order, drop_nulls=drop_nulls
        )

    def sort(
        self,
        *columns: str,
        ascending: bool | list[bool] | None = None,
    ) -> DataFrameModel:
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.sort(*columns, ascending=ascending)),
        )

    def crossJoin(self, other: DataFrameModel | DataFrame) -> DataFrameModel:
        od = other._df if isinstance(other, DataFrameModel) else other
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.crossJoin(od)),
        )

    def count(self) -> int:
        return self._df.count()

    def fillna(
        self,
        value: Any = None,
        *,
        subset: str | list[str] | None = None,
    ) -> DataFrameModel:
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.fillna(value, subset=subset)),
        )

    def dropna(
        self,
        how: str = "any",
        *,
        thresh: int | None = None,
        subset: str | list[str] | None = None,
    ) -> DataFrameModel:
        return cast(
            "DataFrameModel",
            self._from_dataframe(
                self._df.dropna(how=how, thresh=thresh, subset=subset),
            ),
        )

    @property
    def na(self) -> DataFrameModelNaFunctions:
        return DataFrameModelNaFunctions(self)

    def printSchema(self, level: int | None = None) -> None:
        self._df.printSchema(level=level)

    def explain(
        self,
        extended: bool | None = None,
        mode: str | None = None,
        *,
        format: TypingLiteral["text", "json"] = "text",
    ) -> None:
        self._df.explain(extended=extended, mode=mode, format=format)

    def describe(self) -> str:
        return self._df.describe()

    def toPandas(self):
        return self._df.toPandas()

    def show(
        self,
        n: int = 20,
        truncate: bool = True,
        vertical: bool = False,
    ) -> None:
        """Print rows (delegates to :class:`DataFrame`)."""
        self._df.show(n=n, truncate=truncate, vertical=vertical)

    def summary(self) -> str:
        return self._df.summary()

    @property
    def schema(self) -> StructType:
        return self._df.schema

    def __getitem__(self, key: str | list[str]) -> Any:
        return self._df[key]  # type: ignore[index]
