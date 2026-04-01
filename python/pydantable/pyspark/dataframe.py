"""Spark-style :class:`DataFrame` and :class:`DataFrameModel` (core API underneath)."""

from __future__ import annotations

from collections.abc import Sequence  # noqa: TC003
from types import UnionType
from typing import TYPE_CHECKING, Any, Union, cast, get_args, get_origin
from typing import Literal as TypingLiteral

from pydantable.dataframe import DataFrame as CoreDataFrame
from pydantable.dataframe import GroupedDataFrame as CoreGroupedDataFrame
from pydantable.dataframe_model import DataFrameModel as CoreDataFrameModel
from pydantable.expressions import ColumnRef, Expr, Literal, global_row_count
from pydantable.rust_engine import (
    _require_rust_core,
    execute_except_all,
    execute_intersect_all,
)
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

    @staticmethod
    def _normalize_agg_op(op: str) -> str:
        o = str(op).strip()
        if not o:
            raise TypeError("Aggregation operator must be a non-empty string.")
        key = o.lower().replace(" ", "").replace("-", "_")
        synonyms = {
            "avg": "mean",
            "countdistinct": "n_unique",
            "count_distinct": "n_unique",
        }
        return synonyms.get(key, key)

    @classmethod
    def _parse_dict_aggs(
        cls, agg_dict: Mapping[str, Any]
    ) -> dict[str, tuple[str, str]]:
        if not isinstance(agg_dict, dict):
            agg_dict = dict(agg_dict)
        out: dict[str, tuple[str, str]] = {}
        for col, ops in agg_dict.items():
            if not isinstance(col, str) or not col:
                raise TypeError("Dict-form agg keys must be non-empty column names.")
            if isinstance(ops, str):
                ops_list = [ops]
            elif isinstance(ops, (list, tuple)):
                if not ops:
                    raise TypeError(f"agg({col!r}) op list must be non-empty.")
                ops_list = list(ops)
            else:
                raise TypeError(
                    "Dict-form agg values must be an op string or list/tuple of "
                    "op strings."
                )
            for raw_op in ops_list:
                if not isinstance(raw_op, str):
                    raise TypeError("Aggregation operators must be strings.")
                op = cls._normalize_agg_op(raw_op)
                out_name = f"{col}_{op}"
                if out_name in out:
                    raise ValueError(f"Duplicate aggregation output name: {out_name!r}")
                out[out_name] = (op, col)
        return out

    def agg(
        self,
        *exprs: Any,
        streaming: bool | None = None,
        **aggregations: tuple[str, str] | tuple[str, Expr],
    ) -> DataFrame:
        agg_dict = None
        if exprs and isinstance(exprs[0], dict):
            agg_dict = exprs[0]
            exprs = exprs[1:]

        dict_specs: dict[str, tuple[str, str]] = {}
        if agg_dict is not None:
            from collections.abc import Mapping as _Mapping

            if not isinstance(agg_dict, _Mapping):
                raise TypeError("agg(dict) expects a mapping of column -> op(s).")
            dict_specs = self._parse_dict_aggs(agg_dict)

        if exprs:
            from pydantable.pyspark.sql.functions import _GroupedAggSpecAliased

            extra: dict[str, tuple[str, str]] = {}
            for e in exprs:
                if not isinstance(e, _GroupedAggSpecAliased):
                    raise TypeError(
                        "agg(exprs...) expects expressions like "
                        "F.sum('col').alias('out') (Spark-style)."
                    )
                extra[e._out_name] = (e._spec._op, e._spec._col)
            for k in extra:
                if k in aggregations or k in dict_specs:
                    raise ValueError(f"Duplicate aggregation output name: {k!r}")
            for k in dict_specs:
                if k in aggregations:
                    raise ValueError(f"Duplicate aggregation output name: {k!r}")
            out = super().agg(
                streaming=streaming, **(extra | dict_specs | aggregations)
            )
        else:
            for k in dict_specs:
                if k in aggregations:
                    raise ValueError(f"Duplicate aggregation output name: {k!r}")
            out = super().agg(streaming=streaming, **(dict_specs | aggregations))
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

    def agg(self, *exprs: Any, **aggregations: Any) -> DataFrameModel:
        return self._model_type._from_dataframe(
            self._grouped.agg(*exprs, **aggregations)
        )

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
        aggDict: Any | None = None,
        *,
        streaming: bool | None = None,
        **aggregations: tuple[str, str] | tuple[str, Expr],
    ) -> DataFrame:
        if aggDict is not None:
            from collections.abc import Mapping as _Mapping

            if not isinstance(aggDict, _Mapping):
                raise TypeError("agg(dict) expects a mapping of column -> op(s).")
            dict_specs = PySparkGroupedDataFrame._parse_dict_aggs(dict(aggDict))
            for k in dict_specs:
                if k in aggregations:
                    raise ValueError(f"Duplicate aggregation output name: {k!r}")
            aggregations = dict_specs | aggregations

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
        # - multi-value: `<pivot_value>__<out_name>__first`
        #   -> `<pivot_value>_<out_name>`
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

    def count(self, *, streaming: bool | None = None) -> DataFrame:
        """Spark-style pivot count (rows per group + pivot value)."""
        internal_sep = "__"
        use_streaming = streaming
        grouped = self._df.group_by(
            *self._keys,
            self._pivot_col,
            maintain_order=self._maintain_order,
            drop_nulls=self._drop_nulls,
        )
        aggregated = grouped.len(streaming=use_streaming)
        pivoted = aggregated.pivot(
            index=self._keys,
            columns=self._pivot_col,
            values=["len"],
            aggregate_function="first",
            separator=internal_sep,
            pivot_values=self._pivot_values,
            streaming=use_streaming,
        )

        rename_map: dict[str, str] = {}
        for c in pivoted.columns:
            if c in self._keys:
                continue
            parts = c.rsplit(internal_sep, 2)
            if len(parts) == 3 and parts[2] == "first":
                pivot_value, _, _ = parts
                rename_map[c] = f"{pivot_value}_count"
            elif len(parts) == 2 and parts[1] == "first":
                pivot_value, _ = parts
                rename_map[c] = f"{pivot_value}_count"
        if rename_map:
            pivoted = pivoted.rename(rename_map)
        return DataFrame._as_pyspark_df(pivoted)

    def sum(self, *columns: str, streaming: bool | None = None) -> DataFrame:
        if not columns:
            raise TypeError("sum() requires at least one column name.")
        aggs = {c: ("sum", c) for c in columns}
        return self.agg(streaming=streaming, **aggs)

    def avg(self, *columns: str, streaming: bool | None = None) -> DataFrame:
        if not columns:
            raise TypeError("avg() requires at least one column name.")
        aggs = {c: ("mean", c) for c in columns}
        return self.agg(streaming=streaming, **aggs)

    def mean(self, *columns: str, streaming: bool | None = None) -> DataFrame:
        return self.avg(*columns, streaming=streaming)

    def min(self, *columns: str, streaming: bool | None = None) -> DataFrame:
        if not columns:
            raise TypeError("min() requires at least one column name.")
        aggs = {c: ("min", c) for c in columns}
        return self.agg(streaming=streaming, **aggs)

    def max(self, *columns: str, streaming: bool | None = None) -> DataFrame:
        if not columns:
            raise TypeError("max() requires at least one column name.")
        aggs = {c: ("max", c) for c in columns}
        return self.agg(streaming=streaming, **aggs)


class PySparkPivotedGroupedDataFrameModel:
    """Model-level wrapper for grouped pivot; output is a :class:`DataFrame`."""

    __slots__ = ("_grouped",)

    def __init__(self, *, grouped: PySparkPivotedGroupedDataFrame) -> None:
        self._grouped = grouped

    def __repr__(self) -> str:
        return f"PySparkPivotedGroupedDataFrameModel(\n  {self._grouped!r}\n)"

    def agg(self, **aggregations: Any) -> DataFrame:
        return self._grouped.agg(**aggregations)

    def count(self, *, streaming: bool | None = None) -> DataFrame:
        return self._grouped.count(streaming=streaming)

    def sum(self, *columns: str, streaming: bool | None = None) -> DataFrame:
        return self._grouped.sum(*columns, streaming=streaming)

    def avg(self, *columns: str, streaming: bool | None = None) -> DataFrame:
        return self._grouped.avg(*columns, streaming=streaming)

    def mean(self, *columns: str, streaming: bool | None = None) -> DataFrame:
        return self._grouped.mean(*columns, streaming=streaming)

    def min(self, *columns: str, streaming: bool | None = None) -> DataFrame:
        return self._grouped.min(*columns, streaming=streaming)

    def max(self, *columns: str, streaming: bool | None = None) -> DataFrame:
        return self._grouped.max(*columns, streaming=streaming)


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
        if not isinstance(col, Expr):
            raise TypeError(
                "withColumn(name, col) expects a typed column Expr. "
                "Hint: use pyspark.sql.functions.lit(...) for literals, or "
                "df['col'] / df.col('col') for existing columns."
            )
        return self._as_pyspark_df(self.with_columns(**{name: col}))

    def withColumns(self, colsMap: Mapping[str, Any]) -> DataFrame:
        """Add or replace multiple columns (Spark ``withColumns``)."""
        cm = dict(colsMap)
        for _k, v in cm.items():
            if not isinstance(v, Expr):
                raise TypeError(
                    "withColumns(colsMap) expects mapping values to be typed Exprs. "
                    "Hint: use pyspark.sql.functions.lit(...) for literals."
                )
        return self._as_pyspark_df(self.with_columns(**cm))

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

    def sample(
        self,
        withReplacement: bool | None = None,
        fraction: float | None = None,
        seed: int | None = None,
    ) -> DataFrame:
        """Sample rows (Spark ``sample``; fraction required in this facade)."""
        if fraction is None:
            raise ValueError("sample(fraction=...) is required (Spark-style).")
        if withReplacement is None:
            withReplacement = False
        if not isinstance(withReplacement, bool):
            raise TypeError("sample(withReplacement=...) must be a bool.")
        return self._as_pyspark_df(
            super().sample(
                fraction=fraction, seed=seed, with_replacement=withReplacement
            )
        )

    def explode(
        self,
        column: Any,
        *,
        outer: bool = False,
        streaming: bool | None = None,
    ) -> DataFrame:
        """Explode **list** columns (Spark ``explode``).

        Use ``outer=True`` for ``explode_outer``.
        """
        return self._as_pyspark_df(
            super().explode(column, outer=outer, streaming=streaming)
        )

    def explode_outer(self, column: Any, *, streaming: bool | None = None) -> DataFrame:
        """Explode list columns with Spark-ish outer null/empty handling (see docs)."""
        return self._as_pyspark_df(super().explode_outer(column, streaming=streaming))

    def explode_all(self, *, streaming: bool | None = None) -> DataFrame:
        """Explode every list-typed column (schema-driven; not a Spark name)."""
        return self._as_pyspark_df(super().explode_all(streaming=streaming))

    def posexplode(
        self,
        column: str,
        *,
        pos: str = "pos",
        value: str | None = None,
        outer: bool = False,
        streaming: bool | None = None,
    ) -> DataFrame:
        """Explode one list column with a **0-based** index (Spark ``posexplode``)."""
        return self._as_pyspark_df(
            super().posexplode(
                column, pos=pos, value=value, outer=outer, streaming=streaming
            )
        )

    def posexplode_outer(
        self,
        column: str,
        *,
        pos: str = "pos",
        value: str | None = None,
        streaming: bool | None = None,
    ) -> DataFrame:
        """``posexplode(..., outer=True)`` alias."""
        return self._as_pyspark_df(
            super().posexplode_outer(column, pos=pos, value=value, streaming=streaming)
        )

    def unnest(self, column: Any, *, streaming: bool | None = None) -> DataFrame:
        """Expand **struct** columns to top-level fields (Spark struct pattern)."""
        return self._as_pyspark_df(super().unnest(column, streaming=streaming))

    def unnest_all(self, *, streaming: bool | None = None) -> DataFrame:
        """Unnest every struct-typed column in the schema."""
        return self._as_pyspark_df(super().unnest_all(streaming=streaming))

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
        """Spark ``dropDuplicates``; keep-first (engine-dependent without ordering)."""
        return (
            self.distinct(subset=subset, keep="first")
            if subset is not None
            else self.distinct(keep="first")
        )

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
        if set(left) != set(right) and not allowMissingColumns:
            raise ValueError(
                "unionByName requires identical column names on both sides "
                "unless allowMissingColumns=True."
            )
        return self._union_by_name_allow_missing(
            other, allow_missing=allowMissingColumns
        )

    def _union_by_name_allow_missing(
        self, other: DataFrame, *, allow_missing: bool
    ) -> DataFrame:
        left_types = dict(self._current_field_types)
        right_types = dict(other._current_field_types)
        if allow_missing:
            all_names = list(dict.fromkeys([*left_types.keys(), *right_types.keys()]))
        else:
            all_names = list(left_types.keys())

        def _is_optional(ann: Any) -> bool:
            origin = get_origin(ann)
            if origin is None:
                return False
            return (origin is UnionType or origin is Union) and type(None) in get_args(
                ann
            )

        def _strip_optional(ann: Any) -> Any:
            args = tuple(a for a in get_args(ann) if a is not type(None))
            if len(args) == 1:
                return args[0]
            return ann

        def _optionalize(ann: Any) -> Any:
            return ann | None  # PEP604

        def _unify(name: str, lt: Any | None, rt: Any | None) -> Any:
            if lt is None:
                if not allow_missing:
                    raise ValueError(
                        "unionByName requires identical column names on both sides "
                        "unless allowMissingColumns=True."
                    )
                return _optionalize(rt)
            if rt is None:
                if not allow_missing:
                    raise ValueError(
                        "unionByName requires identical column names on both sides "
                        "unless allowMissingColumns=True."
                    )
                return _optionalize(lt)
            if lt == rt:
                return lt

            l_opt = _is_optional(lt)
            r_opt = _is_optional(rt)
            l_base = _strip_optional(lt) if l_opt else lt
            r_base = _strip_optional(rt) if r_opt else rt

            if l_base == r_base:
                return _optionalize(l_base) if (l_opt or r_opt) else l_base
            if {l_base, r_base} == {int, float}:
                return _optionalize(float) if (l_opt or r_opt) else float

            msg = (
                "unionByName has incompatible dtypes for "
                f"column {name!r}: left={lt!r}, right={rt!r}"
            )
            if allow_missing:
                msg = (
                    "unionByName(allowMissingColumns=True) has incompatible dtypes for "
                    + (f"column {name!r}: left={lt!r}, right={rt!r}")
                )
            raise TypeError(msg)

        unified: dict[str, Any] = {}
        for name in all_names:
            unified[name] = _unify(name, left_types.get(name), right_types.get(name))

        def _align(df: CoreDataFrame, side_types: dict[str, Any]) -> CoreDataFrame:
            out = df
            for name in all_names:
                target = unified[name]
                if name not in side_types:
                    out = out.with_columns(**{name: Literal(value=None).cast(target)})
                elif side_types[name] != target:
                    out = out.with_columns(
                        **{
                            name: ColumnRef(name=name, dtype=side_types[name]).cast(
                                target
                            )
                        }
                    )
            return out.select(*all_names)

        left_aligned = _align(self, left_types)
        right_aligned = _align(other, right_types)
        return self._as_pyspark_df(
            CoreDataFrame.concat([left_aligned, right_aligned], how="vertical"),
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

    def except_(self, other: DataFrame) -> DataFrame:
        """Distinct set difference (Spark ``except`` / SQL ``EXCEPT DISTINCT``)."""
        return self.subtract(other).distinct()

    # Python keyword compatibility: expose `.except(...)` name too.
    except__doc__ = "Alias for except_ (Spark except)."

    def exceptAll(self, other: DataFrame) -> DataFrame:
        """Multiset difference (Spark ``EXCEPT ALL``)."""
        if self._current_field_types != other._current_field_types:
            raise ValueError("exceptAll() requires identical schemas.")
        use_streaming = (
            bool(self._engine_streaming_default)
            if self._engine_streaming_default is not None
            else False
        )
        out_data, schema_desc = execute_except_all(
            self._rust_plan,
            self._root_data,
            other._rust_plan,
            other._root_data,
            as_python_lists=True,
            streaming=use_streaming,
        )
        derived_fields = self._field_types_from_descriptors(schema_desc)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        rust_plan = _require_rust_core().make_plan(derived_fields)
        return self._as_pyspark_df(
            self._from_plan(
                root_data=out_data,
                root_schema_type=derived_schema_type,
                current_schema_type=derived_schema_type,
                rust_plan=rust_plan,
            )
        )

    def intersectAll(self, other: DataFrame) -> DataFrame:
        """Multiset intersection (Spark ``INTERSECT ALL``)."""
        if self._current_field_types != other._current_field_types:
            raise ValueError("intersectAll() requires identical schemas.")
        use_streaming = (
            bool(self._engine_streaming_default)
            if self._engine_streaming_default is not None
            else False
        )
        out_data, schema_desc = execute_intersect_all(
            self._rust_plan,
            self._root_data,
            other._rust_plan,
            other._root_data,
            as_python_lists=True,
            streaming=use_streaming,
        )
        derived_fields = self._field_types_from_descriptors(schema_desc)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        rust_plan = _require_rust_core().make_plan(derived_fields)
        return self._as_pyspark_df(
            self._from_plan(
                root_data=out_data,
                root_schema_type=derived_schema_type,
                current_schema_type=derived_schema_type,
                rust_plan=rust_plan,
            )
        )

    def join(
        self,
        other: CoreDataFrame,
        *,
        on: str | ColumnRef | Sequence[str | ColumnRef] | None = None,
        left_on: str | ColumnRef | Sequence[str | ColumnRef] | None = None,
        right_on: str | ColumnRef | Sequence[str | ColumnRef] | None = None,
        how: str = "inner",
        suffix: str = "_right",
        coalesce: bool | None = None,
        validate: str | None = None,
        join_nulls: bool | None = None,
        maintain_order: bool | str | None = None,
        streaming: bool | None = None,
        keepRightJoinKeys: bool = False,
        keepLeftJoinKeys: bool = False,
    ) -> DataFrame:
        """Join two frames (Spark-shaped wrapper over core join).

        Supports Spark-ish join modes like ``left_semi``/``left_anti`` and
        ``right_semi``/``right_anti`` (aliases over core ``semi``/``anti``), plus
        Spark-style validate shorthands: ``validate='1:1'|'1:m'|'m:1'|'m:m'``.
        """
        if not isinstance(keepRightJoinKeys, bool):
            raise TypeError("join(keepRightJoinKeys=...) expects a bool.")
        if not isinstance(keepLeftJoinKeys, bool):
            raise TypeError("join(keepLeftJoinKeys=...) expects a bool.")
        if on is not None and (left_on is not None or right_on is not None):
            raise ValueError(
                "join() use either on=... or left_on=/right_on=..., not both."
            )

        how_norm = str(how).strip().lower()
        how_aliases = {
            "outer": "outer",
            "full_outer": "full",
            "full": "full",
            "right_outer": "right",
            "left_outer": "left",
        }
        how_norm = how_aliases.get(how_norm, how_norm)
        if how_norm == "left_semi":
            how_norm = "semi"
        elif how_norm == "left_anti":
            how_norm = "anti"
        elif how_norm == "right_semi":
            how_norm = "right_semi"
        elif how_norm == "right_anti":
            how_norm = "right_anti"

        supported_hows = {
            "inner",
            "left",
            "right",
            "outer",
            "full",
            "cross",
            "semi",
            "anti",
            "left_semi",
            "left_anti",
            "right_semi",
            "right_anti",
            "left_outer",
            "right_outer",
            "full_outer",
        }
        if how_norm not in {"right_semi", "right_anti"} and how_norm not in {
            "inner",
            "left",
            "right",
            "outer",
            "full",
            "cross",
            "semi",
            "anti",
        }:
            raise ValueError(
                "join(how=...) must be one of: "
                + ", ".join(repr(x) for x in sorted(supported_hows))
            )

        def _resolve_key_arg(
            arg: str | ColumnRef | Sequence[str | ColumnRef] | None, *, arg_name: str
        ) -> str | list[str] | None:
            if arg is None:
                return None
            if isinstance(arg, str):
                return arg
            if isinstance(arg, ColumnRef):
                referenced = arg.referenced_columns()
                if len(referenced) != 1:
                    raise TypeError(
                        f"join({arg_name}=...) ColumnRef must reference exactly "
                        f"one column; referenced_columns={sorted(referenced)!r}"
                    )
                return next(iter(referenced))
            raw = list(arg)
            out: list[str] = []
            for k in raw:
                if isinstance(k, str):
                    out.append(k)
                elif isinstance(k, ColumnRef):
                    referenced = k.referenced_columns()
                    if len(referenced) != 1:
                        raise TypeError(
                            f"join({arg_name}=...) ColumnRef must reference exactly "
                            f"one column; referenced_columns={sorted(referenced)!r}"
                        )
                    out.append(next(iter(referenced)))
                else:
                    raise TypeError(
                        f"join({arg_name}=...) expects str, ColumnRef, or a "
                        "sequence of str|ColumnRef."
                    )
            if len(set(out)) != len(out):
                raise ValueError(
                    f"join({arg_name}=...) must not contain duplicate keys."
                )
            return out

        on_names = _resolve_key_arg(on, arg_name="on")
        left_names = _resolve_key_arg(left_on, arg_name="left_on")
        right_names = _resolve_key_arg(right_on, arg_name="right_on")

        used_on = on_names is not None
        used_lr = left_names is not None or right_names is not None
        if how_norm != "cross":
            if used_on:
                pass
            elif used_lr:
                if left_names is None or right_names is None:
                    raise ValueError(
                        "join() requires both left_on=... and right_on=... when on=... "
                        "is not set."
                    )
                left_list = (
                    [left_names] if isinstance(left_names, str) else list(left_names)
                )
                right_list = (
                    [right_names] if isinstance(right_names, str) else list(right_names)
                )
                if len(left_list) != len(right_list):
                    raise ValueError(
                        "join(left_on=..., right_on=...) key lists must match length."
                    )
            else:
                raise ValueError(
                    "join() requires on=... or left_on=.../right_on=... for "
                    "non-cross joins."
                )

        if how_norm in ("right_semi", "right_anti"):
            swapped_how = "semi" if how_norm == "right_semi" else "anti"
            if used_on:
                swapped = other.join(
                    self,
                    on=on_names,
                    how=swapped_how,
                    suffix=suffix,
                    coalesce=coalesce,
                    validate=validate,
                    join_nulls=join_nulls,
                    maintain_order=maintain_order,
                    streaming=streaming,
                )
            else:
                # swap sides: keys swap too
                swapped = other.join(
                    self,
                    left_on=right_names,
                    right_on=left_names,
                    how=swapped_how,
                    suffix=suffix,
                    coalesce=coalesce,
                    validate=validate,
                    join_nulls=join_nulls,
                    maintain_order=maintain_order,
                    streaming=streaming,
                )
            # right-only output is already guaranteed by semi/anti on swapped join.
            _ = keepLeftJoinKeys  # reserved for future parity knobs
            return self._as_pyspark_df(swapped)

        joined = super().join(
            other,
            on=on_names,
            left_on=left_names,
            right_on=right_names,
            how=how_norm,
            suffix=suffix,
            coalesce=coalesce,
            validate=validate,
            join_nulls=join_nulls,
            maintain_order=maintain_order,
            streaming=streaming,
        )

        # Spark-ish USING behavior: when joining on same-named keys, keep one copy
        # of each join key by default.
        if used_on and on_names is not None and not keepRightJoinKeys:
            # Core join can still produce suffixed duplicates depending on rules;
            # drop any right-side key duplicates if present.
            keys = [on_names] if isinstance(on_names, str) else list(on_names)
            drop_cols: list[str] = []
            for k in keys:
                rk = f"{k}{suffix}"
                if rk in joined._current_field_types:
                    drop_cols.append(rk)
            if drop_cols:
                joined = joined.drop(*drop_cols)
        elif (
            used_lr
            and not keepRightJoinKeys
            and left_names is not None
            and right_names is not None
        ):
            left_list = (
                [left_names] if isinstance(left_names, str) else list(left_names)
            )
            right_list = (
                [right_names] if isinstance(right_names, str) else list(right_names)
            )
            drop_cols: list[str] = []
            for lk, rk in zip(left_list, right_list, strict=True):
                if lk == rk:
                    cand = f"{rk}{suffix}"
                    if cand in joined._current_field_types:
                        drop_cols.append(cand)
            if drop_cols:
                joined = joined.drop(*drop_cols)

        return self._as_pyspark_df(joined)

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

    def sample(
        self,
        withReplacement: bool | None = None,
        fraction: float | None = None,
        seed: int | None = None,
    ) -> DataFrameModel:
        return cast(
            "DataFrameModel",
            self._from_dataframe(
                self._df.sample(
                    withReplacement=withReplacement,
                    fraction=fraction,
                    seed=seed,
                )
            ),
        )

    def explode(
        self,
        columns: Any,
        *,
        outer: bool = False,
        streaming: bool | None = None,
    ) -> DataFrameModel:
        """Explode **list** columns (Spark ``explode``).

        Use ``outer=True`` for ``explode_outer``.
        """
        return cast(
            "DataFrameModel",
            self._from_dataframe(
                self._df.explode(columns, outer=outer, streaming=streaming)
            ),
        )

    def explode_outer(
        self, columns: Any, *, streaming: bool | None = None
    ) -> DataFrameModel:
        """Explode lists with Spark-ish *outer* null/empty handling (see docs)."""
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.explode_outer(columns, streaming=streaming)),
        )

    def explode_all(self, *, streaming: bool | None = None) -> DataFrameModel:
        """Explode every list-typed column in the schema (schema-driven convenience)."""
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.explode_all(streaming=streaming)),
        )

    def posexplode(
        self,
        column: str,
        *,
        pos: str = "pos",
        value: str | None = None,
        outer: bool = False,
        streaming: bool | None = None,
    ) -> DataFrameModel:
        """Explode one list with **0-based** positions (Spark ``posexplode``)."""
        return cast(
            "DataFrameModel",
            self._from_dataframe(
                self._df.posexplode(
                    column, pos=pos, value=value, outer=outer, streaming=streaming
                )
            ),
        )

    def posexplode_outer(
        self,
        column: str,
        *,
        pos: str = "pos",
        value: str | None = None,
        streaming: bool | None = None,
    ) -> DataFrameModel:
        """``posexplode(..., outer=True)`` alias."""
        return cast(
            "DataFrameModel",
            self._from_dataframe(
                self._df.posexplode_outer(
                    column, pos=pos, value=value, streaming=streaming
                )
            ),
        )

    def unnest(self, columns: Any, *, streaming: bool | None = None) -> DataFrameModel:
        """Expand **struct** columns to top-level fields (Spark analogue)."""
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.unnest(columns, streaming=streaming)),
        )

    def unnest_all(self, *, streaming: bool | None = None) -> DataFrameModel:
        """Unnest every struct-typed column in the schema."""
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.unnest_all(streaming=streaming)),
        )

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
        return (
            self.distinct(subset=subset, keep="first")
            if subset is not None
            else self.distinct(keep="first")
        )

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

    def join(
        self,
        other: DataFrameModel | DataFrame,
        *,
        on: str | ColumnRef | Sequence[str | ColumnRef] | None = None,
        left_on: str | ColumnRef | Sequence[str | ColumnRef] | None = None,
        right_on: str | ColumnRef | Sequence[str | ColumnRef] | None = None,
        how: str = "inner",
        suffix: str = "_right",
        coalesce: bool | None = None,
        validate: str | None = None,
        join_nulls: bool | None = None,
        maintain_order: bool | str | None = None,
        streaming: bool | None = None,
        keepRightJoinKeys: bool = False,
        keepLeftJoinKeys: bool = False,
    ) -> DataFrameModel:
        od = other._df if isinstance(other, DataFrameModel) else other
        return cast(
            "DataFrameModel",
            self._from_dataframe(
                self._df.join(
                    od,
                    on=on,
                    left_on=left_on,
                    right_on=right_on,
                    how=how,
                    suffix=suffix,
                    coalesce=coalesce,
                    validate=validate,
                    join_nulls=join_nulls,
                    maintain_order=maintain_order,
                    streaming=streaming,
                    keepRightJoinKeys=keepRightJoinKeys,
                    keepLeftJoinKeys=keepLeftJoinKeys,
                )
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

    def except_(self, other: DataFrameModel | DataFrame) -> DataFrameModel:
        """Distinct set difference (Spark ``except`` / SQL ``EXCEPT DISTINCT``)."""
        od = other._df if isinstance(other, DataFrameModel) else other
        return cast("DataFrameModel", self._from_dataframe(self._df.except_(od)))

    def exceptAll(self, other: DataFrameModel | DataFrame) -> DataFrameModel:
        od = other._df if isinstance(other, DataFrameModel) else other
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.exceptAll(od)),
        )

    def intersectAll(self, other: DataFrameModel | DataFrame) -> DataFrameModel:
        od = other._df if isinstance(other, DataFrameModel) else other
        return cast(
            "DataFrameModel",
            self._from_dataframe(self._df.intersectAll(od)),
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


# Python keyword compatibility: allow `df.except(other)` at runtime.
setattr(DataFrame, "except", DataFrame.except_)
setattr(DataFrameModel, "except", DataFrameModel.except_)
