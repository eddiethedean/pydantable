"""Grouped and time-bucketed aggregation handles (thin wrappers over the engine)."""

from __future__ import annotations

import html
from collections.abc import Sequence
from typing import Any

from pydantable.expressions import Expr
from pydantable.schema import field_types_for_rust, make_derived_schema_type

from ._streaming import _resolve_engine_streaming


class GroupedDataFrame:
    """Result of :meth:`DataFrame.group_by`; call :meth:`agg` to finalize."""

    def __init__(
        self,
        df: Any,
        keys: Sequence[str],
        *,
        maintain_order: bool = False,
        drop_nulls: bool = True,
    ):
        self._df = df
        self._keys = list(keys)
        self._maintain_order = bool(maintain_order)
        self._drop_nulls = bool(drop_nulls)

    def __repr__(self) -> str:
        inner = "\n".join(f"  {line}" for line in repr(self._df).split("\n"))
        return f"GroupedDataFrame(by={self._keys!r})\n{inner}"

    def _repr_html_(self) -> str:
        inner = self._df._repr_html_()
        keys_s = html.escape(repr(self._keys))
        return (
            '<div class="pydantable-render pydantable-render--context" '
            'style="margin:0 0 1rem 0;">'
            '<p style="margin:0 0 10px 0;padding:8px 12px;border-radius:8px;'
            "font:600 12px ui-sans-serif,system-ui,sans-serif;"
            "color:#334155;background:#eef2ff;border:1px solid #c7d2fe;"
            'letter-spacing:0.02em;">'
            f"<b>GroupedDataFrame</b> (by={keys_s})</p>{inner}</div>"
        )

    def agg(
        self,
        *,
        streaming: bool | None = None,
        **aggregations: tuple[str, str] | tuple[str, Expr],
    ) -> Any:
        """One output column per kwarg: ``name=(op, column_or_expr)``."""
        agg_specs: dict[str, tuple[str, str]] = {}
        for out_name, spec in aggregations.items():
            if not isinstance(spec, tuple) or len(spec) != 2:
                raise TypeError(
                    "agg() expects specs like "
                    "output_name=('count'|'sum'|'mean'|'min'|'max'|'median'|"
                    "'std'|'var'|'first'|'last'|'n_unique', column)."
                )
            op, col_spec = spec
            if not isinstance(op, str):
                raise TypeError("Aggregation operator must be a string.")
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
            agg_specs[out_name] = (op, in_col)

        use_streaming = _resolve_engine_streaming(
            streaming=streaming, default=self._df._engine_streaming_default
        )
        grouped_data, schema_descriptors = self._df._engine.execute_groupby_agg(
            self._df._rust_plan,
            self._df._root_data,
            self._keys,
            agg_specs,
            maintain_order=self._maintain_order,
            drop_nulls=self._drop_nulls,
            as_python_lists=True,
            streaming=use_streaming,
        )
        derived_fields = self._df._field_types_from_descriptors(schema_descriptors)
        derived_schema_type = make_derived_schema_type(
            self._df._current_schema_type, derived_fields
        )
        rust_plan = self._df._engine.make_plan(field_types_for_rust(derived_fields))
        return self._df._from_plan(
            root_data=grouped_data,
            root_schema_type=derived_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
            engine=self._df._engine,
        )

    def sum(self, *columns: str, streaming: bool | None = None) -> Any:
        if not columns:
            raise ValueError("sum() requires at least one column name.")
        return self.agg(
            streaming=streaming, **{f"{c}_sum": ("sum", c) for c in columns}
        )

    def mean(self, *columns: str, streaming: bool | None = None) -> Any:
        if not columns:
            raise ValueError("mean() requires at least one column name.")
        return self.agg(
            streaming=streaming, **{f"{c}_mean": ("mean", c) for c in columns}
        )

    def min(self, *columns: str, streaming: bool | None = None) -> Any:
        if not columns:
            raise ValueError("min() requires at least one column name.")
        return self.agg(
            streaming=streaming, **{f"{c}_min": ("min", c) for c in columns}
        )

    def max(self, *columns: str, streaming: bool | None = None) -> Any:
        if not columns:
            raise ValueError("max() requires at least one column name.")
        return self.agg(
            streaming=streaming, **{f"{c}_max": ("max", c) for c in columns}
        )

    def count(self, *columns: str, streaming: bool | None = None) -> Any:
        if not columns:
            raise ValueError("count() requires at least one column name.")
        return self.agg(
            streaming=streaming, **{f"{c}_count": ("count", c) for c in columns}
        )

    def len(self, *, streaming: bool | None = None) -> Any:
        """Per-group row count (includes null rows) via a synthetic constant column."""
        tmp = "__pydantable_group_len__"
        df2 = self._df.with_columns(**{tmp: 1})
        out = df2.group_by(*self._keys).agg(streaming=streaming, len=("sum", tmp))
        return out.drop(tmp, strict=False)


class DynamicGroupedDataFrame:
    """Time buckets from :meth:`DataFrame.group_by_dynamic`; then :meth:`agg`."""

    def __init__(
        self,
        df: Any,
        *,
        index_column: str,
        every: str,
        period: str | None,
        by: Sequence[str],
    ):
        self._df = df
        self._index = index_column
        self._every = every
        self._period = period or every
        self._by = list(by)

    def __repr__(self) -> str:
        inner = "\n".join(f"  {line}" for line in repr(self._df).split("\n"))
        return (
            f"DynamicGroupedDataFrame(index={self._index!r}, "
            f"every={self._every!r}, period={self._period!r}, by={self._by!r})\n"
            f"{inner}"
        )

    def _repr_html_(self) -> str:
        inner = self._df._repr_html_()
        hdr = (
            f"DynamicGroupedDataFrame index={html.escape(repr(self._index))} "
            f"every={html.escape(repr(self._every))} "
            f"period={html.escape(repr(self._period))} by={html.escape(repr(self._by))}"
        )
        return (
            '<div class="pydantable-render pydantable-render--context" '
            'style="margin:0 0 1rem 0;">'
            '<p style="margin:0 0 10px 0;padding:8px 12px;border-radius:8px;'
            "font:600 11px ui-sans-serif,system-ui,sans-serif;line-height:1.45;"
            "color:#334155;background:#ecfdf5;border:1px solid #a7f3d0;"
            'word-break:break-word;">'
            f"<b>{hdr}</b></p>{inner}</div>"
        )

    def agg(
        self,
        *,
        streaming: bool | None = None,
        **aggregations: tuple[str, str],
    ) -> Any:
        """Same as :meth:`GroupedDataFrame.agg`; column specs are strings only."""
        agg_specs: dict[str, tuple[str, str]] = {}
        for out_name, spec in aggregations.items():
            if not isinstance(spec, tuple) or len(spec) != 2:
                raise TypeError(
                    "agg() expects specs like output_name=('count'|'sum'|'mean'|"
                    "'min'|'max', column)."
                )
            op, in_col = spec
            if not isinstance(op, str) or not isinstance(in_col, str):
                raise TypeError("agg() op and column must be strings.")
            agg_specs[out_name] = (op, in_col)

        use_streaming = _resolve_engine_streaming(
            streaming=streaming, default=self._df._engine_streaming_default
        )
        out_data, schema_descriptors = self._df._engine.execute_groupby_dynamic_agg(
            self._df._rust_plan,
            self._df._root_data,
            self._index,
            self._every,
            self._period,
            self._by if self._by else None,
            agg_specs,
            as_python_lists=True,
            streaming=use_streaming,
        )
        derived_fields = self._df._field_types_from_descriptors(schema_descriptors)
        derived_schema_type = make_derived_schema_type(
            self._df._current_schema_type, derived_fields
        )
        rust_plan = self._df._engine.make_plan(field_types_for_rust(derived_fields))
        return self._df._from_plan(
            root_data=out_data,
            root_schema_type=derived_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
            engine=self._df._engine,
        )
