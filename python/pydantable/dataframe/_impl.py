"""Typed :class:`DataFrame`, plan chaining, and grouped aggregation handles.

Logical plans and expression typing live in Rust; Python holds schema state and
forwards transforms. Materialization is via :meth:`DataFrame.collect`,
:meth:`DataFrame.to_dict`, :meth:`DataFrame.to_polars`, or :meth:`DataFrame.to_arrow`.
Non-blocking variants :meth:`DataFrame.acollect`, :meth:`DataFrame.ato_dict`,
:meth:`DataFrame.ato_polars`, and :meth:`DataFrame.ato_arrow` run the same work in a
worker thread.
"""

from __future__ import annotations

import asyncio
import enum
import functools
import html
import importlib
import types
import warnings
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
)

from pydantic import BaseModel, TypeAdapter

from pydantable.expressions import ColumnRef, Expr
from pydantable.rust_engine import (
    _require_rust_core,
    execute_concat,
    execute_explode,
    execute_groupby_agg,
    execute_groupby_dynamic_agg,
    execute_join,
    execute_melt,
    execute_pivot,
    execute_plan,
    execute_unnest,
)
from pydantable.schema import (
    _annotation_nullable_inner,
    make_derived_schema_type,
    merge_field_types_preserving_identity,
    previous_field_types_for_join,
    schema_field_types,
    schema_from_descriptors,
    validate_columns_strict,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from concurrent.futures import Executor


SchemaT = TypeVar("SchemaT", bound=BaseModel)

_NoneType = type(None)

# Cap column listing in :meth:`DataFrame.__repr__` for very wide schemas.
_REPR_MAX_COLUMNS = 32
_REPR_DTYPE_MAX_LEN = 72
# Jupyter / IPython :meth:`DataFrame._repr_html_` preview limits (materializes rows).
_REPR_HTML_MAX_ROWS = 20
_REPR_HTML_MAX_COLS = 40
_REPR_HTML_MAX_CELL_LEN = 500
_UNION_ORIGINS = (types.UnionType, Union)


def _dtype_repr(annotation: Any) -> str:
    """Stable, readable dtype string for schema annotations (repr / logging)."""
    if annotation is None:
        return "Any"
    if isinstance(annotation, type):
        if annotation is _NoneType:
            return "None"
        return getattr(annotation, "__qualname__", annotation.__name__)

    args = get_args(annotation)
    origin = get_origin(annotation)

    if origin is Literal:
        inner = ", ".join(repr(a) for a in args)
        return f"Literal[{inner}]"

    if origin is not None and origin in _UNION_ORIGINS:
        if (
            len(args) == 2
            and _NoneType in args
            and not all(a is _NoneType for a in args)
        ):
            other = args[0] if args[1] is _NoneType else args[1]
            return f"{_dtype_repr(other)} | None"
        return " | ".join(_dtype_repr(a) for a in args)

    if origin is not None:
        oname = getattr(
            origin, "__qualname__", getattr(origin, "__name__", repr(origin))
        )
        if args:
            inner = ", ".join(_dtype_repr(a) for a in args)
            return f"{oname}[{inner}]"
        return oname

    s = repr(annotation)
    if len(s) > _REPR_DTYPE_MAX_LEN:
        return f"{s[: _REPR_DTYPE_MAX_LEN - 1]}…"
    return s


def _html_cell_text(value: Any) -> str:
    """Format a single cell for HTML tables; output must be HTML-escaped."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, (int, float)):
        s = str(value)
    elif isinstance(value, str):
        s = value
    else:
        s = repr(value)
    if len(s) > _REPR_HTML_MAX_CELL_LEN:
        return f"{s[: _REPR_HTML_MAX_CELL_LEN - 1]}…"
    return s


def _dataframe_to_html_fragment(
    *,
    column_dict: dict[str, list[Any]],
    column_order: list[str],
    caption: str | None = None,
    note: str | None = None,
) -> str:
    """Build a styled HTML table fragment (card layout, Jupyter-friendly)."""
    rows = len(next(iter(column_dict.values()))) if column_dict else 0
    # Modern palette: slate neutrals, soft shadow, zebra rows (no external CSS).
    css = """
<style scoped>
.pydantable-render {
  --pt-bg: #ffffff;
  --pt-border: #e2e8f0;
  --pt-header-fg: #0f172a;
  --pt-muted: #64748b;
  --pt-row-alt: #f8fafc;
  --pt-cell-fg: #1e293b;
  --pt-index-bg: #f1f5f9;
  font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto,
    "Helvetica Neue", Arial, sans-serif;
  font-size: 13px;
  line-height: 1.5;
  color: var(--pt-cell-fg);
  max-width: 100%;
  margin: 0;
  box-sizing: border-box;
}
.pydantable-render *, .pydantable-render *::before, .pydantable-render *::after {
  box-sizing: border-box;
}
.pydantable-render .pydantable-surface {
  border-radius: 12px;
  border: 1px solid var(--pt-border);
  background: var(--pt-bg);
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.06), 0 4px 12px rgba(15, 23, 42, 0.06);
  overflow: hidden;
  overflow-x: auto;
}
.pydantable-render table.pydantable-df {
  width: 100%;
  min-width: min-content;
  border-collapse: collapse;
  border-spacing: 0;
}
.pydantable-render caption {
  caption-side: top;
  text-align: left;
  padding: 12px 14px 10px;
  font-weight: 600;
  font-size: 12px;
  letter-spacing: 0.02em;
  color: var(--pt-header-fg);
  background: linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%);
  border-bottom: 1px solid var(--pt-border);
}
.pydantable-render thead th {
  text-align: left;
  padding: 8px 12px;
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--pt-muted);
  background: #f8fafc;
  border-bottom: 1px solid var(--pt-border);
  white-space: nowrap;
}
.pydantable-render thead th.pt-index-head {
  width: 3.25rem;
  text-align: right;
  padding-right: 10px;
  font-variant-numeric: tabular-nums;
}
.pydantable-render tbody th[scope="row"] {
  text-align: right;
  padding: 6px 10px 6px 12px;
  font-weight: 500;
  font-size: 12px;
  font-family: ui-monospace, "Cascadia Code", "SF Mono", Menlo, Consolas, monospace;
  font-variant-numeric: tabular-nums;
  color: var(--pt-muted);
  background: var(--pt-index-bg);
  border-bottom: 1px solid var(--pt-border);
  border-right: 1px solid var(--pt-border);
  vertical-align: top;
}
.pydantable-render tbody td {
  padding: 6px 12px;
  border-bottom: 1px solid var(--pt-border);
  vertical-align: top;
  word-break: break-word;
}
.pydantable-render tbody tr:nth-child(even) td {
  background: var(--pt-row-alt);
}
.pydantable-render tbody tr:nth-child(even) th[scope="row"] {
  background: #e8edf3;
}
.pydantable-render tbody tr:last-child td,
.pydantable-render tbody tr:last-child th {
  border-bottom: none;
}
.pydantable-render .pydantable-note {
  margin: 12px 4px 16px;
  padding: 0 4px;
  font-size: 11px;
  line-height: 1.4;
  color: var(--pt-muted);
}
</style>
"""
    parts: list[str] = [
        '<div class="pydantable-render">',
        css,
        '<div class="pydantable-surface">',
        '<table class="pydantable-df">',
    ]
    if caption:
        parts.append(f"<caption>{html.escape(caption)}</caption>")
    parts.append("<thead><tr>")
    parts.append('<th scope="col" class="pt-index-head"></th>')
    for name in column_order:
        parts.append(f'<th scope="col">{html.escape(name)}</th>')
    parts.append("</tr></thead><tbody>")
    for i in range(rows):
        parts.append("<tr>")
        parts.append(f'<th scope="row">{i}</th>')
        for name in column_order:
            raw = column_dict[name][i]
            text = _html_cell_text(raw)
            parts.append(f"<td>{html.escape(text)}</td>")
        parts.append("</tr>")
    parts.extend(["</tbody></table>", "</div>"])
    if note:
        parts.append(f'<p class="pydantable-note">{html.escape(note)}</p>')
    parts.append("</div>")
    return "\n".join(parts)


def _coerce_enum_columns(
    data: dict[str, list[Any]],
    field_types: Mapping[str, Any],
) -> dict[str, list[Any]]:
    """Rehydrate Rust Utf8 enum cells into concrete ``enum.Enum`` field types."""
    if not data or not field_types:
        return data
    out = dict(data)
    for name, ann in field_types.items():
        if name not in out:
            continue
        inner, _nullable = _annotation_nullable_inner(ann)
        origin = get_origin(inner)
        if origin is list:
            continue
        if not isinstance(inner, type):
            continue
        if issubclass(inner, BaseModel):
            continue
        if not (issubclass(inner, enum.Enum) and inner is not enum.Enum):
            continue
        adapter = TypeAdapter(ann)
        out[name] = [adapter.validate_python(v) for v in out[name]]
    return out


def _rows_from_column_dict(
    data: dict[str, list[Any]], row_type: type[BaseModel]
) -> list[BaseModel]:
    """Build validated row models from aligned column lists (same length per column)."""
    if not data:
        return []
    n = len(next(iter(data.values())))
    out: list[BaseModel] = []
    for i in range(n):
        row_dict = {name: col[i] for name, col in data.items()}
        out.append(row_type.model_validate(row_dict))
    return out


async def _materialize_in_thread(
    fn: Callable[[], Any],
    *,
    executor: Executor | None,
) -> Any:
    """Run a no-arg callable for blocking Rust/Polars work off the event loop."""
    loop = asyncio.get_running_loop()
    if executor is not None:
        return await loop.run_in_executor(executor, fn)
    return await asyncio.to_thread(fn)


def _is_bool_or_nullable_bool(dtype: Any) -> bool:
    """True if ``dtype`` is ``bool`` or optional bool (``| None`` / ``Union``)."""
    if dtype is bool:
        return True
    origin = get_origin(dtype)
    if origin is Union:
        args = tuple(get_args(dtype))
        if len(args) == 2 and _NoneType in args and bool in args:
            return True
    return False


@dataclass(frozen=True)
class SelectStep:
    """Internal: plain column projection step (names only)."""

    columns: list[str]


@dataclass(frozen=True)
class FilterStep:
    """Internal: boolean mask step."""

    condition: Expr


@dataclass(frozen=True)
class WithColumnsStep:
    """Internal: add or replace columns from expressions."""

    columns: dict[str, Expr]


class DataFrame(Generic[SchemaT]):
    """Strongly typed lazy table: schema at construction, transforms, then ``collect``.

    Construct with ``DataFrame[SchemaSubclass](data)``. Column types come from the
    schema model; expressions are built with :class:`~pydantable.expressions.Expr`
    or attribute access (``df.colname``). The Rust core validates operators and
    lowers plans to Polars for execution.
    """

    _schema_type: type[BaseModel] | None = None

    def __class_getitem__(cls, schema_type: Any) -> type[DataFrame[Any]]:
        if not isinstance(schema_type, type) or not issubclass(schema_type, BaseModel):
            raise TypeError("DataFrame[Schema] expects a Pydantic BaseModel type.")

        name = f"{cls.__name__}[{schema_type.__name__}]"
        # Important: avoid referencing `DataFrame[Any]` in a runtime `cast(...)`
        # because it triggers `__class_getitem__` again.
        return type(name, (cls,), {"_schema_type": schema_type})

    def __init__(
        self,
        data: Mapping[str, Sequence[Any]] | Any,
        *,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> None:
        if self._schema_type is None:
            raise TypeError(
                "Use DataFrame[SchemaType](data) to construct a typed DataFrame."
            )

        root_data = validate_columns_strict(
            data,
            self._schema_type,
            validate_elements=None,
            trusted_mode=trusted_mode,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
        )
        self._root_data: Any = root_data
        self._root_schema_type: type[BaseModel] = self._schema_type
        self._current_schema_type: type[BaseModel] = self._schema_type
        self._current_field_types = schema_field_types(self._current_schema_type)
        # Rust owns expression typing, logical planning, and execution.
        self._rust_plan = _require_rust_core().make_plan(self.schema_fields())

    @classmethod
    def _from_plan(
        cls,
        *,
        root_data: Any,
        root_schema_type: type[BaseModel],
        current_schema_type: type[BaseModel],
        rust_plan: Any,
    ) -> DataFrame[Any]:
        obj = cls.__new__(cls)
        obj._root_data = root_data
        obj._root_schema_type = root_schema_type
        obj._current_schema_type = current_schema_type
        obj._current_field_types = schema_field_types(current_schema_type)
        obj._rust_plan = rust_plan
        obj._schema_type = None
        return cast("DataFrame[Any]", obj)

    @property
    def schema_type(self) -> type[BaseModel]:
        return self._current_schema_type

    def schema_fields(self) -> dict[str, Any]:
        return dict(self._current_field_types)

    def _field_types_from_descriptors(
        self,
        descriptors: Mapping[str, Mapping[str, Any]],
        *,
        previous: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        derived = schema_from_descriptors(descriptors)
        prev = self._current_field_types if previous is None else previous
        return merge_field_types_preserving_identity(prev, descriptors, derived)

    def col(self, name: str) -> ColumnRef:
        if name not in self._current_field_types:
            raise KeyError(f"Unknown column {name!r} for current schema.")
        return ColumnRef(name=name, dtype=self._current_field_types[name])

    def __getattr__(self, item: str) -> Any:
        # Called only when attribute resolution fails; treat schema fields as columns.
        if item in self._current_field_types:
            return self.col(item)
        raise AttributeError(item)

    def __repr__(self) -> str:
        fields = self._current_field_types
        schema = self._current_schema_type
        schema_qn = getattr(schema, "__qualname__", schema.__name__)
        cls_name = self.__class__.__name__
        n = len(fields)
        lines = [
            f"{cls_name}",
            f"  schema: {schema_qn}",
            f"  columns ({n}):",
        ]
        if not fields:
            lines.append("    (none)")
            return "\n".join(lines)
        items = list(fields.items())
        shown = items[:_REPR_MAX_COLUMNS]
        name_w = max(len(name) for name, _ in shown)
        for name, ann in shown:
            dtype_s = _dtype_repr(ann)
            lines.append(f"    {name:<{name_w}}  {dtype_s}")
        rest = len(items) - len(shown)
        if rest > 0:
            lines.append(f"    … and {rest} more")
        return "\n".join(lines)

    def _repr_html_(self) -> str:
        """Rich HTML table for Jupyter / IPython (preview only; materializes data).

        Shows up to :data:`_REPR_HTML_MAX_ROWS` rows and :data:`_REPR_HTML_MAX_COLS`
        columns. Full result sets may be larger—use :meth:`to_dict` or
        :meth:`to_polars` for complete data.
        """
        try:
            return self._repr_html_impl()
        except Exception as e:  # pragma: no cover - defensive for notebook UX
            err = html.escape(str(e))
            return (
                '<div class="pydantable-render pydantable-render--error" '
                'style="font-family:ui-sans-serif,system-ui,sans-serif;'
                "font-size:13px;margin:0 0 1rem 0;padding:14px 16px;"
                "border-radius:12px;border:1px solid #fecaca;background:#fef2f2;"
                'color:#991b1b;box-shadow:0 1px 2px rgba(127,29,29,0.06);">'
                '<p style="margin:0 0 8px 0;font-weight:600;">HTML preview failed</p>'
                f'<pre style="margin:0;white-space:pre-wrap;word-break:break-word;'
                f'font-size:12px;color:#7f1d1d;">{err}</pre></div>'
            )

    def _repr_html_impl(self) -> str:
        cols_all = list(self._current_field_types.keys())
        if not cols_all:
            return (
                '<div class="pydantable-render" style="margin:0 0 1rem 0;">'
                '<div class="pydantable-surface" style="border-radius:12px;'
                "border:1px dashed #cbd5e1;background:#f8fafc;"
                "padding:1.5rem 1.25rem;text-align:center;color:#64748b;"
                "font-family:ui-sans-serif,system-ui,sans-serif;font-size:13px;"
                'box-shadow:0 1px 2px rgba(15,23,42,0.04);">'
                '<p style="margin:0;"><em>No columns</em></p></div></div>'
            )

        preview = self.head(_REPR_HTML_MAX_ROWS)
        data = preview.to_dict()
        n_rows = len(next(iter(data.values()))) if data else 0

        col_order = [c for c in cols_all if c in data]
        for c in data:
            if c not in col_order:
                col_order.append(c)

        n_cols_total = len(col_order)
        col_trunc = n_cols_total > _REPR_HTML_MAX_COLS
        shown_cols = col_order[:_REPR_HTML_MAX_COLS]
        sub: dict[str, list[Any]] = {c: data[c] for c in shown_cols}

        st = self._current_schema_type
        schema_qn = getattr(st, "__qualname__", st.__name__)
        caption = f"{self.__class__.__name__} · schema={schema_qn}"

        note_parts: list[str] = [
            f"Preview: {n_rows} row{'s' if n_rows != 1 else ''} x "
            f"{len(shown_cols)} column{'s' if len(shown_cols) != 1 else ''} shown"
        ]
        if n_rows >= _REPR_HTML_MAX_ROWS:
            note_parts.append(f"(up to {_REPR_HTML_MAX_ROWS} rows)")
        if col_trunc:
            rest = n_cols_total - len(shown_cols)
            col_s = "column" if rest == 1 else "columns"
            note_parts.append(f"(… and {rest} more {col_s} omitted)")
        note = " ".join(note_parts)

        return _dataframe_to_html_fragment(
            column_dict=sub,
            column_order=shown_cols,
            caption=caption,
            note=note,
        )

    def with_columns(self, **new_columns: Expr | Any) -> DataFrame[Any]:
        """Add or replace columns.

        Values are :class:`~pydantable.expressions.Expr` or plain literals.
        """
        rust = _require_rust_core()
        rust_columns: dict[str, Any] = {}

        for name, value in new_columns.items():
            if isinstance(value, Expr):
                rust_columns[name] = value._rust_expr
            else:
                rust_columns[name] = rust.make_literal(value=value)

        rust_plan = rust.plan_with_columns(self._rust_plan, rust_columns)
        desc = rust_plan.schema_descriptors()
        derived_fields = self._field_types_from_descriptors(desc)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )

        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
        )

    def select(self, *cols: str | ColumnRef | Expr, **named: Any) -> DataFrame[Any]:
        """Project columns and/or compute a **single-row** frame of global aggregates.

        Positional arguments: base column names, single-column refs, or globals such
        as :func:`~pydantable.expressions.global_sum`. Keyword arguments are only for
        named global aggregates. Plain projections and globals cannot be mixed.
        """
        rust = _require_rust_core()

        named_items: list[tuple[str, Any]] = []
        for name, e in named.items():
            if not isinstance(e, Expr):
                raise TypeError(
                    "select() keyword arguments must be Expr instances "
                    "(global aggregates)."
                )
            named_items.append((name, e._rust_expr))

        aggs: list[tuple[str, Any]] = []
        projects: list[str] = []
        for col in cols:
            if isinstance(col, str):
                projects.append(col)
            elif isinstance(col, Expr):
                if rust.expr_is_global_agg(col._rust_expr):
                    alias = rust.expr_global_default_alias(col._rust_expr)
                    if alias is None:
                        raise TypeError(
                            "global aggregate in select() is missing a default "
                            "output name."
                        )
                    aggs.append((alias, col._rust_expr))
                else:
                    referenced = col.referenced_columns()
                    if len(referenced) != 1:
                        raise TypeError(
                            "select() accepts column names or a ColumnRef expression."
                        )
                    projects.append(next(iter(referenced)))
            else:
                raise TypeError("select() accepts column names or Expr objects.")

        if named_items and (projects or aggs):
            raise TypeError(
                "select() cannot mix keyword aggregates with positional column "
                "names or aggregates."
            )
        if aggs and projects:
            raise TypeError(
                "select() cannot mix global aggregates with plain column projections."
            )
        if named_items:
            rust_plan = rust.plan_global_select(self._rust_plan, named_items)
        elif aggs:
            rust_plan = rust.plan_global_select(self._rust_plan, aggs)
        else:
            if not projects:
                raise ValueError("select() requires at least one column.")
            rust_plan = rust.plan_select(self._rust_plan, projects)
        desc = rust_plan.schema_descriptors()
        derived_fields = self._field_types_from_descriptors(desc)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
        )

    def filter(self, condition: Expr) -> DataFrame[Any]:
        """Keep rows where the boolean ``condition`` is true."""
        rust = _require_rust_core()

        if not isinstance(condition, Expr):
            raise TypeError("filter(condition) expects an Expr.")

        rust_plan = rust.plan_filter(self._rust_plan, condition._rust_expr)
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust_plan,
        )

    def sort(
        self, *by: str | ColumnRef, descending: bool | Sequence[bool] = False
    ) -> DataFrame[Any]:
        """Sort by one or more columns (names or single-column expressions)."""
        rust = _require_rust_core()
        keys: list[str] = []
        for key in by:
            if isinstance(key, str):
                keys.append(key)
            elif isinstance(key, Expr):
                referenced = key.referenced_columns()
                if len(referenced) != 1:
                    raise TypeError(
                        "sort() accepts column names or a ColumnRef expression."
                    )
                keys.append(next(iter(referenced)))
            else:
                raise TypeError("sort() accepts column names or ColumnRef objects.")

        desc = (
            [descending] * len(keys)
            if isinstance(descending, bool)
            else list(descending)
        )
        rust_plan = rust.plan_sort(self._rust_plan, keys, desc)
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust_plan,
        )

    def unique(
        self,
        subset: Sequence[str] | None = None,
        *,
        keep: str = "first",
    ) -> DataFrame[Any]:
        rust = _require_rust_core()
        rust_plan = rust.plan_unique(
            self._rust_plan, None if subset is None else list(subset), keep
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust_plan,
        )

    def distinct(
        self,
        subset: Sequence[str] | None = None,
        *,
        keep: str = "first",
    ) -> DataFrame[Any]:
        return self.unique(subset=subset, keep=keep)

    def drop(self, *columns: str | ColumnRef) -> DataFrame[Any]:
        rust = _require_rust_core()
        selected: list[str] = []
        for col in columns:
            if isinstance(col, str):
                selected.append(col)
            elif isinstance(col, Expr):
                referenced = col.referenced_columns()
                if len(referenced) != 1:
                    raise TypeError(
                        "drop() accepts column names or a ColumnRef expression."
                    )
                selected.append(next(iter(referenced)))
            else:
                raise TypeError("drop() accepts column names or ColumnRef objects.")
        rust_plan = rust.plan_drop(self._rust_plan, selected)
        desc = rust_plan.schema_descriptors()
        derived_fields = self._field_types_from_descriptors(desc)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
        )

    def rename(self, columns: Mapping[str, str]) -> DataFrame[Any]:
        rust = _require_rust_core()
        rename_map = dict(columns)
        rust_plan = rust.plan_rename(self._rust_plan, rename_map)
        desc = rust_plan.schema_descriptors()
        rename_prev: dict[str, Any] = dict(self._current_field_types)
        for old_name, new_name in rename_map.items():
            if old_name in self._current_field_types:
                rename_prev[new_name] = self._current_field_types[old_name]
        derived_fields = self._field_types_from_descriptors(desc, previous=rename_prev)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
        )

    def slice(self, offset: int, length: int) -> DataFrame[Any]:
        rust = _require_rust_core()
        rust_plan = rust.plan_slice(self._rust_plan, int(offset), int(length))
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust_plan,
        )

    def head(self, n: int = 5) -> DataFrame[Any]:
        return self.slice(0, n)

    def tail(self, n: int = 5) -> DataFrame[Any]:
        return self.slice(-n, n)

    def fill_null(
        self,
        value: Any = None,
        *,
        strategy: str | None = None,
        subset: Sequence[str] | None = None,
    ) -> DataFrame[Any]:
        rust = _require_rust_core()
        if value is None and strategy is None:
            raise ValueError("fill_null() requires either value or strategy.")
        if value is not None and strategy is not None:
            raise ValueError("fill_null() accepts value or strategy, not both.")
        rust_plan = rust.plan_fill_null(
            self._rust_plan,
            None if subset is None else list(subset),
            value,
            strategy,
        )
        desc = rust_plan.schema_descriptors()
        derived_fields = self._field_types_from_descriptors(desc)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
        )

    def drop_nulls(self, subset: Sequence[str] | None = None) -> DataFrame[Any]:
        rust = _require_rust_core()
        rust_plan = rust.plan_drop_nulls(
            self._rust_plan, None if subset is None else list(subset)
        )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust_plan,
        )

    def melt(
        self,
        *,
        id_vars: Sequence[str] | None = None,
        value_vars: Sequence[str] | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
    ) -> DataFrame[Any]:
        out_data, schema_descriptors = execute_melt(
            self._rust_plan,
            self._root_data,
            [] if id_vars is None else list(id_vars),
            None if value_vars is None else list(value_vars),
            variable_name,
            value_name,
            as_python_lists=True,
        )
        derived_fields = self._field_types_from_descriptors(schema_descriptors)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        rust_plan = _require_rust_core().make_plan(derived_fields)
        return self._from_plan(
            root_data=out_data,
            root_schema_type=derived_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
        )

    def unpivot(
        self,
        *,
        index: Sequence[str] | None = None,
        on: Sequence[str] | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
    ) -> DataFrame[Any]:
        return self.melt(
            id_vars=index,
            value_vars=on,
            variable_name=variable_name,
            value_name=value_name,
        )

    def pivot(
        self,
        *,
        index: str | Sequence[str],
        columns: str | ColumnRef,
        values: str | Sequence[str],
        aggregate_function: str = "first",
    ) -> DataFrame[Any]:
        index_cols = [index] if isinstance(index, str) else list(index)
        if isinstance(columns, str):
            columns_col = columns
        elif isinstance(columns, Expr):
            referenced = columns.referenced_columns()
            if len(referenced) != 1:
                raise TypeError(
                    "pivot(columns=...) expects a column name or "
                    "single-column ColumnRef."
                )
            columns_col = next(iter(referenced))
        else:
            raise TypeError(
                "pivot(columns=...) expects a column name or single-column ColumnRef."
            )
        value_cols = [values] if isinstance(values, str) else list(values)
        out_data, schema_descriptors = execute_pivot(
            self._rust_plan,
            self._root_data,
            index_cols,
            columns_col,
            value_cols,
            aggregate_function,
            as_python_lists=True,
        )
        derived_fields = self._field_types_from_descriptors(schema_descriptors)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        rust_plan = _require_rust_core().make_plan(derived_fields)
        return self._from_plan(
            root_data=out_data,
            root_schema_type=derived_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
        )

    def explode(self, columns: str | Sequence[str]) -> DataFrame[Any]:
        cols = [columns] if isinstance(columns, str) else list(columns)
        out_data, schema_descriptors = execute_explode(
            self._rust_plan, self._root_data, cols
        )
        derived_fields = self._field_types_from_descriptors(schema_descriptors)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        rust_plan = _require_rust_core().make_plan(derived_fields)
        return self._from_plan(
            root_data=out_data,
            root_schema_type=derived_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
        )

    def unnest(self, columns: str | Sequence[str]) -> DataFrame[Any]:
        cols = [columns] if isinstance(columns, str) else list(columns)
        out_data, schema_descriptors = execute_unnest(
            self._rust_plan, self._root_data, cols
        )
        derived_fields = self._field_types_from_descriptors(schema_descriptors)
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        rust_plan = _require_rust_core().make_plan(derived_fields)
        return self._from_plan(
            root_data=out_data,
            root_schema_type=derived_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
        )

    def join(
        self,
        other: DataFrame[Any],
        *,
        on: str | Sequence[str] | None = None,
        left_on: str | Expr | Sequence[str | Expr] | None = None,
        right_on: str | Expr | Sequence[str | Expr] | None = None,
        how: str = "inner",
        suffix: str = "_right",
    ) -> DataFrame[Any]:
        """Join two frames on key column(s); ``how`` is e.g. ``inner``, ``left``."""
        if not isinstance(other, DataFrame):
            raise TypeError("join(other=...) expects another DataFrame.")
        if on is not None and (left_on is not None or right_on is not None):
            raise ValueError(
                "join() use either on=... or left_on=/right_on=..., not both."
            )

        def _resolve_keys(keys: str | Expr | Sequence[str | Expr] | None) -> list[str]:
            if keys is None:
                return []
            raw: list[str | Expr] = (
                [keys] if isinstance(keys, (str, Expr)) else list(keys)
            )
            out: list[str] = []
            for key in raw:
                if isinstance(key, str):
                    out.append(key)
                elif isinstance(key, Expr):
                    referenced = key.referenced_columns()
                    if len(referenced) != 1:
                        raise TypeError(
                            "join expression keys must reference exactly one column."
                        )
                    out.append(next(iter(referenced)))
                else:
                    raise TypeError(
                        "join keys must be str, Expr, or sequences thereof."
                    )
            return out

        if on is not None:
            left_keys = [on] if isinstance(on, str) else list(on)
            right_keys = list(left_keys)
        else:
            left_keys = _resolve_keys(left_on)
            right_keys = _resolve_keys(right_on)

        if how == "cross":
            if left_keys or right_keys:
                raise ValueError("cross join does not accept on/left_on/right_on keys.")
        else:
            if not left_keys or not right_keys:
                raise ValueError(
                    "join() requires on=... or both left_on=... "
                    "and right_on=... for non-cross joins."
                )
            if len(left_keys) != len(right_keys):
                raise ValueError(
                    "join() left_on and right_on must have the same length."
                )

        joined_data, schema_descriptors = execute_join(
            self._rust_plan,
            self._root_data,
            other._rust_plan,
            other._root_data,
            left_keys,
            right_keys,
            how,
            suffix,
            as_python_lists=True,
        )
        join_prev = previous_field_types_for_join(
            self._current_field_types,
            other._current_field_types,
            suffix=suffix,
            output_columns=list(schema_descriptors.keys()),
        )
        derived_fields = self._field_types_from_descriptors(
            schema_descriptors, previous=join_prev
        )
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, derived_fields
        )
        rust_plan = _require_rust_core().make_plan(derived_fields)
        return self._from_plan(
            root_data=joined_data,
            root_schema_type=derived_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
        )

    def group_by(self, *keys: str | ColumnRef) -> GroupedDataFrame:
        """Group by key column(s); finish with :meth:`GroupedDataFrame.agg`."""
        selected: list[str] = []
        for key in keys:
            if isinstance(key, str):
                selected.append(key)
            elif isinstance(key, Expr):
                referenced = key.referenced_columns()
                if len(referenced) != 1:
                    raise TypeError(
                        "group_by() accepts column names or ColumnRef expressions."
                    )
                selected.append(next(iter(referenced)))
            else:
                raise TypeError(
                    "group_by() accepts column names or ColumnRef expressions."
                )
        return GroupedDataFrame(self, selected)

    def rolling_agg(
        self,
        *,
        on: str,
        column: str,
        window_size: int | str,
        op: str,
        out_name: str,
        by: Sequence[str] | None = None,
        min_periods: int = 1,
    ) -> DataFrame[Any]:
        data = self.collect(as_lists=True)
        if on not in data or column not in data:
            raise KeyError("rolling_agg() requires existing on/column names.")
        by_cols = [] if by is None else list(by)
        for c in by_cols:
            if c not in data:
                raise KeyError(f"rolling_agg() unknown grouping column '{c}'.")
        n = len(data[on])
        idxs = list(range(n))
        idxs.sort(
            key=lambda i: tuple(data[c][i] for c in [*by_cols, on])  # type: ignore[misc]
        )
        out: list[Any] = [None] * n

        def _duration_seconds(v: int | str) -> float:
            if isinstance(v, int):
                return float(v)
            unit = v[-1]
            num = float(v[:-1])
            factors = {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}
            if unit not in factors:
                raise ValueError(
                    "rolling_agg(window_size=...) supports s/m/h/d suffix."
                )
            return num * factors[unit]

        def _to_seconds(x: Any) -> float:
            if isinstance(x, datetime):
                return x.timestamp()
            if isinstance(x, date):
                return float(datetime.combine(x, datetime.min.time()).timestamp())
            if isinstance(x, timedelta):
                return x.total_seconds()
            if isinstance(x, (int, float)):
                return float(x)
            raise TypeError(
                "rolling_agg(on=...) requires numeric/date/datetime/timedelta."
            )

        win_seconds = _duration_seconds(window_size)
        supported = {"sum", "mean", "min", "max", "count"}
        if op not in supported:
            raise ValueError(
                f"Unsupported rolling op '{op}'. Use one of {sorted(supported)}."
            )
        for pos, i in enumerate(idxs):
            current_group = tuple(data[c][i] for c in by_cols)
            current_t = _to_seconds(data[on][i])
            window_idxs: list[int] = []
            j = pos
            while j >= 0:
                k = idxs[j]
                if tuple(data[c][k] for c in by_cols) != current_group:
                    break
                if current_t - _to_seconds(data[on][k]) <= win_seconds:
                    window_idxs.append(k)
                    j -= 1
                else:
                    break
            vals = [
                data[column][k]
                for k in reversed(window_idxs)
                if data[column][k] is not None
            ]
            if len(vals) < min_periods:
                out[i] = None
                continue
            if op == "count":
                out[i] = len(vals)
            elif op == "sum":
                out[i] = sum(vals)
            elif op == "mean":
                out[i] = sum(vals) / len(vals) if vals else None
            elif op == "min":
                out[i] = min(vals) if vals else None
            else:
                out[i] = max(vals) if vals else None

        out_data = dict(data)
        out_data[out_name] = out
        fields = dict(self._current_field_types)
        in_dtype = self._current_field_types[column]
        if op == "count":
            fields[out_name] = int
        elif op == "mean":
            fields[out_name] = float | None
        elif op in {"sum", "min", "max"}:
            fields[out_name] = in_dtype
        else:
            fields[out_name] = in_dtype
        derived_schema_type = make_derived_schema_type(
            self._current_schema_type, fields
        )
        rust_plan = _require_rust_core().make_plan(fields)
        return self._from_plan(
            root_data=out_data,
            root_schema_type=derived_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
        )

    def group_by_dynamic(
        self,
        index_column: str,
        *,
        every: str,
        period: str | None = None,
        by: Sequence[str] | None = None,
    ) -> DynamicGroupedDataFrame:
        return DynamicGroupedDataFrame(
            self,
            index_column=index_column,
            every=every,
            period=period,
            by=[] if by is None else list(by),
        )

    def collect(
        self,
        *,
        as_lists: bool = False,
        as_numpy: bool = False,
        as_polars: bool | None = None,
    ) -> Any:
        """
        Materialize this typed logical DataFrame.

        By default returns a list of Pydantic models, one per row, validated
        against :attr:`schema_type` (the current projected schema).

        Use ``as_lists=True`` for a columnar ``dict[str, list]``. Use
        :meth:`to_dict` as a readable alias for that shape.

        With ``as_numpy=True``, returns ``dict[str, numpy.ndarray]`` (requires
        ``numpy``).

        The ``as_polars`` argument is deprecated: use :meth:`to_polars` for a
        Polars ``DataFrame`` when the optional ``polars`` package is installed.
        """
        if as_polars is not None:
            warnings.warn(
                "as_polars is deprecated; use to_polars() for a Polars DataFrame, "
                "or collect(as_lists=True) / to_dict() for columnar dicts.",
                DeprecationWarning,
                stacklevel=2,
            )
            if as_polars:
                return self.to_polars()
            return self.to_dict()
        if as_numpy and as_lists:
            raise ValueError(
                "collect() cannot specify both as_numpy=True and as_lists=True."
            )
        column_dict = execute_plan(
            self._rust_plan, self._root_data, as_python_lists=True
        )
        column_dict = _coerce_enum_columns(column_dict, self._current_field_types)
        if as_lists:
            return column_dict
        if as_numpy:
            import numpy as np  # type: ignore[import-not-found]

            return {k: np.asarray(v) for k, v in column_dict.items()}
        return _rows_from_column_dict(column_dict, self._current_schema_type)

    def to_dict(self) -> dict[str, list[Any]]:
        """Columnar materialization (alias for ``collect(as_lists=True)`` shape)."""
        raw = execute_plan(self._rust_plan, self._root_data, as_python_lists=True)
        return _coerce_enum_columns(raw, self._current_field_types)

    def to_polars(self) -> Any:
        """
        Materialize as a Polars ``DataFrame`` (requires the optional ``polars``
        Python package: ``pip install 'pydantable[polars]'``).
        """
        try:
            pl = importlib.import_module("polars")
        except ImportError as e:
            raise ImportError(
                "polars is required for to_polars(). Install with: "
                "pip install 'pydantable[polars]'"
            ) from e
        return pl.DataFrame(self.to_dict())

    def to_arrow(self) -> Any:
        """
        Materialize as a PyArrow ``Table`` (requires the optional ``pyarrow``
        package: ``pip install 'pydantable[arrow]'``).

        This runs the same Rust execution path as :meth:`to_dict`, then builds
        Arrow arrays from Python lists—it is not a zero-copy export of internal
        buffers.
        """
        try:
            pa = importlib.import_module("pyarrow")
        except ImportError as e:
            raise ImportError(
                "pyarrow is required for to_arrow(). Install with: "
                "pip install 'pydantable[arrow]'"
            ) from e
        return pa.Table.from_pydict(self.to_dict())

    async def acollect(
        self,
        *,
        as_lists: bool = False,
        as_numpy: bool = False,
        as_polars: bool | None = None,
        executor: Executor | None = None,
    ) -> Any:
        """
        Async version of :meth:`collect`: same semantics, but blocking work runs
        in :func:`asyncio.to_thread` or in ``executor`` when provided.

        Cancelling the awaiting task does **not** cancel in-flight Rust/Polars
        execution; the worker thread runs to completion.
        """
        return await _materialize_in_thread(
            functools.partial(
                self.collect,
                as_lists=as_lists,
                as_numpy=as_numpy,
                as_polars=as_polars,
            ),
            executor=executor,
        )

    async def ato_dict(
        self,
        *,
        executor: Executor | None = None,
    ) -> dict[str, list[Any]]:
        """Async version of :meth:`to_dict` (see :meth:`acollect`)."""
        return await _materialize_in_thread(
            functools.partial(self.to_dict),
            executor=executor,
        )

    async def ato_polars(
        self,
        *,
        executor: Executor | None = None,
    ) -> Any:
        """
        Async version of :meth:`to_polars` (see :meth:`acollect`).

        This still materializes a columnar Python dict first, then builds the
        Polars frame—same copies as the synchronous path.
        """
        return await _materialize_in_thread(
            functools.partial(self.to_polars),
            executor=executor,
        )

    async def ato_arrow(
        self,
        *,
        executor: Executor | None = None,
    ) -> Any:
        """
        Async version of :meth:`to_arrow` (see :meth:`acollect`).

        Same materialization and copies as the synchronous path.
        """
        return await _materialize_in_thread(
            functools.partial(self.to_arrow),
            executor=executor,
        )

    @classmethod
    def concat(
        cls,
        dfs: Sequence[DataFrame[Any]],
        *,
        how: str = "vertical",
    ) -> DataFrame[Any]:
        """Stack or otherwise combine two or more frames (see Rust ``how`` values)."""
        if len(dfs) < 2:
            raise ValueError("concat() requires at least two DataFrame inputs.")
        base = dfs[0]
        out_data = base._root_data
        out_schema_type = base._current_schema_type
        merged_ft = dict(base._current_field_types)
        out_plan = base._rust_plan
        for df in dfs[1:]:
            out_data, schema_descriptors = execute_concat(
                out_plan,
                out_data,
                df._rust_plan,
                df._root_data,
                how,
                as_python_lists=True,
            )
            derived_fields = schema_from_descriptors(schema_descriptors)
            merged_ft = merge_field_types_preserving_identity(
                merged_ft, schema_descriptors, derived_fields
            )
            out_schema_type = make_derived_schema_type(out_schema_type, merged_ft)
            out_plan = _require_rust_core().make_plan(merged_ft)
        return cls._from_plan(
            root_data=out_data,
            root_schema_type=out_schema_type,
            current_schema_type=out_schema_type,
            rust_plan=out_plan,
        )


class GroupedDataFrame:
    """Result of :meth:`DataFrame.group_by`; call :meth:`agg` to finalize."""

    def __init__(self, df: DataFrame[Any], keys: Sequence[str]):
        self._df = df
        self._keys = list(keys)

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

    def agg(self, **aggregations: tuple[str, str] | tuple[str, Expr]) -> DataFrame[Any]:
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

        grouped_data, schema_descriptors = execute_groupby_agg(
            self._df._rust_plan,
            self._df._root_data,
            self._keys,
            agg_specs,
            as_python_lists=True,
        )
        derived_fields = self._df._field_types_from_descriptors(schema_descriptors)
        derived_schema_type = make_derived_schema_type(
            self._df._current_schema_type, derived_fields
        )
        rust_plan = _require_rust_core().make_plan(derived_fields)
        return self._df._from_plan(
            root_data=grouped_data,
            root_schema_type=derived_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
        )


class DynamicGroupedDataFrame:
    """Time buckets from :meth:`DataFrame.group_by_dynamic`; then :meth:`agg`."""

    def __init__(
        self,
        df: DataFrame[Any],
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

    def agg(self, **aggregations: tuple[str, str]) -> DataFrame[Any]:
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

        out_data, schema_descriptors = execute_groupby_dynamic_agg(
            self._df._rust_plan,
            self._df._root_data,
            self._index,
            self._every,
            self._period,
            self._by if self._by else None,
            agg_specs,
            as_python_lists=True,
        )
        derived_fields = self._df._field_types_from_descriptors(schema_descriptors)
        derived_schema_type = make_derived_schema_type(
            self._df._current_schema_type, derived_fields
        )
        rust_plan = _require_rust_core().make_plan(derived_fields)
        return self._df._from_plan(
            root_data=out_data,
            root_schema_type=derived_schema_type,
            current_schema_type=derived_schema_type,
            rust_plan=rust_plan,
        )
