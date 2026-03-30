"""pandas-like method names on the core :class:`DataFrame` and :class:`DataFrameModel`.

``merge``/``assign``/``query`` mirror familiar pandas entry points where supported;
execution remains the Rust engine. Import ``DataFrame`` from this module for the
pandas-shaped API.
"""

from __future__ import annotations

import ast
from typing import Any

from typing_extensions import Self

from .dataframe import DataFrame as CoreDataFrame
from .dataframe import GroupedDataFrame as CoreGroupedDataFrame
from .dataframe_model import DataFrameModel as CoreDataFrameModel
from .dataframe_model import GroupedDataFrameModel as CoreGroupedDataFrameModel
from .expressions import Expr, Literal, coalesce, when
from .rust_engine import _require_rust_core
from .schema import Schema


def _is_pandas_series(value: object) -> bool:
    return type(value).__name__ == "Series" and type(value).__module__.startswith(
        "pandas."
    )


def _as_list_str(x: str | list[str] | None, *, name: str) -> list[str] | None:
    if x is None:
        return None
    if isinstance(x, str):
        return [x]
    if isinstance(x, list) and all(isinstance(v, str) for v in x):
        return list(x)
    raise TypeError(f"{name} must be a str or list[str].")


def _keys_have_duplicates(df: CoreDataFrame, keys: list[str]) -> bool:
    if not keys:
        return False
    cols = df.select(*keys).to_dict()
    n = len(next(iter(cols.values()))) if cols else 0
    seen: set[tuple[object, ...]] = set()
    for i in range(n):
        t = tuple(cols[k][i] for k in keys)
        if t in seen:
            return True
        seen.add(t)
    return False


def _validate_merge_keys(
    *,
    left: CoreDataFrame,
    right: CoreDataFrame,
    left_keys: list[str],
    right_keys: list[str],
    validate: str,
) -> None:
    v = (validate or "").strip().lower()
    allowed = {
        "one_to_one",
        "1:1",
        "one_to_many",
        "1:m",
        "many_to_one",
        "m:1",
        "many_to_many",
        "m:m",
    }
    if v not in allowed:
        raise ValueError(
            "merge(validate=...) must be one of "
            "'one_to_one', 'one_to_many', 'many_to_one', 'many_to_many' "
            "(or '1:1', '1:m', 'm:1', 'm:m')."
        )
    if v in {"many_to_many", "m:m"}:
        return
    left_dupes = _keys_have_duplicates(left, left_keys)
    right_dupes = _keys_have_duplicates(right, right_keys)
    if v in {"one_to_one", "1:1"} and (left_dupes or right_dupes):
        raise ValueError(
            "merge keys are not unique on one or both sides for validate='one_to_one'."
        )
    if v in {"one_to_many", "1:m"} and left_dupes:
        raise ValueError(
            "merge keys are not unique on left side for validate='one_to_many'."
        )
    if v in {"many_to_one", "m:1"} and right_dupes:
        raise ValueError(
            "merge keys are not unique on right side for validate='many_to_one'."
        )


def _merge_indicator_expr(
    df: CoreDataFrame, *, left_keys: list[str], right_key_outputs: list[str]
) -> Expr:
    if not left_keys or not right_key_outputs:
        raise ValueError(
            "merge(indicator=True) requires join key columns to compute '_merge'."
        )
    left_present = df.col(left_keys[0]).is_not_null()
    for k in left_keys[1:]:
        left_present = left_present & df.col(k).is_not_null()
    right_present = df.col(right_key_outputs[0]).is_not_null()
    for k in right_key_outputs[1:]:
        right_present = right_present & df.col(k).is_not_null()
    return (
        when(left_present & right_present, Literal(value="both"))
        .when(left_present & (~right_present), Literal(value="left_only"))
        .otherwise(Literal(value="right_only"))
    )


def _unique_tmp_name(existing: set[str], base: str) -> str:
    if base not in existing:
        return base
    i = 2
    while f"{base}_{i}" in existing:
        i += 1
    return f"{base}_{i}"


class PandasDataFrame(CoreDataFrame):
    """``assign``, ``merge``, ``query``, ``columns``, ``shape``, and related."""

    def assign(self, **kwargs: Any) -> CoreDataFrame:
        compiled: dict[str, Any] = {}
        for name, value in kwargs.items():
            if _is_pandas_series(value):
                raise TypeError(
                    f"assign({name!r}=...): pandas Series is not supported; "
                    "use column expressions or literals."
                )
            if callable(value) and not isinstance(value, Expr):
                value = value(self)
            if _is_pandas_series(value):
                raise TypeError(
                    f"assign({name!r}=...): pandas Series is not supported; "
                    "use column expressions or literals."
                )
            compiled[name] = value
        return self.with_columns(**compiled)

    def merge(
        self,
        other: CoreDataFrame,
        *,
        how: str = "inner",
        on: str | list[str] | None = None,
        left_on: str | list[str] | None = None,
        right_on: str | list[str] | None = None,
        suffixes: tuple[str, str] = ("_x", "_y"),
        indicator: bool = False,
        validate: str | None = None,
        **kw: Any,
    ) -> CoreDataFrame:
        if kw:
            raise TypeError(
                f"merge() got unsupported keyword arguments: {sorted(kw)!r}"
            )
        # `indicator` is handled below (when True).
        suffix = suffixes[1] if suffixes and len(suffixes) >= 2 else "_right"
        on_list = _as_list_str(on, name="on")
        left_list = _as_list_str(left_on, name="left_on")
        right_list = _as_list_str(right_on, name="right_on")

        if on_list is not None and (left_list is not None or right_list is not None):
            raise TypeError(
                "merge() use either on=... or left_on=/right_on=..., not both."
            )

        if on_list is None and (left_list is None or right_list is None):
            raise TypeError(
                "merge(...) requires on=... or both left_on=... and right_on=...."
            )

        if on_list is not None:
            if validate is not None:
                _validate_merge_keys(
                    left=self,
                    right=other,
                    left_keys=on_list,
                    right_keys=on_list,
                    validate=validate,
                )
            if indicator and "_merge" in set(self.schema_fields()) | set(
                other.schema_fields()
            ):
                raise ValueError(
                    "merge(indicator=True) would overwrite existing '_merge' column."
                )

            if indicator:
                joined = self.join(other, on=on_list, how=how, suffix=suffix)
                fields = set(joined.schema_fields())

                def _pick_presence_col(src_cols: list[str]) -> str | None:
                    for c in src_cols:
                        if c in fields:
                            return c
                        cand = f"{c}{suffix}"
                        if cand in fields:
                            return cand
                    return None

                left_non_keys = [c for c in self.schema_fields() if c not in on_list]
                right_non_keys = [c for c in other.schema_fields() if c not in on_list]
                l_col = _pick_presence_col(left_non_keys)
                r_col = _pick_presence_col(right_non_keys)
                if l_col is None or r_col is None:
                    raise NotImplementedError(
                        "merge(indicator=True) currently requires at least one "
                        "non-key column on each side."
                    )
                out = joined.with_columns(
                    _merge=(
                        when(
                            joined.col(l_col).is_not_null()
                            & joined.col(r_col).is_not_null(),
                            Literal(value="both"),
                        )
                        .when(
                            joined.col(l_col).is_not_null(), Literal(value="left_only")
                        )
                        .otherwise(Literal(value="right_only"))
                    )
                )
                if how in {"right", "outer"}:
                    # Some join implementations surface a duplicated/suffixed right
                    # key (e.g. `a_right` or `a_y`) and leave the left key nullable.
                    # Coalesce so `a` is populated for right-only rows.
                    fields2 = set(out.schema_fields())
                    dupes: list[str] = []
                    unify: dict[str, Expr] = {}
                    for k in on_list:
                        dupe = None
                        if f"{k}_right" in fields2:
                            dupe = f"{k}_right"
                        elif f"{k}{suffix}" in fields2:
                            dupe = f"{k}{suffix}"
                        if dupe is not None:
                            dupes.append(dupe)
                            unify[k] = coalesce(out.col(k), out.col(dupe))
                    if unify:
                        out = out.with_columns(**unify)
                    if dupes:
                        out = out.drop(*dupes)
                return out

            return self.join(other, on=on_list, how=how, suffix=suffix)

        assert left_list is not None and right_list is not None
        if len(left_list) != len(right_list):
            raise ValueError("merge() left_on and right_on must have the same length.")
        if validate is not None:
            _validate_merge_keys(
                left=self,
                right=other,
                left_keys=left_list,
                right_keys=right_list,
                validate=validate,
            )

        if indicator and "_merge" in set(self.schema_fields()) | set(
            other.schema_fields()
        ):
            raise ValueError(
                "merge(indicator=True) would overwrite existing '_merge' column."
            )

        joined = self.join(
            other,
            left_on=left_list,
            right_on=right_list,
            how=how,
            suffix=suffix,
        )

        # Pandas-like output policy: keep left keys, drop right key columns.
        joined_cols = set(joined.schema_fields().keys())
        drop_cols: list[str] = []
        right_key_outputs: list[str] = []
        for rk in right_list:
            if rk in joined_cols:
                drop_cols.append(rk)
                right_key_outputs.append(rk)
                continue
            cand = f"{rk}{suffix}"
            if cand in joined_cols:
                drop_cols.append(cand)
                right_key_outputs.append(cand)
        if indicator:
            joined = joined.with_columns(
                _merge=_merge_indicator_expr(
                    joined, left_keys=left_list, right_key_outputs=right_key_outputs
                )
            )
        # Unify key columns (pandas-like): for right-only rows, fill the left key
        # with the corresponding right key value before dropping right keys.
        if how in {"right", "outer"}:
            unify: dict[str, Expr] = {}
            for lk, rk_out in zip(left_list, right_key_outputs, strict=True):
                if lk in joined_cols:
                    unify[lk] = coalesce(joined.col(lk), joined.col(rk_out))
                else:
                    # Some join shapes (notably how='right' with different left_on /
                    # right_on names) may omit the left key column entirely from the
                    # join output. Create it from the right key.
                    unify[lk] = joined.col(rk_out)
            joined = joined.with_columns(**unify)
        return joined.drop(*drop_cols) if drop_cols else joined

    def query(self, expr: str, **kwargs: Any) -> CoreDataFrame:
        if kwargs:
            raise TypeError(
                f"query() got unsupported keyword arguments: {sorted(kwargs)!r}"
            )
        if not isinstance(expr, str) or not expr.strip():
            raise TypeError("query(expr) expects a non-empty string.")

        def _lit(v: object) -> Expr:
            return Literal(value=v)

        def _compile(node: ast.AST) -> Expr:
            if isinstance(node, ast.BoolOp):
                if isinstance(node.op, ast.And):
                    out = _compile(node.values[0])
                    for v in node.values[1:]:
                        out = out & _compile(v)
                    return out
                if isinstance(node.op, ast.Or):
                    out = _compile(node.values[0])
                    for v in node.values[1:]:
                        out = out | _compile(v)
                    return out
                raise NotImplementedError(
                    "query(): only 'and'/'or' boolean ops are supported."
                )
            if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
                return ~_compile(node.operand)
            if isinstance(node, ast.Compare):
                left = _compile(node.left)
                # Support chained comparisons by AND-ing each segment.
                out: Expr | None = None
                cur = left
                for op, right_node in zip(node.ops, node.comparators, strict=True):
                    if (
                        isinstance(right_node, ast.Constant)
                        and right_node.value is None
                    ):
                        if isinstance(op, ast.Eq):
                            part = cur.is_null()
                        elif isinstance(op, ast.NotEq):
                            part = cur.is_not_null()
                        else:
                            raise NotImplementedError(
                                "query(): only ==/!= are supported against None."
                            )
                        out = part if out is None else (out & part)
                        cur = _lit(None)
                        continue
                    right = _compile(right_node)
                    if isinstance(op, ast.Eq):
                        part = cur == right
                    elif isinstance(op, ast.NotEq):
                        part = cur != right
                    elif isinstance(op, ast.Lt):
                        part = cur < right
                    elif isinstance(op, ast.LtE):
                        part = cur <= right
                    elif isinstance(op, ast.Gt):
                        part = cur > right
                    elif isinstance(op, ast.GtE):
                        part = cur >= right
                    else:
                        raise NotImplementedError(
                            "query(): only == != < <= > >= comparisons are supported."
                        )
                    out = part if out is None else (out & part)
                    cur = right
                assert out is not None
                return out
            if isinstance(node, ast.Name):
                # Treat bare identifiers as columns.
                return self.col(node.id)
            if isinstance(node, ast.Constant):
                return _lit(node.value)
            raise NotImplementedError(
                f"query(): unsupported syntax {node.__class__.__name__}."
            )

        try:
            parsed = ast.parse(expr, mode="eval")
        except SyntaxError as e:
            raise ValueError(f"query(): invalid expression: {e}") from e
        compiled = _compile(parsed.body)
        return self.filter(compiled)

    def head(self, n: int = 5) -> CoreDataFrame:
        """
        Return the first ``n`` rows after materializing the current logical plan.

        This is an eager, convenience API (not a zero-copy lazy slice).
        """
        data = self.collect(as_lists=True)
        sliced: dict[str, list[Any]]
        if not data:
            sliced = {name: [] for name in self._current_field_types}
        else:
            nrows = len(next(iter(data.values())))
            take = max(0, min(int(n), nrows))
            sliced = {k: v[:take] for k, v in data.items()}
        rust = _require_rust_core().make_plan(self.schema_fields())
        return self._from_plan(
            root_data=sliced,
            root_schema_type=self._current_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust,
        )

    def tail(self, n: int = 5) -> CoreDataFrame:
        """
        Return the last ``n`` rows after materializing the current logical plan.

        Eager; see :meth:`head`.
        """
        data = self.collect(as_lists=True)
        sliced: dict[str, list[Any]]
        if not data:
            sliced = {name: [] for name in self._current_field_types}
        else:
            nrows = len(next(iter(data.values())))
            take = max(0, min(int(n), nrows))
            start = max(0, nrows - take)
            sliced = {k: v[start:] for k, v in data.items()}
        rust = _require_rust_core().make_plan(self.schema_fields())
        return self._from_plan(
            root_data=sliced,
            root_schema_type=self._current_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust,
        )

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

    def group_by(self, *keys: Any) -> PandasGroupedDataFrame:
        inner = super().group_by(*keys)
        return PandasGroupedDataFrame(inner._df, inner._keys)


class PandasGroupedDataFrame(CoreGroupedDataFrame):
    """Grouped frame with shorthand ``sum`` / ``mean`` / ``count`` over columns."""

    def size(self) -> CoreDataFrame:
        """
        Row count per group (pandas-style `GroupBy.size()`).

        Unlike `count()`, this counts rows including nulls by summing a per-row
        literal marker column.
        """
        existing = set(self._df.schema_fields())
        tmp = _unique_tmp_name(existing, "__pd_size")
        out = "size" if "size" not in existing else "__size"
        marked = self._df.with_columns(**{tmp: Literal(value=1)})
        return marked.group_by(*self._keys).agg(**{out: ("sum", tmp)})

    def sum(self, *columns: str) -> CoreDataFrame:
        if not columns:
            raise TypeError("sum() requires at least one column name.")
        return self.agg(
            streaming=None,
            **{f"{c}_sum": ("sum", c) for c in columns},
        )

    def mean(self, *columns: str) -> CoreDataFrame:
        if not columns:
            raise TypeError("mean() requires at least one column name.")
        return self.agg(
            streaming=None,
            **{f"{c}_mean": ("mean", c) for c in columns},
        )

    def count(self, *columns: str) -> CoreDataFrame:
        if not columns:
            raise TypeError("count() requires at least one column name.")
        return self.agg(
            streaming=None,
            **{f"{c}_count": ("count", c) for c in columns},
        )

    def nunique(self, *columns: str) -> CoreDataFrame:
        if not columns:
            raise TypeError("nunique() requires at least one column name.")
        return self.agg(
            streaming=None,
            **{f"{c}_nunique": ("n_unique", c) for c in columns},
        )


class PandasDataFrameModel(CoreDataFrameModel):
    """:class:`DataFrameModel` using :class:`PandasDataFrame` under the hood."""

    def assign(self, **kwargs: Any) -> CoreDataFrameModel:
        return type(self)._from_dataframe(self._df.assign(**kwargs))

    def merge(self, other: CoreDataFrameModel, **kwargs: Any) -> CoreDataFrameModel:
        if not isinstance(other, CoreDataFrameModel):
            raise TypeError("merge(other=...) expects another DataFrameModel instance.")
        return type(self)._from_dataframe(self._df.merge(other._df, **kwargs))

    def query(self, expr: str, **kwargs: Any) -> CoreDataFrameModel:
        return type(self)._from_dataframe(self._df.query(expr, **kwargs))

    def head(self, n: int = 5) -> Self:
        return type(self)._from_dataframe(self._df.head(n))

    def tail(self, n: int = 5) -> Self:
        return type(self)._from_dataframe(self._df.tail(n))

    def __getitem__(self, key: str | list[str]) -> Any:
        return self._df[key]  # type: ignore[index]

    def group_by(self, *keys: Any) -> PandasGroupedDataFrameModel:
        g = self._df.group_by(*keys)
        return PandasGroupedDataFrameModel(g, type(self))


class PandasGroupedDataFrameModel(CoreGroupedDataFrameModel):
    """Model-level grouped aggregations with pandas naming."""

    def sum(self, *columns: str) -> CoreDataFrameModel:
        return self._model_type._from_dataframe(self._grouped_df.sum(*columns))

    def mean(self, *columns: str) -> CoreDataFrameModel:
        return self._model_type._from_dataframe(self._grouped_df.mean(*columns))

    def count(self, *columns: str) -> CoreDataFrameModel:
        return self._model_type._from_dataframe(self._grouped_df.count(*columns))

    def size(self) -> CoreDataFrameModel:
        return self._model_type._from_dataframe(self._grouped_df.size())

    def nunique(self, *columns: str) -> CoreDataFrameModel:
        return self._model_type._from_dataframe(self._grouped_df.nunique(*columns))


class DataFrame(PandasDataFrame):
    """Default export: pandas-flavored typed ``DataFrame``."""


class DataFrameModel(PandasDataFrameModel):
    _dataframe_cls = DataFrame


__all__ = ["DataFrame", "DataFrameModel", "Expr", "Schema"]
