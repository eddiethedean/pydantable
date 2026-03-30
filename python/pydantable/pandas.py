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
from .schema._impl import make_derived_schema_type


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
        left_index: bool = False,
        right_index: bool = False,
        suffixes: tuple[str, str] = ("_x", "_y"),
        sort: bool = False,
        copy: bool | None = None,
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

        if left_index or right_index:
            if on_list is not None or left_list is not None or right_list is not None:
                raise NotImplementedError(
                    "merge(left_index/right_index=True) is only supported when no "
                    "on/left_on/right_on keys are provided."
                )
            if not (left_index and right_index):
                raise NotImplementedError(
                    "merge(left_index=True, right_index=False) (or vice versa) is not "
                    "supported yet."
                )
            if how == "cross":
                raise NotImplementedError(
                    "merge(..., how='cross') does not use index keys."
                )

            left_data = self.collect(as_lists=True)
            right_data = other.collect(as_lists=True)
            left_n = len(next(iter(left_data.values()))) if left_data else 0
            right_n = len(next(iter(right_data.values()))) if right_data else 0
            left_idx = list(range(left_n))
            right_idx = list(range(right_n))

            left_idx_name = _unique_tmp_name(set(left_data), "__pd_left_index")
            right_idx_name = _unique_tmp_name(set(right_data), "__pd_right_index")

            left_data2 = dict(left_data)
            right_data2 = dict(right_data)
            left_data2[left_idx_name] = left_idx
            right_data2[right_idx_name] = right_idx

            left_fields = {**dict(self.schema_fields()), left_idx_name: int}
            right_fields = {**dict(other.schema_fields()), right_idx_name: int}
            left_schema = make_derived_schema_type(
                self._current_schema_type, left_fields
            )
            right_schema = make_derived_schema_type(
                other._current_schema_type, right_fields
            )

            left_df = self._from_plan(
                root_data=left_data2,
                root_schema_type=left_schema,
                current_schema_type=left_schema,
                rust_plan=_require_rust_core().make_plan(left_fields),
            )
            right_df = other._from_plan(
                root_data=right_data2,
                root_schema_type=right_schema,
                current_schema_type=right_schema,
                rust_plan=_require_rust_core().make_plan(right_fields),
            )

            joined = left_df.join(
                right_df,
                left_on=left_idx_name,
                right_on=right_idx_name,
                how=how,
                suffix=suffix,
            )
            out_data = joined.collect(as_lists=True)
            out_data.pop(left_idx_name, None)
            out_data.pop(right_idx_name, None)
            out_fields = {
                k: v for k, v in joined.schema_fields().items() if k in out_data
            }
            out_schema = make_derived_schema_type(
                joined._current_schema_type, out_fields
            )
            out = joined._from_plan(
                root_data=out_data,
                root_schema_type=out_schema,
                current_schema_type=out_schema,
                rust_plan=_require_rust_core().make_plan(out_fields),
            )
            if indicator:
                if "_merge" in set(self.schema_fields()) | set(other.schema_fields()):
                    raise ValueError(
                        "merge(indicator=True) would overwrite existing "
                        "'_merge' column."
                    )
                out = out.with_columns(_merge=Literal(value="both"))
            if sort:
                raise NotImplementedError(
                    "merge(sort=True) is not supported for index merges."
                )
            return out
        _ = copy  # accepted for pandas parity; logical frames are copy-free

        if on_list is not None and (left_list is not None or right_list is not None):
            raise TypeError(
                "merge() use either on=... or left_on=/right_on=..., not both."
            )

        if how == "cross":
            if on_list is not None or left_list is not None or right_list is not None:
                raise TypeError(
                    "merge(how='cross') does not accept on/left_on/right_on."
                )
            if validate is not None:
                raise TypeError("merge(how='cross') does not support validate=....")
            out = self.join(other, how="cross", suffix=suffix)
            if indicator:
                if "_merge" in set(self.schema_fields()) | set(other.schema_fields()):
                    raise ValueError(
                        "merge(indicator=True) would overwrite existing "
                        "'_merge' column."
                    )
                out = out.with_columns(_merge=Literal(value="both"))
            if sort:
                raise NotImplementedError(
                    "merge(sort=True) is not supported for cross joins."
                )
            return out

        if on_list is None and (left_list is None or right_list is None):
            raise TypeError(
                "merge(...) requires on=... or both left_on=... and right_on=...."
            )

        def _check_suffix_collisions(
            *,
            left_cols: set[str],
            right_cols: list[str],
            right_keys: set[str],
        ) -> None:
            produced: set[str] = set(left_cols)
            for rc in right_cols:
                if rc in right_keys:
                    continue
                out_name = rc if rc not in left_cols else f"{rc}{suffix}"
                if out_name in produced:
                    raise ValueError(
                        "merge() would produce duplicate output column name "
                        f"{out_name!r}; choose a different suffixes[1]."
                    )
                produced.add(out_name)

        if on_list is not None:
            _check_suffix_collisions(
                left_cols=set(self.schema_fields()),
                right_cols=list(other.schema_fields()),
                right_keys=set(on_list),
            )
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
                    # Key-only frames: compute indicator eagerly from key membership.
                    joined2 = self.join(other, on=on_list, how=how, suffix=suffix)
                    out_data = joined2.collect(as_lists=True)
                    left_keys = self.select(*on_list).collect(as_lists=True)
                    right_keys = other.select(*on_list).collect(as_lists=True)
                    ln = len(next(iter(left_keys.values()))) if left_keys else 0
                    rn = len(next(iter(right_keys.values()))) if right_keys else 0
                    left_set = {
                        tuple(left_keys[k][i] for k in on_list) for i in range(ln)
                    }
                    right_set = {
                        tuple(right_keys[k][i] for k in on_list) for i in range(rn)
                    }
                    on_n = len(next(iter(out_data.values()))) if out_data else 0
                    merge_col: list[str] = []
                    for i in range(on_n):
                        key = tuple(out_data[k][i] for k in on_list)
                        l_present = key in left_set
                        r_present = key in right_set
                        if l_present and r_present:
                            merge_col.append("both")
                        elif l_present:
                            merge_col.append("left_only")
                        else:
                            merge_col.append("right_only")
                    out_data["_merge"] = merge_col
                    out_fields = {**dict(joined2.schema_fields()), "_merge": str}
                    out_schema = make_derived_schema_type(
                        joined2._current_schema_type, out_fields
                    )
                    return joined2._from_plan(
                        root_data=out_data,
                        root_schema_type=out_schema,
                        current_schema_type=out_schema,
                        rust_plan=_require_rust_core().make_plan(out_fields),
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
            out = self.join(other, on=on_list, how=how, suffix=suffix)
            if sort:
                out = out.sort(*on_list, descending=False)
            return out

        assert left_list is not None and right_list is not None
        if len(left_list) != len(right_list):
            raise ValueError("merge() left_on and right_on must have the same length.")
        _check_suffix_collisions(
            left_cols=set(self.schema_fields()),
            right_cols=list(other.schema_fields()),
            right_keys=set(right_list),
        )
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
        out = joined.drop(*drop_cols) if drop_cols else joined
        if sort:
            out = out.sort(*left_list, descending=False)
        return out

    def query(
        self,
        expr: str,
        *,
        local_dict: dict[str, object] | None = None,
        global_dict: dict[str, object] | None = None,
        engine: str = "python",
        inplace: bool = False,
        **kwargs: Any,
    ) -> CoreDataFrame:
        if kwargs:
            raise TypeError(
                f"query() got unsupported keyword arguments: {sorted(kwargs)!r}"
            )
        if engine != "python":
            raise NotImplementedError("query(engine!= 'python') is not supported.")
        if inplace:
            raise NotImplementedError("query(inplace=True) is not supported.")
        if not isinstance(expr, str) or not expr.strip():
            raise TypeError("query(expr) expects a non-empty string.")

        def _lit(v: object) -> Expr:
            return Literal(value=v)

        def _resolve_external(name: str) -> object:
            if local_dict and name in local_dict:
                return local_dict[name]
            if global_dict and name in global_dict:
                return global_dict[name]
            raise KeyError(name)

        def _external_to_expr(value: object) -> Expr:
            if isinstance(value, (int, float, str, bool)) or value is None:
                return _lit(value)
            raise NotImplementedError(
                "query(local_dict/global_dict) only support literal constants "
                "(int/float/str/bool/None) and literal lists/tuples of those."
            )

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
            if isinstance(node, ast.UnaryOp) and isinstance(
                node.op, (ast.UAdd, ast.USub)
            ):
                inner = _compile(node.operand)
                if isinstance(node.op, ast.UAdd):
                    return inner
                return -inner
            if isinstance(node, ast.BinOp):
                left = _compile(node.left)
                right = _compile(node.right)
                if isinstance(node.op, ast.Add):
                    return left + right
                if isinstance(node.op, ast.Sub):
                    return left - right
                if isinstance(node.op, ast.Mult):
                    return left * right
                if isinstance(node.op, ast.Div):
                    return left / right
                if isinstance(node.op, ast.Mod):
                    raise NotImplementedError("query(): '%' is not supported.")
                if isinstance(node.op, ast.FloorDiv):
                    raise NotImplementedError("query(): '//' is not supported.")
                if isinstance(node.op, ast.Pow):
                    raise NotImplementedError("query(): '**' is not supported.")
                raise NotImplementedError(
                    "query(): unsupported binary operator "
                    f"{node.op.__class__.__name__}."
                )
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
                    if isinstance(op, (ast.In, ast.NotIn)):
                        if isinstance(right_node, (ast.List, ast.Tuple)):
                            vals: list[object] = []
                            for elt in right_node.elts:
                                if isinstance(elt, ast.Constant):
                                    vals.append(elt.value)
                                elif isinstance(elt, ast.Name):
                                    try:
                                        vals.append(_resolve_external(elt.id))
                                    except KeyError as e:
                                        raise NotImplementedError(
                                            "query(): 'in' list names must come from "
                                            "local_dict/global_dict."
                                        ) from e
                                else:
                                    raise NotImplementedError(
                                        "query(): 'in'/'not in' only support literal "
                                        "lists/tuples."
                                    )
                            part = cur.isin(vals)
                            if isinstance(op, ast.NotIn):
                                part = ~part
                            out = part if out is None else (out & part)
                            cur = _lit(vals[-1] if vals else None)
                            continue
                        if isinstance(right_node, ast.Name):
                            try:
                                v = _resolve_external(right_node.id)
                            except KeyError as e:
                                raise NotImplementedError(
                                    "query(): 'in' name must come from "
                                    "local_dict/global_dict."
                                ) from e
                            if not isinstance(v, (list, tuple)):
                                raise NotImplementedError(
                                    "query(): 'in' name must be a list/tuple literal."
                                )
                            vals = list(v)
                            part = cur.isin(vals)
                            if isinstance(op, ast.NotIn):
                                part = ~part
                            out = part if out is None else (out & part)
                            cur = _lit(vals[-1] if vals else None)
                            continue
                        raise NotImplementedError(
                            "query(): 'in'/'not in' only support literal lists/tuples."
                        )
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
            if isinstance(node, (ast.List, ast.Tuple)):
                raise NotImplementedError(
                    "query(): bare list/tuple literals are only supported as the "
                    "right side of 'in'."
                )
            if isinstance(node, ast.Name):
                # Treat bare identifiers as columns.
                if node.id in self.schema_fields():
                    return self.col(node.id)
                try:
                    v = _resolve_external(node.id)
                except KeyError as e:
                    raise NotImplementedError(
                        f"query(): unknown name {node.id!r} (not a column and not in "
                        "local_dict/global_dict)."
                    ) from e
                return _external_to_expr(v)
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

    def sort_values(
        self,
        by: str | list[str],
        *,
        ascending: bool | list[bool] = True,
        kind: str | None = None,
        na_position: str | None = None,
        ignore_index: bool = False,
        key: Any = None,
    ) -> CoreDataFrame:
        if kind is not None:
            raise NotImplementedError("sort_values(kind=...) is not supported.")
        if na_position is not None:
            raise NotImplementedError(
                "sort_values(na_position=...) is not supported by the engine yet."
            )
        if ignore_index:
            raise NotImplementedError(
                "sort_values(ignore_index=True) is not supported; "
                "pydantable has no pandas Index semantics."
            )
        if key is not None:
            raise NotImplementedError("sort_values(key=...) is not supported.")
        by_list = [by] if isinstance(by, str) else list(by)
        if not by_list:
            raise TypeError("sort_values(by=...) requires at least one column.")
        if isinstance(ascending, bool):
            desc = [not ascending] * len(by_list)
        else:
            desc = [not bool(v) for v in list(ascending)]
            if len(desc) != len(by_list):
                raise ValueError("sort_values(): ascending must match len(by).")
        return self.sort(*by_list, descending=desc)

    def drop(
        self,
        labels: Any = None,
        *,
        index: Any = None,
        columns: str | list[str] | None = None,
        axis: Any = None,
        inplace: bool = False,
        level: Any = None,
        errors: str = "raise",
    ) -> CoreDataFrame:
        if axis is not None:
            raise NotImplementedError("drop(axis=...) is not supported; use columns=.")
        if inplace:
            raise NotImplementedError("drop(inplace=True) is not supported.")
        if level is not None:
            raise NotImplementedError("drop(level=...) is not supported.")
        if labels is not None and columns is not None:
            raise TypeError("drop() specify labels or columns, not both.")
        if index is not None:
            if labels is not None or columns is not None:
                raise TypeError("drop() cannot combine index= with columns/labels.")
            if errors not in {"raise", "ignore"}:
                raise ValueError("drop(errors=...) must be 'raise' or 'ignore'.")
            idx_list = [index] if isinstance(index, int) else list(index)
            data = self.collect(as_lists=True)
            n = len(next(iter(data.values()))) if data else 0
            bad = [i for i in idx_list if not isinstance(i, int) or i < 0 or i >= n]
            if bad and errors == "raise":
                raise IndexError(f"drop(index=...): indices out of range: {bad}")
            drop_set = {i for i in idx_list if isinstance(i, int) and 0 <= i < n}
            kept = [i for i in range(n) if i not in drop_set]
            new_data = {k: [v[i] for i in kept] for k, v in data.items()}
            return type(self)[self._current_schema_type](new_data)
        cols = labels if columns is None else columns
        if cols is None:
            raise TypeError("drop() requires columns=... (or labels positional).")
        col_list = [cols] if isinstance(cols, str) else list(cols)
        if not col_list:
            raise TypeError("drop(columns=...) requires at least one column.")
        if errors not in {"raise", "ignore"}:
            raise ValueError("drop(errors=...) must be 'raise' or 'ignore'.")
        missing = [c for c in col_list if c not in self.schema_fields()]
        if missing:
            if errors == "ignore":
                col_list = [c for c in col_list if c in self.schema_fields()]
            else:
                raise KeyError(f"drop(): columns not found: {missing}")
        return super().drop(*col_list) if col_list else self

    def rename(
        self,
        mapper: Any = None,
        *,
        index: Any = None,
        columns: dict[str, str] | None = None,
        axis: Any = None,
        inplace: bool = False,
        level: Any = None,
        errors: str = "ignore",
    ) -> CoreDataFrame:
        if index is not None:
            raise NotImplementedError("rename(index=...) is not supported.")
        if axis is not None:
            raise NotImplementedError("rename(axis=...) is not supported.")
        if inplace:
            raise NotImplementedError("rename(inplace=True) is not supported.")
        if level is not None:
            raise NotImplementedError("rename(level=...) is not supported.")
        if mapper is not None and columns is not None:
            raise TypeError("rename() specify mapper or columns, not both.")
        mapping = mapper if columns is None else columns
        if mapping is None:
            raise TypeError("rename() requires columns mapping.")
        if not isinstance(mapping, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in mapping.items()
        ):
            raise TypeError("rename(columns=...) expects dict[str, str].")
        if errors not in {"raise", "ignore"}:
            raise ValueError("rename(errors=...) must be 'raise' or 'ignore'.")
        missing = [k for k in mapping if k not in self.schema_fields()]
        if missing and errors == "raise":
            raise KeyError(f"rename(): columns not found: {missing}")
        mapping2 = {k: v for k, v in mapping.items() if k in self.schema_fields()}
        return super().rename(mapping2) if mapping2 else self

    def fillna(
        self,
        value: Any = None,
        *,
        method: str | None = None,
        axis: Any = None,
        inplace: bool = False,
        limit: int | None = None,
        downcast: Any = None,
        subset: str | list[str] | None = None,
    ) -> CoreDataFrame:
        if method is not None and value is not None:
            raise TypeError("fillna() accepts value or method, not both.")
        if axis is not None:
            raise NotImplementedError("fillna(axis=...) is not supported.")
        if inplace:
            raise NotImplementedError("fillna(inplace=True) is not supported.")
        if limit is not None:
            raise NotImplementedError("fillna(limit=...) is not supported.")
        if downcast is not None:
            raise NotImplementedError("fillna(downcast=...) is not supported.")
        cols = None
        if subset is not None:
            cols = [subset] if isinstance(subset, str) else list(subset)
        if method is not None:
            m = str(method).lower()
            if m == "ffill":
                return self.fill_null(strategy="forward", subset=cols)
            if m == "bfill":
                return self.fill_null(strategy="backward", subset=cols)
            raise NotImplementedError(
                "fillna(method=...) supports only 'ffill'/'bfill'."
            )
        if value is None:
            raise TypeError("fillna(value=...) requires a non-None value.")
        return self.fill_null(value=value, subset=cols)

    def astype(
        self, dtype: Any, *, copy: bool | None = None, errors: str = "raise"
    ) -> CoreDataFrame:
        """
        Pandas-like cast.

        Supports:
        - `astype(dtype)` for all columns
        - `astype({\"col\": dtype, ...})` per-column
        """
        if errors not in {"raise", "ignore"}:
            raise ValueError("astype(errors=...) must be 'raise' or 'ignore'.")
        _ = copy  # accepted for parity; logical frames are copy-free
        if isinstance(dtype, dict):
            mapping = dtype
        else:
            mapping = {name: dtype for name in self.schema_fields()}
        if not all(isinstance(k, str) for k in mapping):
            raise TypeError("astype() mapping keys must be column names (str).")
        missing = [k for k in mapping if k not in self.schema_fields()]
        if missing:
            raise KeyError(f"astype(): columns not found: {missing}")
        casts: dict[str, Expr] = {}
        if errors == "ignore":
            # Typed-first, best-effort: only apply casts we can deem safe without
            # risking engine errors (primarily numeric widening). Others are skipped.
            for name, dt in mapping.items():
                cur = self.schema_fields().get(name)
                if (
                    dt in (float, int)
                    and (
                        cur in (int, float)
                        or str(cur).startswith("int |")
                        or str(cur).startswith("float |")
                    )
                ) or (dt is bool and (cur is bool or str(cur).startswith("bool |"))):
                    casts[name] = self.col(name).cast(dt)
                else:
                    # Skip cast (keep original) for ignore-mode.
                    continue
        else:
            for name, dt in mapping.items():
                casts[name] = self.col(name).cast(dt)
        return self.with_columns(**casts) if casts else self

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

    def first(self, *columns: str) -> CoreDataFrame:
        if not columns:
            raise TypeError("first() requires at least one column name.")
        return self.agg(
            streaming=None,
            **{f"{c}_first": ("first", c) for c in columns},
        )

    def last(self, *columns: str) -> CoreDataFrame:
        if not columns:
            raise TypeError("last() requires at least one column name.")
        return self.agg(
            streaming=None,
            **{f"{c}_last": ("last", c) for c in columns},
        )

    def median(self, *columns: str) -> CoreDataFrame:
        if not columns:
            raise TypeError("median() requires at least one column name.")
        return self.agg(
            streaming=None,
            **{f"{c}_median": ("median", c) for c in columns},
        )

    def std(self, *columns: str) -> CoreDataFrame:
        if not columns:
            raise TypeError("std() requires at least one column name.")
        return self.agg(
            streaming=None,
            **{f"{c}_std": ("std", c) for c in columns},
        )

    def var(self, *columns: str) -> CoreDataFrame:
        if not columns:
            raise TypeError("var() requires at least one column name.")
        return self.agg(
            streaming=None,
            **{f"{c}_var": ("var", c) for c in columns},
        )

    def agg_multi(self, **spec: list[str]) -> CoreDataFrame:
        """
        Expand per-column op lists into `agg()` specs.

        Example: `agg_multi(v=[\"sum\",\"mean\"])` -> `agg(v_sum=(\"sum\",\"v\"),`
        `v_mean=(\"mean\",\"v\"))`.
        """
        expanded: dict[str, tuple[str, str]] = {}
        for col, ops in spec.items():
            if not isinstance(col, str) or not col:
                raise TypeError("agg_multi() expects column names as keywords.")
            if (
                not isinstance(ops, list)
                or not ops
                or not all(isinstance(o, str) for o in ops)
            ):
                raise TypeError("agg_multi() expects list[str] ops per column.")
            for op in ops:
                expanded[f"{col}_{op}"] = (op, col)
        return self.agg(streaming=None, **expanded)


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

    def sort_values(self, by: str | list[str], **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.sort_values(by, **kwargs))

    def drop(self, *args: Any, **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.drop(*args, **kwargs))

    def rename(self, *args: Any, **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.rename(*args, **kwargs))

    def fillna(self, *args: Any, **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.fillna(*args, **kwargs))

    def astype(self, *args: Any, **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.astype(*args, **kwargs))

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

    def first(self, *columns: str) -> CoreDataFrameModel:
        return self._model_type._from_dataframe(self._grouped_df.first(*columns))

    def last(self, *columns: str) -> CoreDataFrameModel:
        return self._model_type._from_dataframe(self._grouped_df.last(*columns))

    def median(self, *columns: str) -> CoreDataFrameModel:
        return self._model_type._from_dataframe(self._grouped_df.median(*columns))

    def std(self, *columns: str) -> CoreDataFrameModel:
        return self._model_type._from_dataframe(self._grouped_df.std(*columns))

    def var(self, *columns: str) -> CoreDataFrameModel:
        return self._model_type._from_dataframe(self._grouped_df.var(*columns))

    def agg_multi(self, **spec: list[str]) -> CoreDataFrameModel:
        return self._model_type._from_dataframe(self._grouped_df.agg_multi(**spec))


class DataFrame(PandasDataFrame):
    """Default export: pandas-flavored typed ``DataFrame``."""


class DataFrameModel(PandasDataFrameModel):
    _dataframe_cls = DataFrame


__all__ = ["DataFrame", "DataFrameModel", "Expr", "Schema"]
