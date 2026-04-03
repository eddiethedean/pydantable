"""pandas-like method names on the core :class:`DataFrame` and :class:`DataFrameModel`.

``merge``/``assign``/``query`` mirror familiar pandas entry points where supported;
execution remains the Rust engine. Import ``DataFrame`` from this module for the
pandas-shaped API.
"""

from __future__ import annotations

import ast
import random
import re
from collections.abc import Mapping, Sequence
from typing import Any

from pydantic import create_model
from typing_extensions import Self

from .dataframe import DataFrame as CoreDataFrame
from .dataframe import GroupedDataFrame as CoreGroupedDataFrame
from .dataframe_model import DataFrameModel as CoreDataFrameModel
from .dataframe_model import GroupedDataFrameModel as CoreGroupedDataFrameModel
from .expressions import ColumnRef, Expr, Literal, coalesce, dense_rank, rank, when
from pydantable.engine import get_default_engine
from .schema import Schema
from .schema._impl import make_derived_schema_type, schema_field_types
from .selectors import Selector
from .window_spec import WindowSpec


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


def _row_subset_from_lists(
    data: dict[str, list[Any]], indices: list[int]
) -> dict[str, list[Any]]:
    return {c: [data[c][i] for i in indices] for c in data}


def _rows_to_column_dict(
    rows: list[dict[str, Any]], *, columns: list[str]
) -> dict[str, list[Any]]:
    """Build columnar buffers from row dicts in a fixed column order."""
    if not rows:
        return {c: [] for c in columns}
    return {k: [r.get(k) for r in rows] for k in columns}


def _sanitize_dummy_level(v: Any) -> str:
    label = "nan" if v is None else str(v)
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", label).strip("_")
    return s if s else "x"


def _typing_numeric_name(ann: Any) -> bool:
    """Best-effort: treat int/float and Optional variants as numeric for corr/sample."""
    if ann in (int, float):
        return True
    origin = getattr(ann, "__origin__", None)
    args = getattr(ann, "__args__", ())
    if origin is type(None):
        return False
    if origin is not None and str(origin).endswith("Union"):
        non_none = [a for a in args if a is not type(None)]
        return len(non_none) == 1 and _typing_numeric_name(non_none[0])
    return False


def wide_to_long(
    df: CoreDataFrame,
    stubnames: str | list[str],
    i: str | list[str],
    j: str,
    *,
    sep: str = "_",
    suffix: str = r"\d+",
    value_name: str | None = None,
) -> CoreDataFrame:
    """Narrow ``wide_to_long`` for a **single** stub (see ``docs/PANDAS_UI.md``).

    Columns must match ``stub`` + ``sep`` + ``suffix`` (regex). Extra columns
    are treated as ``id_vars`` alongside ``i``.
    """
    stub_list = [stubnames] if isinstance(stubnames, str) else list(stubnames)
    if len(stub_list) != 1:
        raise NotImplementedError(
            "wide_to_long supports a single stub name (str or len-1 list); "
            "use melt() for other layouts."
        )
    stub = stub_list[0]
    id_cols = [i] if isinstance(i, str) else list(i)
    pat = re.compile(rf"^{re.escape(stub)}{re.escape(sep)}({suffix})$")
    matched: list[str] = []
    for c in df.schema_fields():
        if c in id_cols:
            continue
        if pat.match(c):
            matched.append(c)
    if not matched:
        raise ValueError(
            f"wide_to_long: no columns matched stub={stub!r} "
            f"sep={sep!r} suffix={suffix!r}."
        )
    vn = value_name if value_name is not None else stub
    melted = df.melt(
        id_vars=id_cols,
        value_vars=matched,
        var_name=j,
        value_name=vn,
    )
    pat_extract = rf"^{re.escape(stub)}{re.escape(sep)}({suffix})$"
    vj = melted.col(j)
    return melted.with_columns(**{j: vj.str_extract_regex(pat_extract, 1)})


class PandasDataFrame(CoreDataFrame):
    """``assign``, ``merge``, ``query``, ``columns``, ``shape``, and related."""

    @classmethod
    def concat(
        cls,
        objs: Sequence[CoreDataFrame],
        /,
        *,
        how: str | None = None,
        axis: int = 0,
        join: str = "outer",
        ignore_index: bool = False,
        keys: Any = None,
        levels: Any = None,
        names: Any = None,
        verify_integrity: Any = None,
        sort: Any = None,
        copy: Any = None,
        streaming: bool | None = None,
    ) -> CoreDataFrame:
        if join != "outer":
            raise NotImplementedError("concat(join=...) only supports join='outer'.")
        if ignore_index:
            raise NotImplementedError("concat(ignore_index=True) is not supported.")
        if keys is not None or levels is not None or names is not None:
            raise NotImplementedError("concat(keys/levels/names=...) is not supported.")
        if verify_integrity is not None:
            raise NotImplementedError("concat(verify_integrity=...) is not supported.")
        if sort is not None:
            raise NotImplementedError("concat(sort=...) is not supported.")
        if copy is not None:
            raise NotImplementedError("concat(copy=...) is not supported.")

        if how is not None:
            if how not in ("vertical", "horizontal"):
                raise ValueError(
                    "concat(how=...) must be 'vertical' or 'horizontal' "
                    f"(got {how!r}). Use axis=0|1 for pandas-style stacking."
                )
            how_final = how
        else:
            if axis not in (0, 1):
                raise ValueError("concat(axis=...) must be 0 or 1.")
            how_final = "vertical" if axis == 0 else "horizontal"
        return super().concat(objs, how=how_final, streaming=streaming)

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
        left_by: str | list[str] | None = None,
        right_by: str | list[str] | None = None,
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
        if left_by is not None or right_by is not None:
            raise NotImplementedError(
                "merge(left_by=..., right_by=...) is not supported."
            )
        if not isinstance(suffixes, tuple) or len(suffixes) != 2:
            raise TypeError(
                "merge(suffixes=...) must be a tuple[str, str] of length 2."
            )
        if not all(isinstance(s, str) for s in suffixes):
            raise TypeError("merge(suffixes=...) must be a tuple[str, str].")
        if suffixes == ("", ""):
            raise ValueError("merge(suffixes=...) cannot be ('', '').")
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
                rust_plan=get_default_engine().make_plan(left_fields),
            )
            right_df = other._from_plan(
                root_data=right_data2,
                root_schema_type=right_schema,
                current_schema_type=right_schema,
                rust_plan=get_default_engine().make_plan(right_fields),
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
                rust_plan=get_default_engine().make_plan(out_fields),
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
                        rust_plan=get_default_engine().make_plan(out_fields),
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
            if how in {"right", "outer", "full"}:
                fields2 = set(out.schema_fields())
                dupes: list[str] = []
                unify_keys: dict[str, Expr] = {}
                for k in on_list:
                    dupe = None
                    if f"{k}_right" in fields2:
                        dupe = f"{k}_right"
                    elif f"{k}{suffix}" in fields2:
                        dupe = f"{k}{suffix}"
                    if dupe is not None:
                        dupes.append(dupe)
                        unify_keys[k] = coalesce(out.col(k), out.col(dupe))
                if unify_keys:
                    out = out.with_columns(**unify_keys)
                if dupes:
                    out = out.drop(*dupes)  # type: ignore[arg-type]
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
            if isinstance(node, ast.Call):
                if not isinstance(node.func, ast.Name):
                    raise NotImplementedError(
                        "query(): only simple function calls are supported."
                    )
                fname = node.func.id
                if fname in {"isnull", "notnull", "isna", "notna"}:
                    if len(node.args) != 1 or node.keywords:
                        raise TypeError(
                            f"query(): {fname}() expects one positional argument."
                        )
                    target = _compile(node.args[0])
                    return (
                        target.is_null()
                        if fname in {"isnull", "isna"}
                        else target.is_not_null()
                    )
                if fname in {"contains", "startswith", "endswith"}:
                    if len(node.args) != 2 or node.keywords:
                        raise TypeError(
                            f"query(): {fname}() expects (column, string) "
                            "positional args."
                        )
                    col_expr = _compile(node.args[0])
                    if not isinstance(node.args[1], ast.Constant):
                        raise NotImplementedError(
                            f"query(): {fname}() requires a literal string."
                        )
                    sub = node.args[1].value
                    if not isinstance(sub, str):
                        raise TypeError(
                            f"query(): {fname}() requires a string literal."
                        )
                    if fname == "contains":
                        return col_expr.str_contains(sub)
                    if fname == "startswith":
                        return col_expr.starts_with(sub)
                    return col_expr.ends_with(sub)
                if fname == "between":
                    if len(node.args) != 3 or node.keywords:
                        raise TypeError(
                            "query(): between() expects (expr, low, high) "
                            "positional args."
                        )
                    target = _compile(node.args[0])
                    if isinstance(node.args[1], ast.Constant):
                        low = _compile(node.args[1])
                    elif isinstance(node.args[1], ast.Name):
                        if node.args[1].id in self.schema_fields():
                            raise NotImplementedError(
                                "query(): between() bounds must be literals or "
                                "local_dict/global_dict constants."
                            )
                        low = _compile(node.args[1])
                    else:
                        raise NotImplementedError(
                            "query(): between() bounds must be literals or "
                            "local_dict/global_dict constants."
                        )
                    if isinstance(node.args[2], ast.Constant):
                        high = _compile(node.args[2])
                    elif isinstance(node.args[2], ast.Name):
                        if node.args[2].id in self.schema_fields():
                            raise NotImplementedError(
                                "query(): between() bounds must be literals or "
                                "local_dict/global_dict constants."
                            )
                        high = _compile(node.args[2])
                    else:
                        raise NotImplementedError(
                            "query(): between() bounds must be literals or "
                            "local_dict/global_dict constants."
                        )
                    return (target >= low) & (target <= high)
                if fname in {"lower", "upper", "strip"}:
                    if len(node.args) != 1 or node.keywords:
                        raise TypeError(
                            f"query(): {fname}() expects one positional argument."
                        )
                    target = _compile(node.args[0])
                    if fname == "lower":
                        return target.lower()
                    if fname == "upper":
                        return target.upper()
                    return target.strip()
                if fname in {"len", "length"}:
                    if len(node.args) != 1 or node.keywords:
                        raise TypeError(
                            f"query(): {fname}() expects one positional argument."
                        )
                    target = _compile(node.args[0])
                    return target.char_length()
                raise NotImplementedError(
                    f"query(): unsupported function call {fname!r}."
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
        nl_flags: bool | list[bool] | None = None
        if na_position is not None:
            pos = str(na_position).lower()
            if pos not in {"first", "last"}:
                raise ValueError(
                    "sort_values(na_position=...) must be 'first' or 'last'."
                )
            nl_flags = pos == "last"
        if ignore_index:
            raise NotImplementedError(
                "sort_values(ignore_index=True) is not supported; "
                "pydantable has no pandas Index semantics."
            )
        key_id: str | None
        if key is None:
            key_id = None
        elif isinstance(key, str):
            key_id = key.strip().lower()
        else:
            raise NotImplementedError(
                "sort_values(key=...) only supports string identifiers (plan-only); "
                "Python callables are not supported."
            )
        by_list = [by] if isinstance(by, str) else list(by)
        if not by_list:
            raise TypeError("sort_values(by=...) requires at least one column.")
        if isinstance(ascending, bool):
            desc = [not ascending] * len(by_list)
        else:
            desc = [not bool(v) for v in list(ascending)]
            if len(desc) != len(by_list):
                raise ValueError("sort_values(): ascending must match len(by).")
        if key_id is None:
            return self.sort(*by_list, descending=desc, nulls_last=nl_flags)
        if key_id not in {"lower", "upper", "abs", "strip", "length", "len"}:
            raise NotImplementedError(
                f"sort_values(key={key!r}) is not supported; expected one of "
                "'lower', 'upper', 'abs', 'strip', 'length', 'len', or None."
            )
        tmp_cols: list[str] = []
        tmp_exprs: list[Expr] = []
        for c in by_list:
            tmp = f"__pd_sort_key_{key_id}__{c}"
            tmp_cols.append(tmp)
            base = self.col(c)
            if key_id == "abs":
                tmp_exprs.append(base.abs())
            elif key_id == "lower":
                tmp_exprs.append(base.lower())
            else:
                if key_id == "upper":
                    tmp_exprs.append(base.upper())
                elif key_id == "strip":
                    tmp_exprs.append(base.strip())
                else:
                    # length / len
                    tmp_exprs.append(base.char_length())
        tmp_df = self.with_columns(
            **{n: e for n, e in zip(tmp_cols, tmp_exprs, strict=True)}
        )
        sorted_df = tmp_df.sort(*tmp_cols, descending=desc, nulls_last=nl_flags)
        return CoreDataFrame.drop(sorted_df, *tmp_cols)

    def drop(self, *args: Any, **kwargs: Any) -> CoreDataFrame:
        allowed = frozenset(
            {"index", "columns", "axis", "inplace", "level", "errors", "labels"}
        )
        bad = set(kwargs) - allowed
        if bad:
            raise TypeError(f"drop() got unexpected keyword arguments: {sorted(bad)!r}")
        if not kwargs and args:
            return super().drop(*args)

        labels_kw = kwargs.get("labels")
        if args:
            if len(args) > 1:
                raise TypeError(
                    "drop() takes at most one positional argument when using "
                    "keyword arguments."
                )
            if labels_kw is not None:
                raise TypeError(
                    "drop() cannot specify both a labels positional and labels=."
                )
            labels = args[0]
        else:
            labels = labels_kw

        index = kwargs.get("index")
        columns = kwargs.get("columns")
        axis = kwargs.get("axis")
        inplace = kwargs.get("inplace", False)
        level = kwargs.get("level")
        errors = kwargs.get("errors", "raise")

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

    def rename(self, *args: Any, **kwargs: Any) -> CoreDataFrame:
        allowed = frozenset(
            {"index", "columns", "axis", "inplace", "level", "errors", "mapper"}
        )
        bad = set(kwargs) - allowed
        if bad:
            raise TypeError(
                f"rename() got unexpected keyword arguments: {sorted(bad)!r}"
            )
        if not kwargs and len(args) == 1 and isinstance(args[0], Mapping):
            return super().rename(args[0])

        mapper_kw = kwargs.get("mapper")
        if args:
            if len(args) > 1:
                raise TypeError(
                    "rename() takes at most one positional argument when using "
                    "keyword arguments."
                )
            if mapper_kw is not None:
                raise TypeError(
                    "rename() cannot specify both a mapper positional and mapper=."
                )
            mapper = args[0]
        else:
            mapper = mapper_kw

        index = kwargs.get("index")
        columns = kwargs.get("columns")
        axis = kwargs.get("axis")
        inplace = kwargs.get("inplace", False)
        level = kwargs.get("level")
        errors = kwargs.get("errors", "ignore")

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

    def to_pandas(self) -> Any:
        """
        Materialize this typed frame into a `pandas.DataFrame`.

        This is an eager convenience method.
        """
        try:
            import pandas as pd  # type: ignore[import-not-found]
        except Exception as e:  # pragma: no cover
            raise ModuleNotFoundError(
                "to_pandas() requires the optional 'pandas' dependency."
            ) from e
        data = self.collect(as_lists=True)
        cols = list(self.schema_fields().keys())
        return pd.DataFrame({c: data.get(c, []) for c in cols})

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
        rust = get_default_engine().make_plan(self.schema_fields())
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
        rust = get_default_engine().make_plan(self.schema_fields())
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

    class _ILoc:
        def __init__(self, df: PandasDataFrame):
            self._df = df

        def __getitem__(self, key: int | slice) -> CoreDataFrame:
            if isinstance(key, int):
                n = self._nrows_or_none()
                i = int(key)
                if i < 0:
                    if n is None:
                        raise NotImplementedError(
                            "iloc negative indices require in-memory root data."
                        )
                    i = n + i
                return self._df.slice(i, 1)
            if not isinstance(key, slice):
                raise TypeError("iloc[...] only supports int or slice selectors.")
            if key.step not in (None, 1):
                raise NotImplementedError("iloc slicing does not support step.")
            n = self._nrows_or_none()
            start = 0 if key.start is None else int(key.start)
            stop = None if key.stop is None else int(key.stop)
            if start < 0:
                if n is None:
                    raise NotImplementedError(
                        "iloc negative slices require in-memory root data."
                    )
                start = n + start
            if stop is None:
                if n is None:
                    raise NotImplementedError(
                        "iloc open-ended slices require in-memory root data."
                    )
                stop = n
            if stop < 0:
                if n is None:
                    raise NotImplementedError(
                        "iloc negative slices require in-memory root data."
                    )
                stop = n + stop
            if stop < start:
                return self._df.slice(0, 0)
            return self._df.slice(start, stop - start)

        def _nrows_or_none(self) -> int | None:
            data = getattr(self._df, "_root_data", None)
            if not isinstance(data, dict) or not data:
                return None
            first = next(iter(data.values()))
            return len(first)

    @property
    def iloc(self) -> _ILoc:
        return PandasDataFrame._ILoc(self)

    class _Loc:
        def __init__(self, df: PandasDataFrame):
            self._df = df

        def __getitem__(self, key: object) -> CoreDataFrame:
            if not isinstance(key, tuple) or len(key) != 2:
                raise TypeError("loc[...] expects a 2-tuple: (rows, cols).")
            row_sel, col_sel = key
            df: CoreDataFrame = self._df
            if isinstance(row_sel, slice) and row_sel == slice(None, None, None):
                pass
            elif isinstance(row_sel, Expr):
                df = df.filter(row_sel)
            else:
                raise NotImplementedError(
                    "loc row selection supports ':' or an Expr mask only."
                )
            if col_sel is None or col_sel == slice(None, None, None):
                return df
            if isinstance(col_sel, str):
                return df.select(col_sel)
            if (
                isinstance(col_sel, list)
                and col_sel
                and all(isinstance(c, str) for c in col_sel)
            ):
                return df.select(*col_sel)
            raise NotImplementedError(
                "loc column selection supports str or non-empty list[str] only."
            )

    @property
    def loc(self) -> _Loc:
        return PandasDataFrame._Loc(self)

    def group_by(
        self,
        *keys: Any,
        maintain_order: bool = False,
        drop_nulls: bool = True,
        dropna: Any = None,
        as_index: Any = None,
        sort: Any = None,
        observed: Any = None,
    ) -> PandasGroupedDataFrame:
        if dropna is not None:
            raise NotImplementedError("group_by(dropna=...) is not supported.")
        if as_index is not None:
            raise NotImplementedError("group_by(as_index=...) is not supported.")
        if sort is not None:
            raise NotImplementedError("group_by(sort=...) is not supported.")
        if observed is not None:
            raise NotImplementedError("group_by(observed=...) is not supported.")
        inner = super().group_by(
            *keys, maintain_order=maintain_order, drop_nulls=drop_nulls
        )
        return PandasGroupedDataFrame(inner._df, inner._keys)

    def drop_duplicates(
        self,
        subset: str | list[str] | None = None,
        *,
        keep: str | bool = "first",
        inplace: bool = False,
        ignore_index: bool = False,
    ) -> CoreDataFrame:
        if inplace:
            raise NotImplementedError("drop_duplicates(inplace=True) is not supported.")
        if ignore_index:
            raise NotImplementedError(
                "drop_duplicates(ignore_index=True) is not supported."
            )
        if keep is False:
            if subset is None:
                subset_cols = None
            elif isinstance(subset, str):
                subset_cols = [subset]
            elif (
                isinstance(subset, list)
                and subset
                and all(isinstance(c, str) for c in subset)
            ):
                subset_cols = subset
            else:
                raise TypeError(
                    "drop_duplicates(subset=...) must be a column name, "
                    "non-empty list[str], or None."
                )
            return self.drop_duplicate_groups(subset=subset_cols)
        if keep not in ("first", "last"):
            raise ValueError(
                "drop_duplicates(keep=...) must be 'first', 'last', or False."
            )
        if subset is None:
            subset_cols = None
        elif isinstance(subset, str):
            subset_cols = [subset]
        elif (
            isinstance(subset, list)
            and subset
            and all(isinstance(c, str) for c in subset)
        ):
            subset_cols = subset
        else:
            raise TypeError(
                "drop_duplicates(subset=...) must be a column name, "
                "non-empty list[str], or None."
            )
        return self.unique(subset=subset_cols, keep=keep)

    def duplicated(
        self,
        subset: Sequence[str] | None = None,
        *,
        keep: str | bool = "first",
    ) -> CoreDataFrame:
        if subset is None:
            subset_cols = None
        elif isinstance(subset, str):
            subset_cols = [subset]
        elif (
            isinstance(subset, Sequence)
            and not isinstance(subset, (str, bytes))
            and len(subset) > 0
            and all(isinstance(c, str) for c in subset)
        ):
            subset_cols = list(subset)
        else:
            raise TypeError(
                "duplicated(subset=...) must be a column name, "
                "non-empty list[str], or None."
            )
        return super().duplicated(subset=subset_cols, keep=keep)

    def isna(self) -> CoreDataFrame:
        cols = list(self.schema_fields().keys())
        return self.with_columns(**{c: self.col(c).is_null() for c in cols})

    def isnull(self) -> CoreDataFrame:
        return self.isna()

    def notna(self) -> CoreDataFrame:
        cols = list(self.schema_fields().keys())
        return self.with_columns(**{c: self.col(c).is_not_null() for c in cols})

    def notnull(self) -> CoreDataFrame:
        return self.notna()

    def dropna(
        self,
        *,
        axis: int = 0,
        how: str = "any",
        subset: str | list[str] | None = None,
        inplace: Any = None,
        thresh: Any = None,
    ) -> CoreDataFrame:
        if axis != 0:
            raise NotImplementedError("dropna(axis=1) is not supported.")
        if inplace is not None:
            raise NotImplementedError("dropna(inplace=...) is not supported.")
        if thresh is not None:
            raise NotImplementedError("dropna(thresh=...) is not supported.")
        if how not in ("any", "all"):
            raise ValueError("dropna(how=...) must be 'any' or 'all'.")
        if subset is None:
            subset_cols = list(self.schema_fields().keys())
        elif isinstance(subset, str):
            subset_cols = [subset]
        elif (
            isinstance(subset, list)
            and subset
            and all(isinstance(c, str) for c in subset)
        ):
            subset_cols = subset
        else:
            raise TypeError(
                "dropna(subset=...) must be a column name or non-empty list[str]."
            )
        if how == "any":
            return self.drop_nulls(subset=subset_cols)

        cond: Expr | None = None
        for c in subset_cols:
            e = self.col(c).is_not_null()
            cond = e if cond is None else (cond | e)
        if cond is None:
            return self
        return self.filter(cond)

    def get_dummies(
        self,
        columns: list[str],
        *,
        prefix: str | Mapping[str, str] | None = None,
        prefix_sep: str = "_",
        drop_first: bool = False,
        dummy_na: bool = False,
        dtype: str = "bool",
        max_categories: int = 512,
    ) -> CoreDataFrame:
        """One-hot encode named columns; other columns are kept. Eager category scan."""
        if not columns or not all(isinstance(c, str) for c in columns):
            raise TypeError("get_dummies(columns=...) expects a non-empty list[str].")
        if dtype not in ("bool", "int"):
            raise ValueError("get_dummies(dtype=...) must be 'bool' or 'int'.")
        fields = list(self.schema_fields().keys())
        for c in columns:
            if c not in fields:
                raise KeyError(c)
        if isinstance(prefix, str):
            prefixes = {c: prefix for c in columns}
        elif prefix is None:
            prefixes = {c: c for c in columns}
        else:
            prefixes = {c: prefix.get(c, c) for c in columns}
        keep = [c for c in fields if c not in columns]
        sample = self.select(*columns).collect(as_lists=True)
        updates: dict[str, Any] = {}
        for c in columns:
            series = sample[c]
            raw_vals = list(series)
            distinct: list[Any] = []
            seen: set[Any] = set()
            for v in raw_vals:
                if v is None and not dummy_na:
                    continue
                key = v
                if key in seen:
                    continue
                seen.add(key)
                distinct.append(v)
            distinct.sort(key=lambda v: (str(type(v).__name__), str(v)))
            if len(distinct) > max_categories:
                raise ValueError(
                    f"get_dummies: column {c!r} has {len(distinct)} distinct values "
                    f"(max_categories={max_categories})."
                )
            to_encode = distinct[1:] if drop_first else distinct
            p = prefixes[c]
            for v in to_encode:
                safe = _sanitize_dummy_level(v)
                out_name = f"{p}{prefix_sep}{safe}"
                if out_name in keep or out_name in updates:
                    raise ValueError(
                        f"get_dummies: output column name {out_name!r} "
                        "collides with an existing or other dummy column."
                    )
                if v is None:
                    expr: Expr = self.col(c).is_null()
                else:
                    expr = self.col(c) == Literal(value=v)
                if dtype == "int":
                    expr = when(expr, Literal(value=1)).otherwise(Literal(value=0))
                updates[out_name] = expr
        out = self.with_columns(**updates).drop(*columns)
        return out

    def pivot(
        self,
        *,
        index: str | Sequence[str] | Selector,
        columns: str | Selector | ColumnRef,
        values: str | Sequence[str] | Selector,
        aggregate_function: str = "first",
        pivot_values: Sequence[Any] | None = None,
        sort_columns: bool = False,
        separator: str = "_",
        streaming: bool | None = None,
    ) -> CoreDataFrame:
        """Typed :meth:`~pydantable.dataframe.DataFrame.pivot`.

        Not pandas' unconstrained dynamic pivot.
        """
        return super().pivot(
            index=index,
            columns=columns,
            values=values,
            aggregate_function=aggregate_function,
            pivot_values=pivot_values,
            sort_columns=sort_columns,
            separator=separator,
            streaming=streaming,
        )

    def factorize_column(self, column: str) -> tuple[list[int], list[Any]]:
        """Eager ``(codes, categories)`` using pandas :func:`factorize` semantics."""
        pd = __import__("pandas")
        data = self.collect(as_lists=True)
        if column not in data:
            raise KeyError(column)
        codes, uniques = pd.factorize(pd.Series(data[column]), use_na_sentinel=True)
        return list(codes), list(uniques)

    def cut(
        self,
        column: str,
        bins: Any,
        *,
        new_column: str | None = None,
        labels: Any = None,
        right: bool = True,
        include_lowest: bool = False,
        duplicates: str = "raise",
    ) -> CoreDataFrame:
        """Eager binning via pandas :func:`cut`; adds a string interval column."""
        pd = __import__("pandas")
        data = self.collect(as_lists=True)
        if column not in data:
            raise KeyError(column)
        ser = pd.Series(data[column])
        cats = pd.cut(
            ser,
            bins,
            labels=labels,
            right=right,
            include_lowest=include_lowest,
            duplicates=duplicates,
        )
        nc = new_column or f"{column}_cut"

        def _cell(x: Any) -> str | None:
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            return str(x)

        merged = {**data, nc: [_cell(x) for x in cats]}
        ft = dict(self._current_field_types)
        ft[nc] = str | None
        dyn = make_derived_schema_type(self._current_schema_type, ft)
        return DataFrame[dyn](merged)

    def qcut(
        self,
        column: str,
        q: Any,
        *,
        new_column: str | None = None,
        duplicates: str = "raise",
    ) -> CoreDataFrame:
        """Eager quantile bins via pandas :func:`qcut`."""
        pd = __import__("pandas")
        data = self.collect(as_lists=True)
        if column not in data:
            raise KeyError(column)
        ser = pd.Series(data[column])
        cats = pd.qcut(ser, q, duplicates=duplicates)
        nc = new_column or f"{column}_qcut"

        def _cell(x: Any) -> str | None:
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            return str(x)

        merged = {**data, nc: [_cell(x) for x in cats]}
        ft = dict(self._current_field_types)
        ft[nc] = str | None
        dyn = make_derived_schema_type(self._current_schema_type, ft)
        return DataFrame[dyn](merged)

    def melt(
        self,
        *,
        id_vars: str | Sequence[str] | Selector | None = None,
        value_vars: str | Sequence[str] | Selector | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
        streaming: bool | None = None,
        var_name: str | None = None,
    ) -> CoreDataFrame:
        if var_name is not None and variable_name != "variable":
            raise TypeError("melt(): pass only one of variable_name and var_name.")
        eff_variable = variable_name if var_name is None else var_name

        if isinstance(id_vars, Selector):
            id_norm: Any = id_vars
        elif id_vars is not None:
            if isinstance(id_vars, str):
                id_norm = [id_vars]
            elif (
                isinstance(id_vars, Sequence)
                and not isinstance(id_vars, (str, bytes))
                and len(id_vars) > 0
                and all(isinstance(c, str) for c in id_vars)
            ):
                id_norm = list(id_vars)
            else:
                raise TypeError(
                    "melt(id_vars=...) must be a column name or non-empty list[str]."
                )
        else:
            id_norm = None

        if isinstance(value_vars, Selector):
            val_norm: Any = value_vars
        elif value_vars is None:
            val_norm = None
        elif isinstance(value_vars, str):
            val_norm = [value_vars]
        elif (
            isinstance(value_vars, Sequence)
            and not isinstance(value_vars, (str, bytes))
            and len(value_vars) > 0
            and all(isinstance(c, str) for c in value_vars)
        ):
            val_norm = list(value_vars)
        else:
            raise TypeError(
                "melt(value_vars=...) must be a column name, non-empty list[str], or "
                "None."
            )

        return super().melt(
            id_vars=id_norm,
            value_vars=val_norm,
            variable_name=eff_variable,
            value_name=value_name,
            streaming=streaming,
        )

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any] | list[dict[str, Any]],
        orient: str = "columns",
        *,
        columns: list[str] | None = None,
    ) -> Any:
        if cls._schema_type is None:
            raise TypeError(
                "from_dict() requires a typed frame class such as "
                "DataFrame[MySchema].from_dict(...)."
            )
        o = orient.lower().strip()
        if o in ("columns", "list"):
            if not isinstance(data, Mapping):
                raise TypeError(
                    "from_dict(orient='columns') expects a mapping of column -> values."
                )
            return cls({str(k): v for k, v in data.items()})
        field_cols = list(schema_field_types(cls._schema_type).keys())
        if o == "index":
            if not isinstance(data, Mapping):
                raise TypeError(
                    "from_dict(orient='index') expects dict[row_key, dict[col, val]]."
                )
            rows: list[dict[str, Any]] = []
            for row in data.values():
                if not isinstance(row, Mapping):
                    raise TypeError(
                        "from_dict(orient='index') values must be column dicts."
                    )
                rows.append({str(k): v for k, v in row.items()})
            use_cols = list(columns) if columns is not None else field_cols
            if columns is not None:
                rows = [{k: r.get(k) for k in use_cols} for r in rows]
            return cls(_rows_to_column_dict(rows, columns=use_cols))
        if o == "records":
            if not isinstance(data, list):
                raise TypeError("from_dict(orient='records') expects a list[dict].")
            rows_rec = [dict(r) for r in data]
            return cls(_rows_to_column_dict(rows_rec, columns=field_cols))
        raise ValueError(f"from_dict(orient=...) got unsupported value {orient!r}.")

    def wide_to_long(
        self,
        stubnames: str | list[str],
        i: str | list[str],
        j: str,
        *,
        sep: str = "_",
        suffix: str = r"\d+",
        value_name: str | None = None,
    ) -> CoreDataFrame:
        return wide_to_long(
            self,
            stubnames,
            i,
            j,
            sep=sep,
            suffix=suffix,
            value_name=value_name,
        )

    def stack(
        self,
        *,
        id_vars: str | list[str],
        value_vars: str | list[str] | None = None,
        var_name: str = "variable",
        value_name: str = "value",
    ) -> CoreDataFrame:
        """Narrow stack: typed :meth:`melt` alias (no pandas MultiIndex)."""
        return self.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name,
        )

    def unstack(
        self,
        *,
        index: str | list[str],
        columns: str,
        values: str | list[str],
        aggregate_function: str = "first",
        streaming: bool | None = None,
    ) -> CoreDataFrame:
        """Narrow unstack to typed :meth:`~pydantable.dataframe.DataFrame.pivot`."""
        return super().pivot(
            index=index,
            columns=columns,
            values=values,
            aggregate_function=aggregate_function,
            streaming=streaming,
        )

    def where(self, cond: Expr, other: Any | None = None) -> CoreDataFrame:
        if not isinstance(cond, Expr):
            raise TypeError("where(cond=...) expects an Expr boolean condition.")
        if other is None:
            oth: Expr = Literal(value=None)
        elif isinstance(other, Expr):
            oth = other
        else:
            oth = Literal(value=other)
        cols = list(self.schema_fields().keys())
        return self.with_columns(
            **{c: when(cond, self.col(c)).otherwise(oth) for c in cols}
        )

    def mask(self, cond: Expr, other: Any | None = None) -> CoreDataFrame:
        if not isinstance(cond, Expr):
            raise TypeError("mask(cond=...) expects an Expr boolean condition.")
        return self.where(~cond, other)

    def rank(
        self,
        *,
        axis: int = 0,
        method: str = "average",
        ascending: bool = True,
        na_option: str = "keep",
        pct: bool = False,
    ) -> CoreDataFrame:
        if axis != 0:
            raise NotImplementedError("rank(axis=1) is not supported.")
        if na_option != "keep":
            raise NotImplementedError("rank(na_option=...) only supports 'keep'.")
        if pct:
            raise NotImplementedError("rank(pct=True) is not supported.")
        m = method.lower().strip()
        if m not in ("average", "min", "max", "dense", "first"):
            raise ValueError(
                "rank(method=...) supports 'average', 'min', 'max', 'dense', 'first'."
            )
        if m in ("min", "max", "first"):
            raise NotImplementedError(
                f"rank(method={method!r}) is not implemented; use 'average' or 'dense'."
            )
        fn = dense_rank() if m == "dense" else rank()
        updates: dict[str, Any] = {}
        for c in self.schema_fields():
            spec = WindowSpec(
                partition_by=tuple(),
                order_by=((c, ascending, False),),
            )
            updates[c] = fn.over(spec)
        return self.with_columns(**updates)

    def sample(
        self,
        n: int | None = None,
        frac: float | None = None,
        *,
        fraction: float | None = None,
        seed: int | None = None,
        with_replacement: bool = False,
        replace: bool = False,
        random_state: int | None = None,
        axis: Any = 0,
    ) -> CoreDataFrame:
        if axis not in (0, "index", None):
            if axis == 1:
                raise NotImplementedError("sample(axis=1) is not supported.")
            raise ValueError("sample(axis=...) must be 0 or 'index'.")
        if with_replacement:
            raise NotImplementedError("sample(with_replacement=True) is not supported.")
        if replace:
            raise NotImplementedError("sample(replace=True) is not supported.")
        eff_frac = frac if fraction is None else fraction
        eff_seed = random_state if seed is None else seed
        if n is None and eff_frac is None:
            raise TypeError("sample requires n=... or frac=... or fraction=....")
        data = self.collect(as_lists=True)
        nrow = len(next(iter(data.values()))) if data else 0
        if nrow == 0:
            return self
        rng = random.Random(eff_seed)
        k = round(float(eff_frac) * nrow) if eff_frac is not None else int(n or 0)
        k = max(0, min(int(k), nrow))
        idx = rng.sample(range(nrow), k=k)
        sub = _row_subset_from_lists(data, idx)
        fields = self.schema_fields()
        return self._from_plan(
            root_data=sub,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=get_default_engine().make_plan(fields),
        )

    def take(self, indices):  # type: ignore[no-untyped-def]
        if not isinstance(indices, (list, tuple)):
            raise TypeError("take(indices=...) expects a list or tuple of ints.")
        idx = [int(i) for i in indices]
        data = self.collect(as_lists=True)
        nrow = len(next(iter(data.values()))) if data else 0
        norm: list[int] = []
        for i in idx:
            j = i + nrow if i < 0 else i
            if j < 0 or j >= nrow:
                raise IndexError(f"take(): index {i} out of range for {nrow} rows.")
            norm.append(j)
        sub = _row_subset_from_lists(data, norm)
        fields = self.schema_fields()
        return self._from_plan(
            root_data=sub,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=get_default_engine().make_plan(fields),
        )

    def sort_index(self, *args: Any, **kwargs: Any) -> CoreDataFrame:
        if args:
            raise TypeError("sort_index() keyword-only (by=...) for index columns.")
        by = kwargs.pop("by", None)
        level = kwargs.pop("level", None)
        ascending = kwargs.pop("ascending", True)
        axis = kwargs.pop("axis", 0)
        kind = kwargs.pop("kind", None)
        na_position = kwargs.pop("na_position", None)
        ignore_index = kwargs.pop("ignore_index", None)
        key = kwargs.pop("key", None)
        if kwargs:
            raise TypeError(
                f"sort_index() got unexpected keyword arguments: {sorted(kwargs)!r}"
            )
        if by is not None and level is not None:
            raise TypeError("sort_index(): pass only one of by=... or level=....")
        if axis not in (0, "index"):
            raise NotImplementedError(
                "sort_index(axis=1) is not supported; use column names as key fields."
            )
        if kind is not None:
            raise NotImplementedError("sort_index(kind=...) is not supported.")
        if ignore_index:
            raise NotImplementedError("sort_index(ignore_index=...) is not supported.")
        cols = by if by is not None else level
        if cols is None:
            raise NotImplementedError(
                "sort_index requires by=[...] or level=[...] naming key column(s); "
                "pydantable does not store a pandas Index."
            )
        return self.sort_values(
            by=cols,
            ascending=ascending,
            na_position=na_position,
            key=key,
        )

    def combine_first(self, other: CoreDataFrame, *, on: list[str]) -> CoreDataFrame:
        keys = list(on)
        merged = self.merge(other, on=keys, how="outer", suffixes=("", "_other"))
        others = [n for n in merged.schema_fields() if n.endswith("_other")]
        updates: dict[str, Any] = {}
        for c in self.schema_fields():
            if c in keys:
                continue
            oc = f"{c}_other"
            if oc in merged.schema_fields():
                updates[c] = coalesce(merged.col(c), merged.col(oc))
        out = merged
        if updates:
            out = out.with_columns(**updates)
        if others:
            out = out.drop(*others)  # type: ignore[arg-type]
        return out

    def update(self, other: CoreDataFrame, *, on: list[str]) -> CoreDataFrame:
        keys = list(on)
        merged = self.merge(other, on=keys, how="left", suffixes=("", "_upd"))
        upd_cols = [n for n in merged.schema_fields() if n.endswith("_upd")]
        updates: dict[str, Any] = {}
        for c in self.schema_fields():
            if c in keys:
                continue
            uc = f"{c}_upd"
            if uc in merged.schema_fields():
                updates[c] = coalesce(merged.col(uc), merged.col(c))
        out = merged
        if updates:
            out = out.with_columns(**updates)
        if upd_cols:
            out = out.drop(*upd_cols)  # type: ignore[arg-type]
        return out

    def compare(
        self, other: CoreDataFrame, *, rtol: float = 1e-5, atol: float = 0.0
    ) -> CoreDataFrame:
        _ = rtol, atol
        if set(self.schema_fields()) != set(other.schema_fields()):
            raise ValueError(
                "compare() requires both frames to share the same columns."
            )
        a = self.collect(as_lists=True)
        b = other.collect(as_lists=True)
        n = len(next(iter(a.values()))) if a else 0
        m = len(next(iter(b.values()))) if b else 0
        if n != m:
            raise ValueError("compare() requires the same row count after collect().")
        cols = list(self.schema_fields().keys())
        diff_cols: dict[str, list[bool]] = {}
        for c in cols:
            diff_cols[f"{c}_diff"] = []
            for i in range(n):
                va, vb = a[c][i], b[c][i]
                diff_cols[f"{c}_diff"].append(va != vb)
        dyn = create_model("_CompareOut", **{k: (bool, ...) for k in diff_cols})
        return DataFrame[dyn](diff_cols)

    def corr(self, method: str = "pearson", min_periods: int = 1):  # type: ignore[no-untyped-def]
        _ = min_periods
        if method != "pearson":
            raise NotImplementedError("corr(method=...) only supports 'pearson'.")
        cols = [
            n for n, a in self._current_field_types.items() if _typing_numeric_name(a)
        ]
        if len(cols) < 2:
            raise ValueError("corr() needs at least two numeric columns in the schema.")
        import numpy as np

        data = self.select(*cols).collect(as_lists=True)
        n = len(next(iter(data.values())))
        rows: list[list[float]] = []
        for i in range(n):
            row: list[float] = []
            for c in cols:
                v = data[c][i]
                row.append(float(v) if v is not None else float("nan"))
            rows.append(row)
        arr = np.asarray(rows, dtype=float)
        cm = np.corrcoef(arr, rowvar=False)
        out = {
            cols[i]: [float(x) if np.isfinite(x) else None for x in cm[i]]
            for i in range(len(cols))
        }
        dyn = create_model(
            "_CorrOut",
            **{c: (float | None, None) for c in cols},  # type: ignore[misc]
        )
        return DataFrame[dyn](out)

    def cov(self, min_periods: int = 1):  # type: ignore[no-untyped-def]
        _ = min_periods
        cols = [
            n for n, a in self._current_field_types.items() if _typing_numeric_name(a)
        ]
        if len(cols) < 2:
            raise ValueError("cov() needs at least two numeric columns in the schema.")
        import numpy as np

        data = self.select(*cols).collect(as_lists=True)
        n = len(next(iter(data.values())))
        rows: list[list[float]] = []
        for i in range(n):
            row = [
                float(data[c][i]) if data[c][i] is not None else float("nan")
                for c in cols
            ]
            rows.append(row)
        arr = np.asarray(rows, dtype=float)
        cov_m = np.cov(arr, rowvar=False)
        out = {
            cols[i]: [float(x) if np.isfinite(x) else None for x in cov_m[i]]
            for i in range(len(cols))
        }
        dyn = create_model("_CovOut", **{c: (float | None, None) for c in cols})  # type: ignore[misc]
        return DataFrame[dyn](out)

    def reindex(
        self, other: CoreDataFrame, *, on: str | list[str], **join_kw: Any
    ) -> CoreDataFrame:
        keys = [on] if isinstance(on, str) else list(on)
        bad = set(join_kw) - {"how", "suffix", "streaming"}
        if bad:
            raise TypeError(
                f"reindex() got unexpected keyword arguments: {sorted(bad)!r}"
            )
        return other.select(*keys).join(
            self,
            on=keys,
            how=str(join_kw.get("how", "left")),
            suffix=str(join_kw.get("suffix", "_right")),
            streaming=join_kw.get("streaming"),
        )

    def reindex_like(self, other: CoreDataFrame, **join_kw: Any) -> CoreDataFrame:
        keys = list(other.schema_fields().keys())
        if not keys:
            raise ValueError("reindex_like(other): other has no columns.")
        return self.reindex(other, on=keys, **join_kw)

    def align(
        self, other: CoreDataFrame, *, on: list[str], join: str = "outer"
    ) -> tuple[CoreDataFrame, CoreDataFrame]:
        if join not in ("outer", "inner", "left", "right"):
            raise ValueError("align(join=...) must be outer, inner, left, or right.")
        keys_l = self.select(*on).unique(subset=list(on))
        keys_r = other.select(*on).unique(subset=list(on))
        all_keys = keys_l.merge(keys_r, on=on, how=join)
        left = all_keys.join(self, on=on, how="left")
        right = all_keys.join(other, on=on, how="left")
        return left, right

    def set_index(
        self,
        keys: str | list[str],
        *,
        drop: bool = True,
        append: bool = False,
        inplace: bool = False,
    ) -> CoreDataFrame:
        if inplace:
            raise NotImplementedError("set_index(inplace=True) is not supported.")
        if append:
            raise NotImplementedError("set_index(append=True) is not supported.")
        ks = [keys] if isinstance(keys, str) else list(keys)
        for c in ks:
            if c not in self.schema_fields():
                raise KeyError(c)
        rest = [c for c in self.schema_fields() if c not in ks]
        _ = drop
        return self.select(*(ks + rest))

    def reset_index(
        self,
        level: Any = None,
        *,
        drop: bool = False,
        inplace: bool = False,
    ) -> CoreDataFrame:
        if inplace or level is not None:
            raise NotImplementedError(
                "reset_index(inplace=...) / level=... are not supported; "
                "there is no row Index object to drop."
            )
        _ = drop
        return self

    def eval(
        self, expr: str, *, local_dict: Any = None, global_dict: Any = None, **kw: Any
    ) -> CoreDataFrame:
        if kw:
            raise TypeError(f"eval() got unexpected keyword arguments: {sorted(kw)!r}")
        return self.query(expr, local_dict=local_dict, global_dict=global_dict)

    @property
    def T(self) -> CoreDataFrame:
        return self.transpose()

    def transpose(self, *args: Any, **kwargs: Any) -> CoreDataFrame:
        if args or kwargs:
            raise NotImplementedError(
                "transpose() does not accept arguments in this narrowed API."
            )
        fields = self.schema_fields()
        n, m = self.shape
        if n != m:
            raise NotImplementedError(
                f"transpose only supports square tables (rows==columns); got {n}x{m}."
            )
        dtypes = {fields[k] for k in fields}
        if len(dtypes) != 1:
            raise NotImplementedError(
                "transpose requires every column to share the same dtype."
            )
        data = self.collect(as_lists=True)
        names = list(self.schema_fields().keys())
        mat = list(zip(*[data[c] for c in names], strict=True))
        out = {names[i]: list(mat[i]) for i in range(n)}
        return self._from_plan(
            root_data=out,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=get_default_engine().make_plan(self.schema_fields()),
        )

    def dot(self, other: CoreDataFrame) -> CoreDataFrame:  # type: ignore[override]
        import numpy as np

        sc = list(self.schema_fields().keys())
        oc = list(other.schema_fields().keys())
        n_self, m_self = self.shape
        m_o, _ = other.shape
        if m_self != m_o:
            raise ValueError(
                "dot(other): other row count must match self column count "
                f"({m_self}), got {m_o}."
            )
        for a in list(self._current_field_types.values()) + list(
            other._current_field_types.values()
        ):
            if not _typing_numeric_name(a):
                raise TypeError("dot() requires numeric dtypes only.")
        d_self = self.collect(as_lists=True)
        d_other = other.collect(as_lists=True)
        A = np.asarray(
            [
                [
                    float(d_self[c][i]) if d_self[c][i] is not None else float("nan")
                    for c in sc
                ]
                for i in range(n_self)
            ],
            dtype=float,
        )
        B = np.asarray(
            [
                [
                    float(d_other[c][j]) if d_other[c][j] is not None else float("nan")
                    for c in oc
                ]
                for j in range(m_o)
            ],
            dtype=float,
        )
        out_mat = A @ B
        out_dict = {
            oc[j]: [float(out_mat[i, j]) for i in range(n_self)] for j in range(len(oc))
        }
        dyn = create_model(
            "_DotOut",
            **{c: (float | None, None) for c in oc},  # type: ignore[misc]
        )
        return DataFrame[dyn](out_dict)

    def insert(
        self,
        loc: int,
        column: str,
        value: Any,
        allow_duplicates: bool = False,
    ) -> CoreDataFrame:
        if allow_duplicates:
            raise NotImplementedError("insert(allow_duplicates=True) is not supported.")
        names = list(self.schema_fields().keys())
        if column in names:
            raise ValueError(f"cannot insert {column!r}, already exists")
        if loc < 0 or loc > len(names):
            raise IndexError("insert(loc=...) out of range.")
        expr: Expr | Any = value if isinstance(value, Expr) else Literal(value=value)
        new_order = [*names[:loc], column, *names[loc:]]
        return self.with_columns(**{column: expr}).select(*new_order)

    def pop(self, item: str) -> tuple[Expr, CoreDataFrame]:
        if item not in self.schema_fields():
            raise KeyError(item)
        return self.col(item), self.drop(item)

    def interpolate(
        self,
        *,
        method: str = "linear",
        axis: int = 0,
        limit_direction: str = "forward",
        **kwargs: Any,
    ) -> CoreDataFrame:
        if kwargs:
            raise TypeError(
                f"interpolate() got unexpected keyword arguments: {sorted(kwargs)!r}"
            )
        if axis != 0:
            raise NotImplementedError("interpolate(axis=1) is not supported.")
        m = method.lower().strip()
        if m == "linear":
            raise NotImplementedError(
                "interpolate(method='linear') is not implemented; use fill_null "
                "with forward/backward strategy after engine support lands."
            )
        if m in ("ffill", "pad"):
            strat = "forward"
        elif m in ("bfill", "backfill"):
            strat = "backward"
        else:
            raise NotImplementedError(
                f"interpolate(method={method!r}) supports 'ffill'/'bfill' only."
            )
        _ = limit_direction
        return self.fill_null(strategy=strat)

    class _Ewm:
        __slots__ = ("_adjust", "_alpha", "_com", "_df", "_min_periods", "_span")

        def __init__(
            self,
            df: PandasDataFrame,
            *,
            com: float | None,
            span: float | None,
            alpha: float | None,
            adjust: bool,
            min_periods: int,
        ) -> None:
            self._df = df
            self._com = com
            self._span = span
            self._alpha = alpha
            self._adjust = adjust
            self._min_periods = min_periods

        def mean(self, column: str, *, out_name: str | None = None) -> CoreDataFrame:
            pd = __import__("pandas")
            data = self._df.collect(as_lists=True)
            if column not in data:
                raise KeyError(column)
            s = pd.Series(data[column])
            kw: dict[str, Any] = {}
            if self._com is not None:
                kw["com"] = self._com
            elif self._span is not None:
                kw["span"] = self._span
            else:
                kw["alpha"] = self._alpha
            out = s.ewm(
                adjust=self._adjust,
                min_periods=self._min_periods,
                **kw,
            ).mean()
            name = out_name or f"{column}_ewm_mean"
            merged = {**data, name: out.tolist()}
            ft = dict(self._df._current_field_types)
            ft[name] = float | None
            dyn = make_derived_schema_type(self._df._current_schema_type, ft)
            return DataFrame[dyn](merged)

    class _Expanding:
        __slots__ = ("_df",)

        def __init__(self, df: PandasDataFrame):
            self._df = df

        def sum(self, column: str, *, out_name: str | None = None) -> CoreDataFrame:
            name = out_name or f"{column}_expanding_sum"
            return self._df.with_columns(**{name: self._df.col(column).cumsum()})

        def mean(self, column: str, *, out_name: str | None = None) -> CoreDataFrame:
            raise NotImplementedError(
                "expanding().mean() is not implemented without an explicit "
                "row order key; use window mean over row_number().over(...) "
                "if applicable."
            )

        def count(self, column: str, *, out_name: str | None = None) -> CoreDataFrame:
            name = out_name or f"{column}_expanding_count"
            mark = when(
                self._df.col(column).is_not_null(),
                Literal(value=1),
            ).otherwise(Literal(value=0))
            return self._df.with_columns(**{name: mark.cumsum()})

    def expanding(self, min_periods: int = 1) -> _Expanding:
        _ = min_periods
        return PandasDataFrame._Expanding(self)

    def ewm(
        self,
        *,
        com: float | None = None,
        span: float | None = None,
        alpha: float | None = None,
        adjust: bool = True,
        min_periods: int = 0,
    ) -> PandasDataFrame._Ewm:
        n = sum(1 for x in (com, span, alpha) if x is not None)
        if n != 1:
            raise TypeError("ewm() requires exactly one of com=, span=, or alpha=.")
        return PandasDataFrame._Ewm(
            self,
            com=com,
            span=span,
            alpha=alpha,
            adjust=adjust,
            min_periods=min_periods,
        )

    def nlargest(
        self,
        n: int,
        columns: str | list[str],
        *,
        keep: str = "all",
    ) -> CoreDataFrame:
        if keep != "all":
            raise NotImplementedError("nlargest(keep=...) only supports keep='all'.")
        if n < 0:
            raise ValueError("nlargest(n=...) must be >= 0.")
        cols = [columns] if isinstance(columns, str) else list(columns)
        if not cols:
            raise TypeError("nlargest(columns=...) requires at least one column name.")
        fields = self.schema_fields()
        for c in cols:
            if c not in fields:
                raise KeyError(c)
        ascending = [False] * len(cols)
        sorted_df = self.sort_values(by=cols, ascending=ascending)
        return sorted_df.slice(0, n)

    def nsmallest(
        self,
        n: int,
        columns: str | list[str],
        *,
        keep: str = "all",
    ) -> CoreDataFrame:
        if keep != "all":
            raise NotImplementedError("nsmallest(keep=...) only supports keep='all'.")
        if n < 0:
            raise ValueError("nsmallest(n=...) must be >= 0.")
        cols = [columns] if isinstance(columns, str) else list(columns)
        if not cols:
            raise TypeError("nsmallest(columns=...) requires at least one column name.")
        fields = self.schema_fields()
        for c in cols:
            if c not in fields:
                raise KeyError(c)
        ascending = [True] * len(cols)
        sorted_df = self.sort_values(by=cols, ascending=ascending)
        return sorted_df.slice(0, n)

    def isin(self, values: Any) -> CoreDataFrame:
        if _is_pandas_series(values):
            raise NotImplementedError(
                "isin(values=...) does not support pandas Series."
            )
        name = type(values).__name__
        mod = getattr(type(values), "__module__", "")
        if name == "DataFrame" and mod.startswith("pandas."):
            raise NotImplementedError(
                "isin(values=...) does not support pandas DataFrame."
            )
        cols = list(self.schema_fields().keys())
        if isinstance(values, dict):
            unknown = [k for k in values if k not in self.schema_fields()]
            if unknown:
                raise KeyError(f"isin(dict) unknown columns: {unknown!r}")
            updates: dict[str, Any] = {}
            for c in cols:
                if c in values:
                    v = values[c]
                    if isinstance(v, (str, bytes)) or not isinstance(
                        v, (list, tuple, set)
                    ):
                        updates[c] = self.col(c).isin([v])
                    else:
                        updates[c] = self.col(c).isin(list(v))
                else:
                    updates[c] = Literal(value=False)
            return self.with_columns(**updates)
        if isinstance(values, (list, tuple, set)):
            vlist = list(values)
            return self.with_columns(**{c: self.col(c).isin(vlist) for c in cols})
        raise TypeError(
            "isin(values=...) expects a list/tuple/set or dict[str, iterable]."
        )

    def explode(self, *args: Any, **kwargs: Any) -> CoreDataFrame:
        bad = set(kwargs) - {"streaming"}
        if bad:
            raise TypeError(
                f"explode() got unexpected keyword arguments: {sorted(bad)!r}"
            )
        streaming = kwargs.get("streaming")

        if not args:
            raise TypeError("explode() requires at least one column name.")
        if len(args) == 1:
            col = args[0]
            if isinstance(col, str):
                return super().explode(col, streaming=streaming)
            if isinstance(col, list):
                if not col:
                    raise TypeError("explode() requires at least one column name.")
                return super().explode(col, streaming=streaming)
            raise TypeError(
                "explode() first argument must be str or list[str] when alone."
            )
        if not all(isinstance(c, str) for c in args):
            raise TypeError("explode() column names must be str.")
        return super().explode(list(args), streaming=streaming)

    def copy(self, deep: bool = False) -> CoreDataFrame:
        if deep:
            raise NotImplementedError(
                "copy(deep=True) is not supported; collect(as_lists=True) and "
                "construct a new DataFrame if you need a data copy."
            )
        return self._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=self._rust_plan,
        )

    def pipe(
        self,
        fn: Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        return fn(self, *args, **kwargs)

    def filter(  # type: ignore[override]
        self,
        *args: Any,
        items: list[str] | None = None,
        like: str | None = None,
        regex: str | None = None,
        axis: Any = 0,
    ) -> CoreDataFrame:
        if args:
            if len(args) == 1 and isinstance(args[0], Expr):
                return super().filter(args[0])
            raise TypeError(
                "filter() positional args only support a single Expr row condition; "
                "use filter(items=...), filter(like=...), or filter(regex=...) "
                "for columns."
            )
        if axis not in (0, "index", None):
            if axis == 1:
                raise NotImplementedError(
                    "filter(axis=1) is not supported (use row filter(Expr))."
                )
            raise ValueError("filter(axis=...) must be 0, 'index', or None.")

        n_kw = sum(x is not None for x in (items, like, regex))
        if n_kw == 0:
            raise TypeError(
                "filter() requires items, like, regex, or an Expr argument."
            )
        if n_kw > 1:
            raise TypeError(
                "filter() only one of items, like, or regex can be specified."
            )
        names = list(self.schema_fields().keys())
        if items is not None:
            if not isinstance(items, list) or not all(
                isinstance(x, str) for x in items
            ):
                raise TypeError("filter(items=...) must be list[str].")
            missing = [c for c in items if c not in self.schema_fields()]
            if missing:
                raise KeyError(f"filter(items=...): unknown columns {missing!r}")
            matched = items
        elif like is not None:
            if not isinstance(like, str):
                raise TypeError("filter(like=...) must be str.")
            matched = [c for c in names if like in c]
        else:
            if not isinstance(regex, str):
                raise TypeError("filter(regex=...) must be str.")
            pat = re.compile(regex)
            matched = [c for c in names if pat.search(c) is not None]
        if not matched:
            raise ValueError("filter(...) matched no columns.")
        return self.select(*matched)

    class _Rolling:
        def __init__(
            self,
            df: PandasDataFrame,
            *,
            window: int,
            min_periods: int,
            partition_by: list[str] | None = None,
        ):
            self._df = df
            self._window = int(window)
            self._min_periods = int(min_periods)
            self._partition_by = list(partition_by or ())
            if self._window <= 0:
                raise ValueError("rolling(window=...) must be >= 1.")
            if self._min_periods < 0:
                raise ValueError("rolling(min_periods=...) must be >= 0.")

        def _apply(self, op: str, column: str, out_name: str | None) -> CoreDataFrame:
            if not isinstance(column, str):
                raise TypeError("rolling op requires column as str.")
            name = out_name or f"{column}_{op}"
            rust = get_default_engine().rust_core
            part = self._partition_by if self._partition_by else None
            rust_plan = rust.plan_rolling_agg(
                self._df._rust_plan,
                column,
                self._window,
                self._min_periods,
                op,
                name,
                part,
            )
            desc = rust_plan.schema_descriptors()
            derived_fields = self._df._field_types_from_descriptors(desc)
            derived_schema_type = make_derived_schema_type(
                self._df._current_schema_type, derived_fields
            )
            return self._df._from_plan(
                root_data=self._df._root_data,
                root_schema_type=self._df._root_schema_type,
                current_schema_type=derived_schema_type,
                rust_plan=rust_plan,
            )

        def sum(self, column: str, *, out_name: str | None = None) -> CoreDataFrame:
            return self._apply("sum", column, out_name)

        def mean(self, column: str, *, out_name: str | None = None) -> CoreDataFrame:
            return self._apply("mean", column, out_name)

        def min(self, column: str, *, out_name: str | None = None) -> CoreDataFrame:
            return self._apply("min", column, out_name)

        def max(self, column: str, *, out_name: str | None = None) -> CoreDataFrame:
            return self._apply("max", column, out_name)

        def count(self, column: str, *, out_name: str | None = None) -> CoreDataFrame:
            return self._apply("count", column, out_name)

    def rolling(self, *, window: int, min_periods: int = 1) -> _Rolling:
        return PandasDataFrame._Rolling(self, window=window, min_periods=min_periods)


class PandasGroupedDataFrame(CoreGroupedDataFrame):
    """Grouped frame with shorthand ``sum`` / ``mean`` / ``count`` over columns."""

    class _Rolling:
        __slots__ = ("_inner",)

        def __init__(
            self,
            gdf: PandasGroupedDataFrame,
            *,
            window: int,
            min_periods: int,
        ):
            self._inner = PandasDataFrame._Rolling(
                gdf._df,
                window=window,
                min_periods=min_periods,
                partition_by=list(gdf._keys),
            )

        def sum(self, column: str, *, out_name: str | None = None) -> CoreDataFrame:
            return self._inner.sum(column, out_name=out_name)

        def mean(self, column: str, *, out_name: str | None = None) -> CoreDataFrame:
            return self._inner.mean(column, out_name=out_name)

        def min(self, column: str, *, out_name: str | None = None) -> CoreDataFrame:
            return self._inner.min(column, out_name=out_name)

        def max(self, column: str, *, out_name: str | None = None) -> CoreDataFrame:
            return self._inner.max(column, out_name=out_name)

        def count(self, column: str, *, out_name: str | None = None) -> CoreDataFrame:
            return self._inner.count(column, out_name=out_name)

    def rolling(self, *, window: int, min_periods: int = 1) -> _Rolling:
        return PandasGroupedDataFrame._Rolling(
            self,
            window=window,
            min_periods=min_periods,
        )

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

    def sum(self, *columns: str, streaming: bool | None = None) -> CoreDataFrame:
        if not columns:
            raise TypeError("sum() requires at least one column name.")
        return self.agg(
            streaming=streaming,
            **{f"{c}_sum": ("sum", c) for c in columns},
        )

    def mean(self, *columns: str, streaming: bool | None = None) -> CoreDataFrame:
        if not columns:
            raise TypeError("mean() requires at least one column name.")
        return self.agg(
            streaming=streaming,
            **{f"{c}_mean": ("mean", c) for c in columns},
        )

    def count(self, *columns: str, streaming: bool | None = None) -> CoreDataFrame:
        if not columns:
            raise TypeError("count() requires at least one column name.")
        return self.agg(
            streaming=streaming,
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

    @classmethod
    def concat(
        cls,
        dfs: Sequence[CoreDataFrameModel],
        /,
        *,
        how: str | None = None,
        axis: int = 0,
        join: str = "outer",
        ignore_index: bool = False,
        keys: Any = None,
        levels: Any = None,
        names: Any = None,
        verify_integrity: Any = None,
        sort: Any = None,
        copy: Any = None,
        streaming: bool | None = None,
    ) -> CoreDataFrameModel:
        if len(dfs) < 2:
            raise ValueError("concat() requires at least two DataFrameModel inputs.")
        if not all(isinstance(df, CoreDataFrameModel) for df in dfs):
            raise TypeError("concat() expects a sequence of DataFrameModel objects.")
        out = DataFrame.concat(
            [df._df for df in dfs],
            how=how,
            axis=axis,
            join=join,
            ignore_index=ignore_index,
            keys=keys,
            levels=levels,
            names=names,
            verify_integrity=verify_integrity,
            sort=sort,
            copy=copy,
            streaming=streaming,
        )
        return cls._from_dataframe(out)

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

    def drop_duplicates(self, *args: Any, **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.drop_duplicates(*args, **kwargs))

    def duplicated(self, *args: Any, **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.duplicated(*args, **kwargs))

    def drop(self, *args: Any, **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.drop(*args, **kwargs))

    def rename(self, *args: Any, **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.rename(*args, **kwargs))

    def fillna(self, *args: Any, **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.fillna(*args, **kwargs))

    def astype(self, *args: Any, **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.astype(*args, **kwargs))

    def nlargest(self, *args: Any, **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.nlargest(*args, **kwargs))

    def nsmallest(self, *args: Any, **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.nsmallest(*args, **kwargs))

    def isin(self, values: Any) -> Self:
        return type(self)._from_dataframe(self._df.isin(values))

    def explode(self, *args: Any, **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.explode(*args, **kwargs))

    def copy(self, *args: Any, **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.copy(*args, **kwargs))

    def pipe(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        return fn(self, *args, **kwargs)

    def filter(self, *args: Any, **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.filter(*args, **kwargs))

    class _ModelILoc:
        __slots__ = ("_m",)

        def __init__(self, m: PandasDataFrameModel):
            self._m = m

        def __getitem__(self, key: int | slice) -> CoreDataFrameModel:
            return type(self._m)._from_dataframe(self._m._df.iloc[key])

    class _ModelLoc:
        __slots__ = ("_m",)

        def __init__(self, m: PandasDataFrameModel):
            self._m = m

        def __getitem__(self, key: object) -> CoreDataFrameModel:
            return type(self._m)._from_dataframe(self._m._df.loc[key])

    @property
    def iloc(self) -> _ModelILoc:
        return PandasDataFrameModel._ModelILoc(self)

    @property
    def loc(self) -> _ModelLoc:
        return PandasDataFrameModel._ModelLoc(self)

    def isna(self) -> Self:
        return type(self)._from_dataframe(self._df.isna())

    def isnull(self) -> Self:
        return type(self)._from_dataframe(self._df.isnull())

    def notna(self) -> Self:
        return type(self)._from_dataframe(self._df.notna())

    def notnull(self) -> Self:
        return type(self)._from_dataframe(self._df.notnull())

    def dropna(self, *args: Any, **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.dropna(*args, **kwargs))

    def melt(self, *args: Any, **kwargs: Any) -> Self:
        return type(self)._from_dataframe(self._df.melt(*args, **kwargs))

    class _ModelRolling:
        __slots__ = ("_inner", "_m")

        def __init__(self, m: PandasDataFrameModel, *, window: int, min_periods: int):
            self._m = m
            self._inner = PandasDataFrame._Rolling(
                m._df, window=window, min_periods=min_periods
            )

        def sum(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel:
            return type(self._m)._from_dataframe(
                self._inner.sum(column, out_name=out_name)
            )

        def mean(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel:
            return type(self._m)._from_dataframe(
                self._inner.mean(column, out_name=out_name)
            )

        def min(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel:
            return type(self._m)._from_dataframe(
                self._inner.min(column, out_name=out_name)
            )

        def max(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel:
            return type(self._m)._from_dataframe(
                self._inner.max(column, out_name=out_name)
            )

        def count(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel:
            return type(self._m)._from_dataframe(
                self._inner.count(column, out_name=out_name)
            )

    def rolling(self, *, window: int, min_periods: int = 1) -> _ModelRolling:
        return PandasDataFrameModel._ModelRolling(
            self, window=window, min_periods=min_periods
        )

    def __getitem__(self, key: str | list[str]) -> Any:
        return self._df[key]  # type: ignore[index]

    def group_by(self, *keys: Any, **kwargs: Any) -> PandasGroupedDataFrameModel:
        g = self._df.group_by(*keys, **kwargs)
        return PandasGroupedDataFrameModel(g, type(self))


class PandasGroupedDataFrameModel(CoreGroupedDataFrameModel):
    """Model-level grouped aggregations with pandas naming."""

    class _ModelGroupedRolling:
        __slots__ = ("_inner", "_mt")

        def __init__(self, mt: type, inner: Any):
            self._mt = mt
            self._inner = inner

        def sum(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel:
            return self._mt._from_dataframe(self._inner.sum(column, out_name=out_name))

        def mean(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel:
            return self._mt._from_dataframe(self._inner.mean(column, out_name=out_name))

        def min(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel:
            return self._mt._from_dataframe(self._inner.min(column, out_name=out_name))

        def max(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel:
            return self._mt._from_dataframe(self._inner.max(column, out_name=out_name))

        def count(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel:
            return self._mt._from_dataframe(
                self._inner.count(column, out_name=out_name)
            )

    def rolling(self, *, window: int, min_periods: int = 1) -> _ModelGroupedRolling:
        r = self._grouped_df.rolling(window=window, min_periods=min_periods)
        return PandasGroupedDataFrameModel._ModelGroupedRolling(type(self), r)

    def sum(self, *columns: str, streaming: bool | None = None) -> CoreDataFrameModel:
        return self._model_type._from_dataframe(
            self._grouped_df.sum(*columns, streaming=streaming)
        )

    def mean(self, *columns: str, streaming: bool | None = None) -> CoreDataFrameModel:
        return self._model_type._from_dataframe(
            self._grouped_df.mean(*columns, streaming=streaming)
        )

    def count(self, *columns: str, streaming: bool | None = None) -> CoreDataFrameModel:
        return self._model_type._from_dataframe(
            self._grouped_df.count(*columns, streaming=streaming)
        )

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
