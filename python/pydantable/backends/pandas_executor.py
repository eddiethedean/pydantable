from __future__ import annotations

from datetime import datetime
from functools import cmp_to_key
from typing import Any, cast


def _plan_to_dict(plan: Any) -> dict[str, Any]:
    from pydantable.backend import _require_rust_core

    r = _require_rust_core()
    if hasattr(r, "plan_to_serializable"):
        raw = r.plan_to_serializable(plan)
    else:
        raw = plan.to_serializable()
    return dict(raw)


def _pd():
    import pandas as pd  # type: ignore[import-untyped]

    return pd


def _scalar_to_py(v: Any) -> Any:
    pd = _pd()
    if not pd.api.types.is_scalar(v):
        return v
    if pd.isna(v):
        return None
    if hasattr(v, "item"):
        try:
            return v.item()
        except (ValueError, AttributeError):
            return v
    return v


def _coerce_compare_pair(left: Any, right: Any) -> tuple[Any, Any]:
    """Align Python datetime column values with Rust literal microseconds (ints)."""
    if (
        isinstance(left, datetime)
        and isinstance(right, (int, float))
        and not isinstance(right, bool)
    ):
        return left, datetime.fromtimestamp(float(right) / 1e6)
    if (
        isinstance(right, datetime)
        and isinstance(left, (int, float))
        and not isinstance(left, bool)
    ):
        return datetime.fromtimestamp(float(left) / 1e6), right
    return left, right


def _eval_expr(expr: dict[str, Any], i: int, ctx: dict[str, list[Any]]) -> Any:
    kind = expr["kind"]
    if kind == "column_ref":
        return ctx[expr["name"]][i]
    if kind == "literal":
        return expr.get("value")
    if kind == "binary_op":
        op = expr["op"]
        left = _eval_expr(expr["left"], i, ctx)
        right = _eval_expr(expr["right"], i, ctx)
        if left is None or right is None:
            return None
        if op == "add":
            return left + right
        if op == "sub":
            return left - right
        if op == "mul":
            return left * right
        if op == "div":
            return left / right
        raise ValueError(f"Unknown binary op {op!r}")
    if kind == "compare_op":
        op = expr["op"]
        left = _eval_expr(expr["left"], i, ctx)
        right = _eval_expr(expr["right"], i, ctx)
        if left is None or right is None:
            return None
        left, right = _coerce_compare_pair(left, right)
        if op == "eq":
            return left == right
        if op == "ne":
            return left != right
        if op == "lt":
            return left < right
        if op == "le":
            return left <= right
        if op == "gt":
            return left > right
        if op == "ge":
            return left >= right
        raise ValueError(f"Unknown compare op {op!r}")
    if kind == "is_null":
        inner = expr.get("inner") or expr.get("input")
        if not isinstance(inner, dict):
            raise ValueError("is_null expression missing inner/input")
        return _eval_expr(cast("dict[str, Any]", inner), i, ctx) is None
    if kind == "is_not_null":
        inner = expr.get("inner") or expr.get("input")
        if not isinstance(inner, dict):
            raise ValueError("is_not_null expression missing inner/input")
        return _eval_expr(cast("dict[str, Any]", inner), i, ctx) is not None
    if kind == "coalesce":
        for sub in expr["exprs"]:
            v = _eval_expr(sub, i, ctx)
            if v is not None:
                return v
        return None
    if kind == "case_when":
        for br in expr["branches"]:
            cond = _eval_expr(br["condition"], i, ctx)
            if cond is True:
                return _eval_expr(br["then"], i, ctx)
            if cond not in (False, None):
                raise ValueError("CASE WHEN condition must be bool or null")
        return _eval_expr(expr["else"], i, ctx)
    if kind == "cast":
        inner = expr.get("inner") or expr.get("input")
        if not isinstance(inner, dict):
            raise ValueError("cast expression missing inner/input")
        v = _eval_expr(cast("dict[str, Any]", inner), i, ctx)
        to = expr.get("to")
        if to is None:
            desc = expr.get("dtype") or {}
            to = desc.get("base", "str")
        return _cast_py_value(v, str(to))
    if kind == "in_list":
        x = _eval_expr(expr["inner"], i, ctx)
        if x is None:
            return None
        vals = expr["values"]
        if isinstance(x, float):
            return any(
                isinstance(v, (int, float)) and float(x) == float(v) for v in vals
            )
        return x in vals
    if kind == "between":
        x = _eval_expr(expr["inner"], i, ctx)
        lo = _eval_expr(expr["low"], i, ctx)
        hi = _eval_expr(expr["high"], i, ctx)
        if x is None or lo is None or hi is None:
            return None
        return lo <= x <= hi
    if kind == "string_concat":
        parts = [_eval_expr(p, i, ctx) for p in expr["parts"]]
        if any(p is None for p in parts):
            return None
        return "".join(str(p) for p in parts)
    if kind == "substring":
        s = _eval_expr(expr["inner"], i, ctx)
        start = _eval_expr(expr["start"], i, ctx)
        ln_raw = expr.get("length")
        length = _eval_expr(ln_raw, i, ctx) if ln_raw is not None else None
        if s is None or start is None:
            return None
        if not isinstance(start, int):
            raise ValueError("substring start must be int")
        if length is not None and not isinstance(length, int):
            raise ValueError("substring length must be int")
        if not isinstance(s, str):
            raise ValueError("substring inner must be str")
        return _substr_spark(s, start, length)
    if kind == "string_length":
        s = _eval_expr(expr["inner"], i, ctx)
        if s is None:
            return None
        return len(s)
    raise ValueError(f"Unknown expr kind {kind!r}")


def _cast_py_value(v: Any, to: str) -> Any:
    if v is None:
        return None
    if to == "int":
        if isinstance(v, bool):
            return int(v)
        return int(v)
    if to == "float":
        return float(v)
    if to == "bool":
        return bool(v)
    if to == "str":
        return str(v)
    raise ValueError(f"Unknown cast target {to!r}")


def _substr_spark(s: str, start: int, length: int | None) -> str:
    if start < 1:
        return ""
    idx = start - 1
    if idx >= len(s):
        return ""
    rest = s[idx:]
    if length is None:
        return rest
    if length <= 0:
        return ""
    return rest[:length]


def _cmp_sort_indices(
    ctx: dict[str, list[Any]],
    by: list[tuple[str, bool]],
    i: int,
    j: int,
) -> int:
    for col, asc in by:
        ai = ctx[col][i]
        aj = ctx[col][j]
        if ai is None and aj is None:
            continue
        if ai is None:
            return 1
        if aj is None:
            return -1
        if ai == aj:
            continue
        if ai < aj:
            return -1 if asc else 1
        return 1 if asc else -1
    return 0


def _apply_sort(ctx: dict[str, list[Any]], by: list[tuple[str, bool]]) -> None:
    if not ctx or not by:
        return
    n = len(next(iter(ctx.values())))

    def _pair_cmp(ai: int, bi: int) -> int:
        return _cmp_sort_indices(ctx, by, ai, bi)

    order = sorted(range(n), key=cmp_to_key(_pair_cmp))
    for c in ctx:
        ctx[c] = [ctx[c][k] for k in order]


def _execute_plan_from_blob(
    blob: dict[str, Any], root_data: dict[str, list[Any]]
) -> dict[str, list[Any]]:
    ctx: dict[str, list[Any]] = {k: list(v) for k, v in root_data.items()}
    for step in blob["steps"]:
        sk = step["kind"]
        if sk == "select":
            columns: list[str] = list(step["columns"])
            ctx = {c: ctx[c] for c in columns}
        elif sk == "with_columns":
            cols_map: dict[str, Any] = dict(step["columns"])
            if not ctx:
                for name in cols_map:
                    ctx[name] = []
                continue
            n = len(next(iter(ctx.values())))
            for name, ex in cols_map.items():
                ctx[name] = [_eval_expr(ex, i, ctx) for i in range(n)]
        elif sk == "filter":
            condition = step["condition"]
            if not ctx:
                continue
            n = len(next(iter(ctx.values())))
            keep = [i for i in range(n) if _eval_expr(condition, i, ctx) is True]
            for col in ctx:
                ctx[col] = [ctx[col][i] for i in keep]
        elif sk == "limit":
            lim = int(step["n"])
            for c in ctx:
                ctx[c] = ctx[c][:lim]
        elif sk == "sort":
            raw_by = step["by"]
            desc_list = list(step.get("descending", []))
            if not raw_by:
                continue
            if isinstance(raw_by[0], dict):
                pairs = [(str(p["column"]), bool(p["ascending"])) for p in raw_by]
            else:
                pairs = []
                for i, col in enumerate(raw_by):
                    d = bool(desc_list[i]) if i < len(desc_list) else False
                    pairs.append((str(col), not d))
            _apply_sort(ctx, pairs)
        elif sk == "drop":
            for c in step["columns"]:
                ctx.pop(c, None)
        elif sk == "distinct":
            if not ctx:
                continue
            cols = sorted(ctx.keys())
            n = len(next(iter(ctx.values())))
            seen: set[tuple[Any, ...]] = set()
            kept_rows: list[int] = []
            for i in range(n):
                fp = tuple(ctx[c][i] for c in cols)
                if fp not in seen:
                    seen.add(fp)
                    kept_rows.append(i)
            for c in ctx:
                ctx[c] = [ctx[c][i] for i in kept_rows]
        elif sk == "unique":
            subset = step.get("subset")
            keep_mode = str(step.get("keep", "first"))
            key_cols = list(subset) if subset else sorted(ctx.keys())
            if not ctx or not key_cols:
                continue
            n = len(next(iter(ctx.values())))
            seen: set[tuple[Any, ...]] = set()
            kept_rows: list[int] = []
            if keep_mode == "last":
                for i in range(n - 1, -1, -1):
                    fp = tuple(ctx[c][i] for c in key_cols)
                    if fp not in seen:
                        seen.add(fp)
                        kept_rows.append(i)
                kept_rows.reverse()
            else:
                for i in range(n):
                    fp = tuple(ctx[c][i] for c in key_cols)
                    if fp not in seen:
                        seen.add(fp)
                        kept_rows.append(i)
            for c in ctx:
                ctx[c] = [ctx[c][i] for i in kept_rows]
        elif sk == "slice":
            off = int(step["offset"])
            length = int(step["length"])
            if not ctx:
                continue
            n = len(next(iter(ctx.values())))
            start = off if off >= 0 else max(0, n + off)
            end = min(n, start + length)
            for c in ctx:
                ctx[c] = ctx[c][start:end]
        elif sk == "fill_null":
            subset = step.get("subset")
            fill_val = step.get("value")
            cols = list(subset) if subset else list(ctx.keys())
            for c in cols:
                if c not in ctx:
                    continue
                ctx[c] = [fill_val if v is None else v for v in ctx[c]]
        elif sk == "drop_nulls":
            subset = step.get("subset")
            cols = list(subset) if subset else list(ctx.keys())
            if not ctx:
                continue
            n = len(next(iter(ctx.values())))
            keep_idx = [
                i
                for i in range(n)
                if not any(ctx[c][i] is None for c in cols if c in ctx)
            ]
            for c in ctx:
                ctx[c] = [ctx[c][i] for i in keep_idx]
        elif sk == "rename":
            mapping = step.get("columns") or {}
            if isinstance(mapping, dict):
                for f, t in mapping.items():
                    fs, ts = str(f), str(t)
                    if fs in ctx:
                        ctx[ts] = ctx.pop(fs)
        else:
            raise ValueError(f"Unknown plan step {sk!r}")
    return ctx


def execute_plan_pandas(
    plan: Any, root_data: dict[str, list[Any]]
) -> dict[str, list[Any]]:
    blob = _plan_to_dict(plan)
    return _execute_plan_from_blob(blob, root_data)


def _join_descriptors_for_columns(
    merged_columns: list[str],
    left_desc: dict[str, Any],
    right_desc: dict[str, Any],
    suffix: str,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for col in merged_columns:
        if col in left_desc:
            out[col] = dict(left_desc[col])
        elif col.endswith(suffix) and col[: -len(suffix)] in right_desc:
            out[col] = dict(right_desc[col[: -len(suffix)]])
        elif col in right_desc:
            out[col] = dict(right_desc[col])
        else:
            raise KeyError(
                f"Could not map join output column {col!r} to a dtype descriptor."
            )
    return out


def execute_join_pandas(
    left_plan: Any,
    left_root_data: dict[str, list[Any]],
    right_plan: Any,
    right_root_data: dict[str, list[Any]],
    on: list[str],
    how: str,
    suffix: str,
) -> tuple[dict[str, list[Any]], dict[str, Any]]:
    pd = _pd()
    left_blob = _plan_to_dict(left_plan)
    right_blob = _plan_to_dict(right_plan)
    left_desc = left_blob["schema_descriptors"]
    right_desc = right_blob["schema_descriptors"]

    left_ctx = _execute_plan_from_blob(left_blob, left_root_data)
    right_ctx = _execute_plan_from_blob(right_blob, right_root_data)

    left_df = pd.DataFrame(left_ctx)
    right_df = pd.DataFrame(right_ctx)

    left_names = set(left_df.columns)
    rename_r: dict[str, str] = {}
    for c in right_df.columns:
        if c in on:
            continue
        if c in left_names:
            rename_r[c] = f"{c}{suffix}"
    if rename_r:
        right_df = right_df.rename(columns=rename_r)

    how_map = {"inner": "inner", "left": "left", "full": "outer", "outer": "outer"}
    if how not in how_map:
        raise ValueError(
            f"Unsupported join how {how!r}. Use one of: inner, left, full, outer."
        )
    merged = pd.merge(
        left_df, right_df, on=list(on), how=how_map[how], sort=False, copy=False
    )

    merged_columns = list(merged.columns)
    out_dict: dict[str, list[Any]] = {}
    for col in merged_columns:
        ser = merged[col]
        out_dict[col] = [_scalar_to_py(x) for x in ser.tolist()]

    out_desc = _join_descriptors_for_columns(
        merged_columns, left_desc, right_desc, suffix
    )
    return out_dict, out_desc


def _groupby_output_descriptors(
    schema: dict[str, Any], by: list[str], aggs: list[tuple[str, str, str]]
) -> dict[str, Any]:
    out: dict[str, Any] = {k: dict(schema[k]) for k in by}
    for out_name, op, in_col in aggs:
        in_dtype = schema[in_col]
        base = in_dtype.get("base")
        if op == "count":
            out[out_name] = {"base": "int", "nullable": False}
        elif op == "sum":
            out[out_name] = {"base": base, "nullable": True}
        elif op == "mean":
            out[out_name] = {"base": "float", "nullable": True}
        else:
            raise ValueError(f"Unknown aggregation op {op!r}")
    return out


def _normalize_group_keys(raw: Any, n_keys: int) -> tuple[Any, ...]:
    if n_keys == 1:
        if isinstance(raw, tuple) and len(raw) == 1:
            return raw
        return (raw,)
    return tuple(raw)


def execute_groupby_agg_pandas(
    plan: Any,
    root_data: dict[str, list[Any]],
    by: list[str],
    aggregations: dict[str, tuple[str, str]],
) -> tuple[dict[str, list[Any]], dict[str, Any]]:
    pd = _pd()
    blob = _plan_to_dict(plan)
    schema = blob["schema_descriptors"]
    ctx = _execute_plan_from_blob(blob, root_data)
    df = pd.DataFrame(ctx)

    aggs_list = [(out, op, incol) for out, (op, incol) in aggregations.items()]

    out: dict[str, list[Any]] = {k: [] for k in by}
    for out_name, _, _ in aggs_list:
        out[out_name] = []

    for _keys, sub in df.groupby(by, dropna=False):
        key_vals = _normalize_group_keys(_keys, len(by))
        for i, bcol in enumerate(by):
            out[bcol].append(key_vals[i])
        for out_name, op, in_col in aggs_list:
            col = sub[in_col]
            nn = int(col.notna().sum())
            if op == "count":
                out[out_name].append(nn)
            elif op == "sum":
                if nn == 0:
                    out[out_name].append(None)
                else:
                    s = col.sum()
                    out[out_name].append(None if pd.isna(s) else s)
            elif op == "mean":
                if nn == 0:
                    out[out_name].append(None)
                else:
                    m = col.mean()
                    out[out_name].append(None if pd.isna(m) else float(m))

    out_desc = _groupby_output_descriptors(schema, by, aggs_list)
    return out, out_desc
