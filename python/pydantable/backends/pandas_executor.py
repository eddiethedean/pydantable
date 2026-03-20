from __future__ import annotations

from typing import Any


def _plan_to_dict(plan: Any) -> dict[str, Any]:
    from pydantable.backend import _require_rust_core

    r = _require_rust_core()
    if hasattr(r, "plan_to_serializable"):
        raw = r.plan_to_serializable(plan)
    else:
        raw = plan.to_serializable()
    return dict(raw)


def _pd():
    import pandas as pd

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
    raise ValueError(f"Unknown expr kind {kind!r}")


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
