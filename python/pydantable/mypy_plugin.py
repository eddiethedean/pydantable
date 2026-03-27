from __future__ import annotations

from typing import Any, Callable, cast

from mypy.nodes import (
    ARG_NAMED,
    ARG_POS,
    DictExpr,
    Expression,
    FloatExpr,
    IntExpr,
    ListExpr,
    MemberExpr,
    NameExpr,
    StrExpr,
    TupleExpr,
    TypeInfo,
    Var,
)
from mypy.plugin import MethodContext, Plugin
from mypy.subtypes import is_subtype
from mypy.types import AnyType, Instance, Type, TypeOfAny, get_proper_type


_BASE_FULLNAME = "pydantable.dataframe_model.DataFrameModel"
_GROUPED_BASE_FULLNAME = "pydantable.dataframe_model.GroupedDataFrameModel"
_DYNAMIC_GROUPED_BASE_FULLNAME = "pydantable.dataframe_model.DynamicGroupedDataFrameModel"
_HOOK_NAMES = {
    "with_columns",
    "select",
    "drop",
    "rename",
    "join",
    "agg",
    # Schema-preserving transforms that currently return `DataFrameModel` at runtime.
    "fill_null",
    "drop_nulls",
    "explode",
    "unnest",
    # Schema-evolving transforms.
    "melt",
    "unpivot",
    "rolling_agg",
    # `pivot` is intentionally omitted: output columns depend on data values.
}
_RESERVED_FIELDS = {
    "RowModel",
    "_RowModel_fill_missing_optional",
    "_RowModel_require_optional",
    "_SchemaModel",
    "_df",
    "_dataframe_cls",
}


def _model_fields(info: TypeInfo) -> dict[str, Type]:
    fields: dict[str, Type] = {}
    for name, sym in info.names.items():
        node = sym.node
        if not isinstance(node, Var):
            continue
        if name.startswith("_") or name in _RESERVED_FIELDS:
            continue
        if node.type is None:
            continue
        fields[name] = node.type
    return fields


def _is_dataframe_model_instance(tp: Type) -> Instance | None:
    proper = get_proper_type(tp)
    if not isinstance(proper, Instance):
        return None
    for base in proper.type.mro:
        if base.fullname == _BASE_FULLNAME:
            return proper
    return None


def _is_grouped_model_instance(tp: Type) -> Instance | None:
    proper = get_proper_type(tp)
    if not isinstance(proper, Instance):
        return None
    for base in proper.type.mro:
        if base.fullname in {_GROUPED_BASE_FULLNAME, _DYNAMIC_GROUPED_BASE_FULLNAME}:
            return proper
    return None


def _literal_str(expr: Expression) -> str | None:
    if isinstance(expr, StrExpr):
        return expr.value
    return None


def _literal_str_list(expr: Expression) -> list[str] | None:
    if isinstance(expr, StrExpr):
        return [expr.value]
    if isinstance(expr, ListExpr):
        vals: list[str] = []
        for it in expr.items:
            lit = _literal_str(it)
            if lit is None:
                return None
            vals.append(lit)
        return vals
    if isinstance(expr, TupleExpr):
        vals2: list[str] = []
        for it in expr.items:
            lit = _literal_str(it)
            if lit is None:
                return None
            vals2.append(lit)
        return vals2
    return None


def _literal_type(ctx: MethodContext, expr: Expression) -> Type | None:
    api = cast(Any, ctx.api)
    if isinstance(expr, StrExpr):
        return api.named_type("builtins.str")
    if isinstance(expr, IntExpr):
        return api.named_type("builtins.int")
    if isinstance(expr, FloatExpr):
        return api.named_type("builtins.float")
    if isinstance(expr, NameExpr) and expr.name in {"True", "False"}:
        return api.named_type("builtins.bool")
    return None


def _types_compatible(expected: Type, actual: Type) -> bool:
    pe = get_proper_type(expected)
    pa = get_proper_type(actual)
    if isinstance(pe, AnyType) or isinstance(pa, AnyType):
        return True
    return is_subtype(pa, pe) and is_subtype(pe, pa)


def _resolve_matching_model(
    ctx: MethodContext,
    fields: dict[str, Type],
    *,
    fallback: Type,
) -> Type:
    api = cast(Any, ctx.api)
    for module_id in api.modules:
        mod = api.modules[module_id]
        for sym in mod.names.values():
            node = sym.node
            if not isinstance(node, TypeInfo):
                continue
            if node.fullname == _BASE_FULLNAME:
                continue
            if not any(base.fullname == _BASE_FULLNAME for base in node.mro):
                continue
            candidate = _model_fields(node)
            if set(candidate.keys()) != set(fields.keys()):
                continue
            if all(_types_compatible(candidate[name], fields[name]) for name in fields):
                return Instance(node, [])
    return fallback


def _hook(ctx: MethodContext, method: str) -> Type:
    self_types = ctx.type if isinstance(ctx.type, list) else [ctx.type]
    self_instance: Instance | None = None
    for t in self_types:
        self_instance = _is_dataframe_model_instance(t)
        if self_instance is not None:
            break
    grouped_instance = None if self_instance is not None else _is_grouped_model_instance(self_types[0])
    if self_instance is None and grouped_instance is None:
        return ctx.default_return_type

    if self_instance is not None:
        fields = dict(_model_fields(self_instance.type))
        fallback: Type = self_instance
        grouped_keys: set[str] | None = None
    else:
        model_arg = get_proper_type(grouped_instance.args[0]) if grouped_instance and grouped_instance.args else None
        if not isinstance(model_arg, Instance):
            return ctx.default_return_type
        fields = dict(_model_fields(model_arg.type))
        fallback = model_arg
        grouped_keys = None
    if method == "with_columns":
        # Best-effort: use mypy's inferred type for the kwarg expression when available.
        # Fallback to literal type for simple constants; otherwise Any.
        for args, kinds, names, types in zip(
            ctx.args, ctx.arg_kinds, ctx.arg_names, ctx.arg_types
        ):
            for expr, kind, name, arg_t in zip(args, kinds, names, types):
                if kind != ARG_NAMED or name is None:
                    continue
                inferred = get_proper_type(arg_t)
                if isinstance(inferred, AnyType):
                    lit_t = _literal_type(ctx, expr)
                    fields[name] = lit_t if lit_t is not None else inferred
                else:
                    fields[name] = arg_t

    elif method == "select":
        selected: dict[str, Type] = {}
        for args, kinds in zip(ctx.args, ctx.arg_kinds):
            for expr, kind in zip(args, kinds):
                if kind != ARG_POS:
                    continue
                cols = _literal_str_list(expr)
                if cols is None:
                    continue
                for col in cols:
                    if col in fields:
                        selected[col] = fields[col]
                    else:
                        selected[col] = AnyType(TypeOfAny.special_form)
        if selected:
            fields = selected

    elif method == "drop":
        for args, kinds in zip(ctx.args, ctx.arg_kinds):
            for expr, kind in zip(args, kinds):
                if kind != ARG_POS:
                    continue
                cols = _literal_str_list(expr)
                if cols is None:
                    continue
                for col in cols:
                    fields.pop(col, None)

    elif method == "rename":
        for args, kinds in zip(ctx.args, ctx.arg_kinds):
            for expr, kind in zip(args, kinds):
                if kind not in (ARG_POS, ARG_NAMED):
                    continue
                if isinstance(expr, DictExpr):
                    for k_expr, v_expr in expr.items:
                        if k_expr is None or v_expr is None:
                            continue
                        old = _literal_str(k_expr)
                        new = _literal_str(v_expr)
                        if old is None or new is None:
                            continue
                        if old in fields:
                            fields[new] = fields.pop(old)

    elif method == "join":
        other_model: Instance | None = None
        if ctx.arg_types and ctx.arg_types[0]:
            other_model = _is_dataframe_model_instance(ctx.arg_types[0][0])
        if other_model is not None:
            right_fields = dict(_model_fields(other_model.type))
            on_keys: set[str] = set()
            suffix = "_right"
            for arg_names, args in zip(ctx.arg_names, ctx.args):
                if not arg_names:
                    continue
                for name, expr in zip(arg_names, args):
                    if name == "on":
                        vals = _literal_str_list(expr)
                        if vals is not None:
                            on_keys.update(vals)
                    elif name == "suffix":
                        lit = _literal_str(expr)
                        if lit is not None:
                            suffix = lit
            for name, typ in right_fields.items():
                if name in on_keys and name in fields:
                    continue
                if name in fields:
                    fields[f"{name}{suffix}"] = typ
                else:
                    fields[name] = typ

    elif method == "agg":
        # Recover exact group keys from chained calls like:
        # users.group_by("id").agg(...)
        # users.group_by_dynamic("ts", by=["id"]).agg(...)
        call_expr = cast(Any, ctx.context)
        callee = cast(Any, getattr(call_expr, "callee", None))
        group_call = (
            cast(Any, callee.expr)
            if isinstance(callee, MemberExpr) and hasattr(callee, "expr")
            else None
        )
        group_member = (
            cast(Any, getattr(group_call, "callee", None))
            if group_call is not None
            else None
        )
        group_method = (
            group_member.name if isinstance(group_member, MemberExpr) else None
        )
        if group_method in {"group_by", "group_by_dynamic"}:
            grouped_keys = set()
            group_args = cast(list[Expression], getattr(group_call, "args", []))
            group_kinds = cast(list[Any], getattr(group_call, "arg_kinds", []))
            group_names = cast(list[str | None], getattr(group_call, "arg_names", []))
            for expr, g_kind, name in zip(group_args, group_kinds, group_names):
                if group_method == "group_by_dynamic":
                    # index_column (first positional) and by=[...]
                    if g_kind == ARG_POS and name is None:
                        vals = _literal_str_list(expr)
                        if vals is not None:
                            grouped_keys.update(vals)
                    elif name == "by":
                        vals = _literal_str_list(expr)
                        if vals is not None:
                            grouped_keys.update(vals)
                else:
                    if g_kind == ARG_POS:
                        vals = _literal_str_list(expr)
                        if vals is not None:
                            grouped_keys.update(vals)
        agg_input_cols: set[str] = set()
        agg_outputs: dict[str, Type] = {}
        for args, kinds, names in zip(ctx.args, ctx.arg_kinds, ctx.arg_names):
            for expr, kind, name in zip(args, kinds, names):
                if kind != ARG_NAMED or name is None:
                    continue
                if not isinstance(expr, TupleExpr) or len(expr.items) != 2:
                    agg_outputs[name] = AnyType(TypeOfAny.special_form)
                    continue
                op_expr, col_expr = expr.items
                op = _literal_str(op_expr)
                col_agg: str | None = _literal_str(col_expr)
                if col_agg is not None:
                    agg_input_cols.add(col_agg)
                if op == "count":
                    agg_outputs[name] = cast(Any, ctx.api).named_type("builtins.int")
                elif op in {"mean", "std", "var"}:
                    agg_outputs[name] = cast(Any, ctx.api).named_type("builtins.float")
                elif col_agg is not None and col_agg in fields:
                    agg_outputs[name] = fields[col_agg]
                else:
                    agg_outputs[name] = AnyType(TypeOfAny.special_form)
        grouped_fields: dict[str, Type] = {}
        if grouped_keys:
            grouped_fields = {
                col_name: col_type
                for col_name, col_type in fields.items()
                if col_name in grouped_keys
            }
        else:
            grouped_fields = {
                col_name: col_type
                for col_name, col_type in fields.items()
                if col_name not in agg_input_cols
            }
        fields = {**grouped_fields, **agg_outputs}

    elif method in {"fill_null", "drop_nulls", "explode", "unnest"}:
        # Schema-preserving transforms: keep the same field set/types.
        pass

    elif method in {"melt", "unpivot"}:
        # Best-effort inference when id/index columns are provided as literals.
        id_keys: list[str] | None = None
        variable_name: str = "variable"
        value_name: str = "value"

        for arg_names, args in zip(ctx.arg_names, ctx.args):
            if not arg_names:
                continue
            for name, expr in zip(arg_names, args):
                if method == "melt":
                    if name == "id_vars":
                        id_keys = _literal_str_list(expr)
                    elif name == "variable_name":
                        lit = _literal_str(expr)
                        if lit is not None:
                            variable_name = lit
                    elif name == "value_name":
                        lit = _literal_str(expr)
                        if lit is not None:
                            value_name = lit
                else:
                    if name == "index":
                        id_keys = _literal_str_list(expr)
                    elif name == "variable_name":
                        lit = _literal_str(expr)
                        if lit is not None:
                            variable_name = lit
                    elif name == "value_name":
                        lit = _literal_str(expr)
                        if lit is not None:
                            value_name = lit

        if id_keys is not None:
            out: dict[str, Type] = {}
            for k in id_keys:
                out[k] = fields.get(k, AnyType(TypeOfAny.special_form))
            out[variable_name] = cast(Any, ctx.api).named_type("builtins.str")
            out[value_name] = AnyType(TypeOfAny.special_form)
            fields = out

    elif method == "rolling_agg":
        out_name: str | None = None
        op_lit: str | None = None
        col_lit: str | None = None
        for arg_names, args in zip(ctx.arg_names, ctx.args):
            if not arg_names:
                continue
            for name, expr in zip(arg_names, args):
                if name == "out_name":
                    out_name = _literal_str(expr)
                elif name == "op":
                    op_lit = _literal_str(expr)
                elif name == "column":
                    col_lit = _literal_str(expr)
        if out_name is not None:
            if op_lit == "count":
                out_type = cast(Any, ctx.api).named_type("builtins.int")
            elif op_lit in {"mean", "std", "var"}:
                out_type = cast(Any, ctx.api).named_type("builtins.float")
            elif col_lit is not None and col_lit in fields:
                out_type = fields[col_lit]
            else:
                out_type = AnyType(TypeOfAny.special_form)
            fields[out_name] = out_type

    return _resolve_matching_model(ctx, fields, fallback=fallback)


class PydantableMypyPlugin(Plugin):
    def get_method_hook(self, fullname: str) -> Callable[[MethodContext], Type] | None:
        method = fullname.rsplit(".", 1)[-1]
        if method in _HOOK_NAMES:
            def _typed_hook(ctx: MethodContext, *, _method: str = method) -> Type:
                return _hook(ctx, _method)

            return _typed_hook
        return None


def plugin(version: str) -> type[Plugin]:
    return PydantableMypyPlugin
