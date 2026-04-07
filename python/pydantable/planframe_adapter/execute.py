from __future__ import annotations

from typing import Any

from pydantable.planframe_adapter.errors import require_planframe


def execute_plan(*, adapter: Any, plan: Any, source: Any) -> Any:
    """
    Execute a PlanFrame plan via the adapter and return the backend *lazy frame*.

    For pydantable, the backend frame is a `pydantable.dataframe.DataFrame` which
    remains lazy; this function *does not materialize*.
    """

    require_planframe()
    from planframe.plan import nodes as n

    def _eval(node: Any) -> Any:
        if isinstance(node, n.Source):
            return source
        if isinstance(node, n.Select):
            return adapter.select(_eval(node.prev), node.columns)
        if isinstance(node, n.Drop):
            return adapter.drop(_eval(node.prev), node.columns, strict=node.strict)
        if isinstance(node, n.Rename):
            return adapter.rename(_eval(node.prev), node.mapping)
        if isinstance(node, n.WithColumn):
            prev = _eval(node.prev)
            bexpr = adapter.compile_expr(node.expr)
            return adapter.with_column(prev, node.name, bexpr)
        if isinstance(node, n.Cast):
            return adapter.cast(_eval(node.prev), node.name, node.dtype)
        if isinstance(node, n.Filter):
            prev = _eval(node.prev)
            bexpr = adapter.compile_expr(node.predicate)
            return adapter.filter(prev, bexpr)
        if isinstance(node, n.Sort):
            return adapter.sort(
                _eval(node.prev),
                node.columns,
                descending=node.descending,
                nulls_last=node.nulls_last,
            )
        if isinstance(node, n.Unique):
            return adapter.unique(
                _eval(node.prev),
                node.subset,
                keep=node.keep,
                maintain_order=node.maintain_order,
            )
        if isinstance(node, n.Duplicated):
            return adapter.duplicated(
                _eval(node.prev),
                node.subset,
                keep=node.keep,
                out_name=node.out_name,
            )
        if isinstance(node, n.GroupBy):
            return _eval(node.prev)
        if isinstance(node, n.Agg):
            if not isinstance(node.prev, n.GroupBy):
                raise TypeError("PlanFrame Agg must follow GroupBy")
            return adapter.group_by_agg(
                _eval(node.prev.prev),
                keys=node.prev.keys,
                named_aggs=node.named_aggs,
            )
        if isinstance(node, n.DropNulls):
            return adapter.drop_nulls(_eval(node.prev), node.subset)
        if isinstance(node, n.FillNull):
            return adapter.fill_null(_eval(node.prev), node.value, node.subset)
        if isinstance(node, n.Melt):
            return adapter.melt(
                _eval(node.prev),
                id_vars=node.id_vars,
                value_vars=node.value_vars,
                variable_name=node.variable_name,
                value_name=node.value_name,
            )
        if isinstance(node, n.Join):
            right = node.right
            # PlanFrame Join stores the right Frame object, not a PlanNode.
            right_source = getattr(right, "_data", None)
            right_plan = getattr(right, "_plan", None)
            right_adapter = getattr(right, "_adapter", None)
            if right_source is None or right_plan is None or right_adapter is None:
                raise TypeError("PlanFrame Join right frame is invalid")
            if right_adapter.name != adapter.name:
                raise TypeError("Cannot join frames from different backends")
            right_df = execute_plan(
                adapter=adapter, plan=right_plan, source=right_source
            )
            return adapter.join(
                _eval(node.prev),
                right_df,
                left_on=node.left_keys,
                right_on=node.right_keys,
                how=node.how,
                suffix=node.suffix,
                options=node.options,
            )
        if isinstance(node, n.Slice):
            return adapter.slice(
                _eval(node.prev), offset=node.offset, length=node.length
            )
        if isinstance(node, n.Head):
            return adapter.head(_eval(node.prev), node.n)
        if isinstance(node, n.Tail):
            return adapter.tail(_eval(node.prev), node.n)
        if isinstance(node, n.ConcatVertical):
            other = getattr(node.other, "_data", None)
            other_plan = getattr(node.other, "_plan", None)
            other_adapter = getattr(node.other, "_adapter", None)
            if other is None or other_plan is None or other_adapter is None:
                raise TypeError("PlanFrame concat other frame is invalid")
            if other_adapter.name != adapter.name:
                raise TypeError("Cannot concat frames from different backends")
            other_df = execute_plan(adapter=adapter, plan=other_plan, source=other)
            return adapter.concat_vertical(_eval(node.prev), other_df)
        if isinstance(node, n.ConcatHorizontal):
            other = getattr(node.other, "_data", None)
            other_plan = getattr(node.other, "_plan", None)
            other_adapter = getattr(node.other, "_adapter", None)
            if other is None or other_plan is None or other_adapter is None:
                raise TypeError("PlanFrame concat other frame is invalid")
            if other_adapter.name != adapter.name:
                raise TypeError("Cannot concat frames from different backends")
            other_df = execute_plan(adapter=adapter, plan=other_plan, source=other)
            return adapter.concat_horizontal(_eval(node.prev), other_df)
        if isinstance(node, n.Pivot):
            return adapter.pivot(
                _eval(node.prev),
                index=node.index,
                on=node.on,
                values=node.values,
                agg=node.agg,
                on_columns=node.on_columns,
                separator=node.separator,
            )
        if isinstance(node, n.Explode):
            return adapter.explode(_eval(node.prev), node.column)
        if isinstance(node, n.Unnest):
            return adapter.unnest(_eval(node.prev), node.column)
        if isinstance(node, n.DropNullsAll):
            return adapter.drop_nulls_all(_eval(node.prev), node.subset)
        if isinstance(node, n.Sample):
            return adapter.sample(
                _eval(node.prev),
                n=node.n,
                frac=node.frac,
                with_replacement=node.with_replacement,
                shuffle=node.shuffle,
                seed=node.seed,
            )
        raise NotImplementedError(
            f"Unsupported PlanFrame plan node: {type(node).__name__}"
        )

    return _eval(plan)


def execute_frame(frame: Any) -> Any:
    """
    Compile a PlanFrame `Frame` into the backend lazy frame.
    """

    require_planframe()
    adapter = getattr(frame, "_adapter", None)
    plan = getattr(frame, "_plan", None)
    data = getattr(frame, "_data", None)
    if adapter is None or plan is None or data is None:
        raise TypeError("Invalid PlanFrame Frame object.")
    return execute_plan(adapter=adapter, plan=plan, source=data)
