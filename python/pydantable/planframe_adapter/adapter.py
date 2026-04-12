from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from planframe.backend.adapter import BaseAdapter, CompileExprContext

from pydantable.expressions import Expr as PydExpr
from pydantable.planframe_adapter.errors import require_planframe


@dataclass(frozen=True)
class PydantableAdapter(BaseAdapter[Any, Any]):
    """
    PlanFrame adapter targeting the existing pydantable Rust/native engine.

    This adapter delegates plan-node execution to the existing `pydantable.DataFrame`
    API (which itself targets the Rust/native execution backend).
    """

    engine: Any

    def __post_init__(self) -> None:
        require_planframe()

    name: str = "pydantable"

    # ---- plan node operations (delegate to pydantable.DataFrame) ----

    def select(self, df: Any, columns: tuple[str, ...]) -> Any:
        return df.select(*columns)

    def project(self, df: Any, items: tuple[Any, ...]) -> Any:
        # CompiledProjectItem(name=..., from_column=... | expr=...)
        # → select + with_columns
        computed: dict[str, Any] = {}
        order: list[str] = []
        for it in items:
            name = getattr(it, "name", None)
            from_col = getattr(it, "from_column", None)
            expr = getattr(it, "expr", None)
            if not isinstance(name, str):
                raise TypeError("Invalid project item: missing name.")
            if from_col is not None:
                order.append(str(from_col))
                continue
            if expr is None:
                raise TypeError("Invalid project item: needs from_column or expr.")
            computed[name] = expr
            order.append(name)
        out = df
        if computed:
            out = out.with_columns(**computed)
        return out.select(*order)

    def drop(self, df: Any, columns: tuple[str, ...], *, strict: bool = True) -> Any:
        return df.drop(*columns, strict=strict)

    def rename(self, df: Any, mapping: dict[str, str], *, strict: bool = True) -> Any:
        return df.rename(mapping, strict=strict)

    def with_column(self, df: Any, name: str, expr: Any) -> Any:
        return df.with_columns(**{name: expr})

    def cast(self, df: Any, name: str, dtype: Any) -> Any:
        # Prefer a cast expression; keep narrow to avoid selector re-implementation.
        return df.with_columns(**{name: df.col(name).cast(dtype)})

    def with_row_count(self, df: Any, *, name: str = "row_nr", offset: int = 0) -> Any:
        return df.with_row_count(name=name, offset=offset)

    def filter(self, df: Any, predicate: Any) -> Any:
        return df.filter(predicate)

    def sort(
        self,
        df: Any,
        keys: tuple[Any, ...],
        *,
        descending: tuple[bool, ...],
        nulls_last: tuple[bool, ...],
    ) -> Any:
        cols: list[str] = []
        for k in keys:
            col = getattr(k, "column", None)
            expr = getattr(k, "expr", None)
            if col is not None:
                cols.append(str(col))
            elif expr is not None:
                # pydantable engine sort supports column names or ColumnRef only.
                # For now, require PlanFrame expr sort keys to compile to ColumnRef.
                referenced = expr.referenced_columns()
                if len(referenced) != 1:
                    raise TypeError("sort expr keys must reference exactly one column.")
                cols.append(next(iter(referenced)))
            else:
                raise TypeError("Invalid sort key: expected column or expr.")
        return df.sort(*cols, descending=list(descending), nulls_last=list(nulls_last))

    def unique(
        self,
        df: Any,
        subset: tuple[str, ...] | None,
        *,
        keep: str = "first",
        maintain_order: bool = False,
    ) -> Any:
        return df.unique(
            None if subset is None else list(subset),
            keep=keep,
            maintain_order=maintain_order,
        )

    def duplicated(
        self,
        df: Any,
        subset: tuple[str, ...] | None,
        *,
        keep: str | bool = "first",
        out_name: str = "duplicated",
    ) -> Any:
        # pydantable names the output column `duplicated`; PlanFrame allows overriding.
        out = df.duplicated(None if subset is None else list(subset), keep=keep)
        if out_name != "duplicated":
            out = out.rename(duplicated=out_name)
        return out

    def group_by_agg(
        self,
        df: Any,
        *,
        keys: tuple[Any, ...],
        named_aggs: dict[str, Any],
    ) -> Any:
        group_cols: list[str] = []
        for k in keys:
            col = getattr(k, "column", None)
            expr = getattr(k, "expr", None)
            if col is not None:
                group_cols.append(str(col))
            elif expr is not None:
                referenced = expr.referenced_columns()
                if len(referenced) != 1:
                    raise TypeError(
                        "group_by expr keys must reference exactly one column."
                    )
                group_cols.append(next(iter(referenced)))
            else:
                raise TypeError("Invalid group_by key: expected column or expr.")
        grouped = df.group_by(*group_cols)
        out_aggs: dict[str, Any] = {}
        for out_name, spec in named_aggs.items():
            if isinstance(spec, tuple) and len(spec) == 2:
                op, col = spec
                out_aggs[out_name] = (op, col)
            else:
                out_aggs[out_name] = spec
        return grouped.agg(**out_aggs)

    def group_by_dynamic_agg(
        self,
        df: Any,
        *,
        index_column: str,
        every: str,
        period: str | None = None,
        by: tuple[str, ...] | None = None,
        named_aggs: dict[str, Any],
    ) -> Any:
        grouped = df.group_by_dynamic(index_column, every=every, period=period, by=by)
        normalized: dict[str, Any] = {}
        for out_name, spec in named_aggs.items():
            if isinstance(spec, tuple) and len(spec) == 2:
                op, col = spec
                if isinstance(col, PydExpr):
                    refs = col.referenced_columns()
                    if len(refs) != 1:
                        raise TypeError(
                            "dynamic group_by aggregation expression must reference "
                            "exactly one column."
                        )
                    normalized[out_name] = (op, next(iter(refs)))
                else:
                    normalized[out_name] = (op, col)
            else:
                normalized[out_name] = spec
        return grouped.agg(**normalized)

    def rolling_agg(
        self,
        df: Any,
        *,
        on: str,
        column: str,
        window_size: int | str,
        op: str,
        out_name: str,
        by: tuple[str, ...] | None = None,
        min_periods: int = 1,
    ) -> Any:
        return df.rolling_agg(
            on=on,
            column=column,
            window_size=window_size,
            op=op,
            out_name=out_name,
            by=None if by is None else list(by),
            min_periods=min_periods,
        )

    def drop_nulls(
        self,
        df: Any,
        subset: tuple[str, ...] | None,
        *,
        how: str = "any",
        threshold: int | None = None,
    ) -> Any:
        return df.drop_nulls(
            subset=None if subset is None else list(subset),
            how=how,
            threshold=threshold,
        )

    def fill_null(
        self,
        df: Any,
        value: Any,
        subset: tuple[str, ...] | None,
        *,
        strategy: str | None = None,
    ) -> Any:
        return df.fill_null(
            value,
            strategy=strategy,
            subset=None if subset is None else list(subset),
        )

    def melt(
        self,
        df: Any,
        *,
        id_vars: tuple[str, ...],
        value_vars: tuple[str, ...],
        variable_name: str,
        value_name: str,
    ) -> Any:
        return df.melt(
            id_vars=list(id_vars),
            value_vars=list(value_vars),
            variable_name=variable_name,
            value_name=value_name,
        )

    def join(
        self,
        left: Any,
        right: Any,
        *,
        left_on: tuple[Any, ...],
        right_on: tuple[Any, ...],
        how: str = "inner",
        suffix: str = "_right",
        options: Any | None = None,
    ) -> Any:
        kw: dict[str, Any] = {"how": how, "suffix": suffix}
        if options is not None:
            if options.coalesce is not None:
                kw["coalesce"] = options.coalesce
            if options.validate is not None:
                kw["validate"] = options.validate
            if options.join_nulls is not None:
                kw["join_nulls"] = options.join_nulls
            if options.maintain_order is not None:
                kw["maintain_order"] = options.maintain_order
            if options.streaming is not None:
                kw["streaming"] = options.streaming
        if how == "cross":
            return left.join(right, **kw)
        # pydantable join expects string columns; PlanFrame may provide expr keys.
        lk: list[str] = []
        rk: list[str] = []
        for k in left_on:
            col = getattr(k, "column", None)
            expr = getattr(k, "expr", None)
            if col is not None:
                lk.append(str(col))
            elif expr is not None:
                referenced = expr.referenced_columns()
                if len(referenced) != 1:
                    raise TypeError("join expr keys must reference exactly one column.")
                lk.append(next(iter(referenced)))
            else:
                raise TypeError("Invalid join key.")
        for k in right_on:
            col = getattr(k, "column", None)
            expr = getattr(k, "expr", None)
            if col is not None:
                rk.append(str(col))
            elif expr is not None:
                referenced = expr.referenced_columns()
                if len(referenced) != 1:
                    raise TypeError("join expr keys must reference exactly one column.")
                rk.append(next(iter(referenced)))
            else:
                raise TypeError("Invalid join key.")
        return left.join(
            right,
            left_on=lk,
            right_on=rk,
            **kw,
        )

    def slice(self, df: Any, *, offset: int, length: int | None) -> Any:
        if length is None:
            # pydantable currently requires a length; implement "to end" eagerly.
            d = df.to_dict()
            sliced = {k: v[offset:] for k, v in d.items()}
            schema_t = cast("type", df.schema_type)
            return df.__class__[schema_t](sliced)
        return df.slice(int(offset), int(length))

    def head(self, df: Any, n: int) -> Any:
        return df.head(n)

    def tail(self, df: Any, n: int) -> Any:
        return df.tail(n)

    def concat_vertical(self, left: Any, right: Any) -> Any:
        from pydantable.dataframe import DataFrame

        return DataFrame.concat([left, right], how="vertical")

    def concat_horizontal(self, left: Any, right: Any) -> Any:
        from pydantable.dataframe import DataFrame

        return DataFrame.concat([left, right], how="horizontal")

    def pivot(
        self,
        df: Any,
        *,
        index: tuple[str, ...],
        on: str,
        values: tuple[str, ...],
        agg: str = "first",
        on_columns: tuple[str, ...] | None = None,
        separator: str = "_",
        sort_columns: bool = False,
    ) -> Any:
        vals: Any = list(values) if len(values) != 1 else values[0]
        return df.pivot(
            index=list(index),
            columns=on,
            values=vals,
            aggregate_function=agg,
            pivot_values=None if on_columns is None else list(on_columns),
            sort_columns=sort_columns,
            separator=separator,
        )

    def explode(self, df: Any, columns: tuple[str, ...], *, outer: bool = False) -> Any:
        return df.explode(list(columns), outer=outer)

    def unnest(self, df: Any, items: tuple[Any, ...]) -> Any:
        cols = [i.column for i in items]
        return df.unnest(cols)

    def posexplode(
        self,
        df: Any,
        *,
        column: str,
        pos: str = "pos",
        value: str | None = None,
        outer: bool = False,
    ) -> Any:
        return df.posexplode(column, pos=pos, value=value, outer=outer)

    # ---- writes (delegate where supported; otherwise explicit error) ----

    def write_parquet(
        self,
        df: Any,
        path: str,
        *,
        compression: str = "zstd",
        row_group_size: int | None = None,
        partition_by: tuple[str, ...] | None = None,
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        df.write_parquet(
            path,
            compression=compression,
            row_group_size=row_group_size,
            partition_by=None if partition_by is None else list(partition_by),
            storage_options=storage_options,
        )

    def write_csv(
        self,
        df: Any,
        path: str,
        *,
        separator: str = ",",
        include_header: bool = True,
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        df.write_csv(
            path,
            separator=separator,
            include_header=include_header,
            storage_options=storage_options,
        )

    def write_ndjson(
        self, df: Any, path: str, *, storage_options: dict[str, Any] | None = None
    ) -> None:
        df.write_ndjson(path, storage_options=storage_options)

    def write_ipc(
        self,
        df: Any,
        path: str,
        *,
        compression: str = "uncompressed",
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        df.write_ipc(path, compression=compression, storage_options=storage_options)

    def write_database(
        self,
        df: Any,
        *,
        table_name: str,
        connection: Any,
        if_table_exists: str = "fail",
        engine: str | None = None,
    ) -> None:
        df.write_sql(
            table_name=table_name,
            connection=connection,
            if_table_exists=if_table_exists,
            engine=engine,
        )

    def write_excel(self, df: Any, path: str, *, worksheet: str = "Sheet1") -> None:
        df.write_excel(path, worksheet=worksheet)

    def write_delta(
        self,
        df: Any,
        target: str,
        *,
        mode: str = "error",
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        raise NotImplementedError(
            "write_delta is not implemented in pydantable adapter yet."
        )

    def write_avro(
        self, df: Any, path: str, *, compression: str = "uncompressed", name: str = ""
    ) -> None:
        raise NotImplementedError(
            "write_avro is not implemented in pydantable adapter yet."
        )

    # (legacy explode/unnest adapter hooks removed; PlanFrame 0.4 uses
    # explode(columns=..., outer=...) and unnest(items=...) instead)

    def drop_nulls_all(self, df: Any, subset: tuple[str, ...] | None) -> Any:
        # pydantable has drop_nulls_all helper
        return df.drop_nulls_all(subset=None if subset is None else list(subset))

    def sample(
        self,
        df: Any,
        *,
        n: int | None = None,
        frac: float | None = None,
        with_replacement: bool = False,
        shuffle: bool = False,
        seed: int | None = None,
    ) -> Any:
        return df.sample(
            n=n,
            frac=frac,
            with_replacement=with_replacement,
            shuffle=shuffle,
            seed=seed,
        )

    # ---- expression compilation + materialization ----

    def resolve_backend_dtype_from_frame(self, df: Any, name: str) -> object | None:
        """Return dtype annotation for *name* from a live pydantable ``DataFrame``.

        Used when PlanFrame builds ``CompileExprContext.resolve_backend_dtype`` so
        ``compile_expr`` can resolve columns on the frame missing from the step schema.
        """

        schema_fields = getattr(df, "schema_fields", None)
        if callable(schema_fields):
            fields = schema_fields()
            if isinstance(fields, dict):
                return fields.get(name)
        return None

    def compile_expr(
        self,
        expr: Any,
        *,
        schema: Any | None = None,
        ctx: CompileExprContext | None = None,
    ) -> Any:
        from pydantable.planframe_adapter.expr import compile_expr

        effective_schema = (
            schema if schema is not None else (ctx.schema if ctx is not None else None)
        )
        if effective_schema is None:
            raise TypeError("compile_expr requires schema or ctx.schema")
        compile_ctx = (
            ctx if ctx is not None else CompileExprContext(schema=effective_schema)
        )
        schema_fields = {f.name: f.dtype for f in effective_schema.fields}

        def _resolve_col(n: str) -> object | None:
            return BaseAdapter.resolve_dtype(self, n, ctx=compile_ctx)

        return compile_expr(
            expr,
            schema_fields=schema_fields,
            resolve_col=_resolve_col,
        )

    def collect(self, df: Any, *, options: Any | None = None) -> Any:
        # PlanFrame expects `.collect` to return a backend frame; for pydantable the
        # "collected frame" is a materialized column dict, which we can wrap back
        # into a DataFrame of the same schema.
        if options is None:
            d = df.to_dict()
        else:
            d = df.to_dict(
                streaming=options.streaming,
                engine_streaming=options.engine_streaming,
            )
        schema_t = cast("type", df.schema_type)
        return df.__class__[schema_t](d)

    def to_dicts(
        self, df: Any, *, options: Any | None = None
    ) -> list[dict[str, object]]:
        d = (
            df.to_dict()
            if options is None
            else df.to_dict(
                streaming=options.streaming,
                engine_streaming=options.engine_streaming,
            )
        )
        if not d:
            return []
        keys = list(d.keys())
        n = len(next(iter(d.values())))
        return [dict(zip(keys, (d[k][i] for k in keys), strict=True)) for i in range(n)]

    def to_dict(
        self, df: Any, *, options: Any | None = None
    ) -> dict[str, list[object]]:
        if options is None:
            out = df.to_dict()
        else:
            out = df.to_dict(
                streaming=options.streaming,
                engine_streaming=options.engine_streaming,
            )
        return cast("dict[str, list[object]]", out)
