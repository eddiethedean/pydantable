from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from planframe.backend.adapter import BaseAdapter

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

    def drop(self, df: Any, columns: tuple[str, ...], *, strict: bool = True) -> Any:
        return df.drop(*columns, strict=strict)

    def rename(self, df: Any, mapping: dict[str, str]) -> Any:
        return df.rename(mapping)

    def with_column(self, df: Any, name: str, expr: Any) -> Any:
        from pydantable.planframe_adapter.expr import compiled_to_pydantable_expr

        pexpr = compiled_to_pydantable_expr(expr, df=df)
        return df.with_columns(**{name: pexpr})

    def cast(self, df: Any, name: str, dtype: Any) -> Any:
        # Prefer a cast expression; keep narrow to avoid selector re-implementation.
        return df.with_columns(**{name: df.col(name).cast(dtype)})

    def filter(self, df: Any, predicate: Any) -> Any:
        from pydantable.planframe_adapter.expr import compiled_to_pydantable_expr

        pexpr = compiled_to_pydantable_expr(predicate, df=df)
        return df.filter(pexpr)

    def sort(
        self,
        df: Any,
        columns: tuple[str, ...],
        *,
        descending: tuple[bool, ...],
        nulls_last: tuple[bool, ...],
    ) -> Any:
        return df.sort(
            *columns,
            descending=list(descending),
            nulls_last=list(nulls_last),
        )

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
        keys: tuple[str, ...],
        named_aggs: dict[str, tuple[str, str]],
    ) -> Any:
        grouped = df.group_by(*keys)
        # PlanFrame `Agg.named_aggs` is `{out_name: (op, input_column_name)}`.
        return grouped.agg(**{out: (op, col) for out, (op, col) in named_aggs.items()})

    def drop_nulls(self, df: Any, subset: tuple[str, ...] | None) -> Any:
        return df.drop_nulls(subset=None if subset is None else list(subset))

    def fill_null(self, df: Any, value: Any, subset: tuple[str, ...] | None) -> Any:
        return df.fill_null(value, subset=None if subset is None else list(subset))

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
        left_on: tuple[str, ...],
        right_on: tuple[str, ...],
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
        return left.join(
            right,
            left_on=list(left_on),
            right_on=list(right_on),
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
        return left.concat_vertical(right)

    def concat_horizontal(self, left: Any, right: Any) -> Any:
        return left.concat_horizontal(right)

    def pivot(
        self,
        df: Any,
        *,
        index: tuple[str, ...],
        on: str,
        values: str,
        agg: str = "first",
        on_columns: tuple[str, ...] | None = None,
        separator: str = "_",
    ) -> Any:
        return df.pivot(
            index=list(index),
            on=on,
            values=values,
            aggregate_fn=agg,
            sort_columns=False,
            separator=separator,
            # pydantable pivot doesn't expose on_columns as of 1.16.0
        )

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

    def explode(self, df: Any, column: str) -> Any:
        return df.explode(column)

    def unnest(self, df: Any, column: str) -> Any:
        return df.unnest(column)

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

    def compile_expr(self, expr: Any) -> Any:
        from pydantable.planframe_adapter.expr import compile_expr

        return compile_expr(expr)

    def collect(self, df: Any) -> Any:
        # PlanFrame expects `.collect` to return a backend frame; for pydantable the
        # "collected frame" is a materialized column dict, which we can wrap back
        # into a DataFrame of the same schema.
        d = df.to_dict()
        schema_t = cast("type", df.schema_type)
        return df.__class__[schema_t](d)

    def to_dicts(self, df: Any) -> list[dict[str, object]]:
        d = df.to_dict()
        if not d:
            return []
        keys = list(d.keys())
        n = len(next(iter(d.values())))
        return [dict(zip(keys, (d[k][i] for k in keys), strict=True)) for i in range(n)]

    def to_dict(self, df: Any) -> dict[str, list[object]]:
        return cast("dict[str, list[object]]", df.to_dict())
