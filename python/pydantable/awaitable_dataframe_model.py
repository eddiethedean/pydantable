"""Lazy async chain for :class:`~pydantable.dataframe_model.DataFrameModel` reads.

``aread_*`` (or ``MyModel.Async.read_*``) returns :class:`AwaitableDataFrameModel`.
The Polars **plan stays lazy** until you call a terminal (``await …collect()``,
``await`` the whole chain for a concrete model, etc.). **Opening** the file and
building the scan root may run in :func:`asyncio.to_thread` or an ``executor=`` (see
the **Execution** doc); **materialization** uses the same async path as
:meth:`~pydantable.dataframe_model.DataFrameModel.acollect` on the resolved model.

**Extras**

- **Lazy plan metadata:** ``await adf.columns`` / ``shape`` / ``empty`` / ``dtypes``
  (schema / scan shape; no row collect — for file scans, row count may be zero until
  materialization; see **DATAFRAMEMODEL**, section *Three layers*).
- **Custom step:** :meth:`then` — sync or async callable on the resolved model.
- **Multi-frame:** :meth:`concat` — several models or chains; ``how=`` like
  :meth:`DataFrameModel.concat`.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Sequence
    from concurrent.futures import Executor

GroupedT = TypeVar("GroupedT")
RowT = TypeVar("RowT")

_REPR_LABEL_MAX = 200


def _short_repr_label(text: str, *, max_len: int = _REPR_LABEL_MAX) -> str:
    """Single-line label for :meth:`AwaitableDataFrameModel.__repr__`."""

    collapsed = " ".join(text.split())
    if len(collapsed) <= max_len:
        return collapsed
    return collapsed[: max_len - 3] + "..."


# Methods that return another lazy DataFrameModel (chainable).
_CHAIN: frozenset[str] = frozenset(
    {
        "select",
        "with_columns",
        "filter",
        "sort",
        "unique",
        "distinct",
        "drop",
        "rename",
        "slice",
        "head",
        "tail",
        "fill_null",
        "drop_nulls",
        "melt",
        "unpivot",
        "pivot",
        "explode",
        "unnest",
        "rolling_agg",
    }
)

# Blocking terminal or side-effect APIs — must await the chain to a model first.
_FORBIDDEN_SYNC: frozenset[str] = frozenset(
    {
        "collect_batches",
        "write_parquet",
        "write_csv",
        "write_ipc",
        "write_ndjson",
        "write_parquet_batches",
        "write_ipc_batches",
        "write_csv_batches",
        "write_ndjson_batches",
        "export_parquet",
        "export_csv",
        "export_ndjson",
        "export_ipc",
        "export_json",
        "write_sql",
    }
)

# DataFrameModel @property names — must await without ``()`` (see __getattr__).
_PROPERTY_NAMES: frozenset[str] = frozenset({"columns", "shape", "empty", "dtypes"})


class AwaitableDataFrameModel(Generic[RowT]):
    """Chainable awaitable wrapper around a coroutine that yields a DataFrameModel.

    :meth:`collect` / :meth:`to_dict` / :meth:`acollect` / … resolve the inner model
    first, then delegate to the same async materialization stack as
    :class:`~pydantable.dataframe_model.DataFrameModel` (Rust async bridge or thread
    pool; see the **Execution** documentation). Pending chains expose a readable
    :meth:`__repr__` when the entrypoint set ``repr_label`` (e.g.
    ``UserDF.aread_parquet('/path')``).
    """

    __slots__ = ("_get_df", "_repr_label")

    def __init__(
        self,
        get_df: Callable[[], Coroutine[Any, Any, Any]],
        *,
        repr_label: str | None = None,
    ) -> None:
        self._get_df = get_df
        self._repr_label = repr_label

    def _chain_repr(self, suffix: str) -> str:
        base = self._repr_label or "AwaitableDataFrameModel"
        return _short_repr_label(f"{base}{suffix}")

    def __await__(self) -> Any:
        return self._get_df().__await__()

    def __repr__(self) -> str:
        if self._repr_label:
            return f"<AwaitableDataFrameModel pending: {self._repr_label}>"
        return "<AwaitableDataFrameModel (pending lazy DataFrameModel)>"

    @classmethod
    def concat(
        cls,
        *frames: Any,
        how: str = "vertical",
    ) -> AwaitableDataFrameModel[Any]:
        """Concatenate two or more frames or :class:`AwaitableDataFrameModel` chains.

        Resolves each argument (awaiting pending chains), then calls
        ``type(first).concat(resolved, how=how)``.
        """

        async def _inner() -> Any:
            resolved: list[Any] = []
            for f in frames:
                if isinstance(f, AwaitableDataFrameModel):
                    resolved.append(await f._get_df())
                else:
                    resolved.append(f)
            if len(resolved) < 2:
                raise ValueError(
                    "AwaitableDataFrameModel.concat requires at least two frames"
                )
            model_cls = type(resolved[0])
            return model_cls.concat(resolved, how=how)

        return AwaitableDataFrameModel(
            _inner,
            repr_label=_short_repr_label(
                f"AwaitableDataFrameModel.concat(how={how!r})"
            ),
        )

    def then(
        self,
        fn: Callable[[Any], Any],
    ) -> AwaitableDataFrameModel[Any]:
        """Apply ``fn(df)`` after the chain resolves; extend the lazy pipeline.

        ``fn`` may return a :class:`~pydantable.dataframe_model.DataFrameModel`,
        another :class:`AwaitableDataFrameModel`, or an awaitable that resolves to
        a model.
        """

        async def _inner() -> Any:
            df = await self._get_df()
            out = fn(df)
            if isinstance(out, AwaitableDataFrameModel):
                return await out._get_df()
            if inspect.isawaitable(out):
                out = await out
            return out

        return AwaitableDataFrameModel(
            _inner,
            repr_label=self._chain_repr(".then(...)"),
        )

    def join(
        self,
        other: Any,
        *,
        on: str | Sequence[str] | None = None,
        left_on: Any = None,
        right_on: Any = None,
        how: str = "inner",
        suffix: str = "_right",
    ) -> AwaitableDataFrameModel[Any]:
        async def _inner() -> Any:
            df = await self._get_df()
            if isinstance(other, AwaitableDataFrameModel):
                other_df = await other._get_df()
            else:
                other_df = other
            return df.join(
                other_df,
                on=on,
                left_on=left_on,
                right_on=right_on,
                how=how,
                suffix=suffix,
            )

        return AwaitableDataFrameModel(
            _inner,
            repr_label=self._chain_repr(".join(...)"),
        )

    def group_by(self, *keys: Any) -> AwaitableGroupedDataFrameModel[Any]:
        async def _inner() -> Any:
            df = await self._get_df()
            return df.group_by(*keys)

        return AwaitableGroupedDataFrameModel(
            _inner,
            repr_label=self._chain_repr(".group_by(...)"),
        )

    def group_by_dynamic(
        self,
        index_column: str,
        *,
        every: str,
        period: str | None = None,
        by: Sequence[str] | None = None,
    ) -> AwaitableDynamicGroupedDataFrameModel[Any]:
        async def _inner() -> Any:
            df = await self._get_df()
            return df.group_by_dynamic(index_column, every=every, period=period, by=by)

        return AwaitableDynamicGroupedDataFrameModel(
            _inner,
            repr_label=self._chain_repr(".group_by_dynamic(...)"),
        )

    def acollect(
        self,
        *,
        as_lists: bool = False,
        as_numpy: bool = False,
        as_polars: bool | None = None,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Coroutine[Any, Any, Any]:
        async def _run() -> Any:
            df = await self._get_df()
            return await df.acollect(
                as_lists=as_lists,
                as_numpy=as_numpy,
                as_polars=as_polars,
                streaming=streaming,
                engine_streaming=engine_streaming,
                executor=executor,
            )

        return _run()

    def ato_dict(
        self,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Coroutine[Any, Any, Any]:
        async def _run() -> Any:
            df = await self._get_df()
            return await df.ato_dict(
                streaming=streaming,
                engine_streaming=engine_streaming,
                executor=executor,
            )

        return _run()

    def ato_polars(
        self,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Coroutine[Any, Any, Any]:
        async def _run() -> Any:
            df = await self._get_df()
            return await df.ato_polars(
                streaming=streaming,
                engine_streaming=engine_streaming,
                executor=executor,
            )

        return _run()

    def ato_arrow(
        self,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Coroutine[Any, Any, Any]:
        async def _run() -> Any:
            df = await self._get_df()
            return await df.ato_arrow(
                streaming=streaming,
                engine_streaming=engine_streaming,
                executor=executor,
            )

        return _run()

    def arows(
        self,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Coroutine[Any, Any, Any]:
        async def _run() -> Any:
            df = await self._get_df()
            return await df.arows(
                streaming=streaming,
                engine_streaming=engine_streaming,
                executor=executor,
            )

        return _run()

    def ato_dicts(
        self,
        *,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Coroutine[Any, Any, Any]:
        async def _run() -> Any:
            df = await self._get_df()
            return await df.ato_dicts(
                streaming=streaming,
                engine_streaming=engine_streaming,
                executor=executor,
            )

        return _run()

    def collect(self, **kwargs: Any) -> Coroutine[Any, Any, Any]:
        """Async materialization (alias of :meth:`acollect`)."""

        return self.acollect(**kwargs)

    def to_dict(self, **kwargs: Any) -> Coroutine[Any, Any, Any]:
        """Async column dict (alias of :meth:`ato_dict`)."""

        return self.ato_dict(**kwargs)

    def to_polars(self, **kwargs: Any) -> Coroutine[Any, Any, Any]:
        """Async Polars export (alias of :meth:`ato_polars`)."""

        return self.ato_polars(**kwargs)

    def to_arrow(self, **kwargs: Any) -> Coroutine[Any, Any, Any]:
        """Async Arrow export (alias of :meth:`ato_arrow`)."""

        return self.ato_arrow(**kwargs)

    def rows(self, **kwargs: Any) -> Coroutine[Any, Any, Any]:
        """Async row models (alias of :meth:`arows`)."""

        return self.arows(**kwargs)

    def to_dicts(self, **kwargs: Any) -> Coroutine[Any, Any, Any]:
        """Async row dicts (alias of :meth:`ato_dicts`)."""

        return self.ato_dicts(**kwargs)

    def stream(self, **kwargs: Any) -> Any:
        """Async batch iterator (alias of :meth:`astream`)."""

        return self.astream(**kwargs)

    def submit(
        self,
        *,
        as_lists: bool = False,
        as_numpy: bool = False,
        as_polars: bool | None = None,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Coroutine[Any, Any, Any]:
        async def _run() -> Any:
            df = await self._get_df()
            return df.submit(
                as_lists=as_lists,
                as_numpy=as_numpy,
                as_polars=as_polars,
                streaming=streaming,
                engine_streaming=engine_streaming,
                executor=executor,
            )

        return _run()

    def astream(
        self,
        *,
        batch_size: int = 65_536,
        streaming: bool | None = None,
        engine_streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> Any:
        async def _gen() -> Any:
            df = await self._get_df()
            async for batch in df.astream(
                batch_size=batch_size,
                streaming=streaming,
                engine_streaming=engine_streaming,
                executor=executor,
            ):
                yield batch

        return _gen()

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(
                f"{type(self).__name__!r} object has no attribute {name!r}"
            )
        if name in _FORBIDDEN_SYNC:
            raise TypeError(
                f"{type(self).__name__}: cannot call {name}() on a pending async "
                f"chain; await the chain to a model first, or use async terminals "
                f"(e.g. await …collect() / to_dict(), or acollect / ato_dict). "
                f"For eager file export from column dicts, use "
                f"MyModel.Async.export_* or await MyModel.aexport_* — not on a chain."
            )
        if name in _CHAIN:

            def bound(*args: Any, **kwargs: Any) -> AwaitableDataFrameModel[Any]:
                async def _inner() -> Any:
                    df = await self._get_df()
                    return getattr(df, name)(*args, **kwargs)

                return AwaitableDataFrameModel(
                    _inner,
                    repr_label=self._chain_repr(f".{name}(...)"),
                )

            return bound

        if name in _PROPERTY_NAMES:

            async def _property_coro() -> Any:
                df = await self._get_df()
                return getattr(df, name)

            return _property_coro()

        def any_method(*args: Any, **kwargs: Any) -> Coroutine[Any, Any, Any]:
            async def _run() -> Any:
                df = await self._get_df()
                return getattr(df, name)(*args, **kwargs)

            return _run()

        return any_method


class AwaitableGroupedDataFrameModel(Generic[GroupedT]):
    __slots__ = ("_get_g", "_repr_label")

    def __init__(
        self,
        get_g: Callable[[], Coroutine[Any, Any, Any]],
        *,
        repr_label: str | None = None,
    ) -> None:
        self._get_g = get_g
        self._repr_label = repr_label

    def __repr__(self) -> str:
        if self._repr_label:
            return f"<AwaitableGroupedDataFrameModel pending: {self._repr_label}>"
        return "<AwaitableGroupedDataFrameModel (pending)>"

    def agg(self, **aggregations: Any) -> AwaitableDataFrameModel[Any]:
        async def _inner() -> Any:
            g = await self._get_g()
            return g.agg(**aggregations)

        base = self._repr_label or "AwaitableGroupedDataFrameModel"
        return AwaitableDataFrameModel(
            _inner,
            repr_label=_short_repr_label(f"{base}.agg(...)"),
        )


class AwaitableDynamicGroupedDataFrameModel(Generic[GroupedT]):
    __slots__ = ("_get_g", "_repr_label")

    def __init__(
        self,
        get_g: Callable[[], Coroutine[Any, Any, Any]],
        *,
        repr_label: str | None = None,
    ) -> None:
        self._get_g = get_g
        self._repr_label = repr_label

    def __repr__(self) -> str:
        if self._repr_label:
            return (
                f"<AwaitableDynamicGroupedDataFrameModel pending: {self._repr_label}>"
            )
        return "<AwaitableDynamicGroupedDataFrameModel (pending)>"

    def agg(self, **aggregations: Any) -> AwaitableDataFrameModel[Any]:
        async def _inner() -> Any:
            g = await self._get_g()
            return g.agg(**aggregations)

        base = self._repr_label or "AwaitableDynamicGroupedDataFrameModel"
        return AwaitableDataFrameModel(
            _inner,
            repr_label=_short_repr_label(f"{base}.agg(...)"),
        )
