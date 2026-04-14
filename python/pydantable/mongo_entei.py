"""Mongo lazy :class:`~pydantable.dataframe.DataFrame` via **entei-core** + pydantable.

**Recommended:** define collections with `Beanie <https://github.com/BeanieODM/beanie>`__
``Document`` models and use ``pip install "pydantable[mongo]"`` —
:meth:`EnteiDataFrame.from_beanie` / :meth:`EnteiDataFrameModel.from_beanie` plus
:mod:`pydantable.mongo_beanie`. Plain Pydantic :class:`~pydantable.schema.Schema`
subclasses with :meth:`EnteiDataFrame.from_collection` remain supported when you
already have a sync PyMongo ``Collection``.

**entei-core** supplies ``MongoRoot`` and columnar materialization;
:class:`EnteiPydantableEngine` lives in :mod:`pydantable.mongo_entei_engine` and
implements :class:`~pydantable.engine.protocols.ExecutionEngine` by delegating to
the native planner and scanning Mongo collections into ``dict[str, list]`` at
execution time.

This module defines :class:`EnteiDataFrame` and :class:`EnteiDataFrameModel` on the
pydantable side, mirroring :mod:`pydantable.sql_moltres` and **moltres-core**.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Literal, cast

from .dataframe import DataFrame
from .dataframe_model import DataFrameModel
from .schema import field_types_for_rust, schema_field_types


@dataclass(frozen=True, slots=True)
class BeanieAsyncRoot:
    """Async Beanie-backed root for async-only materialization.

    Unlike ``entei_core.MongoRoot`` (sync PyMongo), this root is materialized by
    calling Beanie queries in
    :class:`~pydantable.mongo_entei_engine.EnteiPydantableEngine` async execution
    paths.
    """

    document_cls: type[Any]
    criteria: Any | None = None
    fields: tuple[str, ...] | None = None
    fetch_links: bool = False
    nesting_depth: int | None = None
    nesting_depths_per_field: dict[str, int] | None = None
    flatten: bool = True
    id_column: Literal["id", "_id"] = "id"


def _import_entei_engine_types() -> tuple[Any, Any]:
    try:
        core = importlib.import_module("entei_core")
        MongoRoot = core.MongoRoot
        from pydantable.mongo_entei_engine import EnteiPydantableEngine
    except ImportError as exc:
        raise ImportError(
            "EnteiDataFrame / EnteiDataFrameModel require the entei-core package. "
            'Install with: pip install "pydantable[mongo]" or pip install entei-core'
        ) from exc
    return EnteiPydantableEngine, MongoRoot


class EnteiDataFrame(DataFrame):
    """Lazy Mongo ``DataFrame`` (see :mod:`pydantable.mongo_entei_engine`).

    Prefer :meth:`from_beanie` when using Beanie ``Document`` models.
    """

    @classmethod
    def from_collection(
        cls,
        collection: Any,
        *,
        fields: Sequence[str] | None = None,
        engine: Any | None = None,
    ) -> Any:
        """Lazy frame over a MongoDB collection (documents loaded at materialization).

        Call on a concrete parametrized class, e.g.
        ``EnteiDataFrame[MySchema].from_collection(coll)``.
        """
        if cls._schema_type is None:
            raise TypeError(
                "Use EnteiDataFrame[Schema].from_collection(...) with a schema."
            )
        EnteiPydantableEngine, MongoRoot = _import_entei_engine_types()
        eng = engine if engine is not None else EnteiPydantableEngine()
        st = cls._schema_type
        fts = schema_field_types(st)
        plan = eng.make_plan(field_types_for_rust(fts))
        field_keys = tuple(fields) if fields is not None else tuple(fts.keys())
        root = MongoRoot(collection, fields=field_keys)
        return cls._from_plan(
            root_data=root,
            root_schema_type=st,
            current_schema_type=st,
            rust_plan=plan,
            engine=eng,
        )

    @classmethod
    def from_beanie(
        cls,
        document_cls: type[Any],
        *,
        database: Any,
        fields: Sequence[str] | None = None,
        engine: Any | None = None,
    ) -> Any:
        """Lazy frame for a Beanie ``Document`` collection (preferred Mongo path).

        Resolves a **sync** PyMongo ``Collection`` via
        :func:`pydantable.mongo_beanie.sync_pymongo_collection` and delegates to
        :meth:`from_collection`. ``database`` must be a synchronous
        ``pymongo.database.Database`` (same **database name** as Beanie was
        initialized with; Beanie itself uses async PyMongo).

        **Beanie** is installed with ``pip install "pydantable[mongo]"``.
        """
        from pydantable.mongo_beanie import sync_pymongo_collection

        coll = sync_pymongo_collection(document_cls, database)
        return cls.from_collection(coll, fields=fields, engine=engine)

    @classmethod
    def from_beanie_async(
        cls,
        document_cls: type[Any],
        *,
        criteria: Any | None = None,
        fields: Sequence[str] | None = None,
        fetch_links: bool = False,
        nesting_depth: int | None = None,
        nesting_depths_per_field: dict[str, int] | None = None,
        flatten: bool = True,
        id_column: Literal["id", "_id"] = "id",
        engine: Any | None = None,
    ) -> Any:
        """Async-first lazy frame for a Beanie ``Document`` collection.

        This path supports **async materialization only** (``acollect`` / ``ato_dict`` /
        ``astream``). Calling sync terminals (``collect``, ``to_dict``) will raise
        :class:`~pydantable.errors.UnsupportedEngineOperationError` because Beanie is
        async-only.
        """
        if cls._schema_type is None:
            raise TypeError(
                "Use EnteiDataFrame[Schema].from_beanie_async(...) with a schema."
            )
        EnteiPydantableEngine, _MongoRoot = _import_entei_engine_types()
        eng = engine if engine is not None else EnteiPydantableEngine()
        st = cls._schema_type
        fts = schema_field_types(st)
        plan = eng.make_plan(field_types_for_rust(fts))
        field_keys = tuple(fields) if fields is not None else tuple(fts.keys())
        root = BeanieAsyncRoot(
            document_cls=document_cls,
            criteria=criteria,
            fields=field_keys,
            fetch_links=fetch_links,
            nesting_depth=nesting_depth,
            nesting_depths_per_field=nesting_depths_per_field,
            flatten=flatten,
            id_column=id_column,
        )
        return cls._from_plan(
            root_data=root,
            root_schema_type=st,
            current_schema_type=st,
            rust_plan=plan,
            engine=eng,
        )


class EnteiDataFrameModel(DataFrameModel):
    """Mongo ``DataFrameModel`` (see :mod:`pydantable.mongo_entei_engine`).

    Prefer :meth:`from_beanie` when using Beanie ``Document`` models.
    """

    _dataframe_cls = EnteiDataFrame

    def __init__(
        self,
        data: Any,
        *,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        validation_profile: str | None = None,
        engine: Any | None = None,
    ) -> None:
        EnteiPydantableEngine, _ = _import_entei_engine_types()
        resolved = engine if engine is not None else EnteiPydantableEngine()
        super().__init__(
            data,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            validation_profile=validation_profile,
            engine=resolved,
        )

    @classmethod
    def from_collection(
        cls,
        collection: Any,
        *,
        fields: Sequence[str] | None = None,
        engine: Any | None = None,
    ) -> Any:
        """Build from a pymongo collection (``EnteiDataFrame.from_collection``)."""
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].from_collection(
            collection,
            fields=fields,
            engine=engine,
        )
        return cls._wrap_inner_df(inner)

    @classmethod
    def from_beanie(
        cls,
        document_cls: type[Any],
        *,
        database: Any,
        fields: Sequence[str] | None = None,
        engine: Any | None = None,
    ) -> Any:
        """Build from Beanie (see :meth:`EnteiDataFrame.from_beanie`)."""
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].from_beanie(
            document_cls,
            database=database,
            fields=fields,
            engine=engine,
        )
        return cls._wrap_inner_df(inner)

    @classmethod
    def from_beanie_async(
        cls,
        document_cls: type[Any],
        *,
        criteria: Any | None = None,
        fields: Sequence[str] | None = None,
        fetch_links: bool = False,
        nesting_depth: int | None = None,
        nesting_depths_per_field: dict[str, int] | None = None,
        flatten: bool = True,
        id_column: Literal["id", "_id"] = "id",
        engine: Any | None = None,
    ) -> Any:
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].from_beanie_async(
            document_cls,
            criteria=criteria,
            fields=fields,
            fetch_links=fetch_links,
            nesting_depth=nesting_depth,
            nesting_depths_per_field=nesting_depths_per_field,
            flatten=flatten,
            id_column=id_column,
            engine=engine,
        )
        return cls._wrap_inner_df(inner)


def __getattr__(name: str) -> Any:
    """Lazy re-exports: engine from pydantable, ``MongoRoot`` from **entei-core**."""
    if name == "EnteiPydantableEngine":
        return _import_entei_engine_types()[0]
    if name == "MongoRoot":
        return _import_entei_engine_types()[1]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "BeanieAsyncRoot",
    "EnteiDataFrame",
    "EnteiDataFrameModel",
    "EnteiPydantableEngine",
    "MongoRoot",
]
