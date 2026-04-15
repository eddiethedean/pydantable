"""Mongo lazy :class:`~pydantable.dataframe.DataFrame` (PyMongo / Beanie).

**Recommended:** define collections with `Beanie <https://github.com/BeanieODM/beanie>`__
``Document`` models and use ``pip install "pydantable[mongo]"`` —
:meth:`MongoDataFrame.from_beanie` / :meth:`MongoDataFrameModel.from_beanie` plus
:mod:`pydantable.mongo_beanie` for collection resolution. **Sync** lazy paths use
plan roots backed by PyMongo ``find`` (not Beanie's ODM query API) — use
:meth:`MongoDataFrame.from_beanie_async` or :func:`pydantable.io.beanie.afetch_beanie`
for Beanie-level reads (links, projections, pre-built queries). Plain Pydantic
:class:`~pydantable.schema.Schema` subclasses with
:meth:`MongoDataFrame.from_collection` remain supported when you already have a
sync PyMongo ``Collection``.

The optional Mongo plan stack (installed with ``[mongo]``) supplies ``MongoRoot``
and columnar materialization; :class:`MongoPydantableEngine` lives in
:mod:`pydantable.mongo_dataframe_engine` and implements
:class:`~pydantable.engine.protocols.ExecutionEngine` by delegating to the native
planner and scanning Mongo collections into ``dict[str, list]`` at execution time.

This module defines :class:`MongoDataFrame` and :class:`MongoDataFrameModel` on the
pydantable side, mirroring :mod:`pydantable.sql_dataframe`.
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

    Unlike sync PyMongo ``MongoRoot`` scans, this root is materialized by
    calling :func:`~pydantable.io.beanie.afetch_beanie` in
    :class:`~pydantable.mongo_dataframe_engine.MongoPydantableEngine` async execution
    paths. The first field matches **afetch_beanie**'s ``document_or_query``: a
    Beanie ``Document`` **class** or a **pre-built query** (e.g.
    ``Doc.find(...).sort(...)``).
    """

    document_or_query: Any
    criteria: Any | None = None
    fields: tuple[str, ...] | None = None
    fetch_links: bool = False
    nesting_depth: int | None = None
    nesting_depths_per_field: dict[str, int] | None = None
    flatten: bool = True
    id_column: Literal["id", "_id"] = "id"


def _import_mongo_engine_types() -> tuple[Any, Any]:
    try:
        core = importlib.import_module("entei_core")
        MongoRoot = core.MongoRoot
        from pydantable.mongo_dataframe_engine import MongoPydantableEngine
    except ImportError as exc:
        raise ImportError(
            "MongoDataFrame / MongoDataFrameModel require the optional Mongo plan "
            'stack. Install with: pip install "pydantable[mongo]"'
        ) from exc
    return MongoPydantableEngine, MongoRoot


class MongoDataFrame(DataFrame):
    """Lazy Mongo ``DataFrame`` (see :mod:`pydantable.mongo_dataframe_engine`).

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
        ``MongoDataFrame[MySchema].from_collection(coll)``.
        """
        if cls._schema_type is None:
            raise TypeError(
                "Use MongoDataFrame[Schema].from_collection(...) with a schema."
            )
        MongoPydantableEngine, MongoRoot = _import_mongo_engine_types()
        eng = engine if engine is not None else MongoPydantableEngine()
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
        :meth:`from_collection`. Reads use plan ``MongoRoot`` (driver-level
        ``find``), not Beanie's ODM ``find`` / ``fetch_links`` — for those, use
        :meth:`from_beanie_async` or :func:`~pydantable.io.beanie.afetch_beanie`.

        ``database`` must be a synchronous ``pymongo.database.Database`` (same
        **database name** as Beanie was initialized with; Beanie itself uses async
        PyMongo). **Beanie** is installed with ``pip install "pydantable[mongo]"``.
        """
        from pydantable.mongo_beanie import sync_pymongo_collection

        coll = sync_pymongo_collection(document_cls, database)
        return cls.from_collection(coll, fields=fields, engine=engine)

    @classmethod
    def from_beanie_async(
        cls,
        document_or_query: Any,
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
        """Async-first lazy frame over Beanie reads (``afetch_beanie`` entrypoints).

        Pass a Beanie ``Document`` **class** (optionally with ``criteria=``) **or**
        a **pre-built query** (e.g. ``MyDocument.find(...).sort(...)``). Pre-built
        queries follow :func:`~pydantable.io.beanie.afetch_beanie` rules (do not pass
        ``criteria=`` for a query object).

        This path supports **async materialization only** (``acollect`` / ``ato_dict`` /
        ``astream``). Calling sync terminals (``collect``, ``to_dict``) will raise
        :class:`~pydantable.errors.UnsupportedEngineOperationError` because Beanie is
        async-only.
        """
        if cls._schema_type is None:
            raise TypeError(
                "Use MongoDataFrame[Schema].from_beanie_async(...) with a schema."
            )
        MongoPydantableEngine, _MongoRoot = _import_mongo_engine_types()
        eng = engine if engine is not None else MongoPydantableEngine()
        st = cls._schema_type
        fts = schema_field_types(st)
        plan = eng.make_plan(field_types_for_rust(fts))
        # If the caller doesn't provide `fields`, allow the Beanie materializer to
        # fetch all keys (including normalized `id` and flattened link keys), and
        # rely on the plan to project to the schema as needed.
        field_keys = tuple(fields) if fields is not None else None
        root = BeanieAsyncRoot(
            document_or_query=document_or_query,
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


class MongoDataFrameModel(DataFrameModel):
    """Mongo ``DataFrameModel`` (see :mod:`pydantable.mongo_dataframe_engine`).

    Prefer :meth:`from_beanie` when using Beanie ``Document`` models.
    """

    _dataframe_cls = MongoDataFrame

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
        MongoPydantableEngine, _ = _import_mongo_engine_types()
        resolved = engine if engine is not None else MongoPydantableEngine()
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
        """Build from a pymongo collection (``MongoDataFrame.from_collection``)."""
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
        """Build from Beanie (see :meth:`MongoDataFrame.from_beanie`)."""
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
        document_or_query: Any,
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
            document_or_query,
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
    """Lazy re-exports for ``MongoPydantableEngine`` and ``MongoRoot``."""
    if name == "MongoPydantableEngine":
        return _import_mongo_engine_types()[0]
    if name == "MongoRoot":
        return _import_mongo_engine_types()[1]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "BeanieAsyncRoot",
    "MongoDataFrame",
    "MongoDataFrameModel",
    "MongoPydantableEngine",
    "MongoRoot",
]
