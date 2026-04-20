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
from .engine import get_default_engine
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


@dataclass(frozen=True, slots=True)
class MongoFindRoot:
    """Mongo root with server-side filter/projection pushdown."""

    collection: Any
    fields: tuple[str, ...] | None = None
    filter: dict[str, Any] | None = None
    projection: dict[str, int] | None = None


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

    def pyspark_ui(self) -> Any:
        """Return a PySpark-shaped wrapper over this Mongo-backed frame."""
        from pydantable.pyspark.mongo_dataframe import (
            MongoDataFrame as PySparkMongoDataFrame,
        )

        return PySparkMongoDataFrame._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=self._rust_plan,
            engine=self._engine,
        )

    def pandas_ui(self) -> Any:
        """Return a pandas-shaped wrapper over this Mongo-backed frame."""
        from pydantable.pandas_mongo_dataframe import (
            MongoDataFrame as PandasMongoDataFrame,
        )

        return PandasMongoDataFrame._from_plan(
            root_data=self._root_data,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=self._rust_plan,
            engine=self._engine,
        )

    @classmethod
    def from_collection(
        cls,
        collection: Any,
        *,
        fields: Sequence[str] | None = None,
        engine: Any | None = None,
        engine_mode: Literal["auto", "default"] = "auto",
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
        if engine is not None:
            eng = engine
        elif engine_mode == "default":
            eng = get_default_engine()
        else:
            eng = MongoPydantableEngine()
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

    def match(self, filter: dict[str, Any]) -> Any:
        """Push down a Mongo `$match` (driver-level `find(filter=...)`)."""
        if not isinstance(filter, dict):
            raise TypeError("match(filter=...) expects a dict.")
        unknown = sorted(set(filter) - set(self._current_field_types))
        if unknown:
            raise KeyError(
                "match() referenced unknown columns: "
                + ", ".join(repr(x) for x in unknown)
            )
        try:
            core = importlib.import_module("entei_core")
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "Mongo match() requires the optional stack. "
                'Install with: pip install "pydantable[mongo]"'
            ) from exc
        MongoRoot = core.MongoRoot
        if not isinstance(self._root_data, MongoRoot):
            raise TypeError("match() is only supported on Mongo collection roots.")
        root = self._root_data
        wrapped = MongoFindRoot(
            collection=root.collection,
            fields=root.fields,
            filter=filter,
            projection=None,
        )
        return self._from_plan(
            root_data=wrapped,
            root_schema_type=self._root_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=self._rust_plan,
            engine=self._engine,
        )

    def project(self, fields: Sequence[str] | dict[str, int]) -> Any:
        """Typed projection with schema update.

        Note: We currently keep the collection scan's materialized root columns
        aligned with the root plan's expected schema. This preserves correctness
        with the native planner; projection is applied via the typed plan.
        """
        if isinstance(fields, dict):
            d = cast("dict[str, int]", fields)
            wanted: list[str] = list(d.keys())
        else:
            seq = cast("Sequence[str]", fields)
            wanted = list(seq)
        if not wanted:
            raise ValueError("project(fields) requires at least one field.")
        unknown = sorted(set(wanted) - set(self._current_field_types))
        if unknown:
            raise KeyError(
                "project() referenced unknown columns: "
                + ", ".join(repr(x) for x in unknown)
            )
        try:
            core = importlib.import_module("entei_core")
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "Mongo project() requires the optional stack. "
                'Install with: pip install "pydantable[mongo]"'
            ) from exc
        MongoRoot = core.MongoRoot
        if not isinstance(self._root_data, MongoRoot):
            raise TypeError("project() is only supported on Mongo collection roots.")

        # Apply a logical projection (typed) and also push down driver projection.
        projected = self.select(*wanted)
        root = projected._root_data
        wrapped = MongoFindRoot(
            collection=root.collection,
            fields=root.fields,
            filter=None,
            projection=None,
        )
        return projected._from_plan(
            root_data=wrapped,
            root_schema_type=projected._root_schema_type,
            current_schema_type=projected._current_schema_type,
            rust_plan=projected._rust_plan,
            engine=projected._engine,
        )

    @classmethod
    def from_beanie(
        cls,
        document_cls: type[Any],
        *,
        database: Any,
        fields: Sequence[str] | None = None,
        engine: Any | None = None,
        engine_mode: Literal["auto", "default"] = "auto",
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
        return cls.from_collection(
            coll, fields=fields, engine=engine, engine_mode=engine_mode
        )

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
        engine_mode: Literal["auto", "default"] = "auto",
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
        if engine is not None:
            eng = engine
        elif engine_mode == "default":
            eng = get_default_engine()
        else:
            eng = MongoPydantableEngine()
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

    def pyspark_ui(self) -> Any:
        """Return a PySpark-shaped wrapper over this Mongo-backed model."""
        return self._wrap_inner_df(self._df.pyspark_ui())

    def pandas_ui(self) -> Any:
        """Return a pandas-shaped wrapper over this Mongo-backed model."""
        return self._wrap_inner_df(self._df.pandas_ui())

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
        engine_mode: Literal["auto", "default"] = "auto",
    ) -> None:
        MongoPydantableEngine, _ = _import_mongo_engine_types()
        if engine is not None:
            resolved = engine
        elif engine_mode == "default":
            resolved = get_default_engine()
        else:
            resolved = MongoPydantableEngine()
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
        engine_mode: Literal["auto", "default"] = "auto",
    ) -> Any:
        """Build from a pymongo collection (``MongoDataFrame.from_collection``)."""
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].from_collection(
            collection,
            fields=fields,
            engine=engine,
            engine_mode=engine_mode,
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
        engine_mode: Literal["auto", "default"] = "auto",
    ) -> Any:
        """Build from Beanie (see :meth:`MongoDataFrame.from_beanie`)."""
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].from_beanie(
            document_cls,
            database=database,
            fields=fields,
            engine=engine,
            engine_mode=engine_mode,
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
        engine_mode: Literal["auto", "default"] = "auto",
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
            engine_mode=engine_mode,
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
