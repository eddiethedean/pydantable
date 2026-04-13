"""Mongo-backed :class:`~pydantable.dataframe.DataFrame` via ``entei-core``.

Install with ``pip install "pydantable[mongo]"`` (pulls **entei-core**) or
``pip install entei-core``. The **entei-core** distribution provides
:class:`entei_core.engine.EnteiPydantableEngine` (a
:class:`~pydantable.engine.protocols.ExecutionEngine` over the native planner plus
Mongo collection materialization) and :class:`entei_core.mongo_root.MongoRoot`.

This module defines :class:`EnteiDataFrame` and :class:`EnteiDataFrameModel` on the
pydantable side, mirroring :mod:`pydantable.sql_moltres` and **moltres-core**.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable, Sequence
from typing import Any, Literal, cast

from .dataframe import DataFrame
from .dataframe_model import DataFrameModel
from .schema import field_types_for_rust, schema_field_types


def _import_entei_engine_types() -> tuple[Any, Any]:
    try:
        core = importlib.import_module("entei_core")
        EnteiPydantableEngine = core.EnteiPydantableEngine
        MongoRoot = core.MongoRoot
    except (ImportError, AttributeError) as exc:
        raise ImportError(
            "EnteiDataFrame / EnteiDataFrameModel require the entei-core package. "
            'Install with: pip install "pydantable[mongo]" or pip install entei-core'
        ) from exc
    return EnteiPydantableEngine, MongoRoot


class EnteiDataFrame(DataFrame):
    """Typed dataframe using ``entei_core.EnteiPydantableEngine`` for Mongo roots."""

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


class EnteiDataFrameModel(DataFrameModel):
    """``DataFrameModel`` using ``entei_core.EnteiPydantableEngine`` by default."""

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


def __getattr__(name: str) -> Any:
    """Lazy re-exports of engine types from **entei-core** (optional dependency)."""
    if name == "EnteiPydantableEngine":
        return _import_entei_engine_types()[0]
    if name == "MongoRoot":
        return _import_entei_engine_types()[1]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "EnteiDataFrame",
    "EnteiDataFrameModel",
    "EnteiPydantableEngine",
    "MongoRoot",
]
