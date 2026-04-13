"""Optional ``DataFrame`` / ``DataFrameModel`` facades for the entei engine."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, Literal, cast

from pydantable import DataFrame, DataFrameModel
from pydantable.schema import schema_field_types
from pydantable.schema._impl import field_types_for_rust

from entei_core.engine import EnteiPydantableEngine
from entei_core.mongo_root import MongoRoot


class EnteiDataFrame(DataFrame):
    """DataFrame typed class using ``EnteiPydantableEngine`` for Mongo roots."""

    @classmethod
    def from_collection(
        cls,
        collection: Any,
        *,
        fields: Sequence[str] | None = None,
        engine: EnteiPydantableEngine | None = None,
    ) -> Any:
        """Lazy frame over a MongoDB collection (documents loaded at materialization).

        Call on a concrete parametrized class, e.g.
        ``EnteiDataFrame[MySchema].from_collection(coll)``.
        """
        if cls._schema_type is None:
            raise TypeError(
                "Use EnteiDataFrame[Schema].from_collection(...) with a schema."
            )
        eng = engine or EnteiPydantableEngine()
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
    """DataFrameModel using ``EnteiPydantableEngine`` for the inner frame by default."""

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
        engine: EnteiPydantableEngine | None = None,
    ) -> Any:
        """Build from a pymongo collection (see ``EnteiDataFrame.from_collection``)."""
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].from_collection(
            collection,
            fields=fields,
            engine=engine,
        )
        return cls._wrap_inner_df(inner)
