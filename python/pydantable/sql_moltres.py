"""SQL-backed :class:`~pydantable.dataframe.DataFrame` via ``moltres-core``.

Install with ``pip install "pydantable[moltres]"``. Requires a
:class:`moltres_core.EngineConfig` (or a pre-built
:class:`moltres_core.MoltresPydantableEngine`).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal, cast

from .dataframe import DataFrame
from .dataframe_model import DataFrameModel
from .schema import field_types_for_rust, schema_field_types


def _import_moltres_engine_types() -> tuple[Any, Any]:
    try:
        from moltres_core import ConnectionManager, MoltresPydantableEngine
    except ImportError as exc:
        raise ImportError(
            "SqlDataFrame and SqlDataFrameModel require the moltres-core package. "
            'Install with: pip install "pydantable[moltres]"'
        ) from exc
    return ConnectionManager, MoltresPydantableEngine


def moltres_engine_from_sql_config(sql_config: Any) -> Any:
    """Build a :class:`moltres_core.MoltresPydantableEngine` from *sql_config*.

    *sql_config* must be a :class:`moltres_core.EngineConfig` instance.
    """
    ConnectionManager, MoltresPydantableEngine = _import_moltres_engine_types()
    cm = ConnectionManager(sql_config)
    return MoltresPydantableEngine(cm, sql_config)


def _resolve_sql_execution_engine(
    *,
    sql_config: Any | None,
    moltres_engine: Any | None,
    engine: Any | None,
) -> Any:
    if engine is not None:
        return engine
    if moltres_engine is not None:
        return moltres_engine
    if sql_config is not None:
        return moltres_engine_from_sql_config(sql_config)
    raise TypeError(
        "Pass one of: sql_config=, moltres_engine=, or engine= (ExecutionEngine)."
    )


class SqlDataFrame(DataFrame):
    """Typed dataframe using Moltres :class:`moltres_core.MoltresPydantableEngine`.

    Pass ``sql_config=`` (:class:`moltres_core.EngineConfig`), or ``moltres_engine=``
    if you already constructed :class:`moltres_core.MoltresPydantableEngine`, or
    ``engine=`` for any compatible :class:`~pydantable.engine.ExecutionEngine`.
    """

    @classmethod
    def from_sql_table(
        cls,
        table: Any,
        *,
        sql_config: Any | None = None,
        moltres_engine: Any | None = None,
        engine: Any | None = None,
        name: str = "root",
    ) -> Any:
        """Lazy frame from a SQLAlchemy ``Table`` / ``FromClause`` (no fetch yet).

        *table* must expose columns whose **names** match the schema model fields.
        Materialization runs ``SELECT`` via Moltres when you call ``to_dict()``,
        ``collect()``, ``head()``, etc.

        **SQLite ``:memory:``:** each new :class:`moltres_core.ConnectionManager`
        opens a separate empty in-memory DB. Create DDL and pass the **same**
        ``moltres_engine=`` (from :func:`moltres_engine_from_sql_config`) you use for
        the frame, or use a **file** URL (``sqlite:///path/to.db``) so distinct
        pools still see one database file.

        Use ``SqlDataFrame[YourSchema].from_sql_table(...)``.
        """
        if getattr(cls, "_schema_type", None) is None:
            raise TypeError(
                "Use SqlDataFrame[YourSchema].from_sql_table(table, ...) "
                "with a concrete schema type."
            )
        _import_moltres_engine_types()
        from moltres_core import SqlRootData

        resolved = _resolve_sql_execution_engine(
            sql_config=sql_config,
            moltres_engine=moltres_engine,
            engine=engine,
        )
        root = SqlRootData(table=table, name=name)
        fts = schema_field_types(cls._schema_type)
        plan = resolved.make_plan(field_types_for_rust(fts))
        return cls._from_plan(
            root_data=root,
            root_schema_type=cls._schema_type,
            current_schema_type=cls._schema_type,
            rust_plan=plan,
            engine=resolved,
        )

    def __init__(
        self,
        data: Mapping[str, Sequence[Any]] | Any,
        *,
        sql_config: Any | None = None,
        moltres_engine: Any | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        column_strictness_default: Literal[
            "inherit", "coerce", "strict", "off"
        ] = "coerce",
        nested_strictness_default: Literal[
            "inherit", "coerce", "strict", "off"
        ] = "inherit",
        engine: Any | None = None,
    ) -> None:
        resolved = _resolve_sql_execution_engine(
            sql_config=sql_config,
            moltres_engine=moltres_engine,
            engine=engine,
        )
        super().__init__(
            data,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            on_validation_errors=on_validation_errors,
            column_strictness_default=column_strictness_default,
            nested_strictness_default=nested_strictness_default,
            engine=resolved,
        )


class SqlDataFrameModel(DataFrameModel):
    """``DataFrameModel`` bound to :class:`moltres_core.MoltresPydantableEngine`."""

    _dataframe_cls = SqlDataFrame

    def __init__(
        self,
        data: Any,
        *,
        sql_config: Any | None = None,
        moltres_engine: Any | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        validation_profile: str | None = None,
        engine: Any | None = None,
    ) -> None:
        resolved = _resolve_sql_execution_engine(
            sql_config=sql_config,
            moltres_engine=moltres_engine,
            engine=engine,
        )
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
    def read_sql_table(
        cls,
        table: Any,
        *,
        sql_config: Any | None = None,
        moltres_engine: Any | None = None,
        engine: Any | None = None,
        name: str = "root",
    ) -> Any:
        """Lazy read from a SQLAlchemy table — same engine rules as the constructor."""
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].from_sql_table(
            table,
            sql_config=sql_config,
            moltres_engine=moltres_engine,
            engine=engine,
            name=name,
        )
        return cls._wrap_inner_df(inner)


__all__ = [
    "SqlDataFrame",
    "SqlDataFrameModel",
    "moltres_engine_from_sql_config",
]
