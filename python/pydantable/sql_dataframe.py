"""SQL-backed lazy :class:`~pydantable.dataframe.DataFrame` (SQLAlchemy 2.x).

Install the optional extra: ``pip install "pydantable[sql]"`` (includes **moltres-core**
and eager SQLModel I/O). Lazy execution is implied by ``SqlDataFrame`` /
``SqlDataFrameModel``. This path needs a SQLAlchemy-oriented engine configuration
object from the same optional stack (see the **Lazy SQL DataFrame** guide).

``sql_engine_from_config`` builds the execution engine pydantable uses for
``SqlDataFrame`` / ``SqlDataFrameModel`` from that configuration.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal, cast

from .dataframe import DataFrame
from .dataframe_model import DataFrameModel
from .schema import field_types_for_rust, schema_field_types


def _import_lazy_sql_engine_types() -> tuple[Any, Any]:
    try:
        from moltres_core import ConnectionManager, MoltresPydantableEngine
    except ImportError as exc:
        raise ImportError(
            "SqlDataFrame and SqlDataFrameModel require the optional lazy-SQL stack. "
            'Install with: pip install "pydantable[sql]"'
        ) from exc
    return ConnectionManager, MoltresPydantableEngine


def sql_engine_from_config(sql_config: Any) -> Any:
    """Build the lazy-SQL execution engine from *sql_config*.

    *sql_config* must be an ``EngineConfig`` instance from the optional lazy-SQL
    dependency (same extra as ``SqlDataFrame``).
    """
    ConnectionManager, LazySqlEngine = _import_lazy_sql_engine_types()
    cm = ConnectionManager(sql_config)
    return LazySqlEngine(cm, sql_config)


def _resolve_sql_execution_engine(
    *,
    sql_config: Any | None,
    sql_engine: Any | None,
    moltres_engine: Any | None,
    engine: Any | None,
) -> Any:
    if moltres_engine is not None:
        warnings.warn(
            "moltres_engine= is deprecated; use sql_engine= instead.",
            DeprecationWarning,
            stacklevel=3,
        )
        if sql_engine is not None:
            raise TypeError("Pass only one of sql_engine= or moltres_engine=")
        sql_engine = moltres_engine
    if engine is not None:
        return engine
    if sql_engine is not None:
        return sql_engine
    if sql_config is not None:
        return sql_engine_from_config(sql_config)
    raise TypeError(
        "Pass one of: sql_config=, sql_engine=, or engine= (ExecutionEngine)."
    )


class SqlDataFrame(DataFrame):
    """Typed dataframe using the optional lazy-SQL execution engine.

    Pass ``sql_config=`` (engine configuration from the lazy-SQL stack), or
    ``sql_engine=`` if you already constructed that engine, or ``engine=`` for any
    compatible :class:`~pydantable.engine.ExecutionEngine`.
    """

    @classmethod
    def from_sql_table(
        cls,
        table: Any,
        *,
        sql_config: Any | None = None,
        sql_engine: Any | None = None,
        engine: Any | None = None,
        name: str = "root",
        **kwargs: Any,
    ) -> Any:
        """Lazy frame from a SQLAlchemy ``Table`` / ``FromClause`` (no fetch yet).

        *table* must expose columns whose **names** match the schema model fields.
        Materialization runs ``SELECT`` via the lazy-SQL engine when you call
        ``to_dict()``, ``collect()``, ``head()``, etc.

        **SQLite ``:memory:``:** each new connection pool may be a separate empty
        in-memory DB. Create DDL and pass the **same** ``sql_engine=`` (from
        :func:`sql_engine_from_config`) you use for the frame, or use a **file**
        URL (``sqlite:///path/to.db``) so distinct pools still see one database file.

        Use ``SqlDataFrame[YourSchema].from_sql_table(...)``.
        """
        moltres_engine = kwargs.pop("moltres_engine", None)
        if kwargs:
            raise TypeError(f"Unexpected keyword arguments: {sorted(kwargs)!r}")
        if getattr(cls, "_schema_type", None) is None:
            raise TypeError(
                "Use SqlDataFrame[YourSchema].from_sql_table(table, ...) "
                "with a concrete schema type."
            )
        _import_lazy_sql_engine_types()
        from moltres_core import SqlRootData

        resolved = _resolve_sql_execution_engine(
            sql_config=sql_config,
            sql_engine=sql_engine,
            moltres_engine=moltres_engine,
            engine=engine,
        )
        root = SqlRootData(table=table, name=name)
        schema_type = cls._schema_type
        assert schema_type is not None
        fts = schema_field_types(schema_type)
        plan = resolved.make_plan(field_types_for_rust(fts))
        return cls._from_plan(
            root_data=root,
            root_schema_type=schema_type,
            current_schema_type=schema_type,
            rust_plan=plan,
            engine=resolved,
        )

    def __init__(
        self,
        data: Mapping[str, Sequence[Any]] | Any,
        *,
        sql_config: Any | None = None,
        sql_engine: Any | None = None,
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
        **kwargs: Any,
    ) -> None:
        moltres_engine = kwargs.pop("moltres_engine", None)
        if kwargs:
            raise TypeError(f"Unexpected keyword arguments: {sorted(kwargs)!r}")
        resolved = _resolve_sql_execution_engine(
            sql_config=sql_config,
            sql_engine=sql_engine,
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
    """``DataFrameModel`` bound to the optional lazy-SQL execution engine."""

    _dataframe_cls = SqlDataFrame

    def __init__(
        self,
        data: Any,
        *,
        sql_config: Any | None = None,
        sql_engine: Any | None = None,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
        validation_profile: str | None = None,
        engine: Any | None = None,
        **kwargs: Any,
    ) -> None:
        moltres_engine = kwargs.pop("moltres_engine", None)
        if kwargs:
            raise TypeError(f"Unexpected keyword arguments: {sorted(kwargs)!r}")
        resolved = _resolve_sql_execution_engine(
            sql_config=sql_config,
            sql_engine=sql_engine,
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
        sql_engine: Any | None = None,
        engine: Any | None = None,
        name: str = "root",
        **kwargs: Any,
    ) -> Any:
        """Lazy read from a SQLAlchemy table — same engine rules as the constructor."""
        moltres_engine = kwargs.pop("moltres_engine", None)
        if kwargs:
            raise TypeError(f"Unexpected keyword arguments: {sorted(kwargs)!r}")
        cls._dfm_require_subclass_with_schema()
        dataframe_cls = cast("Any", cls._dataframe_cls)
        inner = dataframe_cls[cls._SchemaModel].from_sql_table(
            table,
            sql_config=sql_config,
            sql_engine=sql_engine,
            moltres_engine=moltres_engine,
            engine=engine,
            name=name,
        )
        return cls._wrap_inner_df(inner)


__all__ = [
    "SqlDataFrame",
    "SqlDataFrameModel",
    "sql_engine_from_config",
]
