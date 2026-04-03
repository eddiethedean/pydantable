"""SQL-backed :class:`~pydantable.dataframe.DataFrame` via ``moltres-core``.

Install with ``pip install "pydantable[moltres]"``. Requires a
:class:`moltres_core.EngineConfig` (or a pre-built
:class:`moltres_core.MoltresPydantableEngine`).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal

from .dataframe import DataFrame
from .dataframe_model import DataFrameModel


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


__all__ = [
    "SqlDataFrame",
    "SqlDataFrameModel",
    "moltres_engine_from_sql_config",
]
