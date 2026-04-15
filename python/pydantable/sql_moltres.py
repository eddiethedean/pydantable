"""Compatibility re-export of :mod:`pydantable.sql_dataframe`.

Prefer ``from pydantable.sql_dataframe import SqlDataFrame`` (or
``from pydantable import SqlDataFrame``).
"""

from __future__ import annotations

from pydantable.sql_dataframe import *  # noqa: F403
from pydantable.sql_dataframe import __all__ as _sql_df_all
from pydantable.sql_dataframe import (
    sql_engine_from_config as moltres_engine_from_sql_config,
)

__all__ = [*_sql_df_all, "moltres_engine_from_sql_config"]
