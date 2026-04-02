"""SQLModel table schema helpers (optional ``sqlmodel`` / ``pydantable[sql]``).

Shared column-key logic for :mod:`pydantable.io.sqlmodel_read`,
:mod:`pydantable.io.sqlmodel_write`, and schema bridging APIs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .sqlmodel_read import _ensure_table_model, _require_sqlmodel

if TYPE_CHECKING:
    from sqlalchemy.schema import Table


def table_column_key_set(table: Table) -> set[str]:
    """Set of SQLAlchemy :attr:`.Column.key` values for ``table``."""
    return {c.key for c in table.columns}


def sqlmodel_columns(model: type[Any]) -> list[str]:
    """
    Return ordered SQLAlchemy column keys for ``model`` (``table=True`` SQLModel).

    Matches the default key set returned by :func:`~pydantable.io.fetch_sqlmodel`
    and expected by :func:`~pydantable.io.write_sqlmodel` for a full-row payload.
    """
    _require_sqlmodel()
    _ensure_table_model(model)
    return list(model.__table__.columns.keys())
