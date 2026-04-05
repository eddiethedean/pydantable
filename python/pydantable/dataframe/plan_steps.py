"""Internal immutable records for plan lineage (projection, filter, with_columns)."""

from __future__ import annotations

from dataclasses import dataclass

from pydantable.expressions import Expr  # noqa: TC001


@dataclass(frozen=True)
class SelectStep:
    """Internal: plain column projection step (names only)."""

    columns: list[str]


@dataclass(frozen=True)
class FilterStep:
    """Internal: boolean mask step."""

    condition: Expr


@dataclass(frozen=True)
class WithColumnsStep:
    """Internal: add or replace columns from expressions."""

    columns: dict[str, Expr]
