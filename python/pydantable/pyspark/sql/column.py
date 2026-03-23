"""Spark alias :data:`Column` → :class:`pydantable.expressions.Expr` (same type)."""

from __future__ import annotations

from typing import TypeAlias

from pydantable.expressions import Expr

Column: TypeAlias = Expr

__all__ = ["Column"]
