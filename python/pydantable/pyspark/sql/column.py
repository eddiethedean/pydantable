from __future__ import annotations

from typing import TypeAlias

from pydantable.expressions import Expr

# Spark-like name for typed expression trees (same object as Expr).
Column: TypeAlias = Expr

__all__ = ["Column"]
