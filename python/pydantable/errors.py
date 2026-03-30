"""User-facing and contract errors (subclass :exc:`ValueError` where noted)."""

from __future__ import annotations


class PydantableUserError(ValueError):
    """Base for predictable input / schema contract failures.

    Subclasses :exc:`ValueError` so existing ``except ValueError`` continues to
    match; prefer catching specific subclasses in new code.
    """


class ColumnLengthMismatchError(PydantableUserError):
    """Raised when column lists for a schema mapping differ in length."""


__all__ = ["ColumnLengthMismatchError", "PydantableUserError"]
