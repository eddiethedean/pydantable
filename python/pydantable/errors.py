"""User-facing errors for schema and ingest contracts.

Most exceptions subclass :exc:`ValueError` (or :exc:`PydantableUserError`, which is a
:class:`ValueError` subclass) so broad handlers in application code keep working.
Prefer catching specific types in new code and map them to HTTP status codes in APIs
(e.g. :exc:`ColumnLengthMismatchError` → **400** with FastAPI helpers).
"""

from __future__ import annotations


class PydantableUserError(ValueError):
    """Base class for predictable validation and contract failures.

    Subclasses :exc:`ValueError` so ``except ValueError`` still matches; use this
    base or a concrete subclass when you need to distinguish pydantable errors from
    other :exc:`ValueError` sources.
    """


class ColumnLengthMismatchError(PydantableUserError):
    """Raised when per-column list lengths disagree for a rectangular table mapping.

    Typical cause: constructing a ``dict[str, list]`` or equivalent where one column
    has a different row count than the others.
    """


class MissingOptionalDependency(PydantableUserError):
    """Raised when an optional dependency is required for the requested API.

    Install the matching extra (for example ``pip install 'pydantable[sql]'``).
    """


__all__ = [
    "ColumnLengthMismatchError",
    "MissingOptionalDependency",
    "PydantableUserError",
]
