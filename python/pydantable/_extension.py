"""Native extension availability and errors."""

from __future__ import annotations

_MISSING_CORE = (
    "The pydantable native extension (pydantable._core) is required for this "
    "operation. Build from source with: maturin develop --manifest-path "
    "pydantable-core/Cargo.toml — or install a published wheel "
    "(pip install pydantable). See docs/DEVELOPER.md."
)


class MissingRustExtensionError(NotImplementedError):
    """Raised when :mod:`pydantable._core` is not importable or lacks a needed symbol.

    Still subclasses :exc:`NotImplementedError` so broad ``except NotImplementedError``
    handlers keep working.
    """

    def __init__(self, detail: str | None = None) -> None:
        super().__init__(detail if detail is not None else _MISSING_CORE)
