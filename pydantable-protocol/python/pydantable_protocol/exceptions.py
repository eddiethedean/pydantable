"""Errors shared across pydantable distributions (no pydantable dependency)."""

from __future__ import annotations

_MISSING_CORE = (
    "The pydantable native extension (pydantable-native) is required for this "
    "operation. Build from source with: maturin develop --manifest-path "
    "pydantable-core/Cargo.toml — or install a published wheel "
    "(pip install pydantable-native or pydantable-meta). See docs/DEVELOPER.md."
)


class MissingRustExtensionError(NotImplementedError):
    """Raised when the compiled native extension is missing or too old for the API.

    Subclasses :exc:`NotImplementedError` so broad handlers keep matching.
    Install **pydantable-native** (or **pydantable-meta**) or build from source.
    """

    def __init__(self, detail: str | None = None) -> None:
        super().__init__(detail if detail is not None else _MISSING_CORE)


class UnsupportedEngineOperationError(ValueError):
    """Raised when an execution engine cannot perform a requested operation.

    Third-party backends should raise this (or a subclass) so callers can catch
    engine limits without importing ``pydantable``.
    """
