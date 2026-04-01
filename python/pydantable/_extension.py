"""Native extension (:mod:`pydantable._core`) availability and import errors."""

from __future__ import annotations

_MISSING_CORE = (
    "The pydantable native extension (pydantable._core) is required for this "
    "operation. Build from source with: maturin develop --manifest-path "
    "pydantable-core/Cargo.toml — or install a published wheel "
    "(pip install pydantable). See docs/DEVELOPER.md."
)


class MissingRustExtensionError(NotImplementedError):
    """Raised when the compiled extension is missing or too old for the requested API.

    Subclasses :exc:`NotImplementedError` so existing broad handlers continue to match.
    Install a wheel or build from source (see project **README** / **DEVELOPER** docs).
    """

    def __init__(self, detail: str | None = None) -> None:
        super().__init__(detail if detail is not None else _MISSING_CORE)
