"""Custom scalar dtype registry (Phase 3).

This module lets applications register *semantic* scalar types (validated/coerced by
Pydantic) and map them onto one of pydantable's supported scalar bases (e.g. `str`,
`int`). The Rust execution layer continues to operate on the base dtype; the registry
improves Python-side schema validation, strict-mode compatibility checks, and
derived-schema identity preservation.
"""

from __future__ import annotations

from typing import Literal

ScalarBaseName = Literal[
    "int",
    "float",
    "bool",
    "str",
    "binary",
    "uuid",
    "decimal",
    "datetime",
    "date",
    "time",
    "duration",
]


_BASE_NAME_TO_TYPE: dict[str, type] = {
    "int": int,
    "float": float,
    "bool": bool,
    "str": str,
    "binary": bytes,
}


_REGISTRY: dict[type, ScalarBaseName] = {}


def register_scalar(tp: type, *, base: ScalarBaseName) -> None:
    """
    Register a semantic scalar type and its base.

    Example:

    ```python
    class ULID(str): ...
    register_scalar(ULID, base="str")
    ```
    """
    if not isinstance(tp, type):
        raise TypeError("register_scalar(tp=...) expects a type")
    if base not in _BASE_NAME_TO_TYPE:
        raise ValueError(
            f"Unsupported base {base!r}. Supported: {sorted(_BASE_NAME_TO_TYPE)}"
        )
    _REGISTRY[tp] = base


def get_registered_scalar_base(tp: type) -> type | None:
    """Return the Python base type for a registered semantic scalar type."""
    base = _REGISTRY.get(tp)
    if base is None:
        return None
    return _BASE_NAME_TO_TYPE[base]


def list_registered_scalars() -> list[type]:
    return sorted(_REGISTRY.keys(), key=lambda t: f"{t.__module__}.{t.__qualname__}")


def reset_registry_for_tests() -> None:  # pragma: no cover
    _REGISTRY.clear()


__all__ = [
    "ScalarBaseName",
    "get_registered_scalar_base",
    "list_registered_scalars",
    "register_scalar",
    "reset_registry_for_tests",
]
