"""Validation profile presets (Phase 2).

Profiles are a thin preset layer over existing ingest/materialization knobs:
- trusted_mode
- fill_missing_optional
- ignore_errors

They are selectable via kwargs (e.g. ``validation_profile="service_strict"``) and/or
via model policy ``__pydantable__ = {"validation_profile": "..."}``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, TypedDict


TrustedMode = Literal["off", "shape_only", "strict"]


class ValidationProfileDict(TypedDict, total=False):
    trusted_mode: TrustedMode | None
    fill_missing_optional: bool
    ignore_errors: bool


@dataclass(frozen=True)
class ValidationProfile:
    trusted_mode: TrustedMode | None = None
    fill_missing_optional: bool | None = None
    ignore_errors: bool | None = None

    @classmethod
    def from_dict(cls, d: ValidationProfileDict) -> ValidationProfile:
        return cls(
            trusted_mode=d.get("trusted_mode"),
            fill_missing_optional=d.get("fill_missing_optional"),
            ignore_errors=d.get("ignore_errors"),
        )


_REGISTRY: dict[str, ValidationProfile] = {}


def register_validation_profile(name: str, cfg: ValidationProfileDict) -> None:
    if not isinstance(name, str) or not name:
        raise ValueError("validation profile name must be a non-empty string")
    _REGISTRY[name] = ValidationProfile.from_dict(cfg)


def get_validation_profile(name: str) -> ValidationProfile:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise KeyError(f"Unknown validation_profile {name!r}") from None


def list_validation_profiles() -> list[str]:
    return sorted(_REGISTRY.keys())


def reset_validation_profiles_for_tests() -> None:  # pragma: no cover
    _REGISTRY.clear()
    _register_builtins()


def _register_builtins() -> None:
    # Service boundaries: validate strictly and fail fast.
    register_validation_profile(
        "service_strict",
        {
            "trusted_mode": "off",
            "fill_missing_optional": True,
            "ignore_errors": False,
        },
    )
    # Batch/offline: accept partial ingest by default (skip invalid rows) but still
    # run full Pydantic validation on valid rows.
    register_validation_profile(
        "batch_lenient",
        {
            "trusted_mode": "off",
            "fill_missing_optional": True,
            "ignore_errors": True,
        },
    )
    # Trusted upstream: skip per-element validation but keep shape/nullability checks.
    register_validation_profile(
        "trusted_upstream",
        {
            "trusted_mode": "shape_only",
            "fill_missing_optional": True,
            "ignore_errors": False,
        },
    )


_register_builtins()


def apply_validation_profile(
    *,
    profile_name: str | None,
    current_trusted_mode: TrustedMode | None,
    current_fill_missing_optional: bool,
    current_ignore_errors: bool,
) -> tuple[TrustedMode | None, bool, bool]:
    """
    Apply a validation profile to current values.

    Because many call sites use non-optional booleans with defaults, Phase 2 uses a
    conservative rule: only override when the current value equals the library
    default and the profile provides a different default.
    """
    if profile_name is None:
        return current_trusted_mode, current_fill_missing_optional, current_ignore_errors
    p = get_validation_profile(profile_name)

    trusted_mode = current_trusted_mode
    fill_missing_optional = current_fill_missing_optional
    ignore_errors = current_ignore_errors

    if trusted_mode is None and p.trusted_mode is not None:
        trusted_mode = p.trusted_mode
    if fill_missing_optional is True and p.fill_missing_optional is False:
        fill_missing_optional = False
    if ignore_errors is False and p.ignore_errors is True:
        ignore_errors = True

    return trusted_mode, fill_missing_optional, ignore_errors


__all__ = [
    "TrustedMode",
    "ValidationProfile",
    "ValidationProfileDict",
    "apply_validation_profile",
    "get_validation_profile",
    "list_validation_profiles",
    "register_validation_profile",
    "reset_validation_profiles_for_tests",
]

