"""Targeted tests for small modules with remaining coverage gaps."""

from __future__ import annotations

import sys
import types

import pytest
from pydantable.engine.protocols import native_engine_capabilities
from pydantable.model_policies import merged_model_policy, model_policy_value
from pydantable.types import WKB
from pydantable.validation_profiles import (
    apply_validation_profile,
    list_validation_profiles,
    register_validation_profile,
    reset_validation_profiles_for_tests,
)
from pydantic import TypeAdapter


def test_wkb_coerce_returns_same_instance() -> None:
    ta = TypeAdapter(WKB)
    w = WKB(b"\x01")
    assert ta.validate_python(w) is w


def test_native_engine_capabilities_fallback_when_capabilities_import_fails() -> None:
    saved = sys.modules.pop("pydantable_native.capabilities", None)
    try:
        sys.modules["pydantable_native.capabilities"] = types.ModuleType(
            "pydantable_native.capabilities"
        )
        caps = native_engine_capabilities()
        assert caps.backend == "native"
        assert caps.extension_loaded is False
        assert caps.has_execute_plan is False
    finally:
        if saved is not None:
            sys.modules["pydantable_native.capabilities"] = saved


def test_register_validation_profile_rejects_empty_name() -> None:
    with pytest.raises(ValueError, match="non-empty string"):
        register_validation_profile("", {"trusted_mode": "off"})


def test_list_validation_profiles_is_sorted() -> None:
    reset_validation_profiles_for_tests()
    register_validation_profile("zzz_custom", {"trusted_mode": "off"})
    register_validation_profile("aaa_custom", {"trusted_mode": "off"})
    names = list_validation_profiles()
    assert names == sorted(names)


def test_apply_validation_profile_sets_trusted_mode_when_none() -> None:
    reset_validation_profiles_for_tests()
    register_validation_profile(
        "tm_strict",
        {"trusted_mode": "strict"},
    )
    out = apply_validation_profile(
        profile_name="tm_strict",
        current_trusted_mode=None,
        current_fill_missing_optional=True,
        current_ignore_errors=False,
        current_column_strictness_default="coerce",
        current_nested_strictness_default="inherit",
    )
    assert out[0] == "strict"


def test_apply_validation_profile_fill_and_ignore_branches() -> None:
    reset_validation_profiles_for_tests()
    register_validation_profile(
        "fills_off",
        {
            "trusted_mode": "off",
            "fill_missing_optional": False,
            "ignore_errors": True,
        },
    )
    out = apply_validation_profile(
        profile_name="fills_off",
        current_trusted_mode="strict",
        current_fill_missing_optional=True,
        current_ignore_errors=False,
        current_column_strictness_default="coerce",
        current_nested_strictness_default="inherit",
    )
    assert out[1] is False
    assert out[2] is True


def test_apply_validation_profile_strictness_defaults() -> None:
    reset_validation_profiles_for_tests()
    register_validation_profile(
        "strictness",
        {
            "trusted_mode": "off",
            "column_strictness_default": "strict",
            "nested_strictness_default": "coerce",
        },
    )
    out = apply_validation_profile(
        profile_name="strictness",
        current_trusted_mode="strict",
        current_fill_missing_optional=True,
        current_ignore_errors=False,
        current_column_strictness_default="coerce",
        current_nested_strictness_default="inherit",
    )
    assert out[3] == "strict"
    assert out[4] == "coerce"


def test_merged_model_policy_ignores_non_dict_pydantable() -> None:
    class M:
        __pydantable__ = "not-a-dict"  # type: ignore[misc]

    assert merged_model_policy(M) == {}


def test_model_policy_value_default() -> None:
    class M:
        pass

    assert model_policy_value(M, "missing_key", default=42) == 42
