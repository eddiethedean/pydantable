from __future__ import annotations

from pydantable import DataFrameModel
from pydantable.validation_profiles import (
    get_validation_profile,
    register_validation_profile,
    reset_validation_profiles_for_tests,
)


def test_pydantable_policy_merges_inheritance() -> None:
    class Base(DataFrameModel):
        __pydantable__ = {"validation_profile": "trusted_upstream", "x": 1}

        id: int

    class Child(Base):
        __pydantable__ = {"x": 2}

        name: str

    assert Base.pydantable_policy()["x"] == 1
    assert Child.pydantable_policy()["x"] == 2
    assert Child.pydantable_policy()["validation_profile"] == "trusted_upstream"


def test_validation_profile_from_model_policy_applies_defaults() -> None:
    class DF(DataFrameModel):
        __pydantable__ = {"validation_profile": "batch_lenient"}

        id: int

    # batch_lenient sets ignore_errors=True (profile defaults); constructor default is False
    df = DF([{"id": 1}, {"id": "bad"}])
    assert df.to_dict() == {"id": [1]}


def test_validation_profile_registry_can_override() -> None:
    register_validation_profile(
        "my_profile",
        {"trusted_mode": "off", "fill_missing_optional": True, "ignore_errors": True},
    )

    class DF(DataFrameModel):
        __pydantable__ = {"validation_profile": "my_profile"}

        id: int

    df = DF([{"id": 1}, {"id": "bad"}])
    assert df.to_dict() == {"id": [1]}


def test_validation_profile_unknown_raises_keyerror() -> None:
    reset_validation_profiles_for_tests()
    try:
        get_validation_profile("does_not_exist")
        raise AssertionError("expected KeyError")
    except KeyError:
        pass

