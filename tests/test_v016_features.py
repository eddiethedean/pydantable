"""0.16.0: removed ``validate_data``; ``trusted_mode``-only constructors."""

from __future__ import annotations

import pytest
from pydantable import DataFrame, DataFrameModel
from pydantable.schema import Schema
from pydantic import ValidationError


class TwoInt(Schema):
    id: int
    age: int


class MiniDF(DataFrameModel):
    id: int
    age: int | None


@pytest.mark.parametrize(
    "value",
    [True, False, None, 0, 1, "", "yes"],
    ids=["true", "false", "none", "zero", "one", "empty_str", "yes_str"],
)
def test_dataframe_rejects_validate_data_for_all_sentinel_values(value: object) -> None:
    with pytest.raises(TypeError, match="validate_data"):
        DataFrame[TwoInt]({"id": [1], "age": [2]}, validate_data=value)


@pytest.mark.parametrize("value", [True, False, None, 0])
def test_dataframe_model_rejects_validate_data_for_common_values(value: object) -> None:
    with pytest.raises(TypeError, match="validate_data"):
        MiniDF({"id": [1], "age": [2]}, validate_data=value)


def test_validate_data_rejected_even_with_trusted_mode_dataframe() -> None:
    with pytest.raises(TypeError, match="validate_data"):
        DataFrame[TwoInt](
            {"id": [1], "age": [2]},
            trusted_mode="shape_only",
            validate_data=False,
        )


def test_validate_data_rejected_even_with_trusted_mode_model() -> None:
    with pytest.raises(TypeError, match="validate_data"):
        MiniDF(
            {"id": [1], "age": [2]},
            trusted_mode="shape_only",
            validate_data=False,
        )


def test_removed_internal_skip_flag_rejected_on_dataframe() -> None:
    with pytest.raises(TypeError, match="_skip_validate_data_deprecation"):
        DataFrame[TwoInt](
            {"id": [1], "age": [2]},
            _skip_validate_data_deprecation=True,
        )


@pytest.mark.parametrize("kwargs", [{}, {"trusted_mode": "off"}])
def test_omitted_trusted_mode_matches_explicit_off_validation(kwargs: dict) -> None:
    with pytest.raises(ValidationError):
        DataFrame[TwoInt]({"id": ["not-an-int"], "age": [1]}, **kwargs)


def test_trusted_shape_only_numeric_roundtrip_dataframe() -> None:
    out = DataFrame[TwoInt](
        {"id": [1], "age": [2]},
        trusted_mode="shape_only",
    ).collect(as_lists=True)
    assert out == {"id": [1], "age": [2]}


def test_trusted_strict_numeric_roundtrip_dataframe() -> None:
    out = DataFrame[TwoInt](
        {"id": [1], "age": [2]},
        trusted_mode="strict",
    ).collect(as_lists=True)
    assert out == {"id": [1], "age": [2]}


def test_dataframe_ignore_errors_trusted_mode_off_without_validate_data() -> None:
    """Other constructor kwargs still compose; ``validate_data`` must not appear."""
    failed: list[object] = []

    def on_bad(items: list[dict[str, object]]) -> None:
        failed.extend(items)

    df = DataFrame[TwoInt](
        {"id": [1, "bad"], "age": [10, 20]},
        trusted_mode="off",
        ignore_errors=True,
        on_validation_errors=on_bad,
    )
    assert df.collect(as_lists=True) == {"id": [1], "age": [10]}
    assert len(failed) >= 1


def test_dataframe_model_ignore_errors_and_trusted_mode_without_validate_data() -> None:
    failed: list[object] = []

    def on_bad(items: list[dict[str, object]]) -> None:
        failed.extend(items)

    df = MiniDF(
        [{"id": 1, "age": 10}, {"id": "bad", "age": 20}],
        ignore_errors=True,
        trusted_mode="off",
        on_validation_errors=on_bad,
    )
    assert df.collect(as_lists=True) == {"id": [1], "age": [10]}
    assert len(failed) == 1

    with pytest.raises(TypeError, match="validate_data"):
        MiniDF(
            [{"id": 1, "age": 10}],
            ignore_errors=True,
            validate_data=False,
        )
