from __future__ import annotations

import pytest
from pydantable import Schema
from pydantable.errors import ColumnLengthMismatchError
from pydantable.schema._impl import DtypeDriftWarning, validate_columns_strict


def test_validate_columns_strict_rejects_extra_and_missing_required() -> None:
    class Row(Schema):
        a: int
        b: int

    with pytest.raises(ValueError, match="Unknown columns"):
        validate_columns_strict({"a": [1], "b": [2], "extra": [3]}, Row)

    with pytest.raises(ValueError, match="Missing required columns"):
        validate_columns_strict({"a": [1]}, Row)


def test_validate_columns_strict_length_mismatch() -> None:
    class Row(Schema):
        a: int
        b: int

    with pytest.raises(ColumnLengthMismatchError, match="same length"):
        validate_columns_strict({"a": [1, 2], "b": [10]}, Row)


def test_validate_columns_strict_missing_optional_fill_policy_and_defaults() -> None:
    class Row(Schema):
        a: int
        opt: int | None
        opt_default: int | None = 7

    out = validate_columns_strict({"a": [1, 2]}, Row)
    assert out["opt"] == [None, None]
    assert out["opt_default"] == [7, 7]

    with pytest.raises(ValueError, match="Missing optional columns"):
        validate_columns_strict({"a": [1]}, Row, fill_missing_optional=False)


def test_validate_columns_strict_ignore_errors_calls_callback() -> None:
    class Row(Schema):
        a: int
        b: int

    failures: list[dict[str, object]] = []

    def on_err(errs: list[dict[str, object]]) -> None:
        failures.extend(errs)

    # One row has an invalid int.
    out = validate_columns_strict(
        {"a": [1, "bad", 3], "b": [10, 20, 30]},
        Row,
        trusted_mode="off",
        ignore_errors=True,
        on_validation_errors=on_err,
    )
    assert out == {"a": [1, 3], "b": [10, 30]}
    assert failures and failures[0]["row_index"] == 1


def test_validate_columns_strict_polars_dataframe_paths() -> None:
    pl = pytest.importorskip("polars")

    class Row(Schema):
        a: int
        b: int | None

    df = pl.DataFrame({"a": [1], "b": [None]})

    with pytest.raises(TypeError, match="requires trusted ingest"):
        validate_columns_strict(df, Row, trusted_mode="off")

    with pytest.raises(ValueError, match="columns must match schema"):
        validate_columns_strict(
            pl.DataFrame({"a": [1], "c": [2]}), Row, trusted_mode="shape_only"
        )

    # strict dtype check should reject an incompatible dtype (string in int column).
    bad = pl.DataFrame({"a": ["x"], "b": [None]})
    with pytest.raises(ValueError, match="incompatible with schema annotation"):
        validate_columns_strict(bad, Row, trusted_mode="strict")

    # shape_only should warn (not raise) for dtype drift.
    with pytest.warns(DtypeDriftWarning):
        out = validate_columns_strict(bad, Row, trusted_mode="shape_only")
    assert out is bad
