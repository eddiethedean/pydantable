from __future__ import annotations

import pytest


def test_try_as_model_returns_none_on_mismatch() -> None:
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int

    class After(DataFrameModel):
        id: int
        extra: int

    df = Before({"id": [1]})
    out = df.try_as_model(After)
    assert out is None


def test_try_as_model_raises_for_non_model_class() -> None:
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int

    df = Before({"id": [1]})
    with pytest.raises(TypeError, match="try_as_model\\(model=\\.\\.\\.\\)"):
        df.try_as_model(int)  # type: ignore[arg-type]


def test_assert_model_error_includes_diff_details() -> None:
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int

    class After(DataFrameModel):
        id: int
        extra: int

    df = Before({"id": [1]})
    with pytest.raises(TypeError) as exc:
        df.assert_model(After)
    # Stable substrings; exact ordering is intentionally not over-specified.
    msg = str(exc.value)
    assert "assert_model(schema mismatch)" in msg
    assert "missing=" in msg or "extra=" in msg or "mismatched_types=" in msg


def test_assert_model_reports_type_mismatches() -> None:
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int

    class After(DataFrameModel):
        id: str

    df = Before({"id": [1]})
    with pytest.raises(TypeError) as exc:
        df.assert_model(After)
    assert "mismatched_types=" in str(exc.value)


def test_as_model_can_skip_validation() -> None:
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int

    class Wrong(DataFrameModel):
        nope: int

    df = Before({"id": [1]})
    # Escape hatch: allows re-wrapping without schema validation.
    out = df.as_model(Wrong, validate_schema=False)
    assert isinstance(out, Wrong)


def test_try_as_model_can_skip_validation() -> None:
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int

    class Wrong(DataFrameModel):
        nope: int

    df = Before({"id": [1]})
    out = df.try_as_model(Wrong, validate_schema=False)
    assert isinstance(out, Wrong)

