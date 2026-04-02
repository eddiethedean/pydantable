"""Phase 5: sqlmodel_columns and DataFrameModel.assert_sqlmodel_compatible."""

from __future__ import annotations

from typing import Any, cast

import pytest
from pydantable import DataFrameModel
from pydantable.errors import MissingOptionalDependency
from pydantable.io import sqlmodel_columns

pytest.importorskip("sqlmodel")

from sqlmodel import Field, SQLModel


class _Widget(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    label: str


class _SkuRow(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    sku: str


class WidgetDF(DataFrameModel):
    id: int | None
    label: str


class SkuAsProductDF(DataFrameModel):
    id: int | None
    product_sku: str


class MissingLabelDF(DataFrameModel):
    id: int | None


class ExtraColDF(DataFrameModel):
    id: int | None
    label: str
    note: str


class IdOnlyDF(DataFrameModel):
    id: int | None


def test_sqlmodel_columns_order_and_keys() -> None:
    cols = sqlmodel_columns(_Widget)
    assert cols == ["id", "label"]
    assert set(cols) == {c.key for c in _Widget.__table__.columns}


def test_assert_sqlmodel_compatible_write_ok() -> None:
    WidgetDF.assert_sqlmodel_compatible(_Widget, direction="write")


def test_assert_sqlmodel_compatible_read_ok() -> None:
    WidgetDF.assert_sqlmodel_compatible(_Widget, direction="read")


def test_assert_sqlmodel_compatible_write_missing_df_field() -> None:
    with pytest.raises(ValueError, match="missing SQL columns"):
        MissingLabelDF.assert_sqlmodel_compatible(_Widget, direction="write")


def test_assert_sqlmodel_compatible_write_extra_df_field() -> None:
    with pytest.raises(ValueError, match="extra keys vs SQL table"):
        ExtraColDF.assert_sqlmodel_compatible(_Widget, direction="write")


def test_assert_sqlmodel_compatible_write_column_map_ok() -> None:
    SkuAsProductDF.assert_sqlmodel_compatible(
        _SkuRow,
        direction="write",
        column_map={"product_sku": "sku"},
    )
    SkuAsProductDF.assert_sqlmodel_compatible(
        _SkuRow,
        direction="read",
        column_map={"product_sku": "sku"},
    )


def test_assert_sqlmodel_compatible_read_projection() -> None:
    IdOnlyDF.assert_sqlmodel_compatible(
        _Widget,
        direction="read",
        read_keys=["id"],
    )


def test_assert_sqlmodel_compatible_read_projection_fails_when_df_needs_more() -> None:
    with pytest.raises(ValueError, match="not present in expected SQL result keys"):
        WidgetDF.assert_sqlmodel_compatible(
            _Widget,
            direction="read",
            read_keys=["id"],
        )


def test_assert_sqlmodel_compatible_column_map_unknown_key() -> None:
    with pytest.raises(ValueError, match="column_map keys are not DataFrameModel"):
        WidgetDF.assert_sqlmodel_compatible(
            _Widget,
            direction="write",
            column_map={"nosuch": "label"},
        )


def test_assert_sqlmodel_compatible_write_duplicate_targets() -> None:
    with pytest.raises(ValueError, match="duplicate SQL column key"):
        WidgetDF.assert_sqlmodel_compatible(
            _Widget,
            direction="write",
            column_map={"id": "label", "label": "label"},
        )


def test_assert_sqlmodel_compatible_bad_direction() -> None:
    with pytest.raises(ValueError, match="direction must be"):
        WidgetDF.assert_sqlmodel_compatible(_Widget, direction=cast(Any, "both"))  # noqa: TC006


def test_sqlmodel_columns_requires_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    import pydantable.io.sqlmodel_schema as sms

    def _boom() -> None:
        raise MissingOptionalDependency("sqlmodel")

    monkeypatch.setattr(sms, "_require_sqlmodel", _boom)
    with pytest.raises(MissingOptionalDependency):
        sqlmodel_columns(_Widget)
