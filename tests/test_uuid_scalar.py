import uuid

import pytest
from pydantic import ValidationError

from pydantable import DataFrame, Schema
from pydantable.schema import (
    descriptor_matches_column_annotation,
    dtype_descriptor_to_annotation,
    is_supported_scalar_column_annotation,
)


class Row(Schema):
    u: uuid.UUID


def test_uuid_scalar_supported_annotation():
    assert is_supported_scalar_column_annotation(uuid.UUID)
    assert is_supported_scalar_column_annotation(uuid.UUID | None)


def test_uuid_descriptor_roundtrip():
    d = {"base": "uuid", "nullable": False}
    assert dtype_descriptor_to_annotation(d) is uuid.UUID
    assert descriptor_matches_column_annotation(d, uuid.UUID)


def test_dataframe_uuid_roundtrip():
    u = uuid.uuid4()
    df = DataFrame[Row]({"u": [u]})
    assert df.to_dict()["u"][0] == u


def test_dataframe_uuid_validation_rejects_bad_cell():
    with pytest.raises(ValidationError):
        DataFrame[Row]({"u": ["not-a-uuid"]})
