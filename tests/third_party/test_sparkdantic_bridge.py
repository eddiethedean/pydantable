"""``pydantable.pyspark.sparkdantic`` helpers (optional ``[spark]`` stack)."""

from __future__ import annotations

import pytest

pytest.importorskip("sparkdantic")

from pydantable import DataFrameModel, Schema
from pydantable.pyspark.sparkdantic import (
    SparkField,
    SparkModel,
    TypeConversionError,
    dataframe_model_to_pyspark_struct_type,
    dataframe_model_to_spark_ddl_schema,
    dataframe_model_to_spark_json_schema,
    to_pyspark_struct_type,
    to_spark_ddl_schema,
    to_spark_json_schema,
)
from pydantic import Field


class User(Schema):
    id: int
    label: str | None


def test_to_spark_json_schema_from_schema() -> None:
    js = to_spark_json_schema(User)
    assert js["type"] == "struct"
    names = {f["name"] for f in js["fields"]}
    assert names == {"id", "label"}


@pytest.mark.filterwarnings("ignore::pydantic.warnings.PydanticDeprecatedSince20")
def test_exclude_fields_and_spark_field_override() -> None:
    class Row(Schema):
        # SparkDantic also accepts ``json_schema_extra={"spark_type": "..."}`` on Field.
        n: int = SparkField(spark_type="bigint")
        secret: str = Field(exclude=True)

    js = to_spark_json_schema(Row, exclude_fields=True)
    names = {f["name"] for f in js["fields"]}
    assert names == {"n"}
    for field in js["fields"]:
        if field["name"] == "n":
            assert field["type"] in ("long", "bigint")


def test_spark_model_classmethods_need_pyspark() -> None:
    pytest.importorskip("pyspark")

    class M(SparkModel):
        x: int

    st = M.model_spark_schema()
    assert st.fieldNames() == ["x"]
    ddl = M.model_ddl_spark_schema()
    assert "x" in ddl


def test_dataframe_model_json_schema_matches_row_model() -> None:
    class M(DataFrameModel):
        id: int
        label: str | None

    js_row = to_spark_json_schema(M.RowModel)
    assert js_row == to_spark_json_schema(M._SchemaModel)
    assert js_row == dataframe_model_to_spark_json_schema(M)


def test_to_pyspark_struct_type_requires_pyspark() -> None:
    pyspark = pytest.importorskip("pyspark")
    st = to_pyspark_struct_type(User)
    assert type(st).__module__ == pyspark.sql.types.__name__
    assert st.fieldNames() == ["id", "label"]


def test_to_spark_ddl_schema() -> None:
    pytest.importorskip("pyspark")
    ddl = to_spark_ddl_schema(User)
    assert isinstance(ddl, str)
    assert "id" in ddl


def test_dataframe_model_to_pyspark_struct_type() -> None:
    pytest.importorskip("pyspark")

    class M(DataFrameModel):
        id: int
        label: str | None

    st1 = dataframe_model_to_pyspark_struct_type(M)
    st2 = to_pyspark_struct_type(M.RowModel)
    assert st1 == st2
    assert dataframe_model_to_spark_ddl_schema(M)


def test_type_conversion_error_surfaces() -> None:
    class Bad(Schema):
        x: object  # not mappable to Spark

    with pytest.raises(TypeConversionError):
        to_spark_json_schema(Bad)
