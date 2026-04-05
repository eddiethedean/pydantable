from __future__ import annotations

import ipaddress
from typing import Literal

from pydantable import WKB, DataFrameModel
from pydantable.typing_engine import (
    infer_schema_descriptors_drop,
    infer_schema_descriptors_rename,
    infer_schema_descriptors_select,
    infer_schema_descriptors_with_columns,
)


class Users(DataFrameModel):
    id: int
    age: int
    city: str


def test_typing_engine_select_matches_runtime_schema_fields() -> None:
    df = Users({"id": [1], "age": [2], "city": ["x"]})
    out = df.select("id", "age")
    desc = infer_schema_descriptors_select(df.schema_fields(), ["id", "age"])
    assert set(desc) == set(out.schema_fields())


def test_typing_engine_drop_matches_runtime_schema_fields() -> None:
    df = Users({"id": [1], "age": [2], "city": ["x"]})
    out = df.drop("city")
    desc = infer_schema_descriptors_drop(df.schema_fields(), ["city"])
    assert set(desc) == set(out.schema_fields())


def test_typing_engine_rename_matches_runtime_schema_fields() -> None:
    df = Users({"id": [1], "age": [2], "city": ["x"]})
    out = df.rename({"age": "years"})
    desc = infer_schema_descriptors_rename(df.schema_fields(), {"age": "years"})
    assert set(desc) == set(out.schema_fields())


def test_typing_engine_with_columns_matches_runtime_schema_fields() -> None:
    df = Users({"id": [1], "age": [2], "city": ["x"]})
    out = df.with_columns(age2=df.age * 2)
    desc = infer_schema_descriptors_with_columns(
        df.schema_fields(), {"age2": df.age * 2}
    )
    assert set(desc) == set(out.schema_fields())


def test_typing_engine_with_columns_literal_not_expr() -> None:
    df = Users({"id": [1], "age": [2], "city": ["x"]})
    out = df.with_columns(flag=True)
    desc = infer_schema_descriptors_with_columns(df.schema_fields(), {"flag": True})
    assert set(desc) == set(out.schema_fields())


class V12Scalars(DataFrameModel):
    mode: Literal["dev", "prod"]
    addr: ipaddress.IPv4Address
    g: WKB


def _v12_sample() -> V12Scalars:
    return V12Scalars(
        {
            "mode": ["dev"],
            "addr": [ipaddress.IPv4Address("10.0.0.1")],
            "g": [WKB(b"\x01\x02\x00\x00")],
        }
    )


def test_typing_engine_v12_select_matches_runtime() -> None:
    df = _v12_sample()
    out = df.select("mode", "addr")
    desc = infer_schema_descriptors_select(df.schema_fields(), ["mode", "addr"])
    assert set(desc) == set(out.schema_fields())


def test_typing_engine_v12_drop_matches_runtime() -> None:
    df = _v12_sample()
    out = df.drop("g")
    desc = infer_schema_descriptors_drop(df.schema_fields(), ["g"])
    assert set(desc) == set(out.schema_fields())


def test_typing_engine_v12_rename_matches_runtime() -> None:
    df = _v12_sample()
    out = df.rename({"addr": "ip"})
    desc = infer_schema_descriptors_rename(df.schema_fields(), {"addr": "ip"})
    assert set(desc) == set(out.schema_fields())


def test_typing_engine_v12_with_columns_matches_runtime() -> None:
    df = _v12_sample()
    out = df.with_columns(two=df.mode)
    desc = infer_schema_descriptors_with_columns(df.schema_fields(), {"two": df.mode})
    assert set(desc) == set(out.schema_fields())
