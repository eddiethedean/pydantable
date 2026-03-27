"""Tests for Literal, IP, WKB, and Annotated constrained string columns (1.2.0)."""

from __future__ import annotations

import ipaddress
from typing import Annotated, Literal

import pytest
from pydantic import BaseModel, HttpUrl, TypeAdapter

from pydantable import DataFrameModel
from pydantable.schema import (
    descriptor_matches_column_annotation,
    dtype_descriptor_to_annotation,
    is_supported_column_annotation,
)
from pydantable.types import WKB


def test_is_supported_literal_ip_wkb() -> None:
    assert is_supported_column_annotation(Literal["a", "b"])
    assert is_supported_column_annotation(Literal[1, 2])
    assert is_supported_column_annotation(Literal[True, False])
    assert not is_supported_column_annotation(Literal["a", 1])
    assert is_supported_column_annotation(ipaddress.IPv4Address)
    assert is_supported_column_annotation(ipaddress.IPv6Address)
    assert is_supported_column_annotation(WKB)
    assert is_supported_column_annotation(Annotated[str, "meta"])


class TagDF(DataFrameModel):
    mode: Literal["dev", "prod"]
    flag: Literal[1, 2] | None


def test_literal_dataframe_filter_collect() -> None:
    df = TagDF({"mode": ["dev", "prod"], "flag": [1, None]})
    out = df.filter(df.mode == "dev")
    d = out.to_dict()
    assert d == {"mode": ["dev"], "flag": [1]}


def test_literal_compare_rejects_non_member() -> None:
    df = TagDF({"mode": ["dev"], "flag": [1]})
    with pytest.raises(TypeError, match="Literal"):
        _ = df.filter(df.mode == "staging")


def test_literal_descriptor_roundtrip() -> None:
    desc = {"base": "str", "nullable": False, "literals": ["dev", "prod"]}
    ann = dtype_descriptor_to_annotation(desc)
    assert descriptor_matches_column_annotation(desc, ann)


def test_ipv4_roundtrip() -> None:
    class NetDF(DataFrameModel):
        addr: ipaddress.IPv4Address

    ip = ipaddress.IPv4Address("127.0.0.1")
    df = NetDF({"addr": [ip]})
    row = df.collect()[0]
    assert row.addr == ip
    d = df.to_dict()
    assert d["addr"] == [ip]


def test_wkb_roundtrip() -> None:
    class GeoDF(DataFrameModel):
        g: WKB

    b = WKB(b"\x01\x02\x00\x00")
    df = GeoDF({"g": [b]})
    row = df.collect()[0]
    assert row.g == b
    assert isinstance(row.g, WKB)


def test_annotated_url_string_pydantic() -> None:
    class WebDF(DataFrameModel):
        link: Annotated[str, HttpUrl]

    df = WebDF({"link": ["https://example.com/path"]})
    row = df.collect()[0]
    assert str(row.link).startswith("https://example.com/")


def test_type_adapter_ipv4() -> None:
    ta = TypeAdapter(ipaddress.IPv4Address)
    assert ta.validate_python("10.0.0.1") == ipaddress.IPv4Address("10.0.0.1")
