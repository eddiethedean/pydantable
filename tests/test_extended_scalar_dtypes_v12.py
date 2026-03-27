"""Tests for Literal, IP, WKB, and Annotated constrained string columns (1.2.0)."""

from __future__ import annotations

import ipaddress
from typing import Annotated, Literal

import pytest
from pydantic import Field, HttpUrl, TypeAdapter, ValidationError

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


def test_literal_int_descriptor_roundtrip() -> None:
    desc = {"base": "int", "nullable": False, "literals": [1, 2, 3]}
    ann = dtype_descriptor_to_annotation(desc)
    assert descriptor_matches_column_annotation(desc, ann)


def test_literal_bool_descriptor_roundtrip() -> None:
    desc = {"base": "bool", "nullable": True, "literals": [True, False]}
    ann = dtype_descriptor_to_annotation(desc)
    assert descriptor_matches_column_annotation(desc, ann)


def test_ipv6_wkb_descriptor_roundtrip() -> None:
    d4 = {"base": "ipv4", "nullable": False}
    d6 = {"base": "ipv6", "nullable": False}
    dw = {"base": "wkb", "nullable": False}
    assert descriptor_matches_column_annotation(d4, ipaddress.IPv4Address)
    assert descriptor_matches_column_annotation(d6, ipaddress.IPv6Address)
    assert descriptor_matches_column_annotation(dw, WKB)


def test_dtype_descriptor_literals_incompatible_with_wkb_raises() -> None:
    bad = {"base": "wkb", "nullable": False, "literals": [b"a"]}
    with pytest.raises(TypeError, match="literals"):
        dtype_descriptor_to_annotation(bad)


def test_ipv4_roundtrip() -> None:
    class NetDF(DataFrameModel):
        addr: ipaddress.IPv4Address

    ip = ipaddress.IPv4Address("127.0.0.1")
    df = NetDF({"addr": [ip]})
    row = df.collect()[0]
    assert row.addr == ip
    d = df.to_dict()
    assert d["addr"] == [ip]


def test_ipv4_coerces_string_cells_to_address() -> None:
    class NetDF(DataFrameModel):
        addr: ipaddress.IPv4Address

    df = NetDF({"addr": ["10.0.0.1", "192.168.0.2"]})
    rows = df.collect()
    assert rows[0].addr == ipaddress.IPv4Address("10.0.0.1")
    assert rows[1].addr == ipaddress.IPv4Address("192.168.0.2")


def test_ipv6_roundtrip() -> None:
    class NetDF(DataFrameModel):
        addr: ipaddress.IPv6Address

    ip = ipaddress.IPv6Address("2001:db8::1")
    df = NetDF({"addr": [ip]})
    assert df.collect()[0].addr == ip
    assert df.to_dict()["addr"] == [ip]


def test_ipv4_filter_eq_address_operand() -> None:
    class NetDF(DataFrameModel):
        addr: ipaddress.IPv4Address

    needle = ipaddress.IPv4Address("10.0.0.1")
    df = NetDF(
        {
            "addr": [
                needle,
                ipaddress.IPv4Address("10.0.0.2"),
            ]
        }
    )
    out = df.filter(df.addr == needle).to_dict()
    assert out["addr"] == [needle]


def test_ipv6_filter_eq_address_operand() -> None:
    class NetDF(DataFrameModel):
        addr: ipaddress.IPv6Address

    ip = ipaddress.IPv6Address("::1")
    df = NetDF({"addr": [ip, ipaddress.IPv6Address("2001:db8::2")]})
    out = df.filter(df.addr == ip).to_dict()
    assert len(out["addr"]) == 1
    assert out["addr"][0] == ip


def test_literal_int_filter_and_rejects_non_member() -> None:
    class Nums(DataFrameModel):
        tier: Literal[1, 2, 3]

    df = Nums({"tier": [1, 2, 3]})
    assert df.filter(df.tier == 2).to_dict()["tier"] == [2]
    with pytest.raises(TypeError, match="Literal"):
        _ = df.filter(df.tier == 99)


def test_literal_bool_filter_and_rejects_non_member() -> None:
    class Flags(DataFrameModel):
        ok: Literal[True, False]

    df = Flags({"ok": [True, False]})
    assert df.filter(df.ok == True).to_dict()["ok"] == [True]  # noqa: E712
    with pytest.raises(TypeError, match="Literal"):
        _ = df.filter(df.ok == 1)


def test_optional_literal_int_none_roundtrip() -> None:
    df = TagDF({"mode": ["prod"], "flag": [None]})
    row = df.collect()[0]
    assert row.mode == "prod"
    assert row.flag is None


def test_wkb_roundtrip() -> None:
    class GeoDF(DataFrameModel):
        g: WKB

    b = WKB(b"\x01\x02\x00\x00")
    df = GeoDF({"g": [b]})
    row = df.collect()[0]
    assert row.g == b
    assert isinstance(row.g, WKB)


def test_optional_wkb_none_roundtrip() -> None:
    class GeoDF(DataFrameModel):
        g: WKB | None

    df = GeoDF({"g": [WKB(b"\x01"), None]})
    rows = df.collect()
    assert rows[0].g == WKB(b"\x01")
    assert rows[1].g is None


def test_wkb_filter_eq_wkb_and_binary_len() -> None:
    class GeoDF(DataFrameModel):
        g: WKB

    a, b = WKB(b"\xaa"), WKB(b"\xbb\xbb")
    df = GeoDF({"g": [a, b]})
    out = df.filter(df.g == a).to_dict()
    assert out["g"] == [a]
    lens = df.with_columns(n=df.g.cast(bytes).binary_len()).to_dict()["n"]
    assert lens == [1, 2]


def test_annotated_url_string_pydantic() -> None:
    class WebDF(DataFrameModel):
        link: Annotated[str, HttpUrl]

    df = WebDF({"link": ["https://example.com/path"]})
    row = df.collect()[0]
    assert str(row.link).startswith("https://example.com/")


def test_annotated_constrained_str_rejected_on_collect() -> None:
    """Pydantic metadata is applied when row models are built (e.g. ``collect()``)."""

    class WebDF(DataFrameModel):
        link: Annotated[str, Field(min_length=50)]

    df = WebDF({"link": ["short"]})
    with pytest.raises(ValidationError):
        df.collect()


def test_select_after_filter_preserves_literal_column() -> None:
    df = TagDF({"mode": ["dev", "prod"], "flag": [1, 2]})
    out = df.filter(df.mode == "dev").select("mode")
    assert out.to_dict() == {"mode": ["dev"]}


def test_with_columns_copies_literal_column() -> None:
    df = TagDF({"mode": ["dev"], "flag": [1]})
    out = df.with_columns(mode_copy=df.mode).select("mode", "mode_copy")
    d = out.to_dict()
    assert d == {"mode": ["dev"], "mode_copy": ["dev"]}


def test_type_adapter_ipv4() -> None:
    ta = TypeAdapter(ipaddress.IPv4Address)
    assert ta.validate_python("10.0.0.1") == ipaddress.IPv4Address("10.0.0.1")
