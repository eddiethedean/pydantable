"""Cover remaining selector dtype helpers (boolean/temporal/maps/enum/ip/wkb)."""

from __future__ import annotations

import enum
import ipaddress
from datetime import date, datetime, time, timedelta

from pydantable import DataFrame
from pydantable import selectors as s
from pydantable.schema import Schema
from pydantable.types import WKB


class _Color(enum.Enum):
    R = 1


class _Wide(Schema):
    flag: bool
    when: datetime
    day: date
    tm: time
    delta: timedelta
    label: str
    nums: list[int]
    bag: dict[str, int]
    tag: _Color
    v4: ipaddress.IPv4Address
    v6: ipaddress.IPv6Address
    geom: WKB


def test_selector_boolean_temporal_string_lists() -> None:
    df = DataFrame[_Wide](
        {
            "flag": [True],
            "when": [datetime(2020, 1, 1)],
            "day": [date(2020, 1, 1)],
            "tm": [time(12, 0)],
            "delta": [timedelta(days=1)],
            "label": ["x"],
            "nums": [[1]],
            "bag": [{"a": 1}],
            "tag": [_Color.R],
            "v4": [ipaddress.IPv4Address("127.0.0.1")],
            "v6": [ipaddress.IPv6Address("::1")],
            "geom": [WKB(b"\x01")],
        }
    )
    assert set(df.select_schema(s.boolean()).columns) == {"flag"}
    assert set(df.select_schema(s.string()).columns) == {"label"}
    out_t = set(df.select_schema(s.temporal()).columns)
    assert {"when", "day", "tm", "delta"}.issubset(out_t)
    assert set(df.select_schema(s.lists()).columns) == {"nums"}
    assert set(df.select_schema(s.maps()).columns) == {"bag"}
    assert set(df.select_schema(s.enums()).columns) == {"tag"}
    assert set(df.select_schema(s.ipv4s()).columns) == {"v4"}
    assert set(df.select_schema(s.ipv6s()).columns) == {"v6"}
    assert set(df.select_schema(s.wkbs()).columns) == {"geom"}
