"""Integration tests for type-specific Expr: numeric, string, bool, temporal, list."""

from __future__ import annotations

import calendar
from datetime import date, datetime, time, timedelta, timezone

import pytest
from pydantable import DataFrame, Schema


class _Num(Schema):
    x: int
    y: float


class _Str(Schema):
    s: str


class _StrOpt(Schema):
    s: str | None


class _StrDate(Schema):
    s: str


class _Bool(Schema):
    a: bool
    b: bool


class _Dt(Schema):
    ts: datetime


class _Donly(Schema):
    d: date


class _ListInt(Schema):
    items: list[int]


class _ListFloat(Schema):
    nums: list[float]


class _ListStr(Schema):
    tok: list[str]


class _ListIntOpt(Schema):
    items: list[int] | None


class _Tonly(Schema):
    t: time


def test_numeric_unary_ops() -> None:
    df = DataFrame[_Num]({"x": [-1, 2], "y": [2.4, -3.7]})
    out = df.with_columns(
        ax=df.x.abs(),
        rx=df.x.round(0),
        fy=df.y.floor(),
        cy=df.y.ceil(),
    ).collect(as_lists=True)
    assert out["ax"] == [1, 2]
    assert out["rx"] == [-1, 2]
    assert out["fy"][0] == 2.0 and out["fy"][1] == -4.0
    assert out["cy"][0] == 3.0 and out["cy"][1] == -3.0


def test_string_strip_case() -> None:
    df = DataFrame[_Str]({"s": ["  hi  ", "AbC"]})
    out = df.with_columns(
        t=df.s.strip(),
        u=df.s.upper(),
        lo=df.s.lower(),
    ).collect(as_lists=True)
    assert out["t"] == ["hi", "AbC"]
    assert out["u"] == ["  HI  ", "ABC"]
    assert out["lo"] == ["  hi  ", "abc"]


def test_string_starts_ends_contains() -> None:
    df = DataFrame[_StrOpt]({"s": ["hello", "world", None, "hello_world"]})
    out = df.with_columns(
        p=df.s.starts_with("hel"),
        e=df.s.ends_with("ld"),
        c=df.s.str_contains("_"),
    ).collect(as_lists=True)
    assert out["p"] == [True, False, None, True]
    assert out["e"] == [False, True, None, True]
    assert out["c"] == [False, False, None, True]


def test_str_contains_pat_regex() -> None:
    df = DataFrame[_StrOpt]({"s": ["a1", "b2", "no", None]})
    out = df.with_columns(
        m=df.s.str_contains_pat(r"\d", literal=False),
    ).collect(as_lists=True)
    assert out["m"] == [True, True, False, None]


def test_str_contains_pat_literal_true_is_not_regex() -> None:
    """literal=True searches for the raw substring; literal=False uses regex."""
    df = DataFrame[_Str]({"s": ["a1", r"a\d", "no"]})
    out = df.with_columns(
        lit=df.s.str_contains_pat(r"\d", literal=True),
        rx=df.s.str_contains_pat(r"\d", literal=False),
    ).to_dict()
    assert out["lit"] == [False, True, False]
    assert out["rx"] == [True, False, False]


def test_starts_with_empty_prefix_and_str_contains_empty() -> None:
    df = DataFrame[_Str]({"s": ["x", ""]})
    out = df.with_columns(
        p=df.s.starts_with(""),
        c=df.s.str_contains(""),
    ).to_dict()
    assert out["p"] == [True, True]
    assert out["c"] == [True, True]


def test_str_contains_pat_empty_regex_raises() -> None:
    df = DataFrame[_Str]({"s": ["a"]})
    with pytest.raises(ValueError, match="empty"):
        df.with_columns(x=df.s.str_contains_pat("", literal=False))


def test_invalid_regex_str_contains_pat_null_in_to_dict() -> None:
    """Polars may yield null for bad patterns instead of failing the plan."""
    df = DataFrame[_Str]({"s": ["a", "b"]})
    out = df.with_columns(x=df.s.str_contains_pat("[", literal=False)).to_dict()
    assert out["x"] == [None, None]


def test_string_predicates_reject_non_string_column() -> None:
    df = DataFrame[_Num]({"x": [1], "y": [1.0]})
    for meth_name in ("starts_with", "ends_with", "str_contains", "str_split"):
        meth = getattr(df.x, meth_name)
        arg = "," if meth_name == "str_split" else "a"
        with pytest.raises(TypeError, match="string"):
            df.with_columns(z=meth(arg))


def test_dt_weekday_quarter_reject_time_column() -> None:
    df = DataFrame[_Tonly]({"t": [time(12, 0, 0)]})
    for meth_name in ("dt_weekday", "dt_quarter"):
        meth = getattr(df.t, meth_name)
        with pytest.raises(TypeError, match=r"datetime|date|temporal"):
            df.with_columns(z=meth())


def test_str_replace_literal_vs_regex() -> None:
    df = DataFrame[_Str]({"s": ["foo123", "a.c", "x"]})
    out = df.with_columns(
        lit=df.s.str_replace(".", "Z", literal=True),
        rx=df.s.str_replace(r".+", "Q", literal=False),
    ).collect(as_lists=True)
    assert out["lit"] == ["foo123", "aZc", "x"]
    assert out["rx"] == ["Q", "Q", "Q"]


def test_string_replace_strip_prefix_suffix_chars() -> None:
    df = DataFrame[_Str](
        {
            "s": ["foo_bar", "pre:value", "value:suf", "aabba"],
        }
    )
    out = df.with_columns(
        r=df.s.str_replace("_", "-"),
        p=df.s.strip_prefix("pre:"),
        x=df.s.strip_suffix(":suf"),
        c=df.s.strip_chars("ab"),
    ).collect(as_lists=True)
    assert out["r"] == ["foo-bar", "pre:value", "value:suf", "aabba"]
    assert out["p"] == ["foo_bar", "value", "value:suf", "aabba"]
    assert out["x"] == ["foo_bar", "pre:value", "value", "aabba"]
    assert out["c"] == ["foo_bar", "pre:value", "value:suf", ""]


def test_logical_ops() -> None:
    df = DataFrame[_Bool]({"a": [True, False, True], "b": [False, False, True]})
    out = df.with_columns(
        x=df.a & df.b,
        y=df.a | df.b,
        z=~df.a,
    ).collect(as_lists=True)
    assert out["x"] == [False, False, True]
    assert out["y"] == [True, False, True]
    assert out["z"] == [False, True, False]


def test_dt_weekday_and_quarter() -> None:
    # UTC wall calendar matches Polars dt parts for this timestamp.
    df = DataFrame[_Dt](
        {
            "ts": [
                datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc),
            ],
        }
    )
    out = df.with_columns(w=df.ts.dt_weekday(), q=df.ts.dt_quarter()).collect(
        as_lists=True
    )
    assert out["w"] == [date(2024, 3, 15).isoweekday()]
    assert out["q"] == [1]

    df_d = DataFrame[_Donly]({"d": [date(2024, 8, 1)]})
    out_d = df_d.with_columns(w=df_d.d.dt_weekday(), q=df_d.d.dt_quarter()).collect(
        as_lists=True
    )
    assert out_d["w"] == [date(2024, 8, 1).isoweekday()]
    assert out_d["q"] == [3]


def test_temporal_parts_datetime() -> None:
    # Use UTC so Polars temporal parts match wall time regardless of host TZ.
    df = DataFrame[_Dt](
        {
            "ts": [
                datetime(2024, 3, 15, 14, 7, 9, tzinfo=timezone.utc),
                datetime(2000, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
            ],
        }
    )
    out = df.with_columns(
        y=df.ts.dt_year(),
        mo=df.ts.dt_month(),
        d=df.ts.dt_day(),
        h=df.ts.dt_hour(),
        mi=df.ts.dt_minute(),
        s=df.ts.dt_second(),
    ).collect(as_lists=True)
    assert out["y"] == [2024, 2000]
    assert out["mo"] == [3, 1]
    assert out["d"] == [15, 2]
    assert out["h"] == [14, 0]
    assert out["mi"] == [7, 0]
    assert out["s"] == [9, 0]


def test_temporal_parts_date_only() -> None:
    df = DataFrame[_Donly]({"d": [date(2024, 5, 1), date(1999, 12, 31)]})
    out = df.with_columns(
        y=df.d.dt_year(),
        m=df.d.dt_month(),
        day=df.d.dt_day(),
    ).collect(as_lists=True)
    assert out["y"] == [2024, 1999]
    assert out["m"] == [5, 12]
    assert out["day"] == [1, 31]


def test_dt_hour_rejects_date_column() -> None:
    df = DataFrame[_Donly]({"d": [date(2024, 1, 1)]})
    with pytest.raises(TypeError, match="datetime"):
        df.with_columns(h=df.d.dt_hour())


def test_cast_datetime_to_date_and_str() -> None:
    df = DataFrame[_Dt]({"ts": [datetime(2024, 6, 10, 15, 0, 0)]})
    out = df.with_columns(
        as_date=df.ts.cast(date),
        as_str=df.ts.cast(str),
    ).collect(as_lists=True)
    assert out["as_date"] == [date(2024, 6, 10)]
    assert isinstance(out["as_str"][0], str)
    assert "2024" in out["as_str"][0]


def test_cast_str_to_date_iso8601() -> None:
    df = DataFrame[_StrDate]({"s": ["2024-06-10", "2000-01-02"]})
    out = df.with_columns(as_date=df.s.cast(date)).collect(as_lists=True)
    assert out["as_date"] == [date(2024, 6, 10), date(2000, 1, 2)]


def test_cast_str_to_datetime_iso8601_instant() -> None:
    """Cast uses Polars; collect uses local `datetime.fromtimestamp` for display."""
    df = DataFrame[_StrDate]({"s": ["2024-06-10T00:00:00", "2000-01-02T12:30:45"]})
    out = df.with_columns(as_dt=df.s.cast(datetime)).collect(as_lists=True)
    exp0 = calendar.timegm((2024, 6, 10, 0, 0, 0, 0, 0, 0))
    exp1 = calendar.timegm((2000, 1, 2, 12, 30, 45, 0, 0, 0))
    assert abs(out["as_dt"][0].timestamp() - exp0) < 1.0
    assert abs(out["as_dt"][1].timestamp() - exp1) < 1.0


def test_cast_str_to_datetime_with_time_instant() -> None:
    df = DataFrame[_StrDate]({"s": ["2024-03-15T14:07:09"]})
    out = df.with_columns(ts=df.s.cast(datetime)).collect(as_lists=True)
    exp = calendar.timegm((2024, 3, 15, 14, 7, 9, 0, 0, 0))
    assert abs(out["ts"][0].timestamp() - exp) < 1.0


def test_dt_date_method_matches_cast() -> None:
    ts = datetime(2024, 6, 10, 15, 30, 0, tzinfo=timezone.utc)
    df = DataFrame[_Dt]({"ts": [ts]})
    out = df.with_columns(d=df.ts.dt_date()).collect(as_lists=True)
    assert out["d"] == [date(2024, 6, 10)]


def test_datetime_plus_timedelta() -> None:
    # Naive datetimes keep host/Polars interpretation aligned for this smoke test.
    ts = datetime(2024, 1, 1, 12, 0, 0)
    df = DataFrame[_Dt]({"ts": [ts]})
    out = df.with_columns(later=df.ts + timedelta(hours=2)).collect(as_lists=True)
    assert out["later"][0] == ts + timedelta(hours=2)


def test_date_plus_timedelta() -> None:
    df = DataFrame[_Donly]({"d": [date(2024, 1, 5)]})
    out = df.with_columns(n=df.d + timedelta(days=2)).collect(as_lists=True)
    assert out["n"] == [date(2024, 1, 7)]


def test_list_len() -> None:
    df = DataFrame[_ListInt](
        {
            "items": [
                [1, 2, 3],
                [],
                [0],
            ],
        }
    )
    out = df.with_columns(n=df.items.list_len()).collect(as_lists=True)
    assert out["n"] == [3, 0, 1]


def test_list_len_rejects_non_list() -> None:
    df = DataFrame[_Num]({"x": [1], "y": [1.0]})
    with pytest.raises(TypeError, match="list"):
        df.with_columns(n=df.x.list_len())


def test_list_mean_int_and_float() -> None:
    df = DataFrame[_ListInt]({"items": [[1, 2, 3], [], [10]]})
    out = df.with_columns(m=df.items.list_mean()).collect(as_lists=True)
    assert out["m"][0] == 2.0
    assert out["m"][1] is None
    assert out["m"][2] == 10.0

    df2 = DataFrame[_ListFloat]({"nums": [[1.0, 3.0], [0.5]]})
    out2 = df2.with_columns(m=df2.nums.list_mean()).collect(as_lists=True)
    assert out2["m"] == [2.0, 0.5]


def test_str_split_to_list_str() -> None:
    df = DataFrame[_StrOpt]({"s": ["a,b,c", "x", None]})
    out = df.with_columns(parts=df.s.str_split(",")).collect(as_lists=True)
    assert out["parts"][0] == ["a", "b", "c"]
    assert out["parts"][1] == ["x"]
    assert out["parts"][2] is None


def test_str_split_empty_delimiter_utf8_and_empty_string() -> None:
    df = DataFrame[_Str]({"s": ["ab", ""]})
    out = df.with_columns(p=df.s.str_split("")).to_dict()
    assert out["p"] == [["a", "b"], []]


def test_str_split_consecutive_delimiters_preserve_empty_tokens() -> None:
    df = DataFrame[_Str]({"s": ["a,,b", ","]})
    out = df.with_columns(p=df.s.str_split(",")).to_dict()
    assert out["p"] == [["a", "", "b"], ["", ""]]


def test_str_split_unicode_delimiter() -> None:
    df = DataFrame[_Str]({"s": ["café÷x"]})
    out = df.with_columns(p=df.s.str_split("÷")).to_dict()
    assert out["p"] == [["café", "x"]]


def test_filter_uses_string_predicates() -> None:
    df = DataFrame[_Str]({"s": ["alpha", "beta", "gamma"]})
    out = df.filter(df.s.starts_with("a") | df.s.ends_with("a")).collect(as_lists=True)
    assert out["s"] == ["alpha", "beta", "gamma"]


def test_list_mean_rejects_non_numeric_list() -> None:
    df = DataFrame[_ListStr]({"tok": [["a"], ["b"]]})
    with pytest.raises(TypeError, match=r"list_mean|list\[int\]|float"):
        df.with_columns(m=df.tok.list_mean())


def test_list_mean_null_list_cell() -> None:
    df = DataFrame[_ListIntOpt]({"items": [[1, 3], None, [2]]})
    out = df.with_columns(m=df.items.list_mean()).to_dict()
    assert out["m"] == [2.0, None, 2.0]


def test_list_get_contains_min_max_sum() -> None:
    df = DataFrame[_ListInt](
        {
            "items": [
                [10, 20, 30],
                [1, 1, 2],
            ],
        }
    )
    out = df.with_columns(
        g0=df.items.list_get(0),
        g99=df.items.list_get(99),
        has20=df.items.list_contains(20),
        has99=df.items.list_contains(99),
        mn=df.items.list_min(),
        mx=df.items.list_max(),
        sm=df.items.list_sum(),
    ).collect(as_lists=True)
    assert out["g0"] == [10, 1]
    assert out["g99"] == [None, None]
    assert out["has20"] == [True, False]
    assert out["has99"] == [False, False]
    assert out["mn"] == [10, 1]
    assert out["mx"] == [30, 2]
    assert out["sm"] == [60, 4]

    df2 = DataFrame[_ListFloat]({"nums": [[1.5, 2.5], [0.0]]})
    out2 = df2.with_columns(s=df2.nums.list_sum()).collect(as_lists=True)
    assert out2["s"] == [4.0, 0.0]
