"""Integration tests for type-specific Expr: numeric, string, bool, temporal, list."""

from __future__ import annotations

import calendar
from datetime import date, datetime, time, timedelta, timezone

import pytest
from pydantable import DataFrame, Schema


class _Num(Schema):
    x: int
    y: float


class _IntOnly(Schema):
    x: int


class _IntOpt(Schema):
    x: int | None


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


def test_string_format_ops_reject_non_string_column() -> None:
    """str_reverse, pad, zfill, regex extract, JSONPath require string-like columns."""
    df = DataFrame[_IntOnly]({"x": [1]})
    with pytest.raises(TypeError, match="string"):
        df.with_columns(z=df.x.str_reverse())
    with pytest.raises(TypeError, match="string"):
        df.with_columns(z=df.x.str_pad_start(3, "0"))
    with pytest.raises(TypeError, match="string"):
        df.with_columns(z=df.x.str_zfill(3))
    with pytest.raises(TypeError, match="string"):
        df.with_columns(z=df.x.str_extract_regex(r"\d", 0))
    with pytest.raises(TypeError, match="string"):
        df.with_columns(z=df.x.str_json_path_match("$.a"))


def test_list_join_sort_unique_reject_non_list_column() -> None:
    df = DataFrame[_Str]({"s": ["a"]})
    for meth_name, args in (
        ("list_join", (",",)),
        ("list_sort", ()),
        ("list_unique", ()),
    ):
        meth = getattr(df.s, meth_name)
        with pytest.raises(TypeError, match="list"):
            if args:
                df.with_columns(z=meth(*args))
            else:
                df.with_columns(z=meth())


def test_dt_weekday_quarter_reject_time_column() -> None:
    df = DataFrame[_Tonly]({"t": [time(12, 0, 0)]})
    for meth_name in ("dt_weekday", "dt_quarter", "dt_week"):
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


def test_dt_dayofyear_date_and_datetime() -> None:
    df_d = DataFrame[_Donly]({"d": [date(2024, 1, 1), date(2024, 12, 31)]})
    o_d = df_d.with_columns(doy=df_d.d.dt_dayofyear()).collect(as_lists=True)
    assert o_d["doy"] == [1, 366]

    df_dt = DataFrame[_Dt](
        {"ts": [datetime(2024, 3, 1, 0, 0, 0), datetime(2024, 7, 15, 0, 0, 0)]}
    )
    o_dt = df_dt.with_columns(doy=df_dt.ts.dt_dayofyear()).collect(as_lists=True)
    assert o_dt["doy"] == [
        date(2024, 3, 1).timetuple().tm_yday,
        date(2024, 7, 15).timetuple().tm_yday,
    ]


def test_from_unix_time_round_trip_utc_instant() -> None:
    ts = datetime(2024, 6, 10, 12, 30, 45)
    sec = calendar.timegm(ts.timetuple())
    df_i = DataFrame[_IntOpt]({"x": [sec, None]})
    out = df_i.with_columns(d=df_i.x.from_unix_time("seconds")).collect(as_lists=True)
    assert out["d"][0] is not None
    assert abs(out["d"][0].timestamp() - float(sec)) < 1.0
    assert out["d"][1] is None

    msec = sec * 1000
    df_m = DataFrame[_IntOnly]({"x": [msec]})
    om = df_m.with_columns(d=df_m.x.from_unix_time("ms")).collect(as_lists=True)
    assert abs(om["d"][0].timestamp() - float(sec)) < 1.0


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


def test_dt_week_matches_python_isocalendar() -> None:
    samples = [date(2024, 1, 1), date(2015, 1, 1), date(2021, 1, 4)]
    df = DataFrame[_Donly]({"d": samples})
    out = df.with_columns(w=df.d.dt_week()).to_dict()
    assert out["w"] == [d.isocalendar().week for d in samples]

    # Datetime column: week from calendar date (naive wall time).
    dts = [
        datetime(2024, 1, 1, 15, 30, 0),
        datetime(2020, 12, 31, 0, 0, 0),
    ]
    df2 = DataFrame[_Dt]({"ts": dts})
    out2 = df2.with_columns(w=df2.ts.dt_week()).to_dict()
    assert out2["w"] == [dt.date().isocalendar().week for dt in dts]


def test_dt_week_year_boundary_week_53() -> None:
    """ISO week can be 53 in some years; align with Python isocalendar."""
    d = date(2020, 12, 31)
    df = DataFrame[_Donly]({"d": [d]})
    out = df.with_columns(w=df.d.dt_week()).to_dict()
    assert out["w"] == [d.isocalendar().week]
    assert out["w"][0] == 53


def test_list_join_sort_unique() -> None:
    df = DataFrame[_ListStr](
        {
            "tok": [
                ["b", "a", "b"],
                ["x", "y"],
            ],
        }
    )
    out = df.with_columns(
        j=df.tok.list_join(","),
        so=df.tok.list_sort(),
        sd=df.tok.list_sort(descending=True),
        u=df.tok.list_unique(),
        us=df.tok.list_unique(stable=True),
    ).collect(as_lists=True)
    assert out["j"] == ["b,a,b", "x,y"]
    assert out["so"] == [["a", "b", "b"], ["x", "y"]]
    assert out["sd"][0] == ["b", "b", "a"]
    assert out["u"][0] in (["a", "b"], ["b", "a"])
    assert out["us"][0] == ["b", "a"]


def test_list_join_rejects_non_str_list() -> None:
    df = DataFrame[_ListInt]({"items": [[1, 2]]})
    with pytest.raises(TypeError, match=r"list_join|list\[str\]"):
        df.with_columns(x=df.items.list_join(","))


def test_list_join_empty_and_unicode_separator() -> None:
    df = DataFrame[_ListStr]({"tok": [[], ["caf", "é"], ["a", "b"]]})
    out = df.with_columns(
        j=df.tok.list_join(","),
        ju=df.tok.list_join(" · "),
    ).collect(as_lists=True)
    assert out["j"] == ["", "caf,é", "a,b"]
    assert out["ju"] == ["", "caf · é", "a · b"]


def test_list_sort_int_list_nulls_last() -> None:
    df = DataFrame[_ListInt]({"items": [[3, 1, 2], [1, 0, 2]]})
    out = df.with_columns(
        a=df.items.list_sort(),
        d=df.items.list_sort(descending=True),
        nl=df.items.list_sort(nulls_last=True),
    ).collect(as_lists=True)
    assert out["a"] == [[1, 2, 3], [0, 1, 2]]
    assert out["d"] == [[3, 2, 1], [2, 1, 0]]
    assert out["nl"] == [[1, 2, 3], [0, 1, 2]]


def test_list_sort_float_list() -> None:
    df = DataFrame[_ListFloat]({"nums": [[2.5, 1.0], [0.0, -1.0]]})
    out = df.with_columns(s=df.nums.list_sort()).collect(as_lists=True)
    assert out["s"] == [[1.0, 2.5], [-1.0, 0.0]]


def test_str_reverse_unicode_and_null_string() -> None:
    # Polars ``str.reverse`` follows its UTF-8 rules (combining marks may move).
    df = DataFrame[_StrOpt]({"s": ["a\u0301b", None, "😀!"]})
    out = df.with_columns(r=df.s.str_reverse()).collect(as_lists=True)
    assert out["r"][0] == "\u0062\u0061\u0301"
    assert out["r"][1] is None
    assert out["r"][2] == "!😀"


def test_str_pad_fill_char_validation() -> None:
    df = DataFrame[_Str]({"s": ["a"]})
    with pytest.raises(ValueError, match="empty"):
        df.with_columns(x=df.s.str_pad_start(3, ""))
    with pytest.raises(ValueError, match="single"):
        df.with_columns(x=df.s.str_pad_end(3, "ab"))


def test_str_extract_regex_oob_group_null() -> None:
    df = DataFrame[_Str]({"s": ["a1"]})
    out = df.with_columns(huge=df.s.str_extract_regex(r"a(\d)", 99)).to_dict()
    assert out["huge"] == [None]


def test_str_json_path_match_string_scalar_encoded() -> None:
    df = DataFrame[_Str]({"s": [r'{"a": "z"}']})
    out = df.with_columns(v=df.s.str_json_path_match("$.a")).collect(as_lists=True)
    assert out["v"] == ["z"]


def test_string_reverse_pad_zfill() -> None:
    df = DataFrame[_Str]({"s": ["ab", "42", "-7", "x"]})
    out = df.with_columns(
        r=df.s.str_reverse(),
        ps=df.s.str_pad_start(4, "0"),
        pe=df.s.str_pad_end(4, "."),
        z=df.s.str_zfill(4),
    ).collect(as_lists=True)
    assert out["r"] == ["ba", "24", "7-", "x"]
    assert out["ps"] == ["00ab", "0042", "00-7", "000x"]
    assert out["pe"] == ["ab..", "42..", "-7..", "x..."]
    assert out["z"] == ["00ab", "0042", "-007", "000x"]


def test_str_extract_regex_groups() -> None:
    df = DataFrame[_Str]({"s": ["a1b2", "nope"]})
    out = df.with_columns(
        g0=df.s.str_extract_regex(r"a(\d)b(\d)", 0),
        g1=df.s.str_extract_regex(r"a(\d)b(\d)", 1),
        g2=df.s.str_extract_regex(r"a(\d)b(\d)", 2),
    ).to_dict()
    assert out["g0"] == ["a1b2", None]
    assert out["g1"] == ["1", None]
    assert out["g2"] == ["2", None]


def test_str_extract_empty_pattern_raises() -> None:
    df = DataFrame[_Str]({"s": ["a"]})
    with pytest.raises(ValueError, match="empty"):
        df.with_columns(x=df.s.str_extract_regex("", 1))


def test_str_json_path_match_basic() -> None:
    df = DataFrame[_StrOpt]({"s": [r'{"a": 1}', r'{"a": "z"}', "not-json", None]})
    out = df.with_columns(v=df.s.str_json_path_match("$.a")).to_dict()
    assert out["v"][0] is not None
    assert out["v"][2] is None
    assert out["v"][3] is None


def test_str_json_path_empty_raises() -> None:
    df = DataFrame[_Str]({"s": ['{"a":1}']})
    with pytest.raises(ValueError, match="empty"):
        df.with_columns(v=df.s.str_json_path_match(""))
