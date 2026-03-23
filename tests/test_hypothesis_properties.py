"""Property-based tests (Hypothesis) for schema validation and DataFrame invariants."""

from __future__ import annotations

import pytest
from conftest import assert_table_eq_sorted
from hypothesis import assume, given, settings
from hypothesis import strategies as st
from hypothesis.strategies import composite
from pydantable import DataFrame, Schema
from pydantable.schema import (
    dtype_descriptor_to_annotation,
    schema_from_descriptors,
    validate_columns_strict,
)


class TwoInt(Schema):
    id: int
    age: int


class IntOpt(Schema):
    id: int
    age: int | None


@composite
def aligned_two_int_columns(draw):
    n = draw(st.integers(min_value=0, max_value=256))
    ids = draw(
        st.lists(
            st.integers(min_value=-10_000, max_value=10_000),
            min_size=n,
            max_size=n,
        )
    )
    ages = draw(
        st.lists(
            st.integers(min_value=-10_000, max_value=10_000),
            min_size=n,
            max_size=n,
        )
    )
    return {"id": ids, "age": ages}


@composite
def misaligned_two_int_columns(draw):
    n1 = draw(st.integers(min_value=0, max_value=64))
    n2 = draw(st.integers(min_value=0, max_value=64))
    assume(n1 != n2)
    ids = draw(
        st.lists(
            st.integers(min_value=-1000, max_value=1000),
            min_size=n1,
            max_size=n1,
        )
    )
    ages = draw(
        st.lists(
            st.integers(min_value=-1000, max_value=1000),
            min_size=n2,
            max_size=n2,
        )
    )
    return {"id": ids, "age": ages}


@composite
def aligned_int_and_optional_int(draw):
    n = draw(st.integers(min_value=0, max_value=128))
    ids = draw(
        st.lists(
            st.integers(min_value=-500, max_value=500),
            min_size=n,
            max_size=n,
        )
    )
    ages = draw(
        st.lists(
            st.one_of(st.none(), st.integers(min_value=-500, max_value=500)),
            min_size=n,
            max_size=n,
        )
    )
    return {"id": ids, "age": ages}


@composite
def valid_dtype_descriptor(draw):
    base = draw(
        st.sampled_from(["int", "float", "bool", "str", "datetime", "date", "duration"])
    )
    nullable = draw(st.booleans())
    return {"base": base, "nullable": nullable}


@composite
def schema_descriptor_map(draw):
    names = draw(
        st.lists(
            st.text(
                alphabet="abcdefghijklmnopqrstuvwxyz",
                min_size=1,
                max_size=8,
            ),
            min_size=1,
            max_size=8,
            unique=True,
        )
    )
    descs: dict[str, dict[str, object]] = {}
    for n in names:
        descs[n] = draw(valid_dtype_descriptor())
    return descs


@given(data=aligned_two_int_columns())
@settings(max_examples=75)
def test_validate_columns_strict_aligned_int_ok(data: dict[str, list]) -> None:
    out = validate_columns_strict(data, TwoInt)
    assert out == data


@given(data=misaligned_two_int_columns())
@settings(max_examples=50)
def test_validate_columns_strict_misaligned_fails(data: dict[str, list]) -> None:
    with pytest.raises(ValueError, match="same length"):
        validate_columns_strict(data, TwoInt)


@given(data=aligned_two_int_columns())
@settings(max_examples=75)
def test_dataframe_collect_preserves_columns_and_length(data: dict[str, list]) -> None:
    df = DataFrame[TwoInt](data)
    got = df.collect(as_lists=True)
    assert set(got.keys()) == {"id", "age"}
    assert len(got["id"]) == len(data["id"])
    assert_table_eq_sorted(got, data, ["id"])


@given(data=aligned_two_int_columns())
@settings(max_examples=75)
def test_with_columns_sum_matches_rowwise_arithmetic(data: dict[str, list]) -> None:
    df = DataFrame[TwoInt](data)
    out = df.with_columns(s=df.id + df.age).collect(as_lists=True)
    expected_s = [a + b for a, b in zip(data["id"], data["age"], strict=True)]
    assert_table_eq_sorted(
        out,
        {**data, "s": expected_s},
        ["id"],
    )


@given(data=aligned_two_int_columns())
@settings(max_examples=50)
def test_select_identity_subset(data: dict[str, list]) -> None:
    df = DataFrame[TwoInt](data)
    out = df.select("id").collect(as_lists=True)
    assert set(out.keys()) == {"id"}
    assert_table_eq_sorted(out, {"id": list(data["id"])}, ["id"])


@given(data=aligned_two_int_columns())
@settings(max_examples=30, deadline=None)
def test_with_columns_identity_age_column(data: dict[str, list]) -> None:
    df = DataFrame[TwoInt](data)
    out = df.with_columns(age=df.age).collect(as_lists=True)
    assert_table_eq_sorted(out, data, ["id"])


@given(data=aligned_two_int_columns())
@settings(max_examples=50)
def test_filter_self_eq_keeps_all_rows(data: dict[str, list]) -> None:
    df = DataFrame[TwoInt](data)
    out = df.filter(df.age == df.age).collect(as_lists=True)
    assert_table_eq_sorted(out, data, ["id"])


@given(data=aligned_two_int_columns())
@settings(max_examples=40)
def test_filter_gt_min_id_select_id_pipeline(data: dict[str, list]) -> None:
    assume(len(data["id"]) > 0)
    df = DataFrame[TwoInt](data)
    min_id = min(data["id"])
    out = df.filter(df.id > min_id).select("id").collect(as_lists=True)
    expected_ids = [i for i in data["id"] if i > min_id]
    assert_table_eq_sorted(out, {"id": expected_ids}, ["id"])


@given(data=aligned_int_and_optional_int())
@settings(max_examples=50)
def test_nullable_int_column_roundtrip(data: dict[str, list]) -> None:
    df = DataFrame[IntOpt](data)
    got = df.collect(as_lists=True)
    assert_table_eq_sorted(got, data, ["id"])


@given(mapping=schema_descriptor_map())
@settings(max_examples=40)
def test_schema_from_descriptors_roundtrip_keys(mapping: dict[str, dict]) -> None:
    fields = schema_from_descriptors(mapping)
    assert set(fields.keys()) == set(mapping.keys())
    for name, desc in mapping.items():
        assert fields[name] == dtype_descriptor_to_annotation(desc)


@given(every=st.sampled_from(["0s", "0m", "0h", "0d"]))
def test_group_by_dynamic_rejects_zero_step(every: str) -> None:
    class TS(Schema):
        id: int
        ts: int
        v: int

    df = DataFrame[TS]({"id": [1], "ts": [0], "v": [1]})
    with pytest.raises(ValueError, match="positive every"):
        df.group_by_dynamic("ts", every=every, by=["id"]).agg(v_sum=("sum", "v"))
