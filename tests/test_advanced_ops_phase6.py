import pytest
from conftest import assert_table_eq_sorted
from pydantable import DataFrame, DataFrameModel
from pydantable.schema import Schema


class UserSchema(Schema):
    id: int
    age: int | None


class CountrySchema(Schema):
    id: int
    country: str


class UserDF(DataFrameModel):
    id: int
    age: int | None


class CountryDF(DataFrameModel):
    id: int
    country: str


def test_phase6_join_inner_happy_path_and_schema():
    left = DataFrame[UserSchema]({"id": [1, 2, 3], "age": [20, None, 30]})
    right = DataFrame[CountrySchema]({"id": [1, 3], "country": ["US", "CA"]})
    joined = left.join(right, on="id", how="inner")
    out = joined.collect(as_lists=True)
    assert_table_eq_sorted(
        out,
        {"id": [1, 3], "age": [20, 30], "country": ["US", "CA"]},
        keys=["id"],
    )

    schema = joined.schema_fields()
    assert schema["id"] is int
    # Keep nullable semantics from the left input schema, even if the
    # selected inner-join rows happen to contain no nulls.
    assert schema["age"] == int | None
    assert schema["country"] is str


def test_phase6_join_collision_uses_suffix():
    class Left(Schema):
        id: int
        score: int

    class Right(Schema):
        id: int
        score: int

    left = DataFrame[Left]({"id": [1, 2], "score": [10, 20]})
    right = DataFrame[Right]({"id": [1, 2], "score": [100, 200]})
    out = left.join(right, on="id", suffix="_r").collect(as_lists=True)
    assert out == {"id": [1, 2], "score": [10, 20], "score_r": [100, 200]}


def test_phase6_groupby_agg_count_sum_mean():
    df = DataFrame[UserSchema]({"id": [1, 1, 2, 2], "age": [10, None, 20, 30]})
    out = (
        df.group_by("id")
        .agg(
            age_count=("count", "age"), age_sum=("sum", "age"), age_mean=("mean", "age")
        )
        .collect(as_lists=True)
    )
    got = sorted(
        zip(
            out["id"],
            out["age_count"],
            out["age_sum"],
            out["age_mean"],
            strict=True,
        )
    )
    assert got == [(1, 1, 10, 10.0), (2, 2, 50, 25.0)]


def test_phase6_groupby_rejects_non_numeric_sum_mean():
    df = CountryDF({"id": [1, 1], "country": ["US", "CA"]})._df
    with pytest.raises(TypeError, match="sum\\(\\) requires int or float"):
        df.group_by("id").agg(country_sum=("sum", "country"))


def test_phase6_dataframe_model_join_and_groupby_parity():
    users = UserDF({"id": [1, 2, 3], "age": [20, None, 30]})
    countries = CountryDF({"id": [1, 3], "country": ["US", "CA"]})
    joined = users.join(countries, on="id")
    out = joined.collect(as_lists=True)
    assert out == {"id": [1, 3], "age": [20, 30], "country": ["US", "CA"]}

    grouped = users.group_by("id").agg(age_count=("count", "age"))
    g = grouped.collect(as_lists=True)
    gout = sorted(zip(g["id"], g["age_count"], strict=True))
    assert gout == [(1, 1), (2, 0), (3, 1)]
