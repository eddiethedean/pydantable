from __future__ import annotations

from datetime import date, datetime, timedelta

from pydantable.pyspark import DataFrameModel

from tests._support.tables import assert_table_eq_sorted


class Left(DataFrameModel):
    id: int
    bucket: str
    ts: int
    amount: int | None


class Right(DataFrameModel):
    id: int
    label: str


class TemporalRow(DataFrameModel):
    id: int
    ts: datetime | None
    d: date | None
    delta: timedelta | None


def test_pyspark_interface_is_spark_flavored_dataframe() -> None:
    df = Left({"id": [1], "bucket": ["A"], "ts": [0], "amount": [10]})
    assert hasattr(df, "withColumn")
    assert df.collect(as_lists=True) == {
        "id": [1],
        "bucket": ["A"],
        "ts": [0],
        "amount": [10],
    }


def test_pyspark_interface_consolidated_pipeline() -> None:
    left = Left(
        {
            "id": [1, 2, 3, 4],
            "bucket": ["A", "A", "B", "B"],
            "ts": [0, 3600, 7200, 10800],
            "amount": [10, None, 30, 40],
        }
    )
    right = Right({"id": [1, 2, 3], "label": ["x", "x", "y"]})

    base = (
        left.fill_null(0, subset=["amount"])
        .with_columns(amount_f=left.amount.cast(float))
        .filter(left.id > 0)
        .sort("id")
        .select("id", "bucket", "ts", "amount", "amount_f")
    )
    joined = base.join(right, on="id", how="left")
    grouped = joined.groupBy("bucket").agg(
        amount_sum=("sum", "amount"),
        id_count=("count", "id"),
    )
    melted = grouped.melt(id_vars=["bucket"], value_vars=["amount_sum", "id_count"])
    pivoted = melted.pivot(
        index="bucket",
        columns="variable",
        values="value",
        aggregate_function="first",
    )
    rolled = base.rolling_agg(
        on="ts",
        column="amount",
        window_size="2h",
        op="sum",
        out_name="amount_roll",
        by=["bucket"],
    )
    dynamic = base.group_by_dynamic("ts", every="1h", by=["bucket"]).agg(
        amount_sum=("sum", "amount"),
        amount_count=("count", "amount"),
    )

    assert "amount_sum_first" in pivoted.collect(as_lists=True)
    assert "amount_roll" in rolled.collect(as_lists=True)
    assert "amount_sum" in dynamic.collect(as_lists=True)
    assert_table_eq_sorted(
        grouped.collect(as_lists=True),
        {"bucket": ["A", "B"], "amount_sum": [10, 70], "id_count": [2, 2]},
        keys=["bucket"],
    )


def test_pyspark_interface_temporal_columns_and_literals() -> None:
    t0 = datetime(2024, 1, 1, 0, 0, 0)
    t1 = datetime(2024, 1, 1, 1, 0, 0)
    rows = TemporalRow(
        {
            "id": [1, 2, 3],
            "ts": [t0, t1, None],
            "d": [date(2024, 1, 1), date(2024, 1, 2), None],
            "delta": [timedelta(minutes=5), timedelta(minutes=10), None],
        }
    )
    out = (
        rows.filter(rows.ts >= t0)
        .filter(rows.d >= date(2024, 1, 1))
        .collect(as_lists=True)
    )
    assert out["id"] == [1, 2]
