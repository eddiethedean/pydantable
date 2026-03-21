from __future__ import annotations

from time import perf_counter

from pydantable import DataFrameModel


class PerfLeft(DataFrameModel):
    id: int
    ts: int
    v: int | None
    key: str


class PerfRight(DataFrameModel):
    id: int
    ts: int
    tag: str


def test_performance_guardrails_major_transforms() -> None:
    n = 3000
    left = PerfLeft(
        {
            "id": list(range(n)),
            "ts": [i * 60 for i in range(n)],
            "v": [i % 10 for i in range(n)],
            "key": ["A" if i % 2 == 0 else "B" for i in range(n)],
        }
    )
    right = PerfRight(
        {
            "id": list(range(n)),
            "ts": [i * 60 for i in range(n)],
            "tag": ["x" if i % 3 == 0 else "y" for i in range(n)],
        }
    )

    t0 = perf_counter()
    _ = left.join(right, on=["id", "ts"], how="inner").collect(as_lists=True)
    join_s = perf_counter() - t0

    t1 = perf_counter()
    _ = left.group_by("key").agg(
        v_sum=("sum", "v"),
        v_count=("count", "v"),
    ).collect(as_lists=True)
    group_s = perf_counter() - t1

    t2 = perf_counter()
    _ = (
        left.melt(id_vars=["id"], value_vars=["v"])
        .pivot(
            index="id", columns="variable", values="value", aggregate_function="first"
        )
        .collect(as_lists=True)
    )
    reshape_s = perf_counter() - t2

    t3 = perf_counter()
    _ = left.rolling_agg(
        on="ts", column="v", window_size="10m", op="sum", out_name="v_roll", by=["key"]
    ).collect(as_lists=True)
    window_s = perf_counter() - t3

    # Guardrails are intentionally generous to avoid flakiness while still
    # catching major accidental regressions.
    assert join_s < 20
    assert group_s < 20
    assert reshape_s < 20
    assert window_s < 20
