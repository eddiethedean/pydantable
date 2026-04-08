from __future__ import annotations

import os
from time import perf_counter

import pytest
from pydantable import DataFrameModel

# CI runners are slower and noisier than a typical laptop; scale guardrails there.
_PERF_LIMIT_S = 20.0 * (2.5 if os.environ.get("GITHUB_ACTIONS") == "true" else 1.0)


class PerfLeft(DataFrameModel):
    id: int
    ts: int
    v: int | None
    key: str


class PerfRight(DataFrameModel):
    id: int
    ts: int
    tag: str


@pytest.mark.slow
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
    _ = (
        left.group_by("key")
        .agg(
            v_sum=("sum", "v"),
            v_count=("count", "v"),
        )
        .collect(as_lists=True)
    )
    group_s = perf_counter() - t1

    t2 = perf_counter()
    _ = (
        left._df.melt(id_vars=["id"], value_vars=["v"])
        .pivot(
            index="id", columns="variable", values="value", aggregate_function="first"
        )
        .collect(as_lists=True)
    )
    reshape_s = perf_counter() - t2

    t3 = perf_counter()
    _ = left._df.rolling_agg(
        on="ts", column="v", window_size="10m", op="sum", out_name="v_roll", by=["key"]
    ).collect(as_lists=True)
    window_s = perf_counter() - t3

    # Guardrails are intentionally generous to avoid flakiness while still
    # catching major accidental regressions.
    assert join_s < _PERF_LIMIT_S
    assert group_s < _PERF_LIMIT_S
    assert reshape_s < _PERF_LIMIT_S
    assert window_s < _PERF_LIMIT_S
