"""
Baseline Phase 5 execution benchmark.

Run:
    .venv/bin/python benchmarks/phase5_collect_baseline.py
"""

from __future__ import annotations

from statistics import mean
from time import perf_counter

from pydantable import DataFrameModel


class EventDF(DataFrameModel):
    user_id: int
    spend: float


def build_input(n: int) -> dict[str, list[float | int]]:
    return {
        "user_id": list(range(n)),
        "spend": [float(i % 250) for i in range(n)],
    }


def benchmark(n: int, rounds: int) -> float:
    payload = build_input(n)
    samples: list[float] = []
    for _ in range(rounds):
        start = perf_counter()
        df = EventDF(payload)
        (
            df.with_columns(spend2=df.spend * 1.1)
            .filter(df.spend > 50.0)
            .select("user_id", "spend2")
            .collect()
        )
        samples.append(perf_counter() - start)
    return mean(samples)


if __name__ == "__main__":
    for size in (1_000, 10_000, 50_000):
        avg = benchmark(size, rounds=5)
        print(f"rows={size:>6} avg_seconds={avg:.6f}")
