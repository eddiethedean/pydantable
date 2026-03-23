"""
Framed window aggregate wall time (``rowsBetween`` + ``window_sum``).

Use a **release** extension for meaningful numbers::

    .venv/bin/python -m maturin develop --release
    .venv/bin/python benchmarks/framed_window_bench.py --rows 200000
"""

from __future__ import annotations

import argparse
from statistics import mean
from time import perf_counter

from pydantable import DataFrame, Schema
from pydantable.expressions import window_sum
from pydantable.window_spec import Window


class W(Schema):
    g: int
    v: float


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rows", type=int, default=100_000)
    p.add_argument("--rounds", type=int, default=3)
    args = p.parse_args()
    n = args.rows
    # ~10 partitions
    g = [i % 10 for i in range(n)]
    v = [float(i % 97) for i in range(n)]
    df = DataFrame[W]({"g": g, "v": v})
    w = Window.partitionBy("g").orderBy("v").rowsBetween(-2, 0)
    df2 = df.with_columns(s=window_sum(df.v).over(w))

    samples: list[float] = []
    for _ in range(args.rounds):
        t0 = perf_counter()
        _ = df2.collect()
        samples.append(perf_counter() - t0)
    print(f"rows={n} rounds={args.rounds} framed_window_sum_mean_s={mean(samples):.6f}")


if __name__ == "__main__":
    main()
