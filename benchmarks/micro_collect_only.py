"""
Time only collect() on a pre-built logical DataFrame (isolates execution + egress).

    .venv/bin/python -m maturin develop --release
    .venv/bin/python benchmarks/micro_collect_only.py --rows 200000
"""

from __future__ import annotations

import argparse
from statistics import mean
from time import perf_counter

from pydantable import DataFrame, Schema


class T(Schema):
    a: int
    b: float


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rows", type=int, default=100_000)
    p.add_argument("--rounds", type=int, default=5)
    args = p.parse_args()
    n = args.rows
    df = DataFrame[T]({"a": list(range(n)), "b": [float(i % 100) for i in range(n)]})
    df2 = df.select("a", "b")

    samples: list[float] = []
    for _ in range(args.rounds):
        t0 = perf_counter()
        _ = df2.collect()
        samples.append(perf_counter() - t0)
    print(f"rows={n} rounds={args.rounds} collect_only_mean_s={mean(samples):.6f}")


if __name__ == "__main__":
    main()
