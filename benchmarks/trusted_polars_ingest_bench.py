"""
Ingest + ``collect()`` for a Polars ``DataFrame`` with ``trusted_mode=\"strict\"``.

Requires ``pip install 'pydantable[polars]'`` and a **release** native build::

    .venv/bin/python -m maturin develop --release
    .venv/bin/python benchmarks/trusted_polars_ingest_bench.py --rows 500000
"""

from __future__ import annotations

import argparse
from statistics import mean
from time import perf_counter

import polars as pl
from pydantable import DataFrameModel


class RowDF(DataFrameModel):
    a: int
    b: float


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rows", type=int, default=200_000)
    p.add_argument("--rounds", type=int, default=3)
    args = p.parse_args()
    n = args.rows
    pdf = pl.DataFrame(
        {
            "a": pl.Series(range(n), dtype=pl.Int64),
            "b": pl.Series([float(i % 100) for i in range(n)], dtype=pl.Float64),
        }
    )

    samples: list[float] = []
    for _ in range(args.rounds):
        t0 = perf_counter()
        df = RowDF(pdf, trusted_mode="strict")
        _ = df.select("a", "b").collect()
        samples.append(perf_counter() - t0)
    print(
        f"rows={n} rounds={args.rounds} "
        f"trusted_strict_polars_ingest_collect_mean_s={mean(samples):.6f}"
    )


if __name__ == "__main__":
    main()
