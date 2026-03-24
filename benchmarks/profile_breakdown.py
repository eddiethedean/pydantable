"""
Split timing: validation, `DataFrame` build, transform, then columnar ``to_dict()``.

Run from repo root with release extension:

    .venv/bin/python -m maturin develop --release
    .venv/bin/python benchmarks/profile_breakdown.py

Optional:

    .venv/bin/python benchmarks/profile_breakdown.py --rows 50000
    .venv/bin/python benchmarks/profile_breakdown.py --cprofile

See docs/PERFORMANCE.md for interpreting results.
"""

from __future__ import annotations

import argparse
import cProfile
import io
import pstats
from time import perf_counter

from pydantable import DataFrame, Schema
from pydantable.schema import validate_columns_strict


class T(Schema):
    user_id: int
    spend: float


def _payload(n: int) -> dict[str, list]:
    return {
        "user_id": list(range(n)),
        "spend": [float(i % 250) for i in range(n)],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument(
        "--cprofile",
        action="store_true",
        help="Also print cProfile top functions (stdout can be large)",
    )
    args = parser.parse_args()
    n = args.rows
    payload = _payload(n)

    t0 = perf_counter()
    validate_columns_strict(payload, T)
    t_validate = perf_counter() - t0

    t1 = perf_counter()
    df = DataFrame[T](payload)
    t_construct = perf_counter() - t1

    t2 = perf_counter()
    out = (
        df.with_columns(spend2=df.spend * 1.1)
        .filter(df.spend > 50.0)
        .select("user_id", "spend2")
        .to_dict()
    )
    t_pipeline = perf_counter() - t2

    total = t_validate + t_construct + t_pipeline
    print(f"rows={n}")
    v_pct = 100 * t_validate / total
    c_pct = 100 * t_construct / total
    p_pct = 100 * t_pipeline / total
    print(f"  validate_columns_strict: {t_validate:.4f}s ({v_pct:.1f}%)")
    print(f"  DataFrame construct+plan: {t_construct:.4f}s ({c_pct:.1f}%)")
    print(f"  transform+to_dict:       {t_pipeline:.4f}s ({p_pct:.1f}%)")
    print(f"  total:                   {total:.4f}s")
    print(f"  output rows (sample):    {len(out['user_id'])}")

    if args.cprofile:

        def _run() -> None:
            p = _payload(n)
            validate_columns_strict(p, T)
            d = DataFrame[T](p)
            (
                d.with_columns(spend2=d.spend * 1.1)
                .filter(d.spend > 50.0)
                .select("user_id", "spend2")
                .to_dict()
            )

        pr = cProfile.Profile()
        pr.enable()
        _run()
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats(pstats.SortKey.CUMULATIVE)
        ps.print_stats(40)
        print()
        print(s.getvalue())


if __name__ == "__main__":
    main()
