"""
Compare pydantable (Rust core + Python API) against pandas on similar workloads.

**Use a release build of the extension** before benchmarking:

    .venv/bin/python -m maturin develop --release

Or ``benchmarks/run_release.sh`` (release build + vs Polars + vs pandas).

Install pandas (included in the benchmark extra with Polars):

    pip install -e ".[benchmark]"

Run:

    .venv/bin/python benchmarks/pydantable_vs_pandas.py
    .venv/bin/python benchmarks/pydantable_vs_pandas.py --rows 5000 20000 --rounds 7

By default the **timed region excludes ingest**: building payloads and constructing
``DataFrame`` / ``DataFrameModel`` happens once per row count; only transformations
and ``collect()`` are measured. Use ``--with-ingest`` to include construction in
the timer.

Ratios > 1 mean pydantable took longer than pandas for that scenario. Pandas uses
NumPy/C for many columnar ops; pydantable adds Pydantic typing and a Rust plan
layer, so relative speed depends on row count and how much time is Python vs
Rust execution.
"""

from __future__ import annotations

import argparse
import sys
from statistics import mean
from time import perf_counter
from typing import Any

from pydantable import DataFrameModel


class EventDF(DataFrameModel):
    user_id: int
    spend: float


class BenchLeft(DataFrameModel):
    id: int
    ts: int
    v: int | None
    key: str


class BenchRight(DataFrameModel):
    id: int
    ts: int
    tag: str


def _mean_time(fn: Any, rounds: int) -> float:
    samples: list[float] = []
    for _ in range(rounds):
        t0 = perf_counter()
        fn()
        samples.append(perf_counter() - t0)
    return mean(samples)


def _build_event_payload(n: int) -> dict[str, list]:
    return {
        "user_id": list(range(n)),
        "spend": [float(i % 250) for i in range(n)],
    }


def _bench_pipeline_pydantable(n: int) -> None:
    payload = _build_event_payload(n)
    df = EventDF(payload)
    df.with_columns(spend2=df.spend * 1.1).filter(df.spend > 50.0).select(
        "user_id", "spend2"
    ).collect()


def _bench_pipeline_pandas(n: int, pd: Any) -> None:
    payload = _build_event_payload(n)
    df = pd.DataFrame(payload)
    df = df.assign(spend2=df["spend"] * 1.1)
    df.loc[df["spend"] > 50.0, ["user_id", "spend2"]]


def _build_join_payloads(n: int) -> tuple[dict[str, list], dict[str, list]]:
    left = {
        "id": list(range(n)),
        "ts": [i * 60 for i in range(n)],
        "v": [i % 10 for i in range(n)],
        "key": ["A" if i % 2 == 0 else "B" for i in range(n)],
    }
    right = {
        "id": list(range(n)),
        "ts": [i * 60 for i in range(n)],
        "tag": ["x" if i % 3 == 0 else "y" for i in range(n)],
    }
    return left, right


def _bench_join_pydantable(n: int) -> None:
    left_d, right_d = _build_join_payloads(n)
    left = BenchLeft(left_d)
    right = BenchRight(right_d)
    left.join(right, on=["id", "ts"], how="inner").collect()


def _bench_join_pandas(n: int, pd: Any) -> None:
    left_d, right_d = _build_join_payloads(n)
    pd.merge(
        pd.DataFrame(left_d),
        pd.DataFrame(right_d),
        on=["id", "ts"],
        how="inner",
    )


def _build_group_payload(n: int) -> dict[str, list]:
    return {
        "id": list(range(n)),
        "ts": [i * 60 for i in range(n)],
        "v": [i % 10 for i in range(n)],
        "key": ["A" if i % 2 == 0 else "B" for i in range(n)],
    }


def _bench_group_pydantable(n: int) -> None:
    payload = _build_group_payload(n)
    df = BenchLeft(payload)
    df.group_by("key").agg(v_sum=("sum", "v"), v_count=("count", "v")).collect()


def _bench_group_pandas(n: int, pd: Any) -> None:
    payload = _build_group_payload(n)
    df = pd.DataFrame(payload)
    df.groupby("key", sort=False).agg(
        v_sum=("v", "sum"),
        v_count=("v", "count"),
    )


def _run_pipeline_pandas_ops(df: Any) -> None:
    out = df.assign(spend2=df["spend"] * 1.1)
    out.loc[out["spend"] > 50.0, ["user_id", "spend2"]]


def _run_pipeline_pydantable_ops(df: Any) -> None:
    df.with_columns(spend2=df.spend * 1.1).filter(df.spend > 50.0).select(
        "user_id", "spend2"
    ).collect()


def _run_group_pydantable_ops(df: Any) -> None:
    df.group_by("key").agg(v_sum=("sum", "v"), v_count=("count", "v")).collect()


def _run_join_pydantable_ops(left: Any, right: Any) -> None:
    left.join(right, on=["id", "ts"], how="inner").collect()


def _run_join_pandas_ops(left: Any, right: Any, pd: Any) -> None:
    pd.merge(left, right, on=["id", "ts"], how="inner")


def _run_group_pandas_ops(df: Any) -> None:
    df.groupby("key", sort=False).agg(
        v_sum=("v", "sum"),
        v_count=("v", "count"),
    )


def _verify_pipeline_rowcount(n: int, pd: Any) -> None:
    payload = _build_event_payload(n)
    df = EventDF(payload)
    p_rows = len(
        df.with_columns(spend2=df.spend * 1.1)
        .filter(df.spend > 50.0)
        .select("user_id", "spend2")
        .to_dict()["user_id"]
    )
    pdf = pd.DataFrame(payload)
    pdf = pdf.assign(spend2=pdf["spend"] * 1.1)
    pd_rows = len(pdf.loc[pdf["spend"] > 50.0, "user_id"])
    if p_rows != pd_rows:
        raise RuntimeError(
            f"pipeline row count mismatch: pydantable={p_rows} pandas={pd_rows}"
        )


def main() -> None:
    try:
        import pandas as pd
    except ImportError:
        print(
            "pandas is not installed. Install with: pip install pandas  "
            "or pip install -e '.[benchmark]'",
            file=sys.stderr,
        )
        sys.exit(1)

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rows",
        type=int,
        nargs="+",
        default=[1_000, 10_000, 50_000, 1_000_000],
        help="Row counts to benchmark",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=5,
        help="Timed iterations per scenario (mean reported)",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Check pipeline row counts match pandas for the first row count",
    )
    parser.add_argument(
        "--with-ingest",
        action="store_true",
        help=(
            "Include payload construction and DataFrame / DataFrameModel init in "
            "the timed region (default: time only transforms + collect)."
        ),
    )
    args = parser.parse_args()

    if args.verify and args.rows:
        _verify_pipeline_rowcount(args.rows[0], pd)

    if not args.with_ingest:
        print(
            "Timed: transforms + collect only (ingest excluded). "
            "Use --with-ingest for end-to-end timing.\n"
        )

    hdr = (
        f"{'scenario':<22} {'rows':>8} {'pydantable_s':>14} "
        f"{'pandas_s':>12} {'ratio':>8}"
    )
    print(hdr)
    print("-" * 70)

    for n in args.rows:
        if args.with_ingest:
            t_dt = _mean_time(lambda n=n: _bench_pipeline_pydantable(n), args.rounds)
            t_pd = _mean_time(lambda n=n: _bench_pipeline_pandas(n, pd), args.rounds)
        else:
            payload = _build_event_payload(n)
            df_dt = EventDF(payload)
            pdf = pd.DataFrame(payload)
            t_dt = _mean_time(
                lambda df_dt=df_dt: _run_pipeline_pydantable_ops(df_dt), args.rounds
            )
            t_pd = _mean_time(
                lambda pdf=pdf: _run_pipeline_pandas_ops(pdf), args.rounds
            )
        ratio = t_dt / t_pd if t_pd > 0 else float("inf")
        line = (
            f"{'pipeline_filter_select':<22} {n:>8} {t_dt:>14.6f} "
            f"{t_pd:>12.6f} {ratio:>8.2f}x"
        )
        print(line)

    for n in args.rows:
        if args.with_ingest:
            t_dt = _mean_time(lambda n=n: _bench_join_pydantable(n), args.rounds)
            t_pd = _mean_time(lambda n=n: _bench_join_pandas(n, pd), args.rounds)
        else:
            left_d, right_d = _build_join_payloads(n)
            left = BenchLeft(left_d)
            right = BenchRight(right_d)
            pd_left = pd.DataFrame(left_d)
            pd_right = pd.DataFrame(right_d)
            t_dt = _mean_time(
                lambda ldt=left, rdt=right: _run_join_pydantable_ops(ldt, rdt),
                args.rounds,
            )
            t_pd = _mean_time(
                lambda a=pd_left, b=pd_right, p=pd: _run_join_pandas_ops(a, b, p),
                args.rounds,
            )
        ratio = t_dt / t_pd if t_pd > 0 else float("inf")
        line = (
            f"{'join_inner_on_2':<22} {n:>8} {t_dt:>14.6f} "
            f"{t_pd:>12.6f} {ratio:>8.2f}x"
        )
        print(line)

    for n in args.rows:
        if args.with_ingest:
            t_dt = _mean_time(lambda n=n: _bench_group_pydantable(n), args.rounds)
            t_pd = _mean_time(lambda n=n: _bench_group_pandas(n, pd), args.rounds)
        else:
            payload = _build_group_payload(n)
            df_dt = BenchLeft(payload)
            pdf = pd.DataFrame(payload)
            t_dt = _mean_time(
                lambda df_dt=df_dt: _run_group_pydantable_ops(df_dt), args.rounds
            )
            t_pd = _mean_time(
                lambda pdf=pdf: _run_group_pandas_ops(pdf), args.rounds
            )
        ratio = t_dt / t_pd if t_pd > 0 else float("inf")
        line = (
            f"{'groupby_agg_sum_count':<22} {n:>8} {t_dt:>14.6f} "
            f"{t_pd:>12.6f} {ratio:>8.2f}x"
        )
        print(line)


if __name__ == "__main__":
    main()
