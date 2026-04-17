"""pyperf: end-to-end native materialization costs.

Run under a release-built native extension (see `benchmarks/run_release.sh`).

Example:
    python -m pip install pyperf
    python benchmarks/pyperf_native_materialize.py --fast
"""

from __future__ import annotations

from dataclasses import dataclass


def _require_pyperf():
    try:
        import pyperf  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(
            "pyperf is not installed. Install with: python -m pip install pyperf"
        ) from exc
    return pyperf


def _bench_to_dict(rows: int) -> None:
    from pydantable import DataFrameModel

    class Row(DataFrameModel):
        x: int
        y: int

    df = Row({"x": list(range(rows)), "y": list(range(rows))})
    df.to_dict()


def _bench_collect_rows(rows: int) -> None:
    from pydantable import DataFrameModel

    class Row(DataFrameModel):
        x: int
        y: int

    df = Row({"x": list(range(rows)), "y": list(range(rows))})
    df.collect()


@dataclass(frozen=True)
class Case:
    name: str
    fn: callable
    rows: int


def main() -> None:
    pyperf = _require_pyperf()

    runner = pyperf.Runner()
    rows = 200_000

    cases = [
        Case("to_dict_200k", _bench_to_dict, rows),
        Case("collect_rows_200k", _bench_collect_rows, rows),
    ]

    for c in cases:
        runner.bench_func(c.name, lambda c=c: c.fn(c.rows))


if __name__ == "__main__":
    main()

