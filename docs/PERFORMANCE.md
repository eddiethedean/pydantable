# Performance notes

## Benchmarking the native extension

Editable installs often build a **debug** extension. For comparable numbers, use a **release** build:

```bash
.venv/bin/python -m maturin develop --release
```

See also `benchmarks/run_release.sh` and [DEVELOPER.md](DEVELOPER.md).

## Where time goes (typical)

End-to-end work splits roughly into:

1. **Python ingestion validation** — `validate_columns_strict` runs Pydantic `TypeAdapter` per cell (strict default).
2. **Rust ingest** — `root_data_to_polars_df` copies Python column lists into Polars `Series`, or ingests NumPy/PyArrow buffers, or (with `validate_data=False`) a Polars `DataFrame` via Arrow IPC (see `execute_polars.rs`).
3. **Polars execution** — lazy plan `collect()` inside Rust.
4. **Rust → Python** — by default, results are handed off as a Polars `DataFrame` using Arrow IPC (no per-cell `list` materialization on the hot path). Use `collect(as_lists=True)` for the legacy `dict[str, list]` representation.

Ratios vs raw Polars/pandas in `benchmarks/pydantable_vs_*.py` reflect this stack, not only step (3).

## Profiling scripts

| Script | Purpose |
|--------|---------|
| [`benchmarks/profile_breakdown.py`](../benchmarks/profile_breakdown.py) | Wall-time split: validation vs `DataFrame` construction vs transform+`collect()` |
| [`benchmarks/micro_collect_only.py`](../benchmarks/micro_collect_only.py) | Mean time for `collect()` only on a pre-built `DataFrame` (execution + egress) |
| `python -m cProfile` / `py-spy` | Deeper Python stacks; Rust: `perf` / Instruments on the `_core` shared library |

Run `profile_breakdown.py --cprofile` for a cumulative profile of one pipeline.

## Tuning knobs (see code)

- **`validate_data=False`** on `DataFrame` / `DataFrameModel` — skips per-cell Pydantic validation when you trust inputs; keys and column lengths are still checked. NumPy and PyArrow column buffers can be preserved for a lower-copy Rust ingest path (numeric/bool dtypes that match the schema). A Polars `DataFrame` can be passed as the root table when `validate_data=False`.
- **`collect()`** (default) — returns a native Polars `DataFrame` from Rust via Arrow IPC (avoids building Python `list` scalars per cell on the hot path).
- **`collect(as_lists=True)`** — legacy path: `dict[str, list]` column materialization (useful for tests or strict Python-list consumers).
- **`collect(as_numpy=True)`** — returns `dict[str, numpy.ndarray]` (from the Polars result when `as_lists=False`).
- **NumPy / PyArrow columns** — with `validate_data=False`, compatible `numpy.ndarray` and `pyarrow.Array` / `ChunkedArray` columns are converted in Rust without a Python per-element loop where dtypes match.

## Release profile

Wheels and `maturin develop --release` use Cargo’s `release` profile. Optional **thin LTO** is enabled in `pydantable-core/Cargo.toml` to trade longer compile time for slightly faster native code.
