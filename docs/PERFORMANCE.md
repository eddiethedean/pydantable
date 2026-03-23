# Performance notes

## Benchmarking the native extension

Editable installs often build a **debug** extension. For comparable numbers, use a **release** build:

```bash
.venv/bin/python -m maturin develop --release
```

See also `benchmarks/run_release.sh` and [DEVELOPER.md](DEVELOPER.md).

## Where time goes (typical)

End-to-end work splits roughly into:

1. **Python ingestion validation** — `validate_columns_strict` runs Pydantic `TypeAdapter` per cell when **`trusted_mode="off"`** (default).
2. **Rust ingest** — `root_data_to_polars_df` copies Python column lists into Polars `Series`, or ingests NumPy/PyArrow buffers, or (with **`trusted_mode="shape_only"`** / legacy **`validate_data=False`**) a Polars `DataFrame` via Arrow IPC (see `execute_polars.rs`).
3. **Polars execution** — lazy plan `collect()` inside Rust (synchronous; see {doc}`EXECUTION` and {doc}`ROADMAP` for async plans).
4. **Rust → Python** — results are materialized as Python column lists (`dict[str, list]`) for the default Python API; `collect()` wraps rows as Pydantic models, and `to_dict()` exposes the columnar dict directly. Optional `to_polars()` builds a Polars `DataFrame` when the `polars` extra is installed.

Ratios vs raw Polars/pandas in `benchmarks/pydantable_vs_*.py` reflect this stack, not only step (3).

## Profiling scripts

| Script | Purpose |
|--------|---------|
| [`benchmarks/profile_breakdown.py`](../benchmarks/profile_breakdown.py) | Wall-time split: validation vs `DataFrame` construction vs transform+`collect()` |
| [`benchmarks/micro_collect_only.py`](../benchmarks/micro_collect_only.py) | Mean time for `collect()` only on a pre-built `DataFrame` (execution + egress) |
| `python -m cProfile` / `py-spy` | Deeper Python stacks; Rust: `perf` / Instruments on the `_core` shared library |

Run `profile_breakdown.py --cprofile` for a cumulative profile of one pipeline.

## Tuning knobs (see code)

- **`trusted_mode="shape_only"`** (or legacy **`validate_data=False`**) on `DataFrame` / `DataFrameModel` — skips per-cell Pydantic validation when you trust inputs; keys and column lengths are still checked. Use **`trusted_mode="strict"`** when you want additional Polars dtype / nested-shape checks. NumPy and PyArrow column buffers can be preserved for a lower-copy Rust ingest path (numeric/bool dtypes that match the schema). A Polars `DataFrame` can be passed as the root table on trusted paths. See {doc}`DATAFRAMEMODEL` and {doc}`SUPPORTED_TYPES`.
- **`collect()`** (default) — returns a `list` of Pydantic row models (validated against the current schema).
- **`to_dict()`** / **`collect(as_lists=True)`** — columnar `dict[str, list]` (common for tests and column-shaped responses).
- **`to_polars()`** — optional; requires `pip install 'pydantable[polars]'`.
- **`collect(as_numpy=True)`** — returns `dict[str, numpy.ndarray]` from the columnar lists.
- **NumPy / PyArrow columns** — with **`trusted_mode="shape_only"`** (or **`validate_data=False`**), compatible `numpy.ndarray` and `pyarrow.Array` / `ChunkedArray` columns are converted in Rust without a Python per-element loop where dtypes match.

## Release profile

Wheels and `maturin develop --release` use Cargo’s `release` profile. Optional **thin LTO** is enabled in `pydantable-core/Cargo.toml` to trade longer compile time for slightly faster native code.
