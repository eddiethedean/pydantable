# Developer Guide

This guide is for contributors working on `pydantable` internals.

## Prerequisites

- Python `3.10+` (project metadata targets `>=3.10`; current local dev flow uses `3.10`)
- Rust toolchain (`rustup`, `cargo`)
- `maturin` for building the PyO3 extension

## Local Environment Setup

From repo root:

```bash
PYENV_VERSION=3.10.18 python -m venv .venv
.venv/bin/python -m pip install --upgrade pip setuptools wheel
.venv/bin/python -m pip install maturin pytest pytest-asyncio ruff basedpyright
.venv/bin/python -m pip install -e .
```

Activate when working interactively:

```bash
source .venv/bin/activate
```

## Repository Layout

- `python/pydantable/`: thin Python API layer + Pydantic integration
- `python/pydantable/rust_engine.py`: calls into `pydantable._core` for execution (single engine)
- `pydantable-core/src/`: Rust core (`dtype`, `expr`, `plan`, PyO3 exports)
- `tests/`: Python integration/unit tests for behavior contracts
- `docs/`: product docs + roadmap/spec docs

## Architecture (Rust-first)

Current contract direction:

- Python is the ergonomic API boundary (`DataFrameModel`, stubs, docs, Pydantic)
- Rust is the source of truth for expression typing, logical-plan validation, and execution for supported operations
- Python wrappers should avoid duplicating validation that Rust already enforces

Phase 4 boundary contract:

- Rust plan schema metadata is exported as descriptors:
  - `{"base": "int|float|bool|str", "nullable": bool}`
- Python reconstructs type annotations for derived schemas from those descriptors

## Build and Test Commands

### Run Python tests

```bash
.venv/bin/python -m pytest -q
```

### Lint/format/type-check

```bash
.venv/bin/ruff format .
.venv/bin/ruff check .
.venv/bin/basedpyright
```

### Build docs (Sphinx)

```bash
.venv/bin/python -m pip install -e ".[docs]"
.venv/bin/python -m sphinx -b html docs docs/_build
```

### Verify runnable doc snippets (README + `docs/`)

After a normal editable install (so `pydantable._core` is built), run from repo root:

```bash
PYTHONPATH=python .venv/bin/python scripts/verify_doc_examples.py
```

This executes the same patterns as the Quick Start and several doc examples (no network). Fix failures before merging user-facing doc changes.

Optional Python **`polars`** for local `to_polars()` / benchmark comparisons: `pip install -e ".[polars]"` or `".[benchmark]"` (see benchmark sections below).

### Build package wheel (mixed Python/Rust)

```bash
.venv/bin/maturin build --release
```

### Benchmarks (use a **release** Rust build)

Editable installs (`pip install -e .`) often compile `pydantable._core` in **debug** mode. That is fine for development but **not** comparable to production performance. Before running any benchmark that exercises the extension, build the optimized library:

```bash
.venv/bin/python -m maturin develop --release
```

Then run the benchmark scripts (after `pip install -e ".[benchmark]"` for Polars/pandas).

**One-shot (release build + vs Polars + vs pandas):**

```bash
chmod +x benchmarks/run_release.sh   # once
./benchmarks/run_release.sh
./benchmarks/run_release.sh --rows 10000 50000 --rounds 7
```

### Run the Phase 5 execution baseline benchmark

Use a release extension (see above), then:

```bash
.venv/bin/python benchmarks/phase5_collect_baseline.py
```

### Compare PydanTable vs native Polars (Python)

Install the optional benchmark extra (pulls in Polars for the comparison side):

```bash
.venv/bin/python -m pip install -e ".[benchmark]"
.venv/bin/python -m maturin develop --release
.venv/bin/python benchmarks/pydantable_vs_polars.py
```

The script reports mean wall time per scenario (filter/project, join, group-by) and the pydantable/polars ratio. Higher ratios mean more overhead from pydantable’s typed API and wrappers; both sides exercise similar Polars-backed work.

Default `--rows` includes **1,000,000** (along with 1k / 10k / 50k); a full default run can take **minutes**. For a quick check, pass smaller sets, e.g. `--rows 10000 50000`.

### Compare pydantable vs pandas

Uses the same optional `benchmark` extra (installs pandas alongside Polars):

```bash
.venv/bin/python -m pip install -e ".[benchmark]"
.venv/bin/python -m maturin develop --release
.venv/bin/python benchmarks/pydantable_vs_pandas.py
```

Reports pydantable vs pandas wall time and ratio for the same three scenarios (eager pandas `DataFrame` / `merge` / `groupby`). Default `--rows` matches the Polars script (including **1,000,000**); use `--rows` to limit sizes for faster runs.

### Build/install extension in editable flow

Usually handled by `pip install -e .`. If you need a fresh wheel install:

```bash
.venv/bin/maturin build --release
.venv/bin/python -m pip install --force-reinstall pydantable-core/target/wheels/*.whl
```

## Contribution Guidelines

- Keep Python wrappers thin; prefer Rust ownership for planner/typing logic.
- Add tests with behavior changes:
  - Rust-side tests for internal plan contracts
  - Python tests for user-visible behavior and API compatibility
- Preserve existing error contracts unless intentionally changing behavior.
- Update docs (`README.md` + relevant docs in `docs/`) for any user-facing changes.

## Common Pitfalls

- Running tests outside `.venv` can pick up incompatible site-packages.
- PyO3/Rust test linking can vary by environment; Python integration tests are the authoritative CI-facing check for behavior parity.
- If local imports seem stale, rebuild/reinstall from `.venv` and rerun tests.

## Release Notes Checklist (for contributors)

- [ ] Python tests pass in `.venv`
- [ ] Rust changes compile in package build path (`maturin build`)
- [ ] Docs updated for behavior/contract changes
- [ ] Roadmap status updated when a phase milestone changes
