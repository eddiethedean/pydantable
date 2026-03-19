# Developer Guide

This guide is for contributors working on `pydantable` internals.

## Prerequisites

- Python `3.10+` (project metadata targets `>=3.9`; current local dev flow uses `3.10`)
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

### Lint/type-check

```bash
.venv/bin/ruff check .
.venv/bin/basedpyright
```

### Build package wheel (mixed Python/Rust)

```bash
.venv/bin/maturin build --release
```

### Run the Phase 5 execution baseline benchmark

```bash
.venv/bin/python benchmarks/phase5_collect_baseline.py
```

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
