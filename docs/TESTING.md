# Testing

This document describes how to run the Python test suite for `pydantable`, how it aligns with CI, and how optional dependencies and markers are used.

For editor setup, linting, and type checking, see {doc}`DEVELOPER` and {doc}`TYPING`.

## Prerequisites

- Editable install of `pydantable-protocol` and a built `pydantable-native` extension (same as CI). From repo root:

  ```bash
  make install-dev
  ```

  or equivalently `pip install -e ./pydantable-protocol`, `(cd pydantable-native && maturin develop --release)`, then `pip install -e ".[dev]"`.

- **CI parity:** The GitHub Actions job installs test dependencies with an explicit `pip install` list that mirrors `pyproject.toml` **`[project.optional-dependencies] dev`** (plus the lazy-SQL bridge / `greenlet` for SQL engine tests). To compare the declared extras with what CI installs, run:

  ```bash
  python scripts/ci_print_dev_extras.py
  ```

## Running tests

From the repository root (with your virtualenv activated):

```bash
pytest
```

Common variants:

| Command | Purpose |
|--------|---------|
| `pytest -q` | Quiet progress (CI default) |
| `pytest -n auto` | Parallel workers via **pytest-xdist** (matches CI) |
| `pytest -m "not slow"` | Skip timing-based guardrails |
| `pytest -m "not network"` | Skip tests that start loopback HTTP servers |
| `pytest tests/io` | Run only tests under `tests/io/` |

**Parallelism (xdist):** CI runs `pytest -q -n auto`. Tests should not rely on shared global state across processes. If you see order-dependent failures only under `-n auto`, fix the test or isolate state (fixtures, tmp paths).

**Native extension:** Lazy I/O and execution paths require `maturin develop` (or an installed `pydantable-native` wheel). Without the extension, many tests skip or fail early with clear errors.

## Markers

Registered markers are listed in `pyproject.toml` under **`[tool.pytest.ini_options] markers`**. CI runs with **`--strict-markers`**, so every `@pytest.mark.*` used in tests must be registered there (built-in markers like `parametrize` are exempt).

| Marker | Meaning |
|--------|---------|
| `slow` | Timing or performance guardrails; exclude with `-m "not slow"` for a faster loop |
| `network` | Uses loopback HTTP or similar local networking |
| `optional_cloud` | Optional cloud SDK / heavy extras; may skip via `importorskip` |
| `asyncio` | Async tests (pytest-asyncio) |

Prefer **`importorskip`** for optional third-party modules that may be absent in minimal environments; use markers for *categories* of tests (slow, network, cloud mocks).

## Coverage (CI)

On **Ubuntu** with **Python 3.11** only, CI runs pytest with coverage and a minimum line gate (**`--cov-fail-under=83`**; see `.github/workflows/_shared-ci.yml`). To reproduce locally:

```bash
pytest -q -n auto \
  --cov=pydantable \
  --cov-report=term-missing:skip-covered \
  --cov-report=xml \
  --cov-fail-under=83
```

`Makefile` target **`make test-cov`** runs the same arguments (see `Makefile`).

**Gap tracking:** Per-area backlog and how to regenerate the baseline report are in {doc}`COVERAGE_BACKLOG`.

**PRs (recommended for large changes):** After `coverage.xml` exists (from `test-cov` or CI artifacts), run **`make diff-cover`** to compare **changed lines** on the current branch against **`origin/main`** (`diff-cover` is in **`[dev]`**). The Makefile uses **`--fail-under=85`** on touched lines; increase that over time as overall coverage improves (see `COVERAGE_BACKLOG.md`).

## Timeouts

**pytest-timeout** is enabled with a default per-test deadline (see `pyproject.toml`). Override for a single test with `@pytest.mark.timeout(seconds)` or the plugin’s documented options. This helps catch deadlocks in async or thread-heavy tests.

## Random test order (optional)

A scheduled CI job may run the full suite with **pytest-randomly** installed to surface order dependence. Normal PR CI does not require this plugin; see `.github/workflows/_shared-ci.yml` for the **`python-tests-random-order`** job.

## Layout

Tests live under `tests/` with domain subdirectories (`io/`, `dataframe/`, `sql/`, …). A short map of file naming patterns is in `tests/README.md`.

Rust tests for `pydantable-core` are separate (`cargo test` / `make rust-test`); see {doc}`DEVELOPER`.
