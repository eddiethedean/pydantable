# Local checks span three Python trees: core ``python/pydantable``, ``pydantable-protocol``,
# and ``pydantable-native`` (plus Rust in check-rust).
#
# Quick start (from repo root, with a venv activated or ``.venv`` present):
#   make dev-setup              # protocol + maturin develop + editable pydantable
#   make install-dev            # same as dev-setup but with pip install -e ".[dev]"
#
# Or step by step:
#   pip install -e ./pydantable-protocol && make native-develop && pip install -e .
PYTHON ?= .venv/bin/python
RUFF ?= $(PYTHON) -m ruff
TY ?= $(PYTHON) -m ty
PYRIGHT ?= $(PYTHON) -m pyright

CARGO_MANIFEST ?= pydantable-core/Cargo.toml
NATIVE_PYPROJECT ?= pydantable-native/pyproject.toml

# Source roots on PYTHONPATH for Rust ``cargo test`` (Polars in venv + editable packages).
RUST_PYTHONPATH ?= $(CURDIR)/python:$(CURDIR)/pydantable-protocol/python:$(CURDIR)/pydantable-native/python

.PHONY: check-full check-python check-rust check-docs ruff-format-check ruff-check engine-bypass-check ty-check ty-check-minimal pyright-check pyright-check-strict sphinx-check docs-api-rst-check rust-fmt-check rust-clippy rust-check-no-default-features rust-test
.PHONY: native-develop native-develop-fast native-wheel install-editable dev-setup install-dev help
.PHONY: test test-fast test-cov test-moltres test-mongo diff-cover
.PHONY: gen-typing check-typing

check-full: check-python check-docs check-rust

check-python: ruff-format-check ruff-check engine-bypass-check ty-check ty-check-minimal pyright-check check-typing

check-docs: docs-api-rst-check sphinx-check

ruff-format-check:
	$(RUFF) format --check .

ruff-check:
	$(RUFF) check .

engine-bypass-check:
	$(PYTHON) scripts/check_engine_bypass.py

# Uses ``[tool.ty]`` in repo ``pyproject.toml`` (roots + include globs).
ty-check:
	$(TY) check

# Mirror CI: minimal venv (no numpy/pyarrow) to ensure optional imports stay sound under ty.
ty-check-minimal:
	@if [ ! -x .venv-ty-min/bin/python ]; then \
		if [ -x .venv/bin/python ]; then .venv/bin/python -m venv .venv-ty-min; else python3 -m venv .venv-ty-min; fi; \
	fi
	@.venv-ty-min/bin/python -m pip -q install -U pip >/dev/null
	@.venv-ty-min/bin/python -m pip -q install "ty>=0.0.28" "pydantic>=2.0,<3" "typing-extensions>=4.7" >/dev/null
	@.venv-ty-min/bin/python -m ty check --python .venv-ty-min/bin/python

pyright-check:
	$(PYRIGHT) --project pyrightconfig.json

# Optional: type-check the full ``python/pydantable`` tree under Pyright (noisier than
# contract tests; does not replace Astral ``ty`` in ``make check-python``).
pyright-check-strict:
	$(PYRIGHT) --project pyrightconfig-strict.json

# Matches CI "Docs (sphinx -W)" check.
sphinx-check:
	$(PYTHON) -m sphinx -W -b html docs docs/_build/html

docs-api-rst-check:
	$(PYTHON) scripts/check_docs_api_rst.py

gen-typing:
	$(PYTHON) scripts/generate_typing_artifacts.py

check-typing:
	$(PYTHON) scripts/generate_typing_artifacts.py --check
	$(TY) check
	$(PYTHON) -m pytest -q \
		tests/typing/test_mypy_dataframe_model_return_types.py \
		tests/typing/test_mypy_typing_contracts.py \
		tests/typing/test_pyright_dataframe_return_types.py \
		tests/typing/test_pyright_dataframe_model_return_types.py \
		tests/typing/test_pyright_typing_contracts.py \
		tests/typing/test_ty_dataframe_return_types.py \
		tests/typing/test_ty_dataframe_model_return_types.py

# Match CI ``python-tests`` (Ubuntu matrix); requires ``pip install -e ".[dev]"`` and native build.
test:
	$(PYTHON) -m pytest -q -n auto

test-fast:
	$(PYTHON) -m pytest -q -n auto -m "not slow"

# Match CI coverage gate (Ubuntu + Python 3.11 only in Actions).
test-cov:
	$(PYTHON) -m pytest -q -n auto \
		--cov=pydantable --cov-report=xml --cov-report=term-missing:skip-covered --cov-fail-under=83

# Requires ``coverage.xml`` from ``make test-cov`` and ``pip install -e ".[dev]"`` (diff-cover).
DIFF_COVER ?= $(dir $(PYTHON))diff-cover
diff-cover:
	@test -f coverage.xml || (echo "Run make test-cov first to generate coverage.xml"; exit 1)
	$(DIFF_COVER) coverage.xml --compare-branch origin/main --fail-under=85

test-moltres:
	$(PYTHON) -m pytest -q -n auto tests/sql/test_sql_moltres.py

# Mongo integration (``pydantable.mongo_entei`` + optional PyPI ``entei-core``); install ``entei-core`` / ``pydantable[mongo]``.
test-mongo:
	$(PYTHON) -m pytest -q tests/mongo

check-rust: rust-fmt-check rust-clippy rust-check-no-default-features rust-test

rust-fmt-check:
	cargo fmt --manifest-path $(CARGO_MANIFEST) -- --check

rust-clippy:
	cargo clippy --manifest-path $(CARGO_MANIFEST) --all-features --tests \
		-- -D warnings -W clippy::unwrap_used -W clippy::expect_used

rust-check-no-default-features:
	cargo check --manifest-path $(CARGO_MANIFEST) --no-default-features

# PyO3's embedded interpreter does not always load site-packages; prepend repo source
# trees plus venv site-packages so ``polars``, ``pydantable``, and ``pydantable_protocol`` resolve.
rust-test:
	PYO3_PYTHON=$(CURDIR)/$(PYTHON) \
	PYTHONPATH=$(RUST_PYTHONPATH):$$($(CURDIR)/$(PYTHON) -c "import site; print(site.getsitepackages()[0])") \
	cargo test --manifest-path $(CARGO_MANIFEST) --all-features

# --- Editable installs & native extension (maturin) ---

help:
	@echo "PydanTable Makefile"
	@echo ""
	@echo "Setup:"
	@echo "  dev-setup          Install protocol, build native (release), pip install -e ."
	@echo "  install-dev        Like dev-setup but pip install -e \".[dev]\""
	@echo "  install-editable   pip install -e protocol + root package (no Rust build)"
	@echo "  native-develop     pip install -e protocol + maturin develop --release"
	@echo "  native-develop-fast  maturin develop without --release (faster iteration)"
	@echo "  native-wheel       Build a release wheel (does not pip-install)"
	@echo ""
	@echo "Checks: check-full, check-python, check-rust, check-docs"
	@echo "Tests: test, test-fast (-m \"not slow\"), test-cov (CI coverage args), test-moltres (sql/), test-mongo (tests/mongo/)"
	@echo "Coverage: diff-cover (needs test-cov + origin/main)"
	@echo "  pyright-check-strict  Optional Pyright over python/pydantable (maintainers)"

# Install editable Python packages only (assumes pydantable-native already built/installed).
install-editable:
	$(PYTHON) -m pip install -q -e ./pydantable-protocol
	$(PYTHON) -m pip install -q -e .

# Full local dev: native extension + editable main package (matches typical contributor flow).
dev-setup: native-develop install-editable

# Same as dev-setup but with test/lint/docs extras from pyproject.toml.
install-dev: native-develop
	$(PYTHON) -m pip install -q -e ".[dev]"

native-develop:
	$(PYTHON) -m pip install -q "maturin>=1.4,<2.0"
	$(PYTHON) -m pip install -q -e ./pydantable-protocol
	cd pydantable-native && $(CURDIR)/$(PYTHON) -m maturin develop --release

native-develop-fast:
	$(PYTHON) -m pip install -q "maturin>=1.4,<2.0"
	$(PYTHON) -m pip install -q -e ./pydantable-protocol
	cd pydantable-native && $(CURDIR)/$(PYTHON) -m maturin develop

# Build wheel under pydantable-native/target/wheels/ (install with pip install <wheel>).
native-wheel:
	$(PYTHON) -m pip install -q "maturin>=1.4,<2.0"
	$(PYTHON) -m pip install -q -e ./pydantable-protocol
	cd pydantable-native && $(CURDIR)/$(PYTHON) -m maturin build --release

