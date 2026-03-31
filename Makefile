PYTHON ?= .venv/bin/python
RUFF ?= $(PYTHON) -m ruff
MYPY ?= $(PYTHON) -m mypy
PYRIGHT ?= $(PYTHON) -m pyright

CARGO_MANIFEST ?= pydantable-core/Cargo.toml

.PHONY: check-full check-python check-rust check-docs ruff-format-check ruff-check mypy-check pyright-check sphinx-check rust-fmt-check rust-clippy rust-check-no-default-features rust-test
.PHONY: gen-typing check-typing
.PHONY: mypy-check-minimal

check-full: check-python check-docs check-rust

check-python: ruff-format-check ruff-check mypy-check mypy-check-minimal pyright-check check-typing

check-docs: sphinx-check

ruff-format-check:
	$(RUFF) format --check .

ruff-check:
	$(RUFF) check .

mypy-check:
	$(MYPY) python/pydantable

# Mirror CI's mypy environment (no optional deps like numpy installed).
# This catches missing ``# type: ignore[import-not-found]`` on optional imports.
mypy-check-minimal:
	@if [ ! -x .venv-mypy-min/bin/python ]; then \
		if [ -x .venv/bin/python ]; then .venv/bin/python -m venv .venv-mypy-min; else python3 -m venv .venv-mypy-min; fi; \
	fi
	@.venv-mypy-min/bin/python -m pip -q install -U pip >/dev/null
	@.venv-mypy-min/bin/python -m pip -q install mypy pydantic >/dev/null
	@MYPYPATH=python .venv-mypy-min/bin/python -m mypy python/pydantable

pyright-check:
	$(PYRIGHT) --project pyrightconfig.json

# Matches CI "Docs (sphinx -W)" check.
sphinx-check:
	$(PYTHON) -m sphinx -W -b html docs docs/_build/html

gen-typing:
	$(PYTHON) scripts/generate_typing_artifacts.py

check-typing:
	$(PYTHON) scripts/generate_typing_artifacts.py --check
	$(MYPY) python/pydantable
	$(PYTHON) -m pytest -q \
		tests/test_mypy_dataframe_model_return_types.py \
		tests/test_mypy_typing_contracts.py \
		tests/test_pyright_dataframe_model_return_types.py \
		tests/test_pyright_typing_contracts.py

check-rust: rust-fmt-check rust-clippy rust-check-no-default-features rust-test

rust-fmt-check:
	cargo fmt --manifest-path $(CARGO_MANIFEST) -- --check

rust-clippy:
	cargo clippy --manifest-path $(CARGO_MANIFEST) -- -D warnings

rust-check-no-default-features:
	cargo check --manifest-path $(CARGO_MANIFEST) --no-default-features

# PyO3's embedded interpreter does not always load site-packages; point PYTHONPATH at the venv
# so optional deps like `polars` resolve for plan tests that import Python.
rust-test:
	PYO3_PYTHON=$(CURDIR)/.venv/bin/python \
	PYTHONPATH=$$($(CURDIR)/.venv/bin/python -c "import site; print(site.getsitepackages()[0])") \
	cargo test --manifest-path $(CARGO_MANIFEST) --all-features

