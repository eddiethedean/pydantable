# Local checks span three Python trees: core ``python/pydantable``, ``pydantable-protocol``,
# and ``pydantable-native`` (plus Rust in check-rust). Before ``native-develop`` or first
# ``check-full``, install deps from repo root, for example:
#   pip install -e ./pydantable-protocol && pip install -e ".[dev]" && make native-develop
PYTHON ?= .venv/bin/python
RUFF ?= $(PYTHON) -m ruff
TY ?= $(PYTHON) -m ty
PYRIGHT ?= $(PYTHON) -m pyright

CARGO_MANIFEST ?= pydantable-core/Cargo.toml
NATIVE_PYPROJECT ?= pydantable-native/pyproject.toml

# Source roots on PYTHONPATH for Rust ``cargo test`` (Polars in venv + editable packages).
RUST_PYTHONPATH ?= $(CURDIR)/python:$(CURDIR)/pydantable-protocol/python:$(CURDIR)/pydantable-native/python

.PHONY: check-full check-python check-rust check-docs ruff-format-check ruff-check engine-bypass-check ty-check ty-check-minimal pyright-check sphinx-check rust-fmt-check rust-clippy rust-check-no-default-features rust-test
.PHONY: native-develop native-wheel
.PHONY: gen-typing check-typing

check-full: check-python check-docs check-rust

check-python: ruff-format-check ruff-check engine-bypass-check ty-check ty-check-minimal pyright-check check-typing

check-docs: sphinx-check

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

# Matches CI "Docs (sphinx -W)" check.
sphinx-check:
	$(PYTHON) -m sphinx -W -b html docs docs/_build/html

gen-typing:
	$(PYTHON) scripts/generate_typing_artifacts.py

check-typing:
	$(PYTHON) scripts/generate_typing_artifacts.py --check
	$(TY) check
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

# PyO3's embedded interpreter does not always load site-packages; prepend repo source
# trees plus venv site-packages so ``polars``, ``pydantable``, and ``pydantable_protocol`` resolve.
rust-test:
	PYO3_PYTHON=$(CURDIR)/.venv/bin/python \
	PYTHONPATH=$(RUST_PYTHONPATH):$$($(CURDIR)/.venv/bin/python -c "import site; print(site.getsitepackages()[0])") \
	cargo test --manifest-path $(CARGO_MANIFEST) --all-features

native-develop:
	$(PYTHON) -m pip install -q -e ./pydantable-protocol
	cd pydantable-native && $(CURDIR)/$(PYTHON) -m maturin develop --release

