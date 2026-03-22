PYTHON ?= .venv/bin/python
RUFF ?= $(PYTHON) -m ruff
MYPY ?= $(PYTHON) -m mypy

CARGO_MANIFEST ?= pydantable-core/Cargo.toml

.PHONY: check-full check-python check-rust ruff-format-check ruff-check mypy-check rust-fmt-check rust-clippy rust-test

check-full: check-python check-rust

check-python: ruff-format-check ruff-check mypy-check

ruff-format-check:
	$(RUFF) format --check .

ruff-check:
	$(RUFF) check .

mypy-check:
	$(MYPY) python/pydantable

check-rust: rust-fmt-check rust-clippy rust-test

rust-fmt-check:
	cargo fmt --manifest-path $(CARGO_MANIFEST) -- --check

rust-clippy:
	cargo clippy --manifest-path $(CARGO_MANIFEST) -- -D warnings

# PyO3's embedded interpreter does not always load site-packages; point PYTHONPATH at the venv
# so optional deps like `polars` resolve for plan tests that import Python.
rust-test:
	PYO3_PYTHON=$(CURDIR)/.venv/bin/python \
	PYTHONPATH=$$($(CURDIR)/.venv/bin/python -c "import site; print(site.getsitepackages()[0])") \
	cargo test --manifest-path $(CARGO_MANIFEST) --all-features

