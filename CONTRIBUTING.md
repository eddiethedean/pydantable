# Contributing to pydantable

Thank you for considering a contribution. This file is the entrypoint; detailed guides live in the docs site.

## Quick setup

```bash
git clone https://github.com/eddiethedean/pydantable.git
cd pydantable
make dev-setup    # venv, protocol, native extension, editable install with [dev]
make check-full # ruff, ty, pyright, typing tests, MkDocs, Rust
pytest -q       # Python test suite
```

See [Developer guide](docs/project/developer.md) for Rust toolchain pins, engine boundaries, and release notes.

## What to contribute

- **Bug fixes** with a regression test
- **Documentation** — follow [Docs style guide](docs/project/contributing/docs-style-guide.md); run `mkdocs build --strict`
- **Features** — discuss large changes in an issue first; keep ADR/engine boundaries in mind

## Pull requests

1. Branch from **`main`**
2. Keep changes focused; match existing code style (Ruff enforces formatting)
3. Run **`make check-python`** (or **`make check-full`** before Rust/doc changes)
4. Add or update tests under **`tests/`** — see [Testing](docs/project/testing.md)
5. Update **`docs/project/changelog.md`** under **Unreleased** for user-visible changes
6. If you change tutorial snippets, run **`python scripts/verify_doc_examples.py`**

## Reporting issues

Use [GitHub Issues](https://github.com/eddiethedean/pydantable/issues) with:

- Python version and platform
- **`pydantable.__version__`** and whether **`native_engine_capabilities().extension_loaded`** is true
- Minimal reproducer

Security issues: see [SECURITY.md](SECURITY.md) — do not file public issues for vulnerabilities.

## Code of conduct

Be respectful and constructive. Maintainers may close issues or PRs that do not meet the bar above.

## More reading

- [Developer guide](docs/project/developer.md)
- [Testing](docs/project/testing.md)
- [Architecture](docs/project/architecture.md)
- [Roadmap](docs/project/roadmap.md)
