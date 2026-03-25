# Versioning and stability (0.x)

PydanTable is **0.x** today. This page states how maintainers intend to use **semver** on that train. **Behavioral** guarantees for transforms, nulls, joins, windows, materialization, and interchange live in {doc}`INTERFACE_CONTRACT`—treat that document as the semantics source of truth.

## 0.x.y releases

- **Patch (`y`)** — Bug fixes, documentation-only updates, internal refactors, and test hardening that do **not** change documented public behavior.
- **Minor (`0.x`)** — **Additive** changes: new `Expr` methods, new optional parameters with safe defaults, new documented APIs, or clarified contracts that align docs with long-standing behavior. **Changelog** entries should call out anything users might need to adopt consciously.

**Breaking changes before 1.0** should be rare. When they are necessary, ship them in a **minor** bump with explicit **migration** notes in {doc}`changelog` (and, if large, a short section in {doc}`ROADMAP`).

## Python package and Rust extension

`pydantable.__version__` and `pydantable._core.rust_version()` are kept equal to **`pyproject.toml`** / **`pydantable-core/Cargo.toml`** on every release (see `tests/test_version_alignment.py`). Wheels and sdist versions come from **`pyproject.toml`** via Maturin.

## Path to 1.0

The production-ready **`v1.0.0`** bar—semver policy for 1.x, packaging checklist, support matrix, and comms—is described under **Planned v1.0.0** in {doc}`ROADMAP`.

## Related documentation

- {doc}`INTERFACE_CONTRACT` — guaranteed behavior for the typed API.
- {doc}`changelog` — release-by-release highlights.
- {doc}`DEVELOPER` — build, test, and tagging workflow.
