# Versioning and stability (0.x and 1.x)

PydanTable is **1.x** today. This page states how maintainers use **semver** on the stable train. **Behavioral** guarantees for transforms, nulls, joins, windows, materialization, and interchange live in {doc}`INTERFACE_CONTRACT`—treat that document as the semantics source of truth.

## 0.x.y releases (historical)

- **Patch (`y`)** — Bug fixes, documentation-only updates, internal refactors, and test hardening that do **not** change documented public behavior.
- **Minor (`0.x`)** — **Additive** changes: new `Expr` methods, new optional parameters with safe defaults, new documented APIs, or clarified contracts that align docs with long-standing behavior. **Changelog** entries should call out anything users might need to adopt consciously.

**Breaking changes before 1.0** should be rare. When they are necessary, ship them in a **minor** bump with explicit **migration** notes in {doc}`CHANGELOG` (and, if large, a short section in {doc}`ROADMAP`).

## Python package and Rust extension

`pydantable.__version__` and `pydantable._core.rust_version()` are kept equal to **`pyproject.toml`** / **`pydantable-core/Cargo.toml`** on every release (see `tests/test_version_alignment.py`). Wheels and sdist versions come from **`pyproject.toml`** via Maturin.

## Path to 1.0

The production-ready **`v1.0.0`** bar—semver policy for 1.x, packaging checklist, support matrix, and comms—is described under **Planned v1.0.0** in {doc}`ROADMAP`.

## 1.x policy (effective at `v1.0.0`)

For **1.x**, `pydantable` follows semantic versioning with the boundaries below.

### Source of truth

- **Behavioral semantics** (joins, null handling, windows, materialization, reshape rules) are defined in {doc}`INTERFACE_CONTRACT`.
- **Versioning policy** (what kind of release is required for a change) is defined here.
- If these documents conflict, maintainers must update one of them before release.

### Patch releases (`1.x.y`)

Patch releases are for non-breaking changes such as:

- Bug fixes that restore documented behavior.
- Documentation clarifications that do not change runtime behavior.
- Internal refactors/performance improvements with no public API/contract change.
- Dependency/security updates with no user-visible contract change.

### Minor releases (`1.x`)

Minor releases are additive and backward compatible, including:

- New methods or helpers on `DataFrame` / `DataFrameModel` / `Expr`.
- New optional parameters with backward-compatible defaults.
- New optional integrations/extras and new documented I/O helpers.
- Additional dtype support where existing behavior is unchanged.

### Major releases (`2.0.0+`)

Major releases are required for breaking changes, including:

- Removing/renaming public methods or parameters.
- Changing default behavior in a way that can alter outputs or errors for existing code.
- Changing documented semantics in {doc}`INTERFACE_CONTRACT` incompatibly.

### Rust extension boundary (`pydantable._core`)

- `pydantable._core` internals are not a stable API for direct user imports.
- Stability commitments apply to documented Python-facing APIs in `pydantable`.
- `pydantable.__version__` and `pydantable._core.rust_version()` remain aligned on releases.

### Practical examples

- **Patch:** fix a null-propagation regression so runtime matches documented contract.
- **Minor:** add `DataFrame.read_foo(...)` with default-off behavior and docs/tests.
- **Major:** change `collect()` default return shape or remove a long-standing method.

## Planned removals (`2.0.0`)

The following deprecated parameters are scheduled for removal in **`2.0.0`** (a **major** release per the policy above):

- **`as_polars=`** on **`DataFrame.collect`**, **`DataFrame.acollect`**, and the **`DataFrameModel`** collection shims — use **`to_polars()`** / **`ato_polars()`** (and **`collect(as_lists=True)`** / **`to_dict()`** for columnar dicts).

## Related documentation

- {doc}`INTERFACE_CONTRACT` — guaranteed behavior for the typed API.
- {doc}`CHANGELOG` — release-by-release highlights.
- {doc}`DEVELOPER` — build, test, and tagging workflow.
