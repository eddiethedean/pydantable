# Versioning and stability (pre-1.0)

PydanTable is **0.x** until **`v1.0.0`**. This page states how maintainers intend to use **semver** on that train. **Behavioral** guarantees for transforms, nulls, joins, windows, materialization, and interchange live in {doc}`INTERFACE_CONTRACT`—treat that document as the semantics source of truth.

## 0.x.y releases

- **Patch (`y`)** — Bug fixes, documentation-only updates, internal refactors, and test hardening that do **not** change documented public behavior.
- **Minor (`0.x`)** — **Additive** changes: new `Expr` methods, new optional parameters with safe defaults, new documented APIs, or clarified contracts that align docs with long-standing behavior. **Changelog** entries should call out anything users might need to adopt consciously.

**Breaking changes before 1.0** should be rare. When they are necessary, ship them in a **minor** bump with explicit **migration** notes in {doc}`changelog` (and, if large, a short section in {doc}`ROADMAP`).

## 1.x.y releases (semver policy)

Starting at **`v1.0.0`**, PydanTable follows **semver** for the public Python API.

### What is “public API”?

- **Guaranteed behavior** is defined by {doc}`INTERFACE_CONTRACT` (joins, nulls, windows, trusted ingest, materialization, interchange, etc.).\n+- **Docs** (`README`, doc site pages) are part of the contract when they describe user-visible behavior.\n+- **Undocumented internals** (private modules, helper functions, intermediate AST structs) are not part of the API unless explicitly documented.

### Patch releases (1.x.y)

Patch releases include:\n+\n+- Bug fixes that restore intended behavior.\n+- Performance improvements and refactors that do not change documented semantics.\n+- Test/docs hardening.\n+- Dependency updates that do not change the effective contract.\n+\n+Patch releases should not:\n+\n+- Remove or rename public APIs.\n+- Change documented semantics in {doc}`INTERFACE_CONTRACT`.

### Minor releases (1.x)

Minor releases are **backwards compatible** additions:\n+\n+- New `Expr` operators/functions.\n+- New methods on `DataFrame` / `DataFrameModel`.\n+- New optional parameters with safe defaults.\n+- New optional integrations/extras (e.g. additional interop entrypoints).\n+\n+Minor releases may also include contract clarifications **when they match existing behavior** (docs/test alignment) and are called out in {doc}`changelog`.

### Major releases (2.0.0+)

Major releases may include breaking changes:\n+\n+- Removing/renaming public APIs.\n+- Changing documented semantics.\n+- Changing serialization/interchange behaviors in a way that breaks consumers.\n+\n+Major releases require explicit migration notes in {doc}`changelog`.

### Stability notes (what we will and won’t treat as stable)

- **Row ordering** is generally not guaranteed for materialization (`to_dict`, `collect(as_lists=True)`) unless explicitly documented.\n+- **Error messages** are best-effort and may change; where tests depend on errors, they should assert stable **exception types** and high-signal substrings.\n+- **Optional-dependency interop** (`__dataframe__`, `__dataframe_consortium_standard__`) is supported, but behavior may track upstream libraries (PyArrow/Streamlit/pandas) within the documented constraints.\n+
## Python package and Rust extension

`pydantable.__version__` and `pydantable._core.rust_version()` are kept equal to **`pyproject.toml`** / **`pydantable-core/Cargo.toml`** on every release (see `tests/test_version_alignment.py`). Wheels and sdist versions come from **`pyproject.toml`** via Maturin.

## Path to 1.0

The **production-ready** **`v1.0.0`** bar—full semver policy for **patch / minor / major**, packaging checklist, support matrix, and comms—is described under **Planned v1.0.0** in {doc}`ROADMAP`. **0.19.0** was the pre-1.0 **documentation consolidation**; **0.20.0** shipped **UX / discovery** (repr, introspection, display options, richer **`describe`**, **`value_counts`**, etc.) on the same **0.x** train before that gate.

## Related documentation

- {doc}`INTERFACE_CONTRACT` — guaranteed behavior for the typed API.
- {doc}`changelog` — release-by-release highlights.
- {doc}`DEVELOPER` — build, test, and tagging workflow.
