# Versioning and stability

PydanTable follows **semantic versioning**. The **current stable train is 2.x** (as of **v2.0.0**). **Behavioral** guarantees for transforms, nulls, joins, windows, materialization, and interchange live in {doc}`INTERFACE_CONTRACT`—treat that document as the semantics source of truth.

## Source of truth

- **Behavioral semantics** are defined in {doc}`INTERFACE_CONTRACT`.
- **Versioning policy** (what kind of release is required for a change) is defined on this page.
- If these documents conflict, maintainers must update one of them before release.

## Python package and Rust extension

`pydantable.__version__`, **`pydantable_protocol.__version__`**, and `pydantable_native._core.rust_version()` are kept equal to **`pyproject.toml`** / **`pydantable-protocol/pyproject.toml`** / **`pydantable-core/Cargo.toml`** on every release (see **`tests/dataframe/test_version_alignment.py`**). The **pydantable** package is pure Python plus documented re-exports; engine protocols ship in **`pydantable-protocol`** (no dependencies); the native extension ships in the **`pydantable-native`** distribution, which pins **`pydantable-protocol`** but does **not** depend on **`pydantable`**.

## 2.x policy (effective `v2.0.0+`)

### Patch releases (`2.0.y`, `2.1.y`, …)

Non-breaking fixes and safe clarifications:

- Bug fixes that restore documented behavior.
- Documentation clarifications that do not change runtime behavior.
- Internal refactors/performance improvements with no public API/contract change.
- Dependency/security updates with no user-visible contract change.

### Minor releases (`2.x`)

Additive, backward-compatible changes within 2.x, including:

- New methods or helpers on `DataFrame` / `DataFrameModel` / `Expr`.
- New optional parameters with backward-compatible defaults.
- New optional integrations/extras and new documented I/O helpers.
- Additional dtype support where existing behavior is unchanged.

### Major releases (`3.0.0+`)

Required for incompatible changes, including:

- Removing/renaming public methods or parameters.
- Changing default behavior in a way that can alter outputs or errors for existing code.
- Changing documented semantics in {doc}`INTERFACE_CONTRACT` incompatibly.

### Rust extension boundary (`pydantable_native._core`)

- `pydantable_native._core` internals are not a stable API for direct user imports.
- Stability commitments apply to documented Python-facing APIs in `pydantable`.
- `pydantable.__version__` and `pydantable_native._core.rust_version()` remain aligned on releases.

### Practical examples

- **Patch:** fix a null-propagation regression so runtime matches documented contract.
- **Minor:** add `DataFrame.read_foo(...)` with default-off behavior and docs/tests.
- **Major:** remove a long-standing keyword parameter or change `collect()` defaults.

## Upcoming removals

The following APIs remain **available in 2.0.0** but are **deprecated**; migrate before the stated target.

- **`as_polars=`** on **`DataFrame.collect`**, **`DataFrame.acollect`**, and **`DataFrameModel`** collection shims — use **`to_polars()`** / **`ato_polars()`** (and **`collect(as_lists=True)`** / **`to_dict()`** for columnar dicts). **Target removal: `v2.1.0`** (see warnings and {doc}`TYPING`).

The following **legacy string-SQL I/O** names remain as deprecated aliases (since **v1.13.0**); they emit **`DeprecationWarning`**. Use **`fetch_sql_raw`**, **`iter_sql_raw`**, **`write_sql_raw`**, **`afetch_sql_raw`**, **`aiter_sql_raw`**, **`awrite_sql_raw`**, or SQLModel-first **`fetch_sqlmodel`** / **`iter_sqlmodel`** / **`write_sqlmodel`** (and **`DataFrameModel`** mirrors) instead. **Removal in a future major version after 2.0** (timeline: {doc}`SQLMODEL_SQL_ROADMAP`):

- **`fetch_sql`**, **`iter_sql`**, **`write_sql`**, **`afetch_sql`**, **`aiter_sql`**, **`awrite_sql`**, **`write_sql_batches`**, **`awrite_sql_batches`**

## Historical trains

### 1.x policy (prior stable train)

For **1.x**, patch/minor/major boundaries matched the structure above with major = **`2.0.0`** (strict typed API). The **v1.0.0** bar and history live in {doc}`ROADMAP`.

### 0.x.y (historical)

- **Patch (`y`)** — Bug fixes, documentation-only updates, internal refactors, and test hardening that do **not** change documented public behavior.
- **Minor (`0.x`)** — **Additive** changes with migration notes when needed.

## Related documentation

- {doc}`INTERFACE_CONTRACT` — guaranteed behavior for the typed API.
- {doc}`CHANGELOG` — release-by-release highlights.
- {doc}`DEVELOPER` — build, test, and tagging workflow.
