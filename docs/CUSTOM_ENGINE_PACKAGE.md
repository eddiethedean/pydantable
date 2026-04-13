# Custom execution engine packages

This guide is for **authors of separate Python packages** that implement
:class:`~pydantable.engine.protocols.ExecutionEngine` (defined in
`pydantable_protocol`, re-exported under `pydantable.engine.protocols`) so end users can run
PydanTable’s typed `DataFrame` / `DataFrameModel` API on top of **your** backend
(for example a SQL engine, a remote service, or another dataframe library).

For design rationale and guardrails inside this repository, see
{doc}`ADR-engines`. For day-to-day contributor setup, see {doc}`DEVELOPER`.

(custom-engine-deps)=

## Dependencies

**Minimum (protocol only):**

- Add **`pydantable-protocol`** as a normal dependency, with a **version pin**
  that matches the PydanTable releases you support (same `1.x.y` as
  **`pydantable`** on PyPI — see {doc}`VERSIONING`).
- You do **not** need to depend on **`pydantable`** to **define** your engine
  class: implement the structural protocol from **`pydantable_protocol`** and
  raise **`pydantable_protocol.UnsupportedEngineOperationError`** (or a
  subclass) when an operation is not available.

**Recommended for development and tests:**

- **`pydantable`** as a **dev / test** dependency so you can build
  `DataFrame` / `DataFrameModel` instances, run materialization, and run
  integration tests.

**End-user installs:**

- Applications install **`pydantable`** and **your package** (and any DB
  drivers, HTTP clients, etc.). They might also install **`pydantable-native`**
  if they want the default Polars-backed engine for file I/O or mixed stacks.

(custom-engine-implement)=

## Implementing `ExecutionEngine`

The interface is a `typing.Protocol`: **no inheritance required**. At import
time, static checkers and :func:`isinstance` (with
`@runtime_checkable`) can verify your class against
:class:`~pydantable.engine.protocols.ExecutionEngine`.

1. **Plan construction** — Implement **`make_plan`**, **`plan_*`**, expression
   helpers (**`make_literal`**, **`expr_is_global_agg`**, …) for the operations
   you intend to support. PydanTable passes **opaque** plan handles between
   methods; the native engine wraps Rust objects — your backend might use SQL
   strings, ORM query objects, or client-specific handles, as long as you keep
   types consistent across **`execute_*`** and **`plan_*`** for a given frame.

2. **Execution** — Implement **`execute_plan`**, **`async_execute_plan`**,
   **`collect_batches`**, **`async_collect_plan_batches`**, and any
   **`execute_join`**, **`execute_groupby_agg`**, … helpers that your users will
   trigger. Match documented semantics where you claim parity ({doc}`INTERFACE_CONTRACT`).

3. **Sinks** — Implement **`write_parquet`**, **`write_csv`**, **`write_ipc`**,
   **`write_ndjson`** if users can export through your backend; otherwise raise
   **`UnsupportedEngineOperationError`** with a clear message.

4. **Capabilities** — Expose a **`capabilities`** property returning
   **`EngineCapabilities`**. Set **`backend`** to **`"custom"`**. Set feature
   flags (**`has_execute_plan`**, **`has_sink_parquet`**, …) to reflect reality
   so UIs and tests can probe support without trying every API.

5. **Async honesty** — **`has_async_execute_plan`** and
   **`has_async_collect_plan_batches`** must match whether **`async_*`** methods
   truly work.

**Reference implementations:**

- **`StubExecutionEngine`** in this repo (`python/pydantable/engine/stub.py`):
  minimal surface, raises on most calls — good for typing tests.
- **`NativePolarsEngine`** in **`pydantable-native`**: full implementation over
  **`pydantable_native._core`**.

- **Third-party:** [**moltres-core**](https://pypi.org/project/moltres-core/) (PyPI)
  ships **`MoltresPydantableEngine`** for SQL-backed execution in the Moltres stack;
  see the upstream repository’s **`docs/PYDANTABLE_ENGINE.md`**. PydanTable user guide:
  {doc}`MOLTRES_SQL` (**`SqlDataFrame`**, **`SqlDataFrameModel`**, **`pydantable[moltres]`**).

When **PydanTable** adds new protocol members, contract tests in this project
(exercising **`typing_extensions.get_protocol_members`**) and release notes
will flag required updates — pin **`pydantable-protocol`** accordingly.

(custom-engine-wiring)=

## Wiring your engine in applications

**Per frame (preferred for mixing backends):**

- Pass **`engine=`** when constructing **`DataFrame`** or **`DataFrameModel`**
  so the inner `DataFrame` uses your implementation (see `DataFrameModel`
  docstring in the source tree).

**Process-wide default:**

- Call **`pydantable.engine.set_default_engine(your_engine)`** before code
  that uses **`get_default_engine()`**. If **`pydantable-native`** is not
  installed, **`get_default_engine()`** cannot fall back to
  **`NativePolarsEngine`**, so **`set_default_engine`** (or explicit
  **`engine=`** everywhere) is required.

**Global state:** the default engine and expression runtime (below) are process-wide;
document thread and testing implications for your users if they use multiple engines.

(custom-engine-expressions)=

## Expressions (`Expr`)

Expression trees used by **`filter`** / **`with_columns`** / etc. are built via
**`pydantable.engine.get_expression_runtime()`**, which defaults to the native
Rust core **only** when the default engine is **`NativePolarsEngine`**.

For a **non-native default**, either:

- call **`pydantable.engine.set_expression_runtime(lambda: ...)`** to supply an
  object compatible with how **`Expr`** is built in your stack, or
- steer users away from **`Expr`**-heavy APIs until a portable expression IR exists
  ({doc}`ADR-engines`, Track B).

Otherwise PydanTable raises **`UnsupportedEngineOperationError`** when building expressions.

(custom-engine-io)=

## File I/O vs execution engine

Many **lazy `read_*`** entry points (see {doc}`IO_OVERVIEW`) can use **pydantable-native** for
fast local scans. That path is **separate** from **`DataFrame._engine`**: shipping
a custom **`ExecutionEngine`** does not automatically redirect Parquet/CSV reads
through your backend. If your product needs “everything goes to SQL”, you
typically expose your own ingestion APIs and **then** construct
**`DataFrame`** instances already bound to your **`engine=`**.

(custom-engine-errors)=

## Errors

- **`pydantable_protocol.UnsupportedEngineOperationError`**: raise for unsupported
  **`plan_*`**, **`execute_*`**, or sinks. **`pydantable.errors.UnsupportedEngineOperationError`**
  subclasses it, so ``isinstance(exc, pydantable_protocol.UnsupportedEngineOperationError)``
  catches both library and third-party raises.

- **`pydantable_protocol.MissingRustExtensionError`**: reserved for missing or
  incomplete **native** extension scenarios; custom engines normally do not raise it.

(custom-engine-testing)=

## Testing checklist

1. **Unit tests** — Engine methods with fake plan/root handles.
2. **Integration tests** — `pip install` **`pydantable`** + your package; build a
   small **`DataFrameModel`**, run **`collect`** / **`to_dict`**, **`select`**, etc.
3. **Protocol drift** — Periodically assert your class satisfies all
   **`ExecutionEngine`** members (mirror **`tests/test_engine_contract.py`** in
   this repo).
4. **Version matrix** — CI over the **`pydantable`** / **`pydantable-protocol`**
   versions you claim to support.

(custom-engine-publishing)=

## Publishing

- **One PyPI project per engine** — e.g. **`your-org-pydantable-foo`**.
- **Pin** **`pydantable-protocol`** to the minor line you test against; relax or
  tighten as upstream releases new protocol methods.
- **Document** whether **`pydantable-native`** is optional, recommended, or
  unsupported for your integration.
- **Read-only note:** this repo’s **`scripts/check_engine_bypass.py`** applies to
  **`python/pydantable/`** only; your package is not bound by that allowlist, but
  avoiding direct imports of **`pydantable_native._core`** in **pydantable**
  itself keeps alternative engines viable.

## See also

- {doc}`MOLTRES_SQL` — **`SqlDataFrame`** / **`SqlDataFrameModel`** with **moltres-core** (**`pydantable[moltres]`**).
- **`entei-core`** (monorepo: `entei-core/`) — **`EnteiPydantableEngine`**, **`MongoRoot`**, **`EnteiDataFrame`** for PyMongo (**`pydantable[entei]`** optional extra).
- {doc}`ADR-engines` — architecture decisions and extension checklist.
- {doc}`DEVELOPER` — repository layout and native packaging.
- {doc}`EXECUTION` — how materialization uses the engine.
- {doc}`INTERFACE_CONTRACT` — behavioural guarantees users may expect.
- {doc}`VERSIONING` — aligning **`pydantable`**, **`pydantable-protocol`**, and native versions.
