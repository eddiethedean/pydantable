# Roadmap: Leverage PlanFrame’s static typing system

This document is the **typing counterpart** to {doc}`PLANFRAME_ADAPTER_ROADMAP`. Runtime adapter work makes `DataFrameModel` PlanFrame-first for execution; **this roadmap** makes **PlanFrame’s type system** the **primary** way to get precise static types for **lazy transform chains**, instead of duplicating that work in **pydantable’s mypy plugin** and hand-maintained parallel stubs.

## Historical note (posterity)

Pydantable’s **first attempts** at static typing—schema-evolving **`DataFrameModel`**, a [**mypy plugin**](https://github.com/eddiethedean/pydantable/blob/main/python/pydantable/mypy_plugin.py) for chain return types, and **shipped `.pyi` stubs** for checkers that cannot load that plugin—showed both what users wanted (lazy plans with column-level types through transforms) and where Python’s ecosystem **fell short** without a dedicated planning layer and a first-class **Resolve** story.

**PlanFrame was heavily inspired by those pydantable experiments** and was built to supply the **static typing system** that work needed: **`Frame`** / **`Expr[T]`**, **literal** column APIs, **generated stubs**, **`materialize_model`** at boundaries, and the tiered strategy described in PlanFrame’s [typing design](https://planframe.readthedocs.io/en/latest/planframe/design/typing-design/). This roadmap is the **closing of the loop**: pydantable becomes a **PlanFrame adapter** and **leans on** that system instead of **re-implementing** Resolve in the plugin forever.

**PlanFrame’s design** (see [PlanFrame — Resolve typing design for Pyright](https://planframe.readthedocs.io/en/latest/planframe/design/typing-design/) and the [planframe](https://github.com/eddiethedean/planframe) README) already invests in:

- **`Frame[PlanT, BackendT]`** and **`Expr[T]`**
- **Literal column names** at call sites
- **`Resolve`**: column-level types after `select` / `with_column` / `rename` / `group_by`+`agg`, …
- **Generated `.pyi` stubs** (e.g. `Frame`) and **`materialize_model(...)`** at explicit boundaries
- **Tiered strategy**: overloads + stubs + materialization first; optional Pyright plugin later

**North star:** users who care about **checker-accurate transform typing** should **lean on PlanFrame’s types** for the lazy plan; **pydantable** supplies the **engine adapter**, **Pydantic row models**, **I/O**, and **narrow boundaries** back into `DataFrameModel` where the product requires it. The [**mypy plugin**](https://github.com/eddiethedean/pydantable/blob/main/python/pydantable/mypy_plugin.py) becomes a **compatibility layer** for legacy `DataFrameModel`-only chains, not the long-term source of truth for schema evolution.

```{note}
This roadmap describes **intent and phases**. Dates and “done” lines will move as work lands; {doc}`PLANFRAME_ADAPTER_ROADMAP` remains the checklist for **runtime** adapter completeness.
```

## Relationship to runtime adapter work

| Concern | {doc}`PLANFRAME_ADAPTER_ROADMAP` | This document |
|---------|----------------------------------|---------------|
| **Execution** | `PydantableAdapter`, `execute_plan`, `expr.py` lowering | — |
| **Planning surface** | `_pf`, sync `_df`, escape hatches | **Expose `Frame` to type checkers** |
| **Static types** | “Typed-by-default” principle | **Resolve, stubs, boundaries, plugin scope** |

```text
DataFrameModel ──runtime (_pf)──> PlanFrame Frame ──> PydantableAdapter / engine
DataFrameModel ──typing goal──> expose Frame API so checkers use PlanFrame stubs
```

## Current state vs target

| Aspect | Today (summary) | Target |
|--------|-----------------|--------|
| **Transform typing on `DataFrameModel`** | [**mypy plugin**](https://github.com/eddiethedean/pydantable/blob/main/python/pydantable/mypy_plugin.py) refines returns from class fields + literals; **Pyright / `ty`**: loose stubs + `as_model` | **PlanFrame `Frame` chain** carries **Resolve**-style types via upstream stubs; `DataFrameModel` methods documented as ergonomic or thin wrappers |
| **Source of truth for chain types** | Split: plugin logic vs `.pyi` | **PlanFrame** stubs + public `Frame` type path |
| **Boundaries** | `as_model` / `try_as_model` after transforms | **`materialize_model`** (PlanFrame) ↔ **Pydantic `RowModel` / `DataFrameModel`** at documented hand-off points |
| **Duplication** | Plugin re-implements schema evolution rules for mypy | **Minimize**: delegate semantic rules to PlanFrame’s model where possible |

## Principles

1. **Don’t re-implement Resolve in pydantable** unless a checker-agnostic bridge is unavoidable; prefer **exposing** `planframe.frame.Frame` (and documented generic parameters) so **PlanFrame’s overloads and stubs** apply.
2. **Literal column names** for typed paths: align public docs with PlanFrame’s [recommended constraints](https://planframe.readthedocs.io/en/latest/planframe/design/typing-design/#8-recommended-public-typing-constraints) (`Literal[...]`, no dynamic column names in the “typed” API).
3. **Pydantic stays pydantable’s**: row validation, `RowModel`, FastAPI — **not** replaced by PlanFrame’s `materialize_model` alone; the roadmap defines **bridges**, not a second row layer.
4. **Backward compatibility**: shrinking the mypy plugin is **gradual**; major changes follow {doc}`VERSIONING`.

## Non-goals (for this roadmap)

- **Replacing** pydantable `Expr` / Rust lowering with user-facing PlanFrame-only APIs everywhere overnight (runtime {doc}`PLANFRAME_ADAPTER_ROADMAP` still governs what executes).
- **Guaranteeing** identical inference for **every** `DataFrameModel` method without a PlanFrame equivalent—some methods remain **engine / Pydantic**-only with explicit types.
- **Shipping a pydantable Pyright plugin** as Phase 1 (optional long-term tier per PlanFrame’s doc).

---

## Phase T0 — Foundations and inventory

**Goal:** measure gaps and lock terminology before API changes.

**Work**

- **Inventory** every `DataFrameModel` method that mutates `_pf` and map it to PlanFrame `Frame` API (already partly in {doc}`PLANFRAME_FALLBACKS`).
- **Checker matrix**: document expected behavior for **mypy + plugin**, **mypy without plugin**, **Pyright**, **`ty`** when using (a) only `DataFrameModel` chains, (b) `Frame` exposure (once shipped), (c) `materialize_model` bridge.
- **Pin / track** PlanFrame typing artifacts: follow [planframe `generate_typing_stubs.py`](https://github.com/eddiethedean/planframe) churn; note breaking stub changes in changelog.

**Acceptance criteria**

- A single table in {doc}`TYPING` (or this doc) lists **recommended** typing path per checker.
- No new public typing promises until Phase T1 API is drafted.

### Inventory (T0): `DataFrameModel` → PlanFrame `Frame`

This table is a **typing-oriented index** of the PlanFrame-first surface. Runtime behavior details and explicit gaps live in {doc}`PLANFRAME_FALLBACKS`.

| `DataFrameModel` API | PlanFrame API / node | Notes (typing constraints / gotchas) |
|---|---|---|
| `select(*cols)` | `Frame.select(*cols)` | `DataFrameModel` requires at least one `str`; PlanFrame stubs encourage `Literal` column names. |
| `select_schema(selector)` | `Frame.select_schema(ColumnSelector)` | Accepts either pydantable `Selector.resolve(...)` or PlanFrame selector (`selector.select(schema)`). |
| `with_columns(**new_columns)` | `Frame.with_columns(...)` | Runtime supports dynamic output names; PlanFrame typing is strongest when names are literals. |
| `with_columns_cast(selector, ...)` | `cast_subset` / `cast_many` | Selector is pydantable or PlanFrame; mapping path uses “many” form. |
| `with_columns_fill_null(selector, ...)` | `fill_null_subset` / `fill_null_many` | Requires `value=` or `strategy=`; selector/mapping mirrors cast. |
| `filter(condition)` | `Frame.filter(expr)` | Condition may be pydantable `Expr` or PlanFrame expr lowered by the adapter. |
| `sort(*keys, ...)` | `Frame.sort(...)` | Keys allow `str` or limited PlanFrame expr shapes; see {doc}`PLANFRAME_FALLBACKS`. |
| `drop(*columns, strict=...)` | `Frame.drop(...)` | `DataFrameModel.drop()` is a no-op if no columns; PlanFrame still typed for literals. |
| `rename(mapping, strict=...)` | `Frame.rename(mapping=..., strict=...)` | Keyword rename helpers are PlanFrame-backed; mapping keys are `str`. |
| `rename_upper/lower/title/strip(...)` | PlanFrame rename helpers | Optional selector (pydantable or PlanFrame). |
| `unique(...)` / `distinct(...)` | `Frame.unique(...)` | `subset=None` vs `subset=[...]` paths; subset must be `Sequence[str]`. |
| `slice(...)`, `head(...)`, `tail(...)` | `Frame.slice/head/tail` | Schema-preserving. |
| `with_row_count(name, offset)` | `Frame.with_row_index(...)` | User API name differs; PlanFrame uses `with_row_index`. |
| `clip(lower, upper, subset=...)` | `Frame.clip(...)` | `subset=None` means “all numeric columns” in PlanFrame semantics. |
| `fill_null(...)` | `Frame.fill_null(value/strategy)` | `value` may be literal or expression; strategy-based fill supported. |
| `drop_nulls(...)` | `Frame.drop_nulls(how/threshold/subset)` | Selector support is via PlanFrame/pydantable selector normalization. |
| `melt(...)` | PlanFrame `unpivot` family | pydantable keeps `melt` name; narrowed to `str` column names. |
| `unpivot(...)` | `Frame.unpivot(...)` | Same reshape family; narrowed args for PlanFrame typing ethos. |
| `pivot(...)` | `Frame.pivot(...)` | Narrowed to `str` names; `streaming=` rejected on `DataFrameModel`. |
| `pivot_longer(...)` / `pivot_wider(...)` | `Frame.pivot_longer` / `pivot_wider` | Narrowed params (e.g. `pivot_wider` requires string `names_from`). |
| `explode(*cols, outer=...)` / `explode_all(...)` | `Frame.explode(...)` | `explode_all` expands to schema fields; cols are `str` only. |
| `unnest(*cols)` / `unnest_all(...)` | `Frame.unnest(...)` | `unnest_all` expands to schema fields; cols are `str` only. |
| `join(...)` | `Frame.join(..., options=JoinOptions)` | Keys can be `str` or limited PlanFrame expr keys; parallel flags are explicit errors. |
| `group_by(*keys)` | `Frame.group_by(...)` → grouped wrapper | Returns `GroupedDataFrameModel` holding grouped PlanFrame frame; `.agg(...)` executes PlanFrame `Agg`. |
| `group_by_dynamic(...).agg(...)` | `Frame.group_by_dynamic` → dynamic grouped wrapper | Dynamic grouped object’s `.agg(...)` is PlanFrame-backed. |
| `rolling_agg(...)` | `Frame.rolling_agg(...)` | PlanFrame-backed node; output name is a string param. |
| `GroupedDataFrameModel.agg(...)` | `GroupedFrame.agg(...)` | Aggregations accept PlanFrame `AggExpr` shapes; adapter lowers supported ops. |
| `concat(*dfs, how=...)` | `Frame.concat(how=...)` | Horizontal/vertical constraints apply; see {doc}`PLANFRAME_FALLBACKS`. |

### Upstream tracking (T0): PlanFrame typing artifacts

- **Authoritative upstream check:** PlanFrame’s repo describes and enforces stub regeneration with `scripts/generate_typing_stubs.py --check` (see PlanFrame README).\n- **What to watch when bumping PlanFrame:**\n  - Public `Frame` / `Expr` **generic parameters**, method overloads, and selector/protocol types referenced by adapters.\n  - Any signature changes that alter “Resolve” behavior for Pyright (overload set changes can be typing-breaking without runtime breakage).\n- **pydantable bump checklist (typing):** when changing the PlanFrame pin, skim the upstream stub diff (or changelog notes), then run `make check-typing` and `make sphinx-check` in pydantable.\n+
---

## Phase T1 — First-class PlanFrame-typed surface

**Goal:** any static type checker can follow **PlanFrame’s** `Frame` / `Expr` types for the lazy plan **without** going through pydantable-only overloads.

**Work**

1. **Stable accessor** from `DataFrameModel` to the underlying PlanFrame frame, e.g.:
   - `DataFrameModel.planframe` → `planframe.frame.Frame[...]` (exact generic args aligned with PlanFrame 1.x stubs), **or**
   - `DataFrameModel.to_planframe()` → same, if a method is preferred for discoverability.

   Design constraints:

   - Must be **lazy** (no execution).
   - Must match **`_pf`** identity semantics (same plan as internal adapter uses).
   - Document **thread-safety / mutation** rules (read-only view vs copy).

2. **Optional inverse** (later in T1): construct or wrap `DataFrameModel` **from** a `Frame` that was built with PlanFrame APIs **and** a known Pydantic schema (spike: validation + `_pf` assignment).

3. **Naming** in docs: “**Typing-first path**: `model.planframe.select(...)` …” vs “**Pydantic-first path**: `model.select(...)` …”.

### T1 priorities (derived from T0 inventory)

Start with the “highest leverage” PlanFrame-backed methods—these dominate real-world chains and benefit most from PlanFrame’s Resolve / stub typing:

- **Core chain:** `select`, `select_schema`, `with_columns`, `filter`, `sort`, `drop`, `rename`
- **Join + grouping:** `join`, `group_by(...).agg(...)`, `group_by_dynamic(...).agg(...)`, `rolling_agg`
- **Reshape:** `melt`/`unpivot`, `pivot`, `pivot_longer`/`pivot_wider`

Then broaden to the schema-preserving convenience nodes (`head`, `tail`, `slice`, `unique`, `distinct`, `clip`, `fill_null`, `drop_nulls`) and “expand-to-schema-fields” helpers (`explode_all`, `unnest_all`).

**Acceptance criteria**

- Pyright / `ty` tests (or contract tests under `tests/typing/`) that assign the result of `planframe` to a variable annotated with PlanFrame’s public `Frame` type (or a concrete backend frame type if you document that subset).
- {doc}`DATAFRAMEMODEL` and {doc}`TYPING` updated with the recommended pattern.
- A small `tests/typing/` contract snippet shows the canonical workflow compiles under **Pyright** and **`ty`**:\n  `df.planframe.select(\"id\").with_column(\"x\", ...)` (literal names) plus a “bridge back” step (T3) using pydantable APIs.

**Status:** implemented as `DataFrameModel.planframe` (public, lazy) in pydantable; follow-on work is the T3 “wrap back” bridge.

---

## Phase T2 — Stub and annotation alignment

**Goal:** pydantable’s shipped `.pyi` files and `TYPE_CHECKING` blocks **re-export or reference** PlanFrame types at boundaries instead of inventing parallel types.

**Work**

- **`dataframe_model.pyi`**: `planframe` property / method return types use upstream symbols (`from planframe.frame import Frame` in stubs).
- **Join / group / sort** parameters already accepting `planframe.expr.api` shapes: ensure `.pyi` matches **actual** runtime acceptance (see `python/pydantable/dataframe_model.py`).
- **`scripts/generate_typing_artifacts.py`**: extend templates so regenerated stubs stay aligned with Phase T1 accessors (run `--check` in CI).
- **Reduce** redundant overloads on `DataFrameModel` that duplicate PlanFrame once T1 is canonical for “full inference”.

**Status (implemented)**

- `python/pydantable/dataframe_model.pyi` now **imports and references** PlanFrame boundary types directly:
  - `planframe.expr.api` (`Expr` for `sort`/`join`, `Col` for `group_by`)
  - `planframe.selector.ColumnSelector` (for `select_schema` / selector-accepting helpers)
  - pydantable `Selector` is typed explicitly as an alternate selector protocol.
- Join parallel flags remain present but are typed as **`Literal[None]`** in stubs to reflect that runtime raises on non-`None`.

### T2 priorities (derived from T0 inventory)

Once `DataFrameModel.planframe` (or `to_planframe()`) exists, ensure stubs align for the inventory’s “PlanFrame-first core API”:\n

- **Keys that accept PlanFrame exprs:** `sort`, `join`, `group_by` (only `str` / `pf.col(\"x\")` today), and grouped `.agg(...)`.\n- **Selector dual-protocol:** `select_schema`, `rename_*`, `with_columns_cast`, `with_columns_fill_null` accept either a pydantable selector (`resolve`) or a PlanFrame selector (`select`).\n- **Reshape narrowings:** `melt`/`unpivot`/`pivot`/`pivot_wider` constraints must match stubs so checkers don’t accept shapes that runtime rejects.\n

**Acceptance criteria**

- `make check-typing` / generator `--check` passes.
- No duplicate incompatible `Frame` aliases in `typings/` vs `python/pydantable/`.

---

## Phase T3 — Materialization boundaries and Pydantic bridge

**Goal:** where PlanFrame offers **`materialize_model(...)`**, pydantable documents **how** that connects to **`DataFrameModel`**, **`RowModel`**, and **`from pydantable import DataFrameModel` constructors**.

**Work**

- **Spike:** after `pf_out = model.planframe....`, call PlanFrame’s `materialize_model` / collect patterns, then wrap columns into `DataFrameModel` (or validate into `RowModel` list) with **one** supported recipe.
- **Compare** with existing `as_model` / `collect()` typing story; prefer **one** golden path for “exact schema at boundary” in docs.
- **Decide** whether codegen or runtime-only bridge is in scope (PlanFrame doc: exact types at boundary via materialization + stubs/codegen).

**Acceptance criteria**

- Cookbook or {doc}`TYPING` section: end-to-end example **typed** with Pyright using **PlanFrame chain + pydantable boundary**.
- Tests for runtime correctness of the bridge (not only static).

---

## Phase T4 — Mypy plugin: shrink scope, document deprecation path

**Goal:** treat **PlanFrame-exposed chains** as sufficient for **schema-evolving** static analysis; narrow the plugin to what **cannot** be expressed via PlanFrame types.

**Work**

- **Catalog** plugin hooks (`_HOOK_NAMES` and grouped hooks) vs T1/T2 coverage; mark hooks **candidates for removal** when `planframe` path is documented and tested.
- **Policy:** new schema-evolving methods should get **PlanFrame parity** in `_pf` **first**, then plugin support only if still needed for `DataFrameModel`-only ergonomics.
- **Long-term:** optional **deprecation** of individual plugin hooks with {doc}`VERSIONING` timeline (major/minor per policy).

**Acceptance criteria**

- Plugin code size / hook count trends **down** or stays flat with justification in this doc’s changelog section.
- {doc}`TYPING` table updated: **primary** strategy = PlanFrame path for Pyright/`ty`/mypy; plugin = **legacy / convenience**.

---

## Phase T5 — Documentation and education

**Goal:** “adapter” means **typing adapter** as much as **execution adapter**.

**Work**

- **Landing sentence** in README / {doc}`index`: PlanFrame typing is a **first-class** reason to use pydantable.
- Cross-links: PlanFrame [Typing design](https://planframe.readthedocs.io/en/latest/planframe/design/typing-design/), [planframe.readthedocs.io](https://planframe.readthedocs.io/en/latest/).
- **Migrate** examples: prefer `planframe` property in **typing**-focused snippets; keep `DataFrameModel` chains for **beginner** snippets.

**Acceptance criteria**

- {doc}`DOCS_MAP` lists this roadmap; {doc}`PLANFRAME_ADAPTER_ROADMAP` links here in **See also**.

---

## Phase T6 — Optional advanced tiers (future)

Aligned with PlanFrame’s [recommended resolution tiers](https://planframe.readthedocs.io/en/latest/planframe/design/typing-design/#11-recommended-resolution-tiers):

| Tier | Item | Notes |
|------|------|--------|
| **Stub codegen** | Generate `.pyi` for stable `DataFrameModel` subclasses in apps | Only if product need; overlaps PlanFrame’s own stub generation story |
| **Pyright plugin** | Plan-AST-aware Resolve for `DataFrameModel` without exposing `Frame` | Large investment; evaluate only if T1–T4 insufficient |
| **Upstream** | PlanFrame Pyright plugin | If upstream ships, reassess pydantable-specific work |

---

## Testing strategy

- **Static:** extend `tests/typing/` with **Pyright** and **`ty`** snippets mirroring PlanFrame’s expectations (literal columns, `Expr[T]` where applicable).
- **Runtime:** existing `tests/dataframe/test_planframe_adapter_*.py` remains source of truth for execution; typing phases add **no** behavior regressions.
- **Drift:** PlanFrame stub `--check` in upstream CI; pydantable CI already runs `generate_typing_artifacts.py --check` — keep both green when bumping PlanFrame.

## Risks

| Risk | Mitigation |
|------|------------|
| PlanFrame stub breaking changes | Pin ranges; changelog; Phase T0 inventory |
| Two mental models (`DataFrameModel` vs `Frame`) | Clear docs hierarchy; one “typing-first” quick path |
| Mypy users lose inference if plugin shrinks before T1 works | Gradual deprecation; {doc}`VERSIONING` |

## See also

- {doc}`PLANFRAME_ADAPTER_ROADMAP` — runtime adapter phases.
- {doc}`PLANFRAME_FALLBACKS` — PlanFrame-first vs escape hatch.
- {doc}`TYPING` — end-user checker strategies (will incorporate PlanFrame-first path).
- [PlanFrame — Resolve typing design for Pyright](https://planframe.readthedocs.io/en/latest/planframe/design/typing-design/)
- [PlanFrame repository](https://github.com/eddiethedean/planframe)
