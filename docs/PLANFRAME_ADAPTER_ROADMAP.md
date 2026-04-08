# Roadmap: pydantable as a PlanFrame adapter

This document describes the transition for **pydantable** to behave as a “real engine adapter” for **PlanFrame** (typed planning layer) in the sense described by PlanFrame’s adapter guide.

## North star

- **PlanFrame is the single planning surface** for the `DataFrameModel`-first API.
- **pydantable is the backend engine + adapter**: it compiles PlanFrame expression IR into pydantable expressions, lowers PlanFrame plan nodes into pydantable lazy operations, and materializes results only at execution boundaries.
- **No silent legacy path** inside `DataFrameModel`: if something can’t be expressed/executed via PlanFrame + adapter, it must be an explicit and documented escape hatch (or an explicit error).

## Principles and constraints

- **Always-lazy**: adapter methods must return backend-lazy frames; no backend work should execute during transform chaining.
- **Schema-first determinism**: schema evolution must be computed from PlanFrame schema metadata; execution-time hints must not affect schema.
- **Options at execution boundaries**: streaming / engine streaming / join execution hints belong in PlanFrame `ExecutionOptions` / `JoinOptions` and must be passed to adapter materializers (and join where supported).
- **Typed-by-default**: keep PlanFrame’s “literal column names” ethos at the PlanFrame boundary; keep pydantable’s “annotation-defined schema” ethos at the `DataFrameModel` boundary.
- **Tested parity**: every PlanFrame node / expr we claim to support must be exercised end-to-end in tests using the pydantable adapter.

## Current state (baseline)

- `DataFrameModel` holds a PlanFrame frame (`_pf`) and executes via `pydantable.planframe_adapter.PydantableAdapter`.
- PlanFrame `ExecutionOptions` is available (PlanFrame ≥ 0.6.0).
- Most `DataFrameModel` transforms are PlanFrame-backed; remaining gaps are primarily **expression lowering coverage** in `python/pydantable/planframe_adapter/expr.py`.

## Phase 0: “Adapter correctness” hardening

**Goal**: make adapter behavior unambiguous, deterministic, and PlanFrame-aligned.

- **Ensure `BaseAdapter` signatures match PlanFrame**
  - Adapter `collect`/`to_dicts`/`to_dict` accept `options: ExecutionOptions | None`.
  - Join forwards `JoinOptions` hints where the pydantable engine can honor them.
- **Execution boundaries are PlanFrame boundaries**
  - For PlanFrame-backed plans, prefer PlanFrame materializers (`Frame.collect/to_dict/to_dicts`) to route options consistently.
  - Ensure adapter `collect` does not accidentally force extra computation beyond what the backend needs.

**Acceptance criteria**

- `ruff` + `ty-check-minimal` + `pytest -n 10` pass.
- No transform in `DataFrameModel` performs backend work while building the plan.

## Phase 1: Complete PlanFrame expression lowering for the pydantable backend

**Goal**: “If PlanFrame can express it, pydantable can execute it” (within pydantable engine capability).

### 1.1 Implement missing PlanFrame `expr.api` nodes

As of PlanFrame 0.6.0, the remaining missing nodes in `python/pydantable/planframe_adapter/expr.py` are:

- **Strings**: `StrLower`, `StrUpper`, `StrLen`, `StrStrip`, `StrReplace`, `StrSplit`
- **Datetime extraction**: `DtYear`, `DtMonth`, `DtDay`
- **Numeric/math**: `Sqrt`, `IsFinite`
- **Windows**: `Over`
- **Aggregations**: `AggExpr`

Track this work in pydantable issues:

- pydantable #2 (strings)
- pydantable #3 (datetime)
- pydantable #4 (math)
- pydantable #5 (windows)
- pydantable #6 (AggExpr)

### 1.2 Add end-to-end tests for each newly supported node

- Prefer tests that construct a `DataFrameModel`, build a PlanFrame-backed plan using each node, execute it, and assert results.
- Include “nullability” cases where relevant (e.g. `StrLen` on optional strings, date parts on optional timestamps).

**Acceptance criteria**

- No `NotImplementedError("Unsupported PlanFrame expression node: ...")` for PlanFrame’s published `expr.api` surface we claim to support.
- Each supported PlanFrame node has a dedicated test (or is covered by an integration test).

## Phase 2: Make `DataFrameModel` a thin typed wrapper over PlanFrame materialization

**Goal**: PlanFrame becomes the authoritative execution surface for the typed API.

### 2.1 Route materialization through PlanFrame when a PlanFrame plan exists

Where appropriate, prefer:

- `_pf.collect(options=ExecutionOptions(...))`
- `_pf.to_dict(options=...)`
- `_pf.to_dicts(options=...)`

…and keep direct `_df` materialization only for APIs explicitly defined as “core engine only”.

### 2.2 Document and stabilize the escape hatch

Define and document a single supported escape hatch for users who need engine-only behaviors:

- `DataFrameModel.to_dataframe()` or `DataFrameModel.inner_frame()` returning the core pydantable `DataFrame`

This makes “not supported in PlanFrame-first surface” a deliberate, documented choice rather than an accidental fallback.

**Acceptance criteria**

- `DataFrameModel` execution-time options map cleanly to PlanFrame `ExecutionOptions`.
- The “engine-only” escape hatch is documented and covered by a small test.

## Phase 3: Expand PlanFrame-first surface to match “DataFrameModel expectations”

**Goal**: minimize the need for the escape hatch for common workflows.

### 3.1 Parity areas (evaluate and prioritize)

- **Projection with expressions**: PlanFrame already has `project`; expose a typed `DataFrameModel`-level API that stays PlanFrame-first.
- **Expr keys in sort/join/group_by**: PlanFrame supports them; pydantable adapter should support them to the degree the engine can.
- **Reshape ergonomics**: richer `melt`/`pivot` options while keeping selector-free, schema-first semantics.

**Acceptance criteria**

- The “PlanFrame-first core API” documented surface covers the majority of workflows without escape hatches.

## Phase 4: Deprecation and cleanup of legacy paths

**Goal**: eliminate dead code and clarify invariants.

- Remove preserved “old backend code after `raise`” once a method has been PlanFrame-backed for at least one release cycle.
- Delete docs that imply `_df` fallback behavior for `DataFrameModel` when it no longer exists.
- Add CI gates ensuring no new `_df`-only transforms are introduced into `DataFrameModel`.

**Acceptance criteria**

- `DataFrameModel` is PlanFrame-first by construction; any engine-only behavior is explicit and documented.

## Definition of done

The transition is “done” when:

- `DataFrameModel` is effectively a typed PlanFrame façade (composition) over the pydantable engine adapter.
- Adapter coverage is complete for the PlanFrame surface pydantable claims to support.
- Execution-time hints are supported via PlanFrame’s options objects (and forwarded in the adapter).
- Any remaining engine-only API is accessed through a single, documented escape hatch.

