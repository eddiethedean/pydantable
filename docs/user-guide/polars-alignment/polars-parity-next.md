# Polars parity roadmap (next)

This page lays out a roadmap for the **highest-signal remaining Polars parity gaps** after the 1.8.0 parity work (selectors + ordering flags + pivot args).

It complements:

- Current-state tracking: [PARITY_SCORECARD](../../user-guide/polars-alignment/parity-scorecard.md)
- 1.8.0 roadmap history: [POLARS_PARITY_1_8](../../user-guide/polars-alignment/polars-parity-1-8.md)
- Behavior contract: [INTERFACE_CONTRACT](../../semantics/interface-contract.md)

## Principles

- **Schema-first always**: parity work must remain compatible with Pydantic-first schemas and typed contracts.
- **Contract-first**: every parity change lands with tests for success and error paths.
- **Document constraints explicitly**: prefer a clear `NotImplementedError` (with a fix/workaround) over silent partial behavior.

## Highest-signal gaps (current)

1. **Join validation on scan roots**: `join(validate=...)` works for in-memory roots but is **not supported** on scan roots.
2. **Join coalesce semantics**: `join(coalesce=...)` is accepted but only partially modeled; behavior is effectively “already coalesced” for same-named keys.
3. **Selector DSL breadth**: we now have a schema-first selector DSL, but not full Polars selector parity across all selector kinds / all APIs.
4. **Long-tail “popular” DataFrame methods**: additional high-usage Polars methods that map cleanly to schema-first typing (beyond the 1.8.0 list).

## Phase 1 — Join `validate=...` on scan roots (highest impact)

### Goal
Support `join(validate=...)` for scan roots **without forcing Python materialization**.

### Implementation approach options

- **Option A (engine-assisted check, preferred)**: implement the cardinality checks inside the Rust/Polars execution path.\n  - Compute uniqueness of join keys on each side using Polars lazy operations.\n  - Fail fast with precise error messages.\n  - Keep it streaming-aware when possible.
- **Option B (explicit materialization)**: expose a documented helper that materializes only the join keys needed for validation.\n  - Still a materialization cost, but bounded.\n  - Keeps Python surface simple; engine work is smaller.

### Deliverables
- Extend join execution to accept `validate` and enforce:\n  - `one_to_one`, `one_to_many`, `many_to_one`, `many_to_many`\n  - also accept `1:1`, `1:m`, `m:1`, `m:m`
- Contract tests:\n  - scan-root join validation succeeds/fails correctly\n  - error messages include which side violates uniqueness\n  - coverage for multi-key joins\n- Docs:\n  - update [INTERFACE_CONTRACT](../../semantics/interface-contract.md) join validation section with scan-root support + cost model\n  - update [PARITY_SCORECARD](../../user-guide/polars-alignment/parity-scorecard.md) status from Partial → Implemented (or “Partial: scan-root supported for …” if any constraints remain)\n+
### Acceptance criteria
- `join(validate=...)` behaves the same for in-memory and scan roots (modulo documented performance/streaming constraints).

## Phase 2 — `join(coalesce=...)` semantics parity

### Goal
Bring `coalesce=` behavior in line with Polars expectations (within schema-first constraints).

### Deliverables
- Define and implement explicit semantics for:\n  - same-named key joins vs `left_on`/`right_on`\n  - output key columns when inputs differ\n- Add tests that pin:\n  - output columns present/absent\n  - nullability rules\n  - name collision behavior\n- Docs:\n  - clarify coalesce semantics in [INTERFACE_CONTRACT](../../semantics/interface-contract.md)\n  - update [PARITY_SCORECARD](../../user-guide/polars-alignment/parity-scorecard.md)\n+
## Phase 3 — Selector DSL expansion (targeted, schema-first)

### Goal
Expand Selector DSL to cover the *most-used* selector patterns from Polars docs while remaining schema-driven.

### Candidate additions (prioritized)
- **More dtype groups** (only where representable in schema annotations): e.g. “integer”, “float”, “decimal”, “uuid”, “list”, “struct”.\n  (Avoid pretending to support dtypes that aren’t representable in `Schema`.)
- **API coverage**: selectors in more places where Polars users expect them:\n  - `rename` mapping helpers (e.g. rename all columns matching a selector)\n  - `with_columns` conveniences (selector-driven passthrough is likely a non-goal; consider selector-driven `cast`/`fill_null` patterns only if they stay typed)\n- **Better diagnostics**: error messages that include the available schema columns and/or the selector expression summary.

### Deliverables
- Extend `pydantable.selectors` surface with new selectors and tests.\n- Update [SELECTORS](../../user-guide/selectors.md) with the expanded catalog.\n+
## Phase 4 — Long-tail popular DataFrame methods (schema-first shortlist)

### Goal
Implement additional high-usage Polars DataFrame methods and arguments that map cleanly to typed schemas.

### Process (repeatable)
- Choose a shortlist (10–20 methods) from Polars tutorials/recipes.\n- For each method:\n  - define exact typed contract + out-of-scope cases\n  - decide plan-step vs eager helper\n  - add success + error-path tests\n  - add workflow examples\n+
### Candidate areas
- **Reshape**: additional melt/unpivot/pivot conveniences (naming options, column selection ergonomics).\n- **Join**: more join argument parity beyond validate/coalesce (where it doesn’t break typing).\n- **Core ergonomics**: small helpers that reduce verbosity in schema-first workflows.\n+
## Tracking

- Update [PARITY_SCORECARD](../../user-guide/polars-alignment/parity-scorecard.md) for every delivered item.\n- Add a “shipped in X.Y.Z” note to the relevant changelog section when released.\n+
