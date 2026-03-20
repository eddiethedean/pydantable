# Pydantable Interface Contract (Polars-style)

This document records the behavior contract that the typed DataFrame API guarantees.
It is intended to be backend-agnostic at the *type/semantics* level, while allowing
backend-specific physical ordering.

## Ordering

`collect()` output order is **not a stable API guarantee**.

When tests or user assertions need deterministic comparisons, compare on the
subset of columns that define identity (for example, join keys) rather than
row position. The project test-suite uses sorted comparisons to enforce this.

## Join semantics (collision + keys)

### Join keys
- `join(on=...)` requires at least one join key.
- `on` must reference a column that exists on both sides.
- `join(left_on=..., right_on=...)` supports column names or single-column expressions.
- `cross` joins do not accept `on`/`left_on`/`right_on`.

### Collision handling
- Column name collisions introduced by the right-hand side are resolved by
  renaming right-side non-key columns with the provided `suffix` (default:
  `"_right"`).
- Left-hand columns keep their original names.

In other words: collisions on non-key columns become `"<name><suffix>"` for the
right side, while join key columns remain singletons.

### Supported join kinds
- `inner`, `left`, `full`, `right`, `semi`, `anti`, `cross`
- `semi`/`anti` return only left-side columns.

## Null semantics

Null handling is SQL-like (`propagate_nulls`):
- arithmetic: `NULL` + anything yields `NULL`
- comparisons: if either side is `NULL`, the comparison result is `NULL`
- `filter(condition)`: retains rows where the condition evaluates to exactly
  `True`, and drops rows where it is `False` or `NULL`

These rules are enforced in the Rust core so that derived schemas and runtime
values remain aligned.

## Group-by aggregation semantics (all-null groups)

Supported aggregation operators:
- `count`, `sum`, `mean`, `min`, `max`, `median`, `std`, `var`, `first`, `last`, `n_unique`

For groups where all input values for an aggregated column are `NULL`:
- `sum`, `mean`, `min`, `max`, `median`, `std`, `var`, `first`, and `last` yield `None`
- `count` yields `0`
- `n_unique` yields `0` (SQL-like distinct-count behavior over non-null values)

The output field nullability is preserved/derived accordingly:
- nullable aggregates above are typed as `Optional[...]`
- `count` and `n_unique` outputs remain non-optional integers

