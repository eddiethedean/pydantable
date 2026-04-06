"""Selector facade (removed).

Selector-driven schema mutation is not compatible with the strict typed dataframe spec.
See `docs/MIGRATION_1_to_2.md` for strict equivalents.
"""

from __future__ import annotations

raise ImportError(
    "pydantable.selectors is removed in pydantable 2.0 strict mode. "
    "Use explicit ColumnRef tokens (df.col.<field>) and explicit *_as output schemas. "
    "See docs/MIGRATION_1_to_2.md."
)
