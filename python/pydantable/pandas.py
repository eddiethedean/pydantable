"""Pandas UI facade (removed).

The pandas-style facade was removed for strict pydantable 2.0 compliance.
See `docs/MIGRATION_1_to_2.md` for strict equivalents.
"""

from __future__ import annotations

raise ImportError(
    "pydantable.pandas is removed in pydantable 2.0 strict mode. "
    "Use the core API (DataFrameModel/DataFrame[Schema]) with df.col.<field> and "
    "*_as(AfterModel/AfterSchema, ...) for schema evolution. "
    "See docs/MIGRATION_1_to_2.md."
)
