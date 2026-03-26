# Docs style guide

This guide keeps the docs consistent, searchable, and testable.

## Terminology (use consistently)

- **DataFrameModel**: the primary SQLModel-like table class for services.
- **DataFrame[Schema]**: the generic typed frame API over a Pydantic model.
- **read_*** / **aread_***: **lazy** file roots returning `ScanFileRoot` (pipelines).
- **materialize_*** / **amaterialize_***: **eager** reads into `dict[str, list]`.
- **export_*** / **aexport_***: **eager** writes from `dict[str, list]`.
- **write_***: **lazy pipeline sinks** that execute in Rust without building a giant Python dict.
- **trusted_mode**: `off | shape_only | strict`.

Avoid mixing older terms like “read_* returns dict-of-lists” in new docs; that vocabulary was replaced.

## Snippet rules

- Every recipe/tutorial should be **copy/paste runnable**.
- Prefer **small deterministic assertions**.
- When row order is not guaranteed, sort by identity keys before comparing (see {doc}`INTERFACE_CONTRACT`).
- Keep snippets **offline** (no network). For HTTP examples, show structure but don’t require a live URL.

## Cross-linking

- When documenting behavior, link to {doc}`INTERFACE_CONTRACT`.
- When discussing performance/materialization, link to {doc}`EXECUTION`.
- When discussing I/O entrypoints, link to {doc}`IO_DECISION_TREE`.

## Versioning notes

- For 1.x, avoid language like “might change” on stable surfaces unless explicitly marked experimental.
- If a behavior is a hard guarantee, write it once in {doc}`INTERFACE_CONTRACT` and reference it elsewhere.

## PR checklist (docs)

- `sphinx-build -W -b html docs docs/_build/html` passes.
- If you added/changed a tutorial snippet, `PYTHONPATH=python .venv/bin/python scripts/verify_doc_examples.py` covers it.
- Terminology matches the style guide (especially I/O vocabulary).
