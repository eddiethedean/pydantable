"""Map ingest ``source`` paths to published MkDocs URLs on Read the Docs."""

from __future__ import annotations

RTD_DEFAULT_BASE = "https://pydantable.readthedocs.io/en/latest"


def _docs_relative_to_page(rel: str) -> str | None:
    """
    Turn ``docs/…`` file path (without ``docs/`` prefix) into a MkDocs page slug.

    Returns ``""`` for the site root, a path like ``getting-started/quickstart`` for
    inner pages, or ``None`` if the file type is not mapped to an HTML page.
    """
    if not rel or rel in (".",):
        return None
    lower = rel.lower()
    if lower.endswith(".md"):
        page = rel[:-3]
    elif lower.endswith(".rst"):
        page = rel[:-4]
    else:
        return None

    if page == "index":
        return ""
    if page.endswith("/index"):
        return page[: -len("/index")]
    return page


def source_to_readthedocs_url(
    source: str,
    *,
    base: str = RTD_DEFAULT_BASE,
) -> str | None:
    """
    Map an ingest-relative path to the pydantable docs site (MkDocs on RTD).

    Expects paths like ``README.md`` or ``docs/user-guide/execution.md`` as stored
    by :mod:`app.rag.ingest` (relative to the library repo root).
    """
    s = source.replace("\\", "/").strip()
    if not s or "/_build/" in s:
        return None

    base_u = base.rstrip("/")

    if s == "README.md":
        return f"{base_u}/"

    if not s.startswith("docs/"):
        return None

    rel = s[len("docs/") :]
    page = _docs_relative_to_page(rel)
    if page is None:
        return None
    if page == "":
        return f"{base_u}/"
    return f"{base_u}/{page}/"
