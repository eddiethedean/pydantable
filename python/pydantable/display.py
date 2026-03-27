"""HTML preview limits for :meth:`~pydantable.dataframe.DataFrame._repr_html_`.

Environment variables (optional, read when no programmatic override is set):

* ``PYDANTABLE_REPR_HTML_MAX_ROWS`` (default ``20``)
* ``PYDANTABLE_REPR_HTML_MAX_COLS`` (default ``40``)
* ``PYDANTABLE_REPR_HTML_MAX_CELL_LEN`` (default ``500``)

Use :func:`set_display_options` / :func:`reset_display_options` in notebooks to
override without env vars. Thread safety is not guaranteed (typical notebook use).
"""

from __future__ import annotations

import os
from typing import NamedTuple

__all__ = [
    "ReprHtmlLimits",
    "get_repr_html_limits",
    "reset_display_options",
    "set_display_options",
]

_DEFAULT_ROWS = 20
_DEFAULT_COLS = 40
_DEFAULT_CELL = 500

_override: tuple[int, int, int] | None = None


class ReprHtmlLimits(NamedTuple):
    max_rows: int
    max_cols: int
    max_cell_len: int


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        v = int(raw)
        return v if v > 0 else default
    except ValueError:
        return default


def get_repr_html_limits() -> ReprHtmlLimits:
    """Effective limits for HTML table previews (see module docstring)."""
    if _override is not None:
        return ReprHtmlLimits(*_override)
    return ReprHtmlLimits(
        max_rows=_env_int("PYDANTABLE_REPR_HTML_MAX_ROWS", _DEFAULT_ROWS),
        max_cols=_env_int("PYDANTABLE_REPR_HTML_MAX_COLS", _DEFAULT_COLS),
        max_cell_len=_env_int("PYDANTABLE_REPR_HTML_MAX_CELL_LEN", _DEFAULT_CELL),
    )


def set_display_options(
    *,
    max_rows: int | None = None,
    max_cols: int | None = None,
    max_cell_len: int | None = None,
) -> None:
    """Set module-level overrides for HTML preview limits (positive integers only)."""
    cur = get_repr_html_limits()
    nr = cur.max_rows if max_rows is None else max_rows
    nc = cur.max_cols if max_cols is None else max_cols
    nl = cur.max_cell_len if max_cell_len is None else max_cell_len
    if nr <= 0 or nc <= 0 or nl <= 0:
        raise ValueError("Display limits must be positive integers.")
    global _override
    _override = (nr, nc, nl)


def reset_display_options() -> None:
    """Clear programmatic overrides; env vars and defaults apply again."""
    global _override
    _override = None
