from __future__ import annotations

import os
from typing import NamedTuple

class ReprHtmlLimits(NamedTuple):
    max_rows: int
    max_cols: int
    max_cell_len: int

def get_repr_html_limits() -> ReprHtmlLimits:
    ...

def reset_display_options() -> None:
    ...

def set_display_options(*, max_rows: int | None=None, max_cols: int | None=None, max_cell_len: int | None=None) -> None:
    ...

__all__ = ['ReprHtmlLimits', 'get_repr_html_limits', 'reset_display_options', 'set_display_options']
