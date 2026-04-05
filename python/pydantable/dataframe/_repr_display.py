"""HTML table fragments and repr limits for :class:`~pydantable.dataframe.DataFrame`."""

from __future__ import annotations

import html
from typing import Any

# Cap column listing in :meth:`DataFrame.__repr__` for very wide schemas.
_REPR_MAX_COLUMNS = 32
_REPR_DTYPE_MAX_LEN = 72
# Default Jupyter / IPython preview limits; overridden by :mod:`pydantable.display`.
_REPR_HTML_MAX_ROWS = 20
_REPR_HTML_MAX_COLS = 40
_REPR_HTML_MAX_CELL_LEN = 500


def _html_cell_text(value: object, *, max_cell_len: int) -> str:
    """Format a single cell for HTML tables; output must be HTML-escaped."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, (int, float)):
        s = str(value)
    elif isinstance(value, str):
        s = value
    else:
        s = repr(value)
    if len(s) > max_cell_len:
        return f"{s[: max_cell_len - 1]}…"
    return s


def _dataframe_to_html_fragment(
    *,
    # Per-cell values are heterogeneous scalars (see policy in docs/TYPING.md).
    column_dict: dict[str, list[Any]],
    column_order: list[str],
    caption: str | None = None,
    note: str | None = None,
    max_cell_len: int = _REPR_HTML_MAX_CELL_LEN,
) -> str:
    """Build a styled HTML table fragment (card layout, Jupyter-friendly)."""
    rows = len(next(iter(column_dict.values()))) if column_dict else 0
    # Modern palette: slate neutrals, soft shadow, zebra rows (no external CSS).
    css = """
<style scoped>
.pydantable-render {
  --pt-bg: #ffffff;
  --pt-border: #e2e8f0;
  --pt-header-fg: #0f172a;
  --pt-muted: #64748b;
  --pt-row-alt: #f8fafc;
  --pt-cell-fg: #1e293b;
  --pt-index-bg: #f1f5f9;
  font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto,
    "Helvetica Neue", Arial, sans-serif;
  font-size: 13px;
  line-height: 1.5;
  color: var(--pt-cell-fg);
  max-width: 100%;
  margin: 0;
  box-sizing: border-box;
}
.pydantable-render *, .pydantable-render *::before, .pydantable-render *::after {
  box-sizing: border-box;
}
.pydantable-render .pydantable-surface {
  border-radius: 12px;
  border: 1px solid var(--pt-border);
  background: var(--pt-bg);
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.06), 0 4px 12px rgba(15, 23, 42, 0.06);
  overflow: hidden;
  overflow-x: auto;
}
.pydantable-render table.pydantable-df {
  width: 100%;
  min-width: min-content;
  border-collapse: collapse;
  border-spacing: 0;
}
.pydantable-render caption {
  caption-side: top;
  text-align: left;
  padding: 12px 14px 10px;
  font-weight: 600;
  font-size: 12px;
  letter-spacing: 0.02em;
  color: var(--pt-header-fg);
  background: linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%);
  border-bottom: 1px solid var(--pt-border);
}
.pydantable-render thead th {
  text-align: left;
  padding: 8px 12px;
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--pt-muted);
  background: #f8fafc;
  border-bottom: 1px solid var(--pt-border);
  white-space: nowrap;
}
.pydantable-render thead th.pt-index-head {
  width: 3.25rem;
  text-align: right;
  padding-right: 10px;
  font-variant-numeric: tabular-nums;
}
.pydantable-render tbody th[scope="row"] {
  text-align: right;
  padding: 6px 10px 6px 12px;
  font-weight: 500;
  font-size: 12px;
  font-family: ui-monospace, "Cascadia Code", "SF Mono", Menlo, Consolas, monospace;
  font-variant-numeric: tabular-nums;
  color: var(--pt-muted);
  background: var(--pt-index-bg);
  border-bottom: 1px solid var(--pt-border);
  border-right: 1px solid var(--pt-border);
  vertical-align: top;
}
.pydantable-render tbody td {
  padding: 6px 12px;
  border-bottom: 1px solid var(--pt-border);
  vertical-align: top;
  word-break: break-word;
}
.pydantable-render tbody tr:nth-child(even) td {
  background: var(--pt-row-alt);
}
.pydantable-render tbody tr:nth-child(even) th[scope="row"] {
  background: #e8edf3;
}
.pydantable-render tbody tr:last-child td,
.pydantable-render tbody tr:last-child th {
  border-bottom: none;
}
.pydantable-render .pydantable-note {
  margin: 12px 4px 16px;
  padding: 0 4px;
  font-size: 11px;
  line-height: 1.4;
  color: var(--pt-muted);
}
</style>
"""
    parts: list[str] = [
        '<div class="pydantable-render">',
        css,
        '<div class="pydantable-surface">',
        '<table class="pydantable-df">',
    ]
    if caption:
        parts.append(f"<caption>{html.escape(caption)}</caption>")
    parts.append("<thead><tr>")
    parts.append('<th scope="col" class="pt-index-head"></th>')
    for name in column_order:
        parts.append(f'<th scope="col">{html.escape(name)}</th>')
    parts.append("</tr></thead><tbody>")
    for i in range(rows):
        parts.append("<tr>")
        parts.append(f'<th scope="row">{i}</th>')
        for name in column_order:
            raw = column_dict[name][i]
            text = _html_cell_text(raw, max_cell_len=max_cell_len)
            parts.append(f"<td>{html.escape(text)}</td>")
        parts.append("</tr>")
    parts.extend(["</tbody></table>", "</div>"])
    if note:
        parts.append(f'<p class="pydantable-note">{html.escape(note)}</p>')
    parts.append("</div>")
    return "\n".join(parts)
