#!/usr/bin/env python3
"""One-shot migration: MyST/Sphinx patterns in docs/**/*.md → MkDocs Material / pymdown.

- ``{doc}`target`` → Markdown links
- Fenced `` ```{admonition}`` blocks → pymdown ``!!!`` admonitions
- ``literalinclude`` directives → ``--8<--`` snippet includes (paths under ``docs/``)

Run from repo root::

    python scripts/migrate_docs_myst_to_mkdocs.py

Exit 0 always; prints changed file paths.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_DOCS = _REPO / "docs"

_ADMONITION_MAP = {
    "note": "note",
    "seealso": 'abstract "See also"',
    "important": "important",
    "warning": "warning",
    "hint": "hint",
    "tip": "tip",
    "caution": "warning",
    "danger": "danger",
}


def _doc_target_to_md(m: re.Match[str]) -> str:
    raw = m.group(1).strip()
    path = raw.lstrip("/") if raw.startswith("/") else raw
    if not path.endswith(".md"):
        path = f"{path}.md"
    label = path.rsplit("/", 1)[-1]
    if label.endswith(".md"):
        label = label[: -len(".md")]
    return f"[{label}]({path})"


def _replace_doc_roles(text: str) -> str:
    return re.sub(r"\{doc\}`([^`]+)`", _doc_target_to_md, text)


def _fenced_admonition_to_pymdown(text: str) -> str:
    """Convert ```{kind} ... ``` to !!! form (single pass; no nested fences)."""

    def repl(m: re.Match[str]) -> str:
        kind = m.group(1).strip()
        body = m.group(2).rstrip("\n")
        pym = _ADMONITION_MAP.get(kind, "note")
        lines = body.split("\n")
        indented = "\n".join(f"    {ln}" if ln.strip() else "" for ln in lines)
        return f"!!! {pym}\n{indented}\n"

    pattern = re.compile(
        r"^```\{(" + "|".join(map(re.escape, _ADMONITION_MAP)) + r")\}\s*\n(.*?)\n```",
        re.MULTILINE | re.DOTALL,
    )
    return pattern.sub(repl, text)


def _literalinclude_to_snippet(text: str) -> str:
    """``{literalinclude} path`` → ``--8<-- "path"`` (paths relative to ``docs/``)."""

    def repl(m: re.Match[str]) -> str:
        path = m.group(1).strip()
        if path.startswith("../"):
            path = path[3:]
        return f'\n--8<-- "{path}"\n'

    return re.sub(
        r"^```\{literalinclude\}\s+([^\n]+)\n(?::[^\n]+\n)*```\s*$",
        repl,
        text,
        flags=re.MULTILINE,
    )


def _strip_toctree_blocks(text: str) -> str:
    return re.sub(
        r"^```\{toctree\}[\s\S]*?^```\s*\n?",
        "",
        text,
        flags=re.MULTILINE,
    )


def _strip_myst_frontmatter_html_meta(text: str) -> str:
    """Remove MyST-only html_meta YAML blocks at top."""
    if not text.startswith("---\n"):
        return text
    end = text.find("\n---\n", 4)
    if end == -1:
        return text
    block = text[4:end]
    if "html_meta:" in block:
        # Drop entire front matter for index (meta in mkdocs.yml site_description).
        return text[end + 5 :].lstrip("\n")
    return text


def migrate_file(path: Path) -> bool:
    before = path.read_text(encoding="utf-8")
    text = before
    if path.name == "index.md" and path.parent == _DOCS:
        text = _strip_myst_frontmatter_html_meta(text)
    text = _replace_doc_roles(text)
    text = _literalinclude_to_snippet(text)
    text = _fenced_admonition_to_pymdown(text)
    text = _strip_toctree_blocks(text)
    if text != before:
        path.write_text(text, encoding="utf-8", newline="\n")
        return True
    return False


def main() -> None:
    changed: list[Path] = []
    for p in sorted(_DOCS.rglob("*.md")):
        if "async_ideas" in p.parts:
            continue
        if migrate_file(p):
            changed.append(p)
    for p in changed:
        print(p.relative_to(_REPO))
    print(f"migrated {len(changed)} files")


if __name__ == "__main__":
    main()
