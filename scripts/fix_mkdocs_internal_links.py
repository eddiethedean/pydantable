#!/usr/bin/env python3
"""Rewrite internal ``.md`` links to site-root paths (``/Page.md``) for MkDocs."""

from __future__ import annotations

import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_DOCS = _REPO / "docs"

_MD_LINK = re.compile(r"(!?)\[([^\]]*)\]\(([^)]+)\)")


def _fix_href(href: str) -> str:
    href = href.strip()
    if href.startswith(("http://", "https://", "mailto:", "#", "/")):
        return href
    if href.startswith("../") or href.startswith("./"):
        return href
    if not href.endswith(".md"):
        return href
    return "/" + href.lstrip("/")


def fix_text(text: str) -> str:
    def repl(m: re.Match[str]) -> str:
        bang, label, href = m.group(1), m.group(2), m.group(3)
        if bang:
            return m.group(0)
        return f"[{label}]({_fix_href(href)})"

    return _MD_LINK.sub(repl, text)


def main() -> None:
    for p in sorted(_DOCS.rglob("*.md")):
        if "async_ideas" in p.parts:
            continue
        before = p.read_text(encoding="utf-8")
        after = fix_text(before)
        if after != before:
            p.write_text(after, encoding="utf-8", newline="\n")
            print(p.relative_to(_REPO))


if __name__ == "__main__":
    main()
