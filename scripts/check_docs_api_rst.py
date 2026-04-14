from __future__ import annotations

import argparse
from pathlib import Path

UTF8_BOM = b"\xef\xbb\xbf"


def _iter_api_rst(repo_root: Path) -> list[Path]:
    api_dir = repo_root / "docs" / "api"
    if not api_dir.exists():
        return []
    return sorted(api_dir.glob("*.rst"))


def _normalize_bytes(b: bytes) -> bytes:
    if b.startswith(UTF8_BOM):
        b = b[len(UTF8_BOM) :]
    # Ensure a single trailing newline (and normalize line endings).
    txt = b.decode("utf-8")
    txt = txt.replace("\r\n", "\n").replace("\r", "\n")
    if not txt.endswith("\n"):
        txt += "\n"
    return txt.encode("utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check/normalize docs/api/*.rst encoding and newlines."
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Rewrite files in-place to remove UTF-8 BOM and ensure trailing newline.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    paths = _iter_api_rst(repo_root)
    changed: list[Path] = []

    for p in paths:
        before = p.read_bytes()
        after = _normalize_bytes(before)
        if before != after:
            changed.append(p)
            if args.fix:
                p.write_bytes(after)

    if changed:
        print("docs/api rst files need normalization:")
        for p in changed:
            print(f"  - {p.relative_to(repo_root)}")
        if args.fix:
            print("Fixed.")
            return 0
        print("Re-run with: python scripts/check_docs_api_rst.py --fix")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
