from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Example:
    script_rel: str
    out_rel: str


_INCLUDE_RE = re.compile(r'--8<--\s+"(?P<path>examples/[^"]+)"')


def _repo_root() -> Path:
    # scripts/ lives at repo root; use that.
    return Path(__file__).resolve().parents[1]


def _discover_from_docs(docs_dir: Path) -> list[Example]:
    scripts: set[str] = set()
    outs: set[str] = set()

    for md in sorted(docs_dir.rglob("*.md")):
        text = md.read_text(encoding="utf-8")
        for m in _INCLUDE_RE.finditer(text):
            p = m.group("path")
            if p.endswith(".py"):
                scripts.add(p)
            elif p.endswith(".py.out.txt"):
                outs.add(p)

    examples: list[Example] = []
    for out_rel in sorted(outs):
        script_rel = out_rel[: -len(".out.txt")]
        if script_rel not in scripts:
            # Allow output-only files that aren’t embedded on a page right now.
            continue
        examples.append(Example(script_rel=script_rel, out_rel=out_rel))

    return examples


def _run_script(repo_root: Path, script_rel: str) -> str:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    cmd = [sys.executable, str(repo_root / "docs" / script_rel)]
    proc = subprocess.run(
        cmd,
        cwd=str(repo_root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    out = proc.stdout
    if proc.returncode != 0:
        raise RuntimeError(
            f"Example failed (exit={proc.returncode}): {script_rel}\n\n{out}"
        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Run docs examples and refresh included .out.txt files."
    )
    ap.add_argument(
        "--check",
        action="store_true",
        help="Fail if any output files would change (do not write).",
    )
    args = ap.parse_args()

    root = _repo_root()
    docs_dir = root / "docs"
    examples = _discover_from_docs(docs_dir)
    if not examples:
        print("No docs examples discovered.", file=sys.stderr)
        return 1

    changed: list[str] = []

    for ex in examples:
        out = _run_script(root, ex.script_rel)
        out_path = docs_dir / ex.out_rel
        prev = out_path.read_text(encoding="utf-8") if out_path.exists() else None
        if prev != out:
            changed.append(ex.out_rel)
            if not args.check:
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(out, encoding="utf-8")
        print(f"ok: {ex.script_rel} -> {ex.out_rel}")

    if args.check and changed:
        print("\nOutputs out of date:", file=sys.stderr)
        for p in changed:
            print(f"- {p}", file=sys.stderr)
        return 2

    if changed:
        print(f"\nUpdated {len(changed)} output file(s).")
    else:
        print("\nAll outputs already up to date.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

