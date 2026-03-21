#!/usr/bin/env bash
# Build the pydantable Rust extension in release mode, then run comparison benchmarks.
# Debug builds (default for many editable installs) are not representative of production.
#
# Usage (from repo root):
#   ./benchmarks/run_release.sh
#   ./benchmarks/run_release.sh --rows 10000 50000 --rounds 7
#   ./benchmarks/run_release.sh --with-ingest   # include DataFrame construction in timing
#
# Requires: .venv with maturin (pip install maturin) and pip install -e ".[benchmark]"

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON="${PYTHON:-$ROOT/.venv/bin/python}"
if [[ ! -x "$PYTHON" ]]; then
  echo "Set PYTHON=.../python or create .venv at repo root." >&2
  exit 1
fi

echo "==> maturin develop --release (optimized native extension)"
"$PYTHON" -m maturin develop --release

echo ""
echo "==> pydantable vs Polars"
"$PYTHON" benchmarks/pydantable_vs_polars.py "$@"

echo ""
echo "==> pydantable vs pandas"
"$PYTHON" benchmarks/pydantable_vs_pandas.py "$@"
