"""Execute ``docs/examples/io/*.py`` so doc snippets stay runnable."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

_EXAMPLES = [
    "docs/examples/io/overview_roundtrip.py",
    "docs/examples/io/iter_glob_parquet_batches.py",
    "docs/examples/io/parquet_partitioned_write.py",
    "docs/examples/io/parquet_allow_missing_columns.py",
    "docs/examples/io/parquet_lazy_roundtrip.py",
    "docs/examples/io/csv_lazy_roundtrip.py",
    "docs/examples/io/ndjson_roundtrip.py",
    "docs/examples/io/ipc_roundtrip.py",
    "docs/examples/io/http_local_fetch.py",
    "docs/examples/io/sql_sqlite_roundtrip.py",
    "docs/examples/io/sql_sqlite_sqlmodel_roundtrip.py",
    "docs/examples/io/sql_sqlite_sqlmodel_streaming.py",
    "docs/examples/io/sql_sqlite_streaming.py",
    "docs/examples/io/extras_stdin_stdout.py",
    "docs/examples/io/extras_read_excel_optional.py",
]


def _env_with_pythonpath() -> dict[str, str]:
    env = os.environ.copy()
    py_src = str(REPO / "python")
    prev = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = py_src if not prev else f"{py_src}{os.pathsep}{prev}"
    return env


@pytest.mark.parametrize("rel", _EXAMPLES)
def test_doc_io_example_script(rel: str) -> None:
    if "sqlmodel" in rel:
        pytest.importorskip("sqlmodel")
    script = REPO / rel
    assert script.is_file(), f"missing {script}"
    r = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(REPO),
        env=_env_with_pythonpath(),
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode == 0, f"{rel} failed:\n{r.stderr}\n{r.stdout}"
