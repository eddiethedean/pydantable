"""MkDocs hooks: mirror ``docs/conf.py`` import path setup and SQLAlchemy typing patch.

Read the Docs does not install the full package (no Rust build). ``python/`` and
``pydantable-protocol/python/`` must be on ``sys.path`` before mkdocstrings
imports ``pydantable``. The SQLAlchemy ``Engine`` / ``Connection`` patch matches
``conf.py`` ``setup()`` so forward refs resolve like Sphinx autodoc.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _project_paths(config) -> tuple[Path, Path]:
    docs_dir = Path(config["docs_dir"]).resolve()
    repo_root = docs_dir.parent
    return repo_root, docs_dir


def on_config(config, **kwargs):
    repo_root, _docs = _project_paths(config)
    py = repo_root / "python"
    proto = repo_root / "pydantable-protocol" / "python"
    for p in (py, proto):
        s = str(p)
        if s not in sys.path:
            sys.path.insert(0, s)

    try:
        from sqlalchemy.engine import Connection, Engine
    except ImportError:
        return config

    import pydantable.dataframe_model as dataframe_model
    import pydantable.io.sql as io_sql

    dataframe_model.Engine = Engine  # type: ignore[attr-defined]
    dataframe_model.Connection = Connection  # type: ignore[attr-defined]
    io_sql.Engine = Engine  # type: ignore[attr-defined]
    io_sql.Connection = Connection  # type: ignore[attr-defined]

    return config
