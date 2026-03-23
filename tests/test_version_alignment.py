"""Release hygiene: ``__version__`` matches the Rust extension's ``rust_version()``."""

from __future__ import annotations

import pydantable
from pydantable import _core


def test_python_package_version_matches_rust_extension() -> None:
    assert pydantable.__version__ == _core.rust_version()
