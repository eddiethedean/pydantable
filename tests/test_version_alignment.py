"""Release hygiene: ``__version__`` matches the Rust extension's ``rust_version()``."""

from __future__ import annotations

import pydantable
import pydantable_native._core as _core  # type: ignore[import-not-found]
import pydantable_protocol


def test_python_package_version_matches_rust_extension() -> None:
    assert pydantable.__version__ == _core.rust_version()


def test_protocol_package_version_matches_core() -> None:
    assert pydantable_protocol.__version__ == pydantable.__version__
