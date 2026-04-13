"""Release hygiene: ``__version__`` matches the Rust extension's ``rust_version()``."""

from __future__ import annotations

import sys

import pydantable
import pydantable_protocol
import pytest

from tests._support.paths import repo_root

try:
    import pydantable_native._core as _core  # type: ignore[import-not-found]
except ImportError:
    _core = None

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

_REPO_ROOT = repo_root()

_NATIVE_SKIP = pytest.mark.skipif(
    _core is None,
    reason=(
        "pydantable_native._core is not built (run `make native-develop` or "
        "`maturin develop --manifest-path pydantable-core/Cargo.toml`)."
    ),
)


@_NATIVE_SKIP
def test_python_package_version_matches_rust_extension() -> None:
    assert _core is not None
    assert pydantable.__version__ == _core.rust_version()


def test_protocol_package_version_matches_core() -> None:
    assert pydantable_protocol.__version__ == pydantable.__version__


def test_packaging_metadata_and_pins_match_release_version() -> None:
    """Packaging TOMLs and strict dependency pins match the release version."""
    v = pydantable.__version__

    root = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert root["project"]["version"] == v
    deps: list[str] = root["project"]["dependencies"]
    proto_pin = f"pydantable-protocol=={v}"
    native_pin = f"pydantable-native=={v}"
    assert proto_pin in deps, f"expected {proto_pin} in pydantable dependencies"
    assert native_pin in deps, f"expected {native_pin} in pydantable dependencies"

    proto_pkg = tomllib.loads(
        (_REPO_ROOT / "pydantable-protocol" / "pyproject.toml").read_text(
            encoding="utf-8"
        )
    )
    assert proto_pkg["project"]["version"] == v

    native_pkg = tomllib.loads(
        (_REPO_ROOT / "pydantable-native" / "pyproject.toml").read_text(
            encoding="utf-8"
        )
    )
    assert native_pkg["project"]["version"] == v
    ndeps: list[str] = native_pkg["project"]["dependencies"]
    assert ndeps == [proto_pin], (
        f"expected native to depend only on {proto_pin}, got {ndeps}"
    )

    cargo = tomllib.loads(
        (_REPO_ROOT / "pydantable-core" / "Cargo.toml").read_text(encoding="utf-8")
    )
    assert cargo["package"]["version"] == v
