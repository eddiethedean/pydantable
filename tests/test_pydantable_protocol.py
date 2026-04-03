"""Tests for the zero-dependency ``pydantable-protocol`` distribution."""

from __future__ import annotations

from pathlib import Path

import pytest
import tomllib
from pydantable import MissingRustExtensionError as PydantableMissingRust
from pydantable.errors import UnsupportedEngineOperationError as PydantableEngineOpError
from pydantable_protocol import ExecutionEngine
from pydantable_protocol import MissingRustExtensionError as ProtocolMissingRust
from pydantable_protocol import UnsupportedEngineOperationError as ProtocolEngineOpError
from pydantable_protocol import __version__ as protocol_version

_REPO_ROOT = Path(__file__).resolve().parents[1]
_PROTOCOL_PYPROJECT = _REPO_ROOT / "pydantable-protocol" / "pyproject.toml"
_NATIVE_PYPROJECT = _REPO_ROOT / "pydantable-native" / "pyproject.toml"


def test_protocol_pyproject_has_no_runtime_dependencies() -> None:
    data = tomllib.loads(_PROTOCOL_PYPROJECT.read_text(encoding="utf-8"))
    deps = data.get("project", {}).get("dependencies")
    assert deps == [], "third-party engines must not be forced to install pydantable"


def test_native_pyproject_does_not_depend_on_pydantable() -> None:
    data = tomllib.loads(_NATIVE_PYPROJECT.read_text(encoding="utf-8"))
    deps = data.get("project", {}).get("dependencies", [])
    assert len(deps) == 1
    assert deps[0].startswith("pydantable-protocol=="), (
        "pydantable-native should depend only on pydantable-protocol"
    )


def test_pydantable_engine_error_is_protocol_subclass() -> None:
    assert issubclass(PydantableEngineOpError, ProtocolEngineOpError)


def test_missing_rust_extension_error_is_canonical_protocol_type() -> None:
    assert PydantableMissingRust is ProtocolMissingRust
    assert issubclass(ProtocolMissingRust, NotImplementedError)


def test_protocol_base_catches_pydantable_engine_error() -> None:
    with pytest.raises(ProtocolEngineOpError, match="no lists"):
        raise PydantableEngineOpError("no lists")


def test_protocol_version_is_non_empty() -> None:
    assert protocol_version
    parts = protocol_version.split(".")
    assert len(parts) >= 2


def test_execution_engine_protocol_is_runtime_checkable() -> None:
    class _Dummy:
        """Minimal placeholder — not a full engine; isinstance checks struct."""

        capabilities = None

    assert not isinstance(_Dummy(), ExecutionEngine)
