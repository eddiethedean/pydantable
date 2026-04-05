"""Exercise :class:`pydantable.types.WKB` coercion paths."""

from __future__ import annotations

import pytest
from pydantable.types import WKB
from pydantic import TypeAdapter, ValidationError


def test_wkb_identity_and_coercion() -> None:
    ta = TypeAdapter(WKB)
    b = b"\x01\x02"
    w = WKB(b)
    assert ta.validate_python(w) is w
    assert bytes(ta.validate_python(b)) == b
    assert bytes(ta.validate_python(bytearray(b))) == b


def test_wkb_repr() -> None:
    assert "WKB(" in repr(WKB(b"ab"))


def test_wkb_rejects_non_bytes() -> None:
    ta = TypeAdapter(WKB)
    with pytest.raises(ValidationError):
        ta.validate_python(1.5)
