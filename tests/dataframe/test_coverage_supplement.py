"""Tests for low-coverage helpers: redaction, WKB, engine re-exports, ingest errors."""

from __future__ import annotations

import pytest
from pydantable import types as pt_types
from pydantable.ingest_errors import IngestRowFailure, coerce_validation_failures
from pydantable.redaction import apply_redaction_to_row_dicts, redaction_mask_for_value
from pydantable.repr_label import short_repr_label
from pydantic import BaseModel, Field, TypeAdapter, ValidationError


def test_redaction_mask_for_value_types() -> None:
    assert redaction_mask_for_value(None) is None
    assert redaction_mask_for_value("x") == "***"
    assert redaction_mask_for_value(42) == 0
    assert redaction_mask_for_value(3.5) == 0
    assert redaction_mask_for_value([1]) is None


def test_apply_redaction_to_row_dicts_with_policy() -> None:
    class Row(BaseModel):
        email: str = Field(json_schema_extra={"pydantable": {"redact": True}})
        name: str

    rows = [{"email": "a@b.com", "name": "n"}]
    out = apply_redaction_to_row_dicts(Row, rows)
    assert out == [{"email": "***", "name": "n"}]


def test_apply_redaction_to_row_dicts_no_policy() -> None:
    class Row(BaseModel):
        x: int

    rows = [{"x": 1}]
    assert apply_redaction_to_row_dicts(Row, rows) is rows


def test_apply_redaction_skips_columns_absent_from_row_dict() -> None:
    class Row(BaseModel):
        email: str = Field(json_schema_extra={"pydantable": {"redact": True}})

    rows = [{"other": "keep"}]
    assert apply_redaction_to_row_dicts(Row, rows) == [{"other": "keep"}]


def test_short_repr_label_truncates() -> None:
    long = "a " * 150
    s = short_repr_label(long, max_len=20)
    assert s.endswith("...")
    assert len(s) == 20


def test_short_repr_label_no_truncation() -> None:
    assert short_repr_label("hello  world") == "hello world"


def test_coerce_validation_failures_none_and_list() -> None:
    assert coerce_validation_failures(None) == []
    f = IngestRowFailure(
        row_index=0,
        row={"a": 1},
        errors=[{"loc": ("a",), "msg": "x", "type": "value_error"}],
    )
    out = coerce_validation_failures([f])
    assert len(out) == 1
    assert out[0].row_index == 0


def test_coerce_validation_failures_from_dict() -> None:
    item = {
        "row_index": 1,
        "row": {"k": 2},
        "errors": [{"loc": ("k",), "msg": "bad", "type": "type_error"}],
    }
    out = coerce_validation_failures([item])
    assert out[0].row_index == 1


def test_coerce_validation_failures_type_error() -> None:
    with pytest.raises(TypeError, match="validation failures"):
        coerce_validation_failures({})  # type: ignore[arg-type]


def test_wkb_coercion_and_repr() -> None:
    b = b"\x01\x02"
    w = pt_types.WKB(b)
    assert isinstance(w, pt_types.WKB)
    assert pt_types.WKB(w) == w
    assert pt_types.WKB(bytearray(b)) == b
    assert pt_types.WKB(memoryview(b)) == b
    assert "WKB" in repr(w)


def test_wkb_pydantic_roundtrip_preserves_instance() -> None:
    from pydantic import BaseModel

    class Geo(BaseModel):
        g: pt_types.WKB

    w = pt_types.WKB(b"\xff")
    m = Geo(g=w)
    assert m.g is w


def test_wkb_rejects_non_bytes() -> None:
    ta = TypeAdapter(pt_types.WKB)
    with pytest.raises(ValidationError):
        ta.validate_python(object())


def test_engine_binding_reexports() -> None:
    """Cover ``python/pydantable/engine/_binding.py`` (thin re-export layer)."""
    import pydantable.engine._binding as b

    assert hasattr(b, "MISSING_SYMBOL_PREFIX")
    assert callable(b.load_rust_core)
    assert callable(b.rust_core_loaded)


def test_dtypes_register_and_list() -> None:
    from pydantable import dtypes as dt

    class Tag(str):
        pass

    dt.register_scalar(Tag, base="str")
    assert dt.get_registered_scalar_base(Tag) is str
    names = dt.list_registered_scalars()
    assert Tag in names


def test_pyspark_init_getattr_sql_moltres() -> None:
    pytest.importorskip("moltres_core")
    import pydantable.pyspark as ps

    sdf = ps.SqlDataFrame
    sdfm = ps.SqlDataFrameModel
    assert sdf is not None
    assert sdfm is not None


def test_pyspark_init_getattr_unknown() -> None:
    import pydantable.pyspark as ps

    with pytest.raises(AttributeError, match="has no attribute"):
        _ = ps.not_a_real_export_  # type: ignore[attr-defined]
