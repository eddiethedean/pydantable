from __future__ import annotations

import types

import pytest
from pydantable import DataFrameModel, Schema


class _Thing(Schema):
    a: int


def test_dataframe_model_string_forward_ref_resolves() -> None:
    class ForwardRefDF(DataFrameModel):
        x: _Thing

    assert ForwardRefDF.schema_model().model_fields["x"].annotation is _Thing


def test_dataframe_model_string_annotation_does_not_execute_code() -> None:
    # Ensure string annotations aren't resolved via `eval()` (code execution footgun).
    sentinel = {"ran": False}

    mod = types.ModuleType("_pydantable_test_mod_eval_guard")

    def _flip() -> object:
        sentinel["ran"] = True
        return object()

    mod._flip = _flip  # type: ignore[attr-defined]
    # The class body will be executed (normal Python), but the string annotation
    # resolution should *not* call `_flip()`.
    ns: dict[str, object] = {"DataFrameModel": DataFrameModel, "_flip": _flip}
    with pytest.raises(TypeError, match="unsupported"):
        exec(
            'class DangerousDF(DataFrameModel):\n    x: "_flip()"\n',
            ns,
            ns,
        )
    assert sentinel["ran"] is False
