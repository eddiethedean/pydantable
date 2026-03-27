from __future__ import annotations

import pydantable.typing as typing_mod


def test_typing_module_is_importable_and_exports_protocol() -> None:
    assert set(typing_mod.__all__) == {"DataFrameModelWithRow", "RowT"}
    assert typing_mod.DataFrameModelWithRow.__name__ == "DataFrameModelWithRow"
