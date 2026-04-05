from __future__ import annotations

import json

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("streamlit")


def test_streamlit_data_editor_roundtrip_default_value() -> None:
    """`st.data_editor` accepts the supported fallback and returns round-trippable data.

    Note: Streamlit's AppTest does not currently support simulating cell edits for
    `st.data_editor`, so this test validates the integration/return-type path for the
    default (unchanged) value.
    """

    from streamlit.testing.v1 import AppTest

    def app() -> None:
        import streamlit as st
        from pydantable import DataFrameModel

        class SmallDF(DataFrameModel):
            id: int
            name: str
            age: int | None

        df = SmallDF({"id": [1, 2], "name": ["a", "b"], "age": [10, None]})

        edited = st.data_editor(df.to_arrow(), key="editor")

        # Normalize to a columnar dict for assertion via AppTest.
        try:
            import pandas as pd  # type: ignore[import-untyped]

            if isinstance(edited, pd.DataFrame):
                out = edited.to_dict(orient="list")
            else:
                out = edited.to_pydict()
        except Exception:
            out = edited.to_pydict()

        st.json(out)

    at = AppTest.from_function(app).run()
    assert len(at.exception) == 0
    assert len(at.json) == 1

    payload = json.loads(at.json[0].value)
    assert payload["id"] == [1, 2]
    assert payload["name"] == ["a", "b"]
    # Streamlit/pandas may represent null integers as NaN.
    assert payload["age"][0] in (10, 10.0)
