from __future__ import annotations

import warnings

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("streamlit")

def test_streamlit_dataframe_interchange_smoke() -> None:
    from streamlit.testing.v1 import AppTest

    def app() -> None:
        import streamlit as st
        from pydantable import DataFrameModel
        from streamlit.errors import StreamlitAPIException

        class SmallDF(DataFrameModel):
            id: int
            name: str
            age: int | None

        df = SmallDF({"id": [1, 2], "name": ["a", "b"], "age": [10, None]})
        assert df.__dataframe__() is not None
        st.write(df)
        st.dataframe(df)
        # As of current Streamlit, `st.data_editor` does not accept interchange-protocol
        # objects directly; it does accept Arrow/Pandas/Polars. Keep this test aligned
        # with Streamlit behavior while still covering the editor path.
        try:
            st.data_editor(df)
        except StreamlitAPIException:
            pass
        st.data_editor(df.to_arrow())

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r"^Could not serialize pd\.DataFrame\.attrs: .* defaulting to empty attributes\.$",
        )
        at = AppTest.from_function(app).run()

    # If the protocol export fails, Streamlit typically surfaces an exception during app run.
    assert len(at.dataframe) >= 1
    assert len(at.exception) == 0
