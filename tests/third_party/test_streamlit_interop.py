from __future__ import annotations

import warnings

import pytest

pytest.importorskip("pyarrow")
pd = pytest.importorskip("pandas")
pytest.importorskip("streamlit")


def test_streamlit_dataframe_interchange_smoke() -> None:
    from streamlit.testing.v1 import AppTest

    def app() -> None:
        from contextlib import suppress

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
        with suppress(StreamlitAPIException):
            st.data_editor(df)
        # Streamlit may internally convert through pandas; give it an explicit pandas DF
        # to avoid pyarrow metadata/attrs serialization edge cases across versions.
        st.data_editor(df.to_arrow().to_pandas())

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=(
                r"^Could not serialize pd\.DataFrame\.attrs: .* "
                r"defaulting to empty attributes\.$"
            ),
        )
        # Streamlit may still route some inputs through pandas' interchange importer
        # (deprecated as of pandas 4 / Pandas4Warning). Remove this filter when
        # Streamlit uses Arrow-native paths for pydantable inputs (upstream change).
        if hasattr(pd.errors, "Pandas4Warning"):
            warnings.filterwarnings("ignore", category=pd.errors.Pandas4Warning)
        at = AppTest.from_function(app).run()

    # If protocol export fails, Streamlit surfaces an exception during app run.
    assert len(at.dataframe) >= 1
    assert len(at.exception) == 0
