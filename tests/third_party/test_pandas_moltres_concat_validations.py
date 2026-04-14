from __future__ import annotations

import pytest


def test_sql_dataframe_model_concat_validates_inputs():
    # This module is tiny but was missing coverage; keep the test narrow and focused
    # on its own validation branches.
    moltres = pytest.importorskip("moltres_core")

    _ = moltres  # keep importorskip referenced for clarity

    from pydantable.pandas_moltres import SqlDataFrameModel

    with pytest.raises(ValueError, match="at least two"):
        SqlDataFrameModel.concat([])

    with pytest.raises(TypeError, match="DataFrameModel"):
        SqlDataFrameModel.concat([object(), object()])  # type: ignore[list-item]
