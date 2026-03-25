---
orphan: true
---

# Streamlit integration

Streamlit can render interactive tables from objects that implement the **Python DataFrame Interchange Protocol** (`__dataframe__`). As of **0.21.0**, `pydantable` implements `__dataframe__` on `DataFrame` (and `DataFrameModel` via delegation), so you can pass a typed frame directly to `st.dataframe`.

## Install

You need Streamlit plus the `pydantable` Arrow extra:

```bash
pip install streamlit
pip install 'pydantable[arrow]'
```

## Usage

```python
import streamlit as st
from pydantable import DataFrameModel


class User(DataFrameModel):
    id: int
    name: str
    age: int | None


df = User({"id": [1, 2], "name": ["a", "b"], "age": [10, None]})

st.write(df)
st.dataframe(df)

# `st.data_editor` currently expects a concrete frame type (Arrow/pandas/Polars),
# so use an explicit conversion:
st.data_editor(df.to_arrow())
```

## Fallbacks (when interchange is unavailable)

- If you don’t have `pyarrow` installed, `__dataframe__` will fail. Use one of:
  - `st.dataframe(df.to_arrow())` (requires `pydantable[arrow]`)
  - `st.dataframe(df.to_polars())` (requires `pydantable[polars]`)
- For editing, prefer `st.data_editor(df.to_arrow())` (requires `pydantable[arrow]`) or `st.data_editor(df.to_polars())` (requires `pydantable[polars]`).
- `st.write(df)` will still show either the HTML preview (`_repr_html_`) or the plain `repr` depending on the Streamlit rendering path, but it won’t be an interactive table unless Streamlit can treat it as a dataframe-like object.

## Costs and limitations

- `pydantable`’s `__dataframe__` path **materializes** the current lazy plan (same cost class as {doc}`EXECUTION` → `to_arrow()`), then delegates to PyArrow’s protocol implementation. It is **not** a zero-copy export of internal engine buffers.
- For large frames, prefer applying a lazy `head(...)` / `slice(...)` before displaying.
