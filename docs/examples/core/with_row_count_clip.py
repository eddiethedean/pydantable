from __future__ import annotations

from pydantable import DataFrame
from pydantable.schema import Schema


class S(Schema):
    x: int
    y: float


df = DataFrame[S]({"x": [1, 5], "y": [1.5, -2.0]})

# Add a deterministic row number column.
df2 = df.with_row_count()  # adds `row_nr: int`

# Clamp numeric values (optionally with subset=... as names or selectors).
df3 = df2.clip(lower=0, upper=3)

print(df3.to_dict())
