from __future__ import annotations

from pydantable import DataFrame
from pydantable import selectors as s
from pydantable.schema import Schema


class S(Schema):
    a: int | None
    b: int | None


df = DataFrame[S]({"a": [None, 1], "b": [2, None]})

print(df.with_columns_fill_null(s.by_name("a"), value=0).to_dict())
print(df.with_columns_cast(s.by_name("b"), float).to_dict())
print(df.rename_upper(s.by_name("a")).to_dict())
print(df.select_schema(s.by_name("b")).to_dict())
