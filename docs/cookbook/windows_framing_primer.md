# Windows: framing primer (rowsBetween / rangeBetween)

This recipe is a short mental model for window functions and framing in pydantable.

## Key idea

- **Unframed** windows behave like “compute over partition/order context” (Polars-style).
- **Framed** windows (`rowsBetween`, `rangeBetween`) define an explicit frame around each row.

See [WINDOW_SQL_SEMANTICS](../semantics/window-sql-semantics.md) for the detailed rules, including multi-key `rangeBetween`
axis behavior.

## Recipe: rank within partition

```python
from pydantable import DataFrameModel
from pydantable.window_spec import Window
from pydantable.expressions import row_number


class Row(DataFrameModel):
    group: str
    v: int


df = Row({"group": ["a", "a", "b"], "v": [2, 1, 5]})
w = Window.partitionBy("group").orderBy("v")
out = df.with_columns(rn=row_number().over(w)).to_dict()
assert "rn" in out
```

## Pitfalls

- Prefer the named window APIs (`Window.partitionBy(...).orderBy(...)`) over generic `Expr.over(...)` (see [INTERFACE_CONTRACT](../semantics/interface-contract.md)).

