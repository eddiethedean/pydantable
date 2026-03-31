## Selector DSL (schema-first)

PydanTable includes a **schema-driven selector DSL** for picking columns by name patterns and dtype groups. Unlike Polars selectors, these selectors resolve **only** against the current logical schema (`df.schema_fields()`), which keeps them deterministic and compatible with schema-first typing.

### Import

```python
from pydantable import selectors as s
```

### Name-based selectors

```python
df.select(s.by_name("id", "age"))
df.select(s.starts_with("age"))
df.select(s.ends_with("_id"))
df.select(s.contains("score"))
df.select(s.matches(r"^age\\d+$"))
```

### Dtype-group selectors

```python
df.select(s.numeric())
df.select(s.integers())
df.select(s.floats())
df.select(s.decimals())
df.select(s.string())
df.select(s.temporal())
df.select(s.boolean())
df.select(s.lists())
df.select(s.structs())
df.select(s.uuids())
```

### Composition

Selectors can be composed:

```python
# union, intersection, difference
df.select(s.starts_with("age") | s.by_name("id"))
df.select(s.numeric() & ~s.by_name("id"))
df.drop(s.starts_with("tmp_") - s.by_name("tmp_keep"))

# exclude helper (same as s1 - s2)
df.select(s.everything().exclude(s.ends_with("_debug")))
```

### Error behavior

- **`select(Selector)`** raises **`ValueError`** when the selector matches no columns (includes the selector summary and available schema columns).
- **`drop(Selector)`** follows the existing `drop(strict=...)` rules:\n  - `strict=True`: missing columns error at plan validation time\n  - `strict=False`: missing columns are ignored (no-op if all requested columns are missing)\n+

### Rename helper

Use `rename_with_selector` to rename a subset of columns based on a selector:

```python
df2 = df.rename_with_selector(s.starts_with("tmp_"), lambda c: c.removeprefix("tmp_"))
```
