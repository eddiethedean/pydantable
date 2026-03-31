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
df.select(s.binary())
df.select(s.lists())
df.select(s.maps())
df.select(s.structs())
df.select(s.enums())
df.select(s.ipv4s())
df.select(s.ipv6s())
df.select(s.uuids())
df.select(s.wkbs())
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

### Excluding columns in `select`

Use `exclude=` to remove columns from a projection (names or selectors):

```python
df.select("id", "age", "age2", exclude=s.starts_with("age"))
df.select(exclude=["debug_col"])  # everything except debug_col
```

### Error behavior

- **`select(Selector)`** raises **`ValueError`** when the selector matches no columns (includes the selector summary and available schema columns).
- **`drop(Selector)`** follows the existing `drop(strict=...)` rules:
  - `strict=True`: missing columns error at plan validation time
  - `strict=False`: missing columns are ignored (no-op if all requested columns are missing)

### Rename helper

Use `rename_with_selector` to rename a subset of columns based on a selector:

```python
df2 = df.rename_with_selector(s.starts_with("tmp_"), lambda c: c.removeprefix("tmp_"))
```

You can also build a mapping using `rename_map` and pass it to `rename(...)`:

```python
m = s.rename_map(s.starts_with("tmp_"), lambda c: c.removeprefix("tmp_"))(df.schema_fields())
df2 = df.rename(m)
```

### Selector-driven column transforms

Some schema-first convenience helpers expand a selector into a concrete column list and then apply a typed-safe transform:

```python
# cast a subset
df2 = df.with_columns_cast(s.numeric(), float)

# fill nulls for a subset
df3 = df.with_columns_fill_null(s.by_name("age"), value=0)

# explicit selector-first projection (alias of select(selector))
df4 = df.select_schema(s.starts_with("tmp_"))
```

### Selector-driven rename conveniences

```python
df2 = df.rename_upper(s.starts_with("tmp_"))
df3 = df.rename_strip(chars="_")
```
