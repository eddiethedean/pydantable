# Migration guide: PydanTable 1.x → 2.0 (strict typed dataframe)

This guide explains how to migrate code written against PydanTable 1.x to the 2.0 strict API.

## What changed (high level)

- **All schema evolution is explicit**: operations that change the schema require an `*_as(...)` method with an explicit `AfterModel` / `AfterSchema`.
- **Column identity is typed**: schema-changing APIs take `ColumnRef` (e.g. `df.col.id`), not strings.
- **Legacy APIs are removed**: methods like `select(...)`, `with_columns(...)`, `drop(...)`, `rename(...)`, `join(...)`, `group_by(...)`, `melt(...)`, `pivot(...)`, `explode(...)`, and `unnest(...)` no longer exist on strict 2.0 frames/models.
- **No untyped escape hatch**: 2.0 is “typed all the way down”.

## Quick mapping (1.x → 2.0)

### Column access

- **1.x**: `df.age` or `df["age"]`
- **2.0**: `df.col.age`

### `select`

- **1.x**:

```python
out = df.select("id", "age")
```

- **2.0**:

```python
class After(Schema):
    id: int
    age: int

out = df.select_as(After, df.col.id, df.col.age)
```

For `DataFrameModel`:

```python
class After(DataFrameModel):
    id: int
    age: int

out = df.select_as(After, df.col.id, df.col.age)
```

### `with_columns`

- **1.x**:

```python
out = df.with_columns(age2=df.col.age * 2)
```

- **2.0**: you must include the full output schema.

```python
class AfterFull(Schema):
    id: int
    age: int
    age2: int

out = df.with_columns_as(AfterFull, age2=df.col.age * 2)
```

If you want to end with a narrower schema, make it an explicit second step:

```python
class After(Schema):
    id: int
    age2: int

out2 = out.drop_as(After, out.col.age)
```

### `drop`

- **1.x**:

```python
out = df.drop("age")
```

- **2.0**:

```python
class After(Schema):
    id: int

out = df.drop_as(After, df.col.age)
```

### `rename`

- **1.x**:

```python
out = df.rename({"age": "age_years"})
```

- **2.0**:

```python
class After(Schema):
    id: int
    age_years: int

out = df.rename_as(After, {df.col.age: "age_years"})
```

### `join`

- **1.x**:

```python
out = left.join(right, on="id")
```

- **2.0**:

```python
class After(Schema):
    id: int
    # ... plus the rest of the post-join schema ...

out = left.join_as(After, right, on=[left.col.id])
```

### `group_by(...).agg(...)`

- **1.x**:

```python
out = df.group_by("g").agg(total=("sum", "v"))
```

- **2.0**:

```python
class After(Schema):
    g: int
    total: int

out = df.group_by_agg_as(After, keys=[df.col.g], total=("sum", df.col.v))
```

### Reshape (`melt`, `pivot`, `explode`, `unnest`)

These are **only available** as explicit-schema APIs:

- `melt_as(AfterSchema, ...)`
- `pivot_as(AfterSchema, ..., pivot_values=[...])` (required)
- `explode_as(AfterSchema, ...)`
- `unnest_as(AfterSchema, ...)`

Examples:

```python
class Melted(Schema):
    id: int
    variable: str
    value: int

out = df.melt_as(Melted, id_vars=[df.col.id], value_vars=[df.col.v])
```

```python
class Pivoted(Schema):
    id: int
    A_first: int | None
    B_first: int | None

out = df.pivot_as(
    Pivoted,
    index=[df.col.id],
    columns=df.col.kind,
    values=[df.col.v],
    aggregate_function="first",
    pivot_values=["A", "B"],
)
```

## Common migration pitfalls

- **Forgetting intermediate schemas**: `with_columns_as` and `join_as` validate that the engine-produced schema exactly matches `AfterSchema`. If you need multiple steps, define intermediate schemas and chain them explicitly.
- **Strings in schema-changing ops**: use `df.col.<field>` / `ColumnRef` instead of `"field"`.
- **Removed convenience rename helpers** (`rename_upper`, `rename_lower`, ...): replace with explicit `rename_as` mappings.

## Suggested migration approach

1. **Add explicit schema types** for each pipeline stage (small `Schema` / `DataFrameModel` classes).
2. Replace each schema-changing call with its `*_as(...)` equivalent.
3. Run your pipeline once to let strict runtime schema validation catch mismatches early.

