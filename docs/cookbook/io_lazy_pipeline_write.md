# I/O: lazy read → transform → write (out-of-core pattern)

Use `read_*` for large local files so transforms run on a lazy scan root, then write
the result with `DataFrame.write_*` without materializing a giant `dict[str, list]`.

## Recipe (Parquet)

```python
from pydantable import DataFrameModel


class User(DataFrameModel):
    id: int
    age: int | None


df = User.read_parquet("users.parquet")
out = df.filter(df.col.age.is_not_null()).select_as(User, df.col.id, df.col.age)
out.write_parquet("users_filtered.parquet")
```

## Notes

- Validation options on lazy roots (`trusted_mode`, `ignore_errors`, `on_validation_errors`)
  apply when you materialize (or when you write, since writing requires execution).
- See {doc}`/EXECUTION` for the lazy/eager I/O vocabulary and the streaming engine knobs.

