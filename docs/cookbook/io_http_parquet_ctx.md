# I/O: Parquet over HTTP with automatic temp-file cleanup

Lazy HTTP Parquet reads download to a temp file. Prefer the `*_ctx` context managers
to ensure the temp file is removed when the pipeline is done.

## Recipe

```python
from pydantable import DataFrameModel


class User(DataFrameModel):
    id: int
    age: int | None


url = "https://example.com/users.parquet"

with User.read_parquet_url_ctx(url) as df:
    out = df.filter(df.age.is_not_null()).select("id", "age")
    # Materialize inside the context
    _ = out.to_dict()
```

## Pitfalls

- Do not keep a lazy frame alive after the context exits; the backing file is deleted.
- For safety limits on HTTP reads, see [IO_HTTP](../io/http.md) (`max_bytes`).

