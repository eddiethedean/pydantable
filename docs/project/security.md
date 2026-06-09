# Security considerations

Practical guidance for services using pydantable. This is not a formal threat model.

## Input validation

| Mode | When to use |
|------|-------------|
| **`trusted_mode="off"`** (default) | Untrusted HTTP bodies, uploaded files, external APIs |
| **`trusted_mode="shape_only"`** | Upstream already validated types; skip per-cell Pydantic |
| **`trusted_mode="strict"`** | Trusted Polars/Arrow buffers with dtype enforcement |

Never use **`shape_only`** or **`strict`** on client-controlled data without an explicit trust boundary. See [FastAPI guide](../integrations/fastapi/fastapi.md) and [Performance](performance.md).

## Filesystem and HTTP paths

- **`read_*`** on user-supplied paths: allowlist directories, reject **`..`** and unsafe symlinks
- **`read_parquet_url`**: treat URLs as untrusted; set **`max_bytes`** where documented
- Prefer context managers (**`read_parquet_url_ctx`**) so temp files are removed

## Database and secrets

- Pass SQLAlchemy URLs via environment variables or secret managers — not committed config
- **`fetch_sqlmodel`** / **`write_sqlmodel`** use your app's DB credentials; pydantable does not store them
- Use least-privilege DB roles for read-only analytics routes

## Error handling in APIs

Register **`pydantable.fastapi.register_exception_handlers`** so:

- **`MissingRustExtensionError`** → **503** (misconfiguration)
- **`ColumnLengthMismatchError`** → **400** (malformed columnar body)
- Pydantic **`ValidationError`** → **422**

## Async and resource limits

- **`await acollect()`** does not cancel in-flight native work on client disconnect — use timeouts and reverse-proxy limits
- Size caps on uploads and materialized responses prevent memory exhaustion

## Dependency updates

Pin **`pydantable`**, **`pydantable-native`**, and **`pydantable-protocol`** to the same release. Monitor [Changelog](changelog.md) for security-related dependency bumps.

Reporting vulnerabilities: [SECURITY.md](https://github.com/eddiethedean/pydantable/blob/main/SECURITY.md) in the repository root.
