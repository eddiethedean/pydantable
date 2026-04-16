# FastAPI: settings with pydantic-settings

Use **`pydantic-settings`** (`BaseSettings`) for deployment configuration (data directories,
database URLs, executor sizing) and wire values into FastAPI’s **`lifespan`** alongside
`pydantable.fastapi.executor_lifespan`.

## Install

```bash
pip install "pydantable[fastapi]" pydantic-settings
```

`pydantic-settings` is **not** a runtime dependency of `pydantable`; add it in your service.

## Recipe

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic_settings import BaseSettings, SettingsConfigDict

from pydantable.fastapi import executor_lifespan


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MYAPP_")

    data_dir: str = "/var/data"
    sql_url: str = "sqlite:///./app.db"
    executor_max_workers: int | None = None


settings = Settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with executor_lifespan(
        app,
        max_workers=settings.executor_max_workers,
        thread_name_prefix="pydantable",
    ):
        yield


app = FastAPI(lifespan=lifespan)
```

Read paths and URLs from **`Settings`** in route dependencies or services; pass
**`executor_max_workers`** (or `None` for the default pool size) to match worker capacity.

Example environment (with **`MYAPP_`** prefix from the recipe above):

```bash
export MYAPP_DATA_DIR=/data/incoming
export MYAPP_SQL_URL=postgresql+psycopg://user:pass@db:5432/app
export MYAPP_EXECUTOR_MAX_WORKERS=8
```

Tune **`executor_max_workers`** from CPU count, expected concurrent long-running
**`acollect`** / eager **`pydantable`** I/O calls, and process memory — not “as high as possible.”

## Pitfalls

- **Secrets:** use env vars or a secret manager; do not commit credentials.
- **Executor size:** too many threads vs. CPU cores can hurt latency; tune per deployment.

## See also

- [GOLDEN_PATH_FASTAPI](/integrations/fastapi/golden-path/)
- [FASTAPI](/integrations/fastapi/fastapi/)
