---
orphan: true
---

# NOTE: This file is rendered for realism but not linked from toctrees.

# Example FastAPI service layout

Copy this folder as a starting point for a **multi-router** app that uses:

- **`executor_lifespan`** + **`register_exception_handlers`** in `main.py`
- **`routers/`** for health and batch ingest
- **`columnar_dependency`** on a small **`UserBatch`** schema (IDs, emails, optional scores)

## Run

```bash
cd docs/examples/fastapi/service_layout
pip install "pydantable[fastapi]" uvicorn
uvicorn main:app --reload
```

## Try it

```bash
curl -sS http://127.0.0.1:8000/health/live
curl -sS -X POST http://127.0.0.1:8000/ingest/columnar \
  -H 'Content-Type: application/json' \
  -d '{"user_id":[10,20],"email":["ada@example.com","bob@example.org"],"score":[99.5,null]}'
```

Expected output (example):

```text
{"status":"ok","service":"pydantable-example-layout"}
{"email":["ada@example.com","bob@example.org"],"user_id":[10,20],"score":[99.5,null]}
```

OpenAPI: `http://127.0.0.1:8000/docs`

This tree is **not** an installable package; run **`uvicorn`** from **this directory** (or put this folder on **`PYTHONPATH`**) so `routers` imports resolve.

 **`ColumnLengthMismatchError`** (mismatched column lengths) returns **400** when lengths disagree across columns—handled by **`register_exception_handlers`** in `main.py`.
