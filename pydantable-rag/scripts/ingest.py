from __future__ import annotations

from app.main import IngestRequest, ingest


if __name__ == "__main__":
    resp = ingest(IngestRequest(paths=None))
    print(resp)
