"""Smoke-test the documented `docs/examples/fastapi/service_layout` app."""

from __future__ import annotations

import importlib.util
import sys

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from tests._support.paths import repo_root


def test_service_layout_health_and_columnar_ingest() -> None:
    root = repo_root()
    layout = root / "docs/examples/fastapi/service_layout"
    path_s = str(layout.resolve())
    inserted = path_s not in sys.path
    if inserted:
        sys.path.insert(0, path_s)
    try:
        spec = importlib.util.spec_from_file_location(
            "_pydantable_docs_service_layout_main",
            layout / "main.py",
        )
        assert spec and spec.loader
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        with TestClient(mod.app) as client:
            live = client.get("/health/live")
            assert live.status_code == 200
            body = live.json()
            assert body["status"] == "ok"
            assert body["service"] == "pydantable-example-layout"

            r = client.post(
                "/ingest/columnar",
                json={
                    "user_id": [1, 2, 3],
                    "email": ["ada@example.com", "bob@example.org", "c@example.net"],
                    "score": [98.2, None, 12.0],
                },
            )
            assert r.status_code == 200
            assert r.json() == {
                "user_id": [1, 2, 3],
                "email": ["ada@example.com", "bob@example.org", "c@example.net"],
                "score": [98.2, None, 12.0],
            }

            bad = client.post(
                "/ingest/columnar",
                json={"user_id": [1, 2], "email": ["a@b.co"], "score": [1.0, 2.0]},
            )
            assert bad.status_code == 400
            assert "same length" in bad.json()["detail"]
    finally:
        if inserted and path_s in sys.path:
            sys.path.remove(path_s)
