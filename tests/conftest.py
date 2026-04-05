from __future__ import annotations

from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from tests._support.http import http_server_thread
from tests._support.tables import (
    assert_table_eq_sorted,
    sort_rows_by_keys,
)

# Backward-compatible re-exports for tests that still import from ``conftest``.
__all__ = [
    "_keys_for_sort",
    "assert_table_eq_sorted",
    "http_serve",
    "http_server_thread",
    "small_two_int_column_dict",
    "sort_rows_by_keys",
]


@pytest.fixture
def http_serve():
    """Return a callable ``http_serve(Handler)`` → base URL; teardown closes servers."""
    servers: list[HTTPServer] = []

    def _serve(handler_cls: type[BaseHTTPRequestHandler]) -> str:
        server, _ = http_server_thread(handler_cls)
        servers.append(server)
        return f"http://127.0.0.1:{server.server_port}"

    yield _serve
    for srv in servers:
        srv.shutdown()
        srv.server_close()


@pytest.fixture
def _keys_for_sort() -> list[str]:
    # Small fixture for future reuse.
    return ["id"]


@pytest.fixture
def small_two_int_column_dict() -> dict[str, list[int]]:
    """Shared columnar payload for contract tests (id + v)."""
    return {"id": [1, 2, 3], "v": [10, 20, 30]}
