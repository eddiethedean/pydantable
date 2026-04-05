from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


def http_server_thread(
    handler_cls: type[BaseHTTPRequestHandler],
) -> tuple[HTTPServer, threading.Thread]:
    """Start a daemon thread serving ``handler_cls`` on a loopback port (for tests)."""
    server = HTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread
