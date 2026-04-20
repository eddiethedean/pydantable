from __future__ import annotations

import asyncio
import html
import logging
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast

from fastapi import BackgroundTasks, Body, FastAPI, HTTPException
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel, Field
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address
from starlette.requests import Request

from app.openai_env import openai_api_key_configured
from app.rag.embeddings import (
    embed_deployment_ready,
    embedder_is_loading,
    embedding_compute_active,
    release_embedder_models,
)
from app.rag.ingest import ingest_repo_docs
from app.rag.llm import ChatMessage
from app.rag.pipeline import rag_chat
from app.rag.store import check_vector_backend, get_counts
from app.rtd_links import source_to_readthedocs_url
from app.settings import (
    Settings,
    get_settings,
    resolve_db_path,
    resolve_ingest_repo_root,
)
from app.version import app_version

_log = logging.getLogger(__name__)


def _uses_openai_llm(s: Settings) -> bool:
    return s.llm_backend == "openai"


def _generative_llm_ready(s: Settings) -> bool:
    """Generative chat ready (OpenAI) or not needed (extractive)."""
    if _uses_openai_llm(s):
        return openai_api_key_configured()
    return True


def _healthz_llm_loaded(s: Settings) -> bool:
    """``llm_loaded``: OpenAI chat key present when using generative backend."""
    if _uses_openai_llm(s):
        return openai_api_key_configured()
    return True


@asynccontextmanager
async def lifespan(_app: FastAPI):
    s = get_settings()
    want_ingest = s.auto_ingest_on_startup
    dbp = resolve_db_path(s.db_path)
    counts = get_counts(db_path=dbp)
    if want_ingest:
        if s.auto_ingest_if_db_empty and counts["docs"] > 0 and counts["vecs"] > 0:
            want_ingest = False

    if want_ingest:
        _log.info(
            "pydantable-rag: startup ingest want_ingest=%s blocking=%s docs=%s vecs=%s",
            want_ingest,
            s.blocking_startup_warmup,
            counts.get("docs"),
            counts.get("vecs"),
        )

    def _ingest_sync() -> None:
        rr = resolve_ingest_repo_root()
        try:
            ingest_repo_docs(settings=s, repo_root=rr, paths=None)
        except Exception:
            _log.exception("pydantable-rag: startup ingest failed")

    if want_ingest:
        if s.blocking_startup_warmup:
            try:
                await asyncio.to_thread(_ingest_sync)
            except Exception:
                _log.exception("pydantable-rag: startup blocking ingest failed")
        else:
            threading.Thread(
                target=_ingest_sync, name="pydantable-rag-ingest", daemon=True
            ).start()

    yield


# Per-IP limits (``slowapi`` / ``limits`` syntax). Tunable via ``.env``:
# ``RATELIMIT_ENABLED``, ``RATELIMIT_DEFAULT``, ``RATELIMIT_HEADERS_ENABLED``.
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["100/minute"],
    headers_enabled=True,
)

app = FastAPI(title="pydantable-rag", lifespan=lifespan)
app.state.limiter = limiter


async def _rate_limit_handler(request: Request, exc: Exception) -> Response:
    return _rate_limit_exceeded_handler(request, cast(RateLimitExceeded, exc))


app.add_exception_handler(RateLimitExceeded, _rate_limit_handler)
app.add_middleware(SlowAPIMiddleware)


def _ingest_then_release_embedder(
    *, settings: Settings, repo_root: Path, paths: list[str] | None
) -> None:
    ingest_repo_docs(settings=settings, repo_root=repo_root, paths=paths)
    release_embedder_models()


def _bootstrap_ingest(
    *, settings: Settings, repo_root: Path, paths: list[str] | None
) -> None:
    try:
        dbp = resolve_db_path(settings.db_path)
        counts = get_counts(db_path=dbp)
        # Do not re-ingest when the image already ships a populated index — full
        # ingest starts with ``reset_db`` and would wipe SQLite on each replica.
        if paths is None and counts.get("docs", 0) > 0 and counts.get("vecs", 0) > 0:
            return
        _ingest_then_release_embedder(
            settings=settings, repo_root=repo_root, paths=paths
        )
    except Exception:
        _log.exception("pydantable-rag: POST /bootstrap background task failed")


class BootstrapResponse(BaseModel):
    ok: bool
    started: list[str]


class HealthzResponse(BaseModel):
    ok: bool
    version: str
    db_path: str
    embed_model: str
    embed_dims: int
    llm_backend: str = Field(
        description="openai = OpenAI chat API; extractive = chunks only.",
    )
    llm_model: str
    llm_loaded: bool
    llm_loading: bool = Field(
        description="True while the LLM is downloading or initializing in-process.",
    )
    embed_loaded: bool = Field(
        description="True when OPENAI_API_KEY is set (OpenAI embeddings API).",
    )
    embed_loading: bool = Field(
        description="True while the embedding model is downloading or initializing.",
    )
    embed_computing: bool = Field(
        description="True while a forward pass is computing vectors (ingest or chat).",
    )


class VectorBackendStatus(BaseModel):
    ok: bool
    backend: str | None = None
    error: str | None = None


class CountsStatus(BaseModel):
    docs: int
    vecs: int
    backend: str | None = None
    uninitialized: bool | None = None
    error: str | None = None


class ReadyzResponse(BaseModel):
    ok: bool
    counts: CountsStatus
    llm_backend: str
    llm_loaded: bool
    llm_loading: bool
    embed_loaded: bool
    embed_loading: bool
    embed_computing: bool
    db_path: str


class DiagResponse(BaseModel):
    version: str
    db_path: str
    vector_backend: VectorBackendStatus
    counts: CountsStatus
    llm_backend: str
    llm_loaded: bool
    llm_loading: bool
    llm_last_error: str | None = None
    embed_loaded: bool
    embed_loading: bool
    embed_computing: bool


class ChatRequest(BaseModel):
    message: str = Field(min_length=1)
    history: list[ChatMessage] | None = None


class ChatSourceItem(BaseModel):
    source: str
    chunk_id: str
    distance: float
    url: str | None = Field(
        default=None,
        description="Read the Docs page when the ingest path maps to MkDocs HTML.",
    )


class ChatResponse(BaseModel):
    answer: str
    sources: list[ChatSourceItem]


class IngestRequest(BaseModel):
    paths: list[str] | None = None


_INGEST_BODY = Body(default_factory=IngestRequest)


def _status_page_html() -> str:
    """Human-readable loading / readiness view for ``GET /``."""
    s = get_settings()
    dbp = resolve_db_path(s.db_path)
    counts = get_counts(db_path=dbp)
    n_docs = int(counts.get("docs", 0) or 0)
    n_vecs = int(counts.get("vecs", 0) or 0)
    embed_load = embedder_is_loading(s.embed_model, s.embed_dims)
    embed_ok = embed_deployment_ready(s.embed_model, s.embed_dims)
    embed_busy = embedding_compute_active()
    llm_load = False
    llm_ok = _healthz_llm_loaded(s)
    index_ok = n_docs > 0 and n_vecs > 0
    oai_llm = _uses_openai_llm(s)
    chat_ready = index_ok and openai_api_key_configured() and _generative_llm_ready(s)

    def row(label: str, ok: bool, loading: bool, detail: str) -> str:
        if loading:
            state = '<span class="load">Loading…</span>'
        elif ok:
            state = '<span class="ok">Ready</span>'
        else:
            state = '<span class="wait">Waiting</span>'
        detail_e = html.escape(detail, quote=True)
        return f"<tr><td>{label}</td><td>{state}</td><td>{detail_e}</td></tr>"

    index_loading = (not index_ok) and (embed_busy or embed_load)

    rows = [
        row(
            "Embedding model",
            embed_ok,
            embed_load,
            f"{s.embed_model} ({s.embed_dims}d)",
        ),
        (
            row(
                "LLM (chat)",
                llm_ok,
                llm_load,
                f"OpenAI API · {s.llm_model}",
            )
            if oai_llm
            else row(
                "LLM (chat)",
                True,
                False,
                "extractive mode — no generative chat model",
            )
        ),
        row(
            "Vector index",
            index_ok,
            index_loading,
            f"{n_docs} doc chunks, {n_vecs} vectors",
        ),
    ]

    overall = (
        '<p class="ok"><strong>Ready for chat.</strong></p>'
        if chat_ready
        else '<p class="load"><strong>Still starting up.</strong> This page refreshes '
        "every 5s.</p>"
    )
    if embed_busy and index_ok:
        overall += '<p class="note">Embedding compute active (ingest or query).</p>'

    ver = html.escape(app_version(), quote=True)
    db_esc = html.escape(str(dbp), quote=True)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta http-equiv="refresh" content="5"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>pydantable-rag status</title>
  <style>
    body {{ font-family: system-ui, sans-serif; max-width: 52rem; margin: 2rem auto;
      padding: 0 1rem; line-height: 1.45; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ text-align: left; border-bottom: 1px solid #ccc;
      padding: 0.5rem 0.4rem; }}
    .ok {{ color: #0a0; }}
    .load {{ color: #a60; }}
    .wait {{ color: #666; }}
    .note {{ color: #555; font-size: 0.95rem; }}
    a {{ color: #06c; }}
  </style>
</head>
<body>
  <h1>pydantable-rag</h1>
  {overall}
  <table>
    <thead><tr><th>Component</th><th>State</th><th>Detail</th></tr></thead>
    <tbody>{"".join(rows)}</tbody>
  </table>
  <p class="note">Version {ver} · DB <code>{db_esc}</code></p>
  <p><a href="/chat-app">Chat</a> · <a href="/docs">OpenAPI docs</a> ·
  <a href="/healthz">/healthz</a> ·
  <a href="/readyz">/readyz</a> · <a href="/diag">/diag</a></p>
</body>
</html>"""


_CHAT_APP_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <meta name="color-scheme" content="dark"/>
  <title>pydantable-rag · Chat</title>
  <script
    src="https://cdn.jsdelivr.net/npm/marked@12.0.2/marked.min.js"
    crossorigin="anonymous"
  ></script>
  <script
    src="https://cdn.jsdelivr.net/npm/dompurify@3.1.7/dist/purify.min.js"
    crossorigin="anonymous"
  ></script>
  <link
    rel="stylesheet"
    href="https://cdn.jsdelivr.net/gh/highlightjs/cdn-release@11.10.0/build/styles/github-dark.min.css"
    crossorigin="anonymous"
  />
  <script
    src="https://cdn.jsdelivr.net/gh/highlightjs/cdn-release@11.10.0/build/highlight.min.js"
    crossorigin="anonymous"
  ></script>
  <style>
    :root {
      --bg: #212121;
      --surface: #2f2f2f;
      --surface-hover: #3a3a3a;
      --border: rgba(255,255,255,0.1);
      --text: #ececec;
      --text-muted: #9b9b9b;
      --accent: #10a37f;
      --accent-dim: #0d8c6d;
      --user-bubble: #2f2f2f;
      --assistant-fg: #d1d5db;
      --danger: #f87171;
      --radius: 1.25rem;
      --radius-sm: 0.75rem;
      --font: "Söhne", ui-sans-serif, system-ui, -apple-system, "Segoe UI",
        Roboto, Helvetica, Arial, sans-serif;
    }
    * { box-sizing: border-box; }
    html, body { height: 100%; margin: 0; }
    body {
      font-family: var(--font);
      font-size: 15px;
      line-height: 1.6;
      color: var(--text);
      background: var(--bg);
      -webkit-font-smoothing: antialiased;
    }
    #app {
      display: flex;
      flex-direction: column;
      min-height: 100dvh;
      max-width: 48rem;
      margin: 0 auto;
    }
    .topbar {
      flex-shrink: 0;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 0.75rem;
      padding: 0.65rem 1rem;
      border-bottom: 1px solid var(--border);
      background: rgba(33,33,33,0.85);
      backdrop-filter: blur(8px);
      position: sticky;
      top: 0;
      z-index: 10;
    }
    .brand {
      font-weight: 600;
      font-size: 0.95rem;
      letter-spacing: -0.02em;
    }
    .topbar a {
      color: var(--text-muted);
      text-decoration: none;
      font-size: 0.8rem;
    }
    .topbar a:hover { color: var(--text); }
    .topbar-actions { display: flex; align-items: center; gap: 0.85rem; }
    #clear {
      font: inherit;
      font-size: 0.8rem;
      color: var(--text-muted);
      background: transparent;
      border: 1px solid var(--border);
      border-radius: 0.4rem;
      padding: 0.35rem 0.65rem;
      cursor: pointer;
    }
    #clear:hover {
      color: var(--text);
      background: var(--surface);
    }
    #thread {
      flex: 1;
      overflow-y: auto;
      scroll-behavior: smooth;
      padding: 1.25rem 1rem 0.5rem;
    }
    #thread.has-chat #empty { display: none; }
    #messages { padding-bottom: 0.25rem; }
    #empty {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      min-height: min(42vh, 320px);
      text-align: center;
      padding: 1rem;
    }
    #empty h2 {
      margin: 0 0 0.35rem;
      font-size: 1.5rem;
      font-weight: 600;
      letter-spacing: -0.03em;
      color: var(--text);
    }
    #empty p {
      margin: 0 0 1.25rem;
      color: var(--text-muted);
      font-size: 0.9rem;
      max-width: 22rem;
    }
    .starters {
      display: flex;
      flex-wrap: wrap;
      gap: 0.5rem;
      justify-content: center;
      max-width: 36rem;
    }
    .starter {
      padding: 0.55rem 0.85rem;
      font: inherit;
      font-size: 0.82rem;
      color: var(--text);
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 999px;
      cursor: pointer;
      transition: background 0.15s, border-color 0.15s;
    }
    .starter:hover {
      background: var(--surface-hover);
      border-color: rgba(255,255,255,0.15);
    }
    .turn {
      display: flex;
      gap: 0.75rem;
      margin-bottom: 1.35rem;
      animation: fadeIn 0.25s ease;
    }
    @keyframes fadeIn {
      from { opacity: 0; transform: translateY(4px); }
      to { opacity: 1; transform: none; }
    }
    .turn.user { flex-direction: row-reverse; }
    .avatar {
      flex-shrink: 0;
      width: 2rem;
      height: 2rem;
      border-radius: 0.35rem;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 0.65rem;
      font-weight: 700;
      letter-spacing: 0.02em;
    }
    .turn.user .avatar {
      background: linear-gradient(145deg, #5b8cff, #3b5bdb);
      color: #fff;
    }
    .turn.assistant .avatar {
      background: var(--accent);
      color: #fff;
    }
    .block {
      flex: 1;
      min-width: 0;
      max-width: 100%;
    }
    .turn.user .block { display: flex; justify-content: flex-end; }
    .bubble {
      display: inline-block;
      max-width: min(100%, 34rem);
      padding: 0.75rem 1rem;
      border-radius: var(--radius);
      white-space: pre-wrap;
      word-break: break-word;
    }
    .turn.user .bubble {
      background: var(--user-bubble);
      border: 1px solid var(--border);
      border-radius: var(--radius) var(--radius-sm) var(--radius-sm) var(--radius);
    }
    .turn.assistant .bubble {
      padding-left: 0;
      color: var(--assistant-fg);
      line-height: 1.65;
    }
    .turn.assistant .bubble.md {
      white-space: normal;
    }
    .turn.assistant .bubble.md :where(p, ul, ol, pre, h3, h4) {
      margin: 0 0 0.65rem;
    }
    .turn.assistant .bubble.md pre {
      background: rgba(0,0,0,0.35);
      border: 1px solid var(--border);
      border-radius: 0.55rem;
      padding: 0.75rem 0.85rem;
      overflow-x: auto;
      font-size: 0.82rem;
      line-height: 1.45;
    }
    .turn.assistant .bubble.md code {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 0.84em;
    }
    .turn.assistant .bubble.md p > code,
    .turn.assistant .bubble.md li > code {
      background: rgba(0,0,0,0.35);
      padding: 0.12rem 0.35rem;
      border-radius: 0.3rem;
    }
    .turn.assistant .bubble.md pre code {
      background: transparent;
      padding: 0;
      font-size: inherit;
    }
    /* highlight.js (fenced blocks): keep our pre shell; spans carry token colors */
    .turn.assistant .bubble.md pre code.hljs {
      background: transparent !important;
    }
    .turn.assistant .bubble.md ul,
    .turn.assistant .bubble.md ol {
      padding-left: 1.25rem;
    }
    .turn.assistant .bubble.md h3 {
      font-size: 1rem;
      font-weight: 600;
      margin: 1rem 0 0.5rem;
      letter-spacing: -0.02em;
      color: var(--text);
    }
    .turn.assistant .bubble.md h3:first-child {
      margin-top: 0;
    }
    .turn.assistant .bubble.md hr {
      border: none;
      border-top: 1px solid var(--border);
      margin: 0.85rem 0;
    }
    .turn.assistant .bubble.md blockquote {
      margin: 0 0 0.65rem;
      padding-left: 0.75rem;
      border-left: 3px solid var(--border);
      color: var(--text-muted);
    }
    .turn.assistant .bubble.md a {
      color: #6ee7b7;
      text-decoration: underline;
      text-underline-offset: 2px;
    }
    .sources {
      margin-top: 0.65rem;
      padding: 0.65rem 0.75rem;
      font-size: 0.78rem;
      color: var(--text-muted);
      background: rgba(0,0,0,0.25);
      border: 1px solid var(--border);
      border-radius: var(--radius-sm);
    }
    .sources strong { color: var(--text-muted); font-weight: 600; }
    .sources ul { margin: 0.35rem 0 0 1rem; padding: 0; }
    .sources li { margin-bottom: 0.2rem; }
    .sources a {
      color: #6ee7b7;
      text-decoration: underline;
      text-underline-offset: 2px;
    }
    .sources a:hover { color: #a7f3d0; }
    .err {
      color: var(--danger);
      font-size: 0.88rem;
      padding: 0.5rem 0.75rem;
      background: rgba(248,113,113,0.08);
      border-radius: var(--radius-sm);
      margin-bottom: 0.75rem;
    }
    .assistant-actions {
      display: flex;
      justify-content: flex-end;
      align-items: center;
      gap: 0.35rem;
      margin: 0 0 0.35rem;
      min-height: 1.5rem;
    }
    .copy-btn {
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      font: inherit;
      font-size: 0.72rem;
      font-weight: 500;
      color: var(--text-muted);
      background: rgba(255,255,255,0.06);
      border: 1px solid var(--border);
      border-radius: 0.4rem;
      padding: 0.3rem 0.5rem;
      cursor: pointer;
      transition: background 0.15s, color 0.15s, border-color 0.15s;
    }
    .copy-btn:hover {
      color: var(--text);
      background: rgba(255,255,255,0.1);
      border-color: rgba(255,255,255,0.14);
    }
    .copy-btn:focus-visible {
      outline: 2px solid var(--accent);
      outline-offset: 2px;
    }
    .copy-btn svg {
      flex-shrink: 0;
      opacity: 0.9;
    }
    .copy-btn.copied {
      color: #6ee7b7;
      border-color: rgba(110,231,183,0.35);
    }
    .copy-reply .copy-label {
      line-height: 1;
    }
    .code-block-wrap {
      position: relative;
      margin: 0 0 0.65rem;
    }
    .turn.assistant .bubble.md .code-block-wrap pre {
      margin-bottom: 0;
    }
    .code-block-wrap .copy-code {
      position: absolute;
      top: 0.45rem;
      right: 0.45rem;
      z-index: 2;
      padding: 0.3rem;
      opacity: 0.85;
    }
    @media (hover: hover) {
      .code-block-wrap .copy-code {
        opacity: 0;
      }
      .code-block-wrap:hover .copy-code,
      .code-block-wrap:focus-within .copy-code {
        opacity: 1;
      }
    }
    .composer-wrap {
      flex-shrink: 0;
      padding: 0.65rem 1rem 1rem;
      background: linear-gradient(to top, var(--bg) 70%, transparent);
    }
    .composer {
      display: flex;
      align-items: flex-end;
      gap: 0.5rem;
      padding: 0.45rem 0.55rem 0.45rem 0.85rem;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: 0 4px 24px rgba(0,0,0,0.25);
    }
    .composer:focus-within {
      border-color: rgba(255,255,255,0.18);
    }
    #q {
      flex: 1;
      min-height: 2.5rem;
      max-height: 12rem;
      padding: 0.5rem 0;
      font: inherit;
      color: var(--text);
      background: transparent;
      border: none;
      outline: none;
      resize: none;
      line-height: 1.5;
    }
    #q::placeholder { color: var(--text-muted); }
    #send {
      flex-shrink: 0;
      width: 2.35rem;
      height: 2.35rem;
      padding: 0;
      display: flex;
      align-items: center;
      justify-content: center;
      border: none;
      border-radius: 0.5rem;
      background: var(--accent);
      color: #fff;
      cursor: pointer;
      transition: background 0.15s, transform 0.1s;
    }
    #send:hover:not(:disabled) { background: var(--accent-dim); }
    #send:active:not(:disabled) { transform: scale(0.96); }
    #send:disabled {
      opacity: 0.35;
      cursor: not-allowed;
    }
    .fineprint {
      margin: 0.65rem 0 0;
      text-align: center;
      font-size: 0.72rem;
      color: var(--text-muted);
      line-height: 1.4;
    }
    .fineprint code { font-size: 0.7rem; opacity: 0.9; }
    .fineprint a { color: var(--text-muted); }
    .fineprint a:hover { color: var(--text); }
    .visually-hidden {
      position: absolute; width: 1px; height: 1px; padding: 0;
      margin: -1px; overflow: hidden; clip: rect(0,0,0,0); border: 0;
    }
  </style>
</head>
<body>
  <div id="app">
    <header class="topbar">
      <span class="brand">pydantable-rag</span>
      <div class="topbar-actions">
        <button type="button" id="clear" title="New chat">New chat</button>
        <a href="/">Status</a>
        <a href="/docs">API</a>
      </div>
    </header>
    <main id="thread">
      <div id="empty">
        <h2>Ask about pydantable</h2>
        <p>Documentation-grounded answers. Follow-up questions use your thread
        history with <code style="color:var(--text-muted)">POST /chat</code>.</p>
        <div class="starters" id="starters"></div>
      </div>
      <div id="messages" aria-live="polite"></div>
    </main>
    <footer class="composer-wrap">
      <label class="visually-hidden" for="q">Message</label>
      <div class="composer">
        <textarea id="q" rows="1" placeholder="Message pydantable-rag…"></textarea>
        <button type="button" id="send" aria-label="Send message" disabled>
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none"
            stroke="currentColor" stroke-width="2" stroke-linecap="round"
            aria-hidden="true">
            <path d="M12 19V5M5 12l7-7 7 7"/>
          </svg>
        </button>
      </div>
      <p class="fineprint">__VERSION__ · Retrieval RAG ·
      <a href="/docs">OpenAPI</a></p>
    </footer>
  </div>
  <script>
(function () {
  var thread = document.getElementById("thread");
  var messages = document.getElementById("messages");
  var input = document.getElementById("q");
  var sendBtn = document.getElementById("send");
  var history = [];
  var starterQs = [
    "What is pydantable?",
    "How do I create a DataFrame?",
    "Explain Schema validation",
    "What is Expr used for?"
  ];

  function wireStarters() {
    var startersEl = document.getElementById("starters");
    if (!startersEl) return;
    startersEl.innerHTML = "";
    starterQs.forEach(function (q) {
      var b = document.createElement("button");
      b.type = "button";
      b.className = "starter";
      b.textContent = q;
      b.addEventListener("click", function () {
        input.value = q;
        input.focus();
        autosize();
        updateSendState();
      });
      startersEl.appendChild(b);
    });
  }
  wireStarters();

  function autosize() {
    input.style.height = "auto";
    input.style.height = Math.min(input.scrollHeight, 192) + "px";
  }
  input.addEventListener("input", function () {
    autosize();
    updateSendState();
  });

  function updateSendState() {
    sendBtn.disabled = input.value.trim().length === 0;
  }

  function esc(t) {
    var d = document.createElement("div");
    d.textContent = t;
    return d.innerHTML;
  }

  function escAttr(t) {
    return String(t)
      .replace(/&/g, "&amp;")
      .replace(/"/g, "&quot;")
      .replace(/</g, "&lt;");
  }

  function renderAssistantMarkdown(raw) {
    if (typeof marked === "undefined" || typeof DOMPurify === "undefined") {
      return esc(raw).replace(/\\n/g, "<br/>");
    }
    try {
      marked.setOptions({ gfm: true, breaks: false });
      var html = marked.parse(raw, { async: false });
      return DOMPurify.sanitize(html);
    } catch (e) {
      return esc(raw).replace(/\\n/g, "<br/>");
    }
  }

  function applySyntaxHighlight(root) {
    if (typeof hljs === "undefined" || !hljs.highlightElement) return;
    root.querySelectorAll("pre code").forEach(function (el) {
      try {
        hljs.highlightElement(el);
      } catch (e) {}
    });
  }

  function clipboardIconSvg() {
    return (
      '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" ' +
      'stroke="currentColor" stroke-width="2" stroke-linecap="round" ' +
      'stroke-linejoin="round" aria-hidden="true">' +
      '<rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>' +
      '<path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>' +
      "</svg>"
    );
  }

  function copyToClipboard(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      return navigator.clipboard.writeText(text);
    }
    return new Promise(function (resolve, reject) {
      var ta = document.createElement("textarea");
      ta.value = text;
      ta.setAttribute("readonly", "");
      ta.style.position = "fixed";
      ta.style.left = "-9999px";
      document.body.appendChild(ta);
      ta.select();
      try {
        if (document.execCommand("copy")) resolve();
        else reject(new Error("copy failed"));
      } catch (err) {
        reject(err);
      } finally {
        document.body.removeChild(ta);
      }
    });
  }

  function flashCopyButton(btn) {
    var prev = btn.getAttribute("aria-label") || "";
    btn.setAttribute("aria-label", "Copied!");
    btn.classList.add("copied");
    window.setTimeout(function () {
      if (prev) btn.setAttribute("aria-label", prev);
      btn.classList.remove("copied");
    }, 2000);
  }

  function wrapCodeBlocksInBubble(bubble) {
    var pres = bubble.querySelectorAll("pre");
    for (var i = 0; i < pres.length; i++) {
      var pre = pres[i];
      if (pre.closest(".code-block-wrap")) continue;
      var wrap = document.createElement("div");
      wrap.className = "code-block-wrap";
      pre.parentNode.insertBefore(wrap, pre);
      wrap.appendChild(pre);
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "copy-btn copy-code";
      btn.setAttribute("aria-label", "Copy code");
      btn.title = "Copy code";
      btn.innerHTML = clipboardIconSvg();
      btn.addEventListener("click", function (ev) {
        ev.stopPropagation();
        var w = ev.currentTarget.closest(".code-block-wrap");
        if (!w) return;
        var p = w.querySelector("pre");
        var code = p ? p.querySelector("code") : null;
        var t = code ? code.innerText : p ? p.innerText : "";
        copyToClipboard(t).then(
          function () {
            flashCopyButton(ev.currentTarget);
          },
          function () {}
        );
      });
      wrap.appendChild(btn);
    }
  }

  function scrollToBottom() {
    thread.scrollTop = thread.scrollHeight;
  }

  function appendToThread(node) {
    messages.appendChild(node);
  }

  function setHasChat() {
    thread.classList.add("has-chat");
  }

  function addBubble(role, text, sources) {
    setHasChat();
    var turn = document.createElement("div");
    turn.className = "turn " + (role === "user" ? "user" : "assistant");
    var av = document.createElement("div");
    av.className = "avatar";
    av.setAttribute("aria-hidden", "true");
    av.textContent = role === "user" ? "You" : "AI";
    var block = document.createElement("div");
    block.className = "block";
    var bubble = document.createElement("div");
    bubble.className = "bubble" + (role === "assistant" ? " md" : "");
    if (role === "user") {
      bubble.innerHTML = esc(text).replace(/\\n/g, "<br/>");
      var inner = document.createElement("div");
      inner.appendChild(bubble);
      block.appendChild(inner);
    } else {
      bubble.innerHTML = renderAssistantMarkdown(text);
      applySyntaxHighlight(bubble);
      wrapCodeBlocksInBubble(bubble);
      var actions = document.createElement("div");
      actions.className = "assistant-actions";
      var copyReply = document.createElement("button");
      copyReply.type = "button";
      copyReply.className = "copy-btn copy-reply";
      copyReply.setAttribute("aria-label", "Copy response");
      copyReply.title = "Copy response";
      copyReply.innerHTML =
        clipboardIconSvg() + '<span class="copy-label">Copy</span>';
      copyReply.addEventListener("click", function () {
        copyToClipboard(text).then(
          function () {
            flashCopyButton(copyReply);
          },
          function () {}
        );
      });
      actions.appendChild(copyReply);
      block.appendChild(actions);
      block.appendChild(bubble);
    }
    if (sources && sources.length) {
      var src = document.createElement("div");
      src.className = "sources";
      var parts = ["<strong>Sources</strong><ul>"];
      for (var i = 0; i < sources.length; i++) {
        var s = sources[i];
        var dist = typeof s.distance === "number" ? s.distance.toFixed(4) : "";
        var label = esc(s.source || "");
        var line;
        if (s.url) {
          line =
            '<a href="' +
            escAttr(s.url) +
            '" target="_blank" rel="noopener noreferrer">' +
            label +
            "</a>";
        } else {
          line = label;
        }
        if (dist) line += " · " + dist;
        parts.push("<li>" + line + "</li>");
      }
      parts.push("</ul>");
      src.innerHTML = parts.join("");
      block.appendChild(src);
    }
    turn.appendChild(av);
    turn.appendChild(block);
    appendToThread(turn);
    scrollToBottom();
  }

  function addError(msg) {
    setHasChat();
    var e = document.createElement("p");
    e.className = "err";
    e.textContent = msg;
    appendToThread(e);
    scrollToBottom();
  }

  async function sendMessage() {
    var text = input.value.trim();
    if (!text) return;
    input.value = "";
    autosize();
    updateSendState();
    addBubble("user", text);
    sendBtn.disabled = true;
    try {
      var res = await fetch("/chat", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({ message: text, history: history }),
      });
      var ct = res.headers.get("content-type") || "";
      var data = null;
      if (ct.indexOf("application/json") !== -1) {
        data = await res.json();
      } else {
        addError((await res.text()) || res.statusText);
        return;
      }
      if (!res.ok) {
        var detail =
          data && data.detail
            ? typeof data.detail === "string"
              ? data.detail
              : JSON.stringify(data.detail)
            : res.status + " " + res.statusText;
        addError(detail);
        return;
      }
      history.push({ role: "user", content: text });
      history.push({ role: "assistant", content: data.answer });
      if (history.length > 24) history = history.slice(-24);
      addBubble("assistant", data.answer, data.sources);
    } catch (err) {
      addError(err && err.message ? err.message : "Request failed");
    } finally {
      updateSendState();
    }
  }

  document.getElementById("send").addEventListener("click", sendMessage);
  document.getElementById("clear").addEventListener("click", function () {
    history = [];
    thread.classList.remove("has-chat");
    messages.innerHTML = "";
    wireStarters();
    input.focus();
  });
  input.addEventListener("keydown", function (ev) {
    if (ev.key === "Enter" && !ev.shiftKey) {
      ev.preventDefault();
      if (!sendBtn.disabled) sendMessage();
    }
  });
  updateSendState();
  input.focus();
})();
  </script>
</body>
</html>
"""


def _chat_app_html() -> str:
    return _CHAT_APP_HTML.replace(
        "__VERSION__",
        html.escape(app_version(), quote=True),
    )


@app.get("/", response_class=HTMLResponse)
def root_status() -> str:
    """Browser-friendly view of embed / LLM / index loading state (auto-refresh)."""
    return _status_page_html()


@app.get("/chat-app", response_class=HTMLResponse)
def chat_app() -> str:
    """Minimal browser UI that calls ``POST /chat``."""
    return _chat_app_html()


@app.post("/bootstrap")
@limiter.limit("20/minute")
def bootstrap(background_tasks: BackgroundTasks, request: Request) -> BootstrapResponse:
    """
    Kick off ingestion without blocking the request (hosted cold starts).
    """
    s = get_settings()
    repo_root = resolve_ingest_repo_root()
    dbp = resolve_db_path(s.db_path)
    counts = get_counts(db_path=dbp)
    has_index = counts.get("docs", 0) > 0 and counts.get("vecs", 0) > 0
    started: list[str] = []
    if not has_index:
        started.append("ingest")
    background_tasks.add_task(
        _bootstrap_ingest,
        settings=s,
        repo_root=repo_root,
        paths=None,
    )
    return BootstrapResponse(ok=True, started=started)


@app.get("/healthz")
def healthz() -> HealthzResponse:
    s = get_settings()
    dbp = resolve_db_path(s.db_path)
    return HealthzResponse(
        ok=True,
        version=app_version(),
        db_path=str(dbp),
        embed_model=s.embed_model,
        embed_dims=s.embed_dims,
        llm_backend=s.llm_backend,
        llm_model=s.llm_model,
        llm_loaded=_healthz_llm_loaded(s),
        llm_loading=False,
        embed_loaded=embed_deployment_ready(s.embed_model, s.embed_dims),
        embed_loading=embedder_is_loading(s.embed_model, s.embed_dims),
        embed_computing=embedding_compute_active(),
    )


@app.get("/readyz")
def readyz() -> ReadyzResponse:
    s = get_settings()
    dbp = resolve_db_path(s.db_path)
    counts = get_counts(db_path=dbp)
    docs_ready = counts["docs"] > 0 and counts["vecs"] > 0
    api_ready = openai_api_key_configured()
    return ReadyzResponse(
        ok=bool(docs_ready and api_ready),
        counts=CountsStatus.model_validate(counts),
        llm_backend=s.llm_backend,
        llm_loaded=_healthz_llm_loaded(s),
        llm_loading=False,
        embed_loaded=embed_deployment_ready(s.embed_model, s.embed_dims),
        embed_loading=embedder_is_loading(s.embed_model, s.embed_dims),
        embed_computing=embedding_compute_active(),
        db_path=str(dbp),
    )


@app.get("/diag")
def diag() -> DiagResponse:
    s = get_settings()
    dbp = resolve_db_path(s.db_path)
    return DiagResponse(
        version=app_version(),
        db_path=str(dbp),
        vector_backend=VectorBackendStatus.model_validate(
            check_vector_backend(db_path=dbp)
        ),
        counts=CountsStatus.model_validate(get_counts(db_path=dbp)),
        llm_backend=s.llm_backend,
        llm_loaded=_healthz_llm_loaded(s),
        llm_loading=False,
        llm_last_error=None,
        embed_loaded=embed_deployment_ready(s.embed_model, s.embed_dims),
        embed_loading=embedder_is_loading(s.embed_model, s.embed_dims),
        embed_computing=embedding_compute_active(),
    )


@app.get("/health")
def health_compat() -> HealthzResponse:
    return healthz()


class IngestResponse(BaseModel):
    ok: bool
    started: bool


@app.post("/ingest", response_model=IngestResponse)
@limiter.limit("10/minute")
def ingest(
    background_tasks: BackgroundTasks,
    request: Request,
    req: IngestRequest = _INGEST_BODY,
) -> IngestResponse:
    s = get_settings()
    repo_root = resolve_ingest_repo_root()
    background_tasks.add_task(
        ingest_repo_docs, settings=s, repo_root=repo_root, paths=req.paths
    )
    return IngestResponse(ok=True, started=True)


@app.post("/chat", response_model=ChatResponse)
@limiter.limit("30/minute")
def chat(req: ChatRequest, request: Request) -> ChatResponse:
    s = get_settings()
    db_path = resolve_db_path(s.db_path)

    if not openai_api_key_configured():
        raise HTTPException(
            status_code=503,
            detail=(
                "OPENAI_API_KEY is not set. Retrieval embeddings and optional "
                "generative chat use the OpenAI API. Set the key in the environment."
            ),
        )

    result = rag_chat(
        question=req.message,
        db_path=str(db_path),
        embed_model=s.embed_model,
        embed_dims=s.embed_dims,
        top_k=s.top_k,
        llm_model=s.llm_model,
        llm_backend=s.llm_backend,
        chat_history=req.history,
    )

    rtd_base = s.readthedocs_base_url
    return ChatResponse(
        answer=result.answer,
        sources=[
            ChatSourceItem(
                source=c.source,
                chunk_id=c.chunk_id,
                distance=c.distance,
                url=source_to_readthedocs_url(c.source, base=rtd_base),
            )
            for c in result.retrieved
        ],
    )
