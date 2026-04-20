from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from app.rag.embeddings import get_embedder
from app.rag.llm import ChatMessage, generate_answer_openai
from app.rag.store import RetrievedChunk, search

_MAX_CHARS_PER_CHUNK = 4000


def _skip_chunk_source(source: str) -> bool:
    """Drop RTD/MkDocs build mirrors — they duplicate real ``docs/`` files."""
    s = source.replace("\\", "/")
    return "/_build/" in s


def _display_source_label(source: str) -> str:
    s = source.replace("\\", "/")
    if len(s) > 80:
        return "…" + s[-79:]
    return s


def _clean_chunk_text(text: str) -> str:
    """
    Light cleanup so mkdocstrings / MyST directives read ok in Markdown chat UI.
    """
    out_lines: list[str] = []
    for line in text.splitlines():
        s = line.rstrip()
        stripped = s.strip()
        if stripped.startswith(":::"):
            rest = stripped[3:].strip()
            if rest:
                out_lines.append(
                    f"*API reference (see the official docs for full signatures): "
                    f"`{rest}`*"
                )
            else:
                out_lines.append("*API reference embed (see official docs).*")
            continue
        out_lines.append(s)
    joined = "\n".join(out_lines)
    joined = re.sub(r"\n{4,}", "\n\n\n", joined)
    return joined.strip()


@dataclass(frozen=True)
class RagResult:
    answer: str
    retrieved: list[RetrievedChunk]


SYSTEM_PROMPT = """You are the pydantable documentation assistant: you help people use
the Python library pydantable (schema-first, typed DataFrame-style APIs).

How this chat is wired (important):
- The user only sends a question. They did **not** paste or upload the text blocks you
  see in their message.
- The pydantable-rag backend runs retrieval over an indexed copy of the docs and
  **injects** the closest matching excerpts into the prompt for you. Those excerpts are
  your primary source of truth for this turn.
- Respond as a knowledgeable assistant who understands pydantable—not as someone
  reacting to "material the user provided." Do **not** thank the user for context,
  do **not** open with phrases like "based on the chunks you shared" or "given the
  excerpts you supplied."
- Ground your answer in the retrieved excerpts when they are relevant. If they are
  incomplete or off-topic, say what is missing and suggest what to read or try next.
- Prefer concise explanations and small, runnable code examples when they help.
- Do not invent APIs or parameters; if something is not supported in the excerpts,
  say you are unsure or point to the docs rather than guessing.
"""


def _extractive_answer(_question: str, retrieved: list[RetrievedChunk]) -> str:
    """No generative model — return ranked excerpts (for low-RAM hosts)."""
    if not retrieved:
        return (
            "No matching documentation chunks were found. Try different keywords "
            "or set RAG_LLM_BACKEND=openai with OPENAI_API_KEY."
        )
    filtered = [c for c in retrieved if not _skip_chunk_source(c.source)]
    chunks = filtered if filtered else retrieved

    intro = (
        "Here are the **closest matching excerpts** from the indexed pydantable "
        "documentation.\n\n"
        "*No local generative model is running* — this answer is extractive. "
        "Use the **Sources** list under the reply for per-chunk match scores."
    )

    sections: list[str] = [intro]
    for i, c in enumerate(chunks, start=1):
        label = _display_source_label(c.source)
        body = _clean_chunk_text(c.text)
        if len(body) > _MAX_CHARS_PER_CHUNK:
            body = body[:_MAX_CHARS_PER_CHUNK].rstrip() + "\n\n…"
        sections.append(f"### {i}. `{label}`\n\n{body}")

    return "\n\n".join(sections)


def rag_chat(
    *,
    question: str,
    db_path: str,
    embed_model: str,
    embed_dims: int,
    top_k: int,
    llm_model: str,
    llm_backend: str = "extractive",
    chat_history: list[ChatMessage] | None = None,
) -> RagResult:
    embedder = get_embedder(embed_model, embed_dims)
    q = embedder.embed([question])[0]
    retrieved = search(db_path=Path(db_path), query_embedding=q, top_k=top_k)

    if llm_backend == "extractive":
        answer = _extractive_answer(question, retrieved)
        return RagResult(answer=answer, retrieved=retrieved)

    context = "\n\n".join(
        [
            f"[source={c.source} chunk={c.chunk_id} dist={c.distance:.4f}]\n{c.text}"
            for c in retrieved
        ]
    )

    msgs: list[ChatMessage] = []
    if chat_history:
        msgs.extend(chat_history[-12:])
    msgs.append(
        ChatMessage(
            role="user",
            content=(
                "Retrieved documentation excerpts (injected by the backend for this "
                "request; the user did not paste these):\n\n"
                f"{context}\n\n"
                "---\n"
                f"User question:\n{question}"
            ),
        )
    )

    if llm_backend == "openai":
        answer = generate_answer_openai(
            model=llm_model,
            system_prompt=SYSTEM_PROMPT,
            messages=msgs,
        )
        return RagResult(answer=answer, retrieved=retrieved)

    # Unknown backend (e.g. legacy env) — safe fallback.
    answer = _extractive_answer(question, retrieved)
    return RagResult(answer=answer, retrieved=retrieved)
