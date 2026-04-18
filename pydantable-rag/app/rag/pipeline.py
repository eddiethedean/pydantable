from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from app.rag.embeddings import get_embedder
from app.rag.llm import ChatMessage, generate_answer_hf
from app.rag.store import RetrievedChunk, search

_MAX_CHARS_PER_CHUNK = 4000


@dataclass(frozen=True)
class RagResult:
    answer: str
    retrieved: list[RetrievedChunk]


SYSTEM_PROMPT = """You are a helpful assistant for the Python library 'pydantable'.

 Use the provided context excerpts to answer the user's question about pydantable usage.
 - If the context doesn't contain the answer, say what is missing and suggest where to
   look.
 - Prefer concise, actionable code examples.
 - Do not invent APIs; if unsure, say you're unsure.
 """


def _extractive_answer(question: str, retrieved: list[RetrievedChunk]) -> str:
    """No generative model — return ranked excerpts (for low-RAM hosts)."""
    if not retrieved:
        return (
            "No matching documentation chunks were found. Try different keywords "
            "or enable a generative backend (RAG_LLM_BACKEND=hf) on a larger host."
        )
    parts: list[str] = [
        (
            f"**{c.source}** (chunk {c.chunk_id}, distance {c.distance:.4f})\n\n"
            f"{c.text[:_MAX_CHARS_PER_CHUNK]}"
            + ("…" if len(c.text) > _MAX_CHARS_PER_CHUNK else "")
        )
        for c in retrieved
    ]
    header = (
        "Retrieval-only mode (no local generative model). "
        f"Question: {question!r}\n\n---\n\n"
    )
    return header + "\n\n---\n\n".join(parts)


def rag_chat(
    *,
    question: str,
    db_path: str,
    embed_model: str,
    embed_dims: int,
    top_k: int,
    llm_model: str,
    llm_backend: str = "hf",
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
                f"Context:\n{context}\n\n"
                f"Question:\n{question}\n\n"
                "Answer using the context above."
            ),
        )
    )

    answer = generate_answer_hf(
        model=llm_model,
        system_prompt=SYSTEM_PROMPT,
        messages=msgs,
    )
    return RagResult(answer=answer, retrieved=retrieved)
