from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from app.rag.embeddings import get_embedder
from app.rag.llm import ChatMessage, generate_answer_hf
from app.rag.store import RetrievedChunk, search


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


def rag_chat(
    *,
    question: str,
    db_path: str,
    embed_model: str,
    embed_dims: int,
    top_k: int,
    llm_model: str,
    chat_history: list[ChatMessage] | None = None,
) -> RagResult:
    embedder = get_embedder(embed_model, embed_dims)
    q = embedder.embed([question])[0]
    retrieved = search(db_path=Path(db_path), query_embedding=q, top_k=top_k)

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
