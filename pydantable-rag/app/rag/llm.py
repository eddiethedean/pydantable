from __future__ import annotations

from pydantic import BaseModel, Field


class ChatMessage(BaseModel):
    role: str = Field(pattern="^(system|user|assistant)$")
    content: str


def generate_answer(*, model: str, system_prompt: str, messages: list[ChatMessage]) -> str:
    import ollama

    resp = ollama.chat(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            *[{"role": m.role, "content": m.content} for m in messages],
        ],
    )
    return resp["message"]["content"]
