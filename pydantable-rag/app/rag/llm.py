from __future__ import annotations

from pydantic import BaseModel, Field

from app.openai_env import openai_api_key_configured


class ChatMessage(BaseModel):
    role: str = Field(pattern="^(system|user|assistant)$")
    content: str


def openai_api_configured() -> bool:
    """True when ``OPENAI_API_KEY`` is set (chat + embeddings use the same key)."""
    return openai_api_key_configured()


def generate_answer_openai(
    *, model: str, system_prompt: str, messages: list[ChatMessage]
) -> str:
    """
    Chat completion via OpenAI's HTTP API.

    Uses ``OPENAI_API_KEY``. Optional ``OPENAI_BASE_URL`` for compatible proxies.
    """
    import os

    from openai import OpenAI

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    base_url = os.getenv("OPENAI_BASE_URL", "").strip() or None
    client = OpenAI(api_key=api_key, base_url=base_url)

    api_messages: list[dict[str, str]] = [
        {"role": "system", "content": system_prompt.strip()},
    ]
    for m in messages:
        api_messages.append({"role": m.role, "content": m.content.strip()})

    resp = client.chat.completions.create(
        model=model,
        messages=api_messages,
        temperature=0.2,
        max_tokens=1024,
    )
    choice = resp.choices[0].message.content
    return (choice or "").strip()
