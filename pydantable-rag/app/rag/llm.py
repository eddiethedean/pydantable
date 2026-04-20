from __future__ import annotations

import logging
import threading
import time
from functools import lru_cache

from pydantic import BaseModel, Field

_LOADING: set[str] = set()
_LOADED: set[str] = set()
_LLM_LAST_ERROR: dict[str, str | None] = {}
# Serialize warm-up so concurrent /chat requests do not double-download weights.
_WARM_LOCK = threading.Lock()
_log = logging.getLogger(__name__)


def _hf_token() -> str | None:
    import os

    return os.getenv("HF_TOKEN") or os.getenv("HUGGING_FACE_HUB_TOKEN")


class ChatMessage(BaseModel):
    role: str = Field(pattern="^(system|user|assistant)$")
    content: str


@lru_cache(maxsize=2)
def _load_llm(model_name: str):
    import torch
    import torch.nn as nn
    from transformers import AutoModelForCausalLM, AutoTokenizer

    token = _hf_token()
    device = "cuda" if torch.cuda.is_available() else "cpu"
    dtype = torch.float16 if device == "cuda" else torch.float32
    tok = AutoTokenizer.from_pretrained(model_name, token=token)
    mdl = AutoModelForCausalLM.from_pretrained(
        model_name,
        low_cpu_mem_usage=True,
        dtype=dtype,
        token=token,
    )
    mdl.eval()
    # Use unbound ``Module.to`` so static analysis does not confuse ``mdl.to`` with
    # ``functools`` internals in ``transformers`` stubs.
    nn.Module.to(mdl, device)
    _LOADED.add(model_name)
    _log.info("pydantable-rag: LLM ready: %s (%s)", model_name, device)
    return tok, mdl, device


def llm_is_loaded(model_name: str) -> bool:
    return model_name in _LOADED


def llm_is_loading(model_name: str) -> bool:
    return model_name in _LOADING


def llm_last_error(model_name: str) -> str | None:
    return _LLM_LAST_ERROR.get(model_name)


def warm_llm(model_name: str) -> None:
    if model_name in _LOADED:
        _LLM_LAST_ERROR.pop(model_name, None)
        return
    with _WARM_LOCK:
        if model_name in _LOADED:
            _LLM_LAST_ERROR.pop(model_name, None)
            return
        _LOADING.add(model_name)
        try:
            last_exc: BaseException | None = None
            for attempt in range(3):
                try:
                    _load_llm(model_name)
                    _LLM_LAST_ERROR.pop(model_name, None)
                    return
                except Exception as e:
                    last_exc = e
                    _log.warning(
                        "warm_llm %s attempt %s/3 failed: %s",
                        model_name,
                        attempt + 1,
                        e,
                    )
                    if attempt < 2:
                        time.sleep(min(2.0**attempt, 8.0))
            assert last_exc is not None
            _LLM_LAST_ERROR[model_name] = f"{type(last_exc).__name__}: {last_exc}"
            _log.exception("warm_llm failed after retries for %s", model_name)
            raise last_exc
        finally:
            _LOADING.discard(model_name)


def generate_answer_hf(
    *, model: str, system_prompt: str, messages: list[ChatMessage]
) -> str:
    import torch

    tok, mdl, device = _load_llm(model)

    # Minimal chat formatting that works for most instruct models.
    prompt = system_prompt.strip() + "\n\n"
    for m in messages:
        prompt += f"{m.role.upper()}:\n{m.content.strip()}\n\n"
    prompt += "ASSISTANT:\n"

    inputs = tok(prompt, return_tensors="pt")
    inputs = {k: v.to(device) for k, v in inputs.items()}

    with torch.no_grad():
        out = mdl.generate(
            **inputs,
            max_new_tokens=256,
            do_sample=False,
            eos_token_id=tok.eos_token_id,
        )

    text = tok.decode(out[0], skip_special_tokens=True)
    # Return the suffix after the assistant tag if present.
    marker = "ASSISTANT:\n"
    if marker in text:
        return text.split(marker, 1)[-1].strip()
    return text.strip()


def openai_api_configured() -> bool:
    import os

    return bool(os.getenv("OPENAI_API_KEY", "").strip())


def generate_answer_openai(
    *, model: str, system_prompt: str, messages: list[ChatMessage]
) -> str:
    """
    Chat completion via OpenAI's HTTP API (no local generative weights).

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
