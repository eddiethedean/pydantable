from __future__ import annotations

import logging
import time
from functools import lru_cache

from pydantic import BaseModel, Field

_LOADING: set[str] = set()
_LOADED: set[str] = set()
_LLM_LAST_ERROR: dict[str, str | None] = {}
_log = logging.getLogger(__name__)


class ChatMessage(BaseModel):
    role: str = Field(pattern="^(system|user|assistant)$")
    content: str


@lru_cache(maxsize=2)
def _load_llm(model_name: str):
    import torch
    import torch.nn as nn
    from transformers import AutoModelForCausalLM, AutoTokenizer

    device = "cuda" if torch.cuda.is_available() else "cpu"
    dtype = torch.float16 if device == "cuda" else torch.float32
    tok = AutoTokenizer.from_pretrained(model_name)
    mdl = AutoModelForCausalLM.from_pretrained(
        model_name, low_cpu_mem_usage=True, dtype=dtype
    )
    mdl.eval()
    # Use unbound ``Module.to`` so static analysis does not confuse ``mdl.to`` with
    # ``functools`` internals in ``transformers`` stubs.
    nn.Module.to(mdl, device)
    _LOADED.add(model_name)
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
