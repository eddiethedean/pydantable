from __future__ import annotations

from functools import lru_cache

from pydantic import BaseModel, Field

_LOADING: set[str] = set()
_LOADED: set[str] = set()


class ChatMessage(BaseModel):
    role: str = Field(pattern="^(system|user|assistant)$")
    content: str


@lru_cache(maxsize=2)
def _load_llm(model_name: str):
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer

    device = "cuda" if torch.cuda.is_available() else "cpu"
    dtype = torch.float16 if device == "cuda" else torch.float32
    tok = AutoTokenizer.from_pretrained(model_name)
    mdl = AutoModelForCausalLM.from_pretrained(
        model_name, low_cpu_mem_usage=True, torch_dtype=dtype
    )
    mdl.eval()
    mdl.to(device)
    _LOADED.add(model_name)
    return tok, mdl, device


def llm_is_loaded(model_name: str) -> bool:
    return model_name in _LOADED


def llm_is_loading(model_name: str) -> bool:
    return model_name in _LOADING


def warm_llm(model_name: str) -> None:
    if model_name in _LOADED:
        return
    _LOADING.add(model_name)
    try:
        _load_llm(model_name)
    finally:
        _LOADING.discard(model_name)


def generate_answer_hf(*, model: str, system_prompt: str, messages: list[ChatMessage]) -> str:
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
