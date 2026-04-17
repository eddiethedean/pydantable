from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

import numpy as np

# Match previous ``lru_cache(maxsize=2)`` behavior: at most two embedders in memory.
_MAX_EMBEDDERS = 2

_embed_lock = threading.Lock()
_embedder_by_key: OrderedDict[tuple[str, int], Embedder] = OrderedDict()
_loading_keys: set[tuple[str, int]] = set()
_init_locks: dict[tuple[str, int], threading.Lock] = {}
_compute_active = 0


@dataclass(frozen=True)
class Embedder:
    model_name: str
    dims: int
    tokenizer: Any
    model: Any
    device: str

    def embed(self, texts: list[str]) -> np.ndarray:
        global _compute_active
        import torch

        with _embed_lock:
            _compute_active += 1
        try:
            if not texts:
                return np.zeros((0, self.dims), dtype=np.float32)

            tok = self.tokenizer(
                texts,
                padding=True,
                truncation=True,
                max_length=512,
                return_tensors="pt",
            )
            tok = {k: v.to(self.device) for k, v in tok.items()}

            with torch.no_grad():
                out = self.model(**tok)
                last_hidden = out.last_hidden_state  # (B, T, H)
                attn = (
                    tok["attention_mask"]
                    .unsqueeze(-1)
                    .expand(last_hidden.size())
                    .float()
                )
                pooled = (last_hidden * attn).sum(dim=1) / attn.sum(dim=1).clamp(
                    min=1.0
                )

            emb = pooled.detach().cpu().to(torch.float32).numpy()
            emb = emb / (np.linalg.norm(emb, axis=1, keepdims=True) + 1e-12)

            if emb.shape[1] != self.dims:
                raise ValueError(
                    f"Embedding dim mismatch: got {emb.shape[1]} expected {self.dims} "
                    f"for model={self.model_name}"
                )
            return emb.astype(np.float32, copy=False)
        finally:
            with _embed_lock:
                _compute_active -= 1


def _evict_embedder_if_needed() -> None:
    while len(_embedder_by_key) > _MAX_EMBEDDERS:
        _embedder_by_key.popitem(last=False)


def _init_lock_for(key: tuple[str, int]) -> threading.Lock:
    with _embed_lock:
        if key not in _init_locks:
            _init_locks[key] = threading.Lock()
        return _init_locks[key]


def _build_embedder(model_name: str, dims: int) -> Embedder:
    import torch
    from transformers import AutoModel, AutoTokenizer

    device = "cuda" if torch.cuda.is_available() else "cpu"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)
    model.eval()
    model.to(device)
    return Embedder(
        model_name=model_name,
        dims=dims,
        tokenizer=tokenizer,
        model=model,
        device=device,
    )


def embedder_is_loaded(model_name: str, dims: int) -> bool:
    key = (model_name, dims)
    with _embed_lock:
        return key in _embedder_by_key


def embedder_is_loading(model_name: str, dims: int) -> bool:
    key = (model_name, dims)
    with _embed_lock:
        return key in _loading_keys


def embedding_compute_active() -> bool:
    with _embed_lock:
        return _compute_active > 0


def get_embedder(model_name: str, dims: int) -> Embedder:
    """
    Load (or return cached) sentence embedding model weights. Thread-safe.
    """
    key = (model_name, dims)
    with _embed_lock:
        if key in _embedder_by_key:
            _embedder_by_key.move_to_end(key)
            return _embedder_by_key[key]

    with _init_lock_for(key):
        with _embed_lock:
            if key in _embedder_by_key:
                _embedder_by_key.move_to_end(key)
                return _embedder_by_key[key]
            _loading_keys.add(key)
        try:
            built = _build_embedder(model_name, dims)
        finally:
            with _embed_lock:
                _loading_keys.discard(key)
        with _embed_lock:
            _embedder_by_key[key] = built
            _embedder_by_key.move_to_end(key)
            _evict_embedder_if_needed()
            return _embedder_by_key[key]
