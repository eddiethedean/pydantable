from __future__ import annotations

import os
import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

import numpy as np

from app.openai_env import openai_api_key_configured

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

    def embed(self, texts: list[str]) -> np.ndarray:
        global _compute_active
        from openai import OpenAI

        if not texts:
            return np.zeros((0, self.dims), dtype=np.float32)
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not set (required for embeddings)")

        base_url = os.getenv("OPENAI_BASE_URL", "").strip() or None
        client = OpenAI(api_key=api_key, base_url=base_url)

        with _embed_lock:
            _compute_active += 1
        try:
            out_rows: list[np.ndarray] = []
            batch_size = 100
            for start in range(0, len(texts), batch_size):
                chunk = texts[start : start + batch_size]
                kwargs: dict[str, Any] = {"model": self.model_name, "input": chunk}
                if self.model_name.startswith("text-embedding-3"):
                    kwargs["dimensions"] = self.dims
                resp = client.embeddings.create(**kwargs)
                for item in resp.data:
                    v = np.array(item.embedding, dtype=np.float32)
                    if v.shape[0] != self.dims:
                        raise ValueError(
                            f"Embedding length {v.shape[0]} != expected {self.dims} "
                            f"for model={self.model_name}"
                        )
                    out_rows.append(v)
            embs = np.vstack(out_rows)
            embs = embs / (np.linalg.norm(embs, axis=1, keepdims=True) + 1e-12)
            return embs.astype(np.float32, copy=False)
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
    return Embedder(model_name=model_name, dims=dims)


def embed_deployment_ready(model_name: str, dims: int) -> bool:
    """
    True when ``OPENAI_API_KEY`` is set (OpenAI embeddings API; no local weights).
    """
    del model_name, dims  # API-only; key is what matters for readiness.
    return openai_api_key_configured()


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


def release_embedder_models() -> None:
    """Drop cached embedder handles (lightweight; no GPU memory)."""
    with _embed_lock:
        _embedder_by_key.clear()


def get_embedder(model_name: str, dims: int) -> Embedder:
    """
    Return a cached OpenAI embedding handle. Thread-safe.
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
