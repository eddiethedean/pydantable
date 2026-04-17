from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

import numpy as np


@dataclass(frozen=True)
class Embedder:
    model_name: str
    dims: int
    tokenizer: object
    model: object
    device: str

    def embed(self, texts: list[str]) -> np.ndarray:
        import torch

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
            attn = tok["attention_mask"].unsqueeze(-1).expand(last_hidden.size()).float()
            pooled = (last_hidden * attn).sum(dim=1) / attn.sum(dim=1).clamp(min=1.0)

        emb = pooled.detach().cpu().to(torch.float32).numpy()
        emb = emb / (np.linalg.norm(emb, axis=1, keepdims=True) + 1e-12)

        if emb.shape[1] != self.dims:
            raise ValueError(
                f"Embedding dim mismatch: got {emb.shape[1]} expected {self.dims} "
                f"for model={self.model_name}"
            )
        return emb.astype(np.float32, copy=False)


@lru_cache(maxsize=2)
def get_embedder(model_name: str, dims: int) -> Embedder:
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
