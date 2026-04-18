"""Cap PyTorch CPU thread usage for small containers (reduces RAM spikes / OOM)."""

from __future__ import annotations

_configured = False


def configure_torch_cpu() -> None:
    """Idempotent; call once early in process lifetime (e.g. FastAPI lifespan)."""
    global _configured
    if _configured:
        return
    import os

    raw = os.environ.get("TORCH_NUM_THREADS") or os.environ.get("OMP_NUM_THREADS", "1")
    try:
        n = max(1, int(raw))
    except ValueError:
        n = 1
    try:
        import torch

        torch.set_num_threads(n)
        torch.set_num_interop_threads(1)
    except Exception:
        pass
    _configured = True
