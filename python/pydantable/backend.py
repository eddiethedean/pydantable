from __future__ import annotations

from typing import Any, Optional


def _load_rust_core() -> Optional[Any]:
    """
    Import the compiled Rust extension module (if available).

    The skeleton must remain importable without building Rust extensions.
    """

    try:
        # Built by maturin as `pydantable._core`.
        from . import _core as rust_core  # type: ignore

        return rust_core
    except ImportError:
        return None


_RUST_CORE = _load_rust_core()


def execute_plan_rust(plan: Any, data: Any) -> Any:
    """
    Execute a typed logical plan via Rust.

    In `0.4.0` this is intentionally a stub and will raise a clear error.
    """

    if _RUST_CORE is None:
        raise NotImplementedError(
            "Rust execution is not available. Build the extension with maturin."
        )

    if not hasattr(_RUST_CORE, "execute_plan"):
        raise NotImplementedError("Rust extension does not implement `execute_plan`.")

    # The Rust stub will currently raise NotImplementedError.
    return _RUST_CORE.execute_plan(plan, data)

