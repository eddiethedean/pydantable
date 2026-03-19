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

    `plan` is expected to be the Rust `PyPlan` handle produced by the logical
    plan constructors, and `data` is the root column dictionary.
    """

    if _RUST_CORE is None:
        raise NotImplementedError(
            "Rust extension is not available. Build the PyO3 module so `pydantable._core` can be imported."
        )

    if not hasattr(_RUST_CORE, "execute_plan"):
        raise NotImplementedError("Rust extension does not implement `execute_plan`.")

    return _RUST_CORE.execute_plan(plan, data)

