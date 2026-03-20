from __future__ import annotations

from typing import Any


def _load_rust_core() -> Any | None:
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


def _require_rust_core() -> Any:
    if _RUST_CORE is None:
        raise NotImplementedError(
            "Rust extension is not available. "
            "Build the PyO3 module so `pydantable._core` can be imported."
        )
    return _RUST_CORE


def execute_plan_rust(plan: Any, data: Any) -> Any:
    """
    Execute a typed logical plan via Rust.

    `plan` is expected to be the Rust `PyPlan` handle produced by the logical
    plan constructors, and `data` is the root column dictionary.
    """

    rust = _require_rust_core()
    if not hasattr(rust, "execute_plan"):
        raise NotImplementedError("Rust extension does not implement `execute_plan`.")

    return rust.execute_plan(plan, data)


def execute_join_rust(
    left_plan: Any,
    left_root_data: Any,
    right_plan: Any,
    right_root_data: Any,
    on: list[str],
    how: str,
    suffix: str,
) -> tuple[Any, Any]:
    rust = _require_rust_core()
    if not hasattr(rust, "execute_join"):
        raise NotImplementedError("Rust extension does not implement `execute_join`.")
    return rust.execute_join(
        left_plan,
        left_root_data,
        right_plan,
        right_root_data,
        on,
        how,
        suffix,
    )


def execute_groupby_agg_rust(
    plan: Any,
    root_data: Any,
    by: list[str],
    aggregations: Any,
) -> tuple[Any, Any]:
    rust = _require_rust_core()
    if not hasattr(rust, "execute_groupby_agg"):
        raise NotImplementedError(
            "Rust extension does not implement `execute_groupby_agg`."
        )
    return rust.execute_groupby_agg(plan, root_data, by, aggregations)
