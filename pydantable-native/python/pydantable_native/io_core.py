"""Bindings to Rust I/O helpers on ``pydantable_native._core`` (optional extension)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable

T = TypeVar("T")


def _core_fn(name: str) -> Callable[..., Any] | None:
    try:
        import pydantable_native._core as _c  # type: ignore[import-not-found]
    except ImportError:
        return None
    return getattr(_c, name, None)


def rust_read_parquet_path(path: str) -> dict[str, list[Any]]:
    fn = _core_fn("io_read_parquet_path")
    if fn is None:
        raise RuntimeError("Rust Parquet reader is not available in this build.")
    return fn(path)


def rust_read_csv_path(path: str) -> dict[str, list[Any]]:
    fn = _core_fn("io_read_csv_path")
    if fn is None:
        raise RuntimeError("Rust CSV reader is not available in this build.")
    return fn(path)


def rust_read_ndjson_path(path: str) -> dict[str, list[Any]]:
    fn = _core_fn("io_read_ndjson_path")
    if fn is None:
        raise RuntimeError("Rust NDJSON reader is not available in this build.")
    return fn(path)


def rust_read_ipc_path(path: str) -> dict[str, list[Any]]:
    fn = _core_fn("io_read_ipc_path")
    if fn is None:
        raise RuntimeError("Rust IPC reader is not available in this build.")
    return fn(path)


def rust_write_parquet_path(path: str, data: dict[str, list[Any]]) -> None:
    fn = _core_fn("io_write_parquet_path")
    if fn is None:
        raise RuntimeError("Rust Parquet writer is not available in this build.")
    fn(path, data)


def rust_write_csv_path(path: str, data: dict[str, list[Any]]) -> None:
    fn = _core_fn("io_write_csv_path")
    if fn is None:
        raise RuntimeError("Rust CSV writer is not available in this build.")
    fn(path, data)


def rust_write_ndjson_path(path: str, data: dict[str, list[Any]]) -> None:
    fn = _core_fn("io_write_ndjson_path")
    if fn is None:
        raise RuntimeError("Rust NDJSON writer is not available in this build.")
    fn(path, data)


def rust_write_ipc_path(path: str, data: dict[str, list[Any]]) -> None:
    fn = _core_fn("io_write_ipc_path")
    if fn is None:
        raise RuntimeError("Rust IPC writer is not available in this build.")
    fn(path, data)
