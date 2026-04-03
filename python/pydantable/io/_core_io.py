"""Native Rust I/O shim.

The real implementation lives in the optional `pydantable-native` distribution.
This module exists for backward-compatible imports and tests.
"""

from __future__ import annotations

from pydantable_native.io_core import (  # type: ignore[import-not-found]
    rust_read_csv_path,
    rust_read_ipc_path,
    rust_read_ndjson_path,
    rust_read_parquet_path,
    rust_write_csv_path,
    rust_write_ipc_path,
    rust_write_ndjson_path,
    rust_write_parquet_path,
)

__all__ = [
    "rust_read_csv_path",
    "rust_read_ipc_path",
    "rust_read_ndjson_path",
    "rust_read_parquet_path",
    "rust_write_csv_path",
    "rust_write_ipc_path",
    "rust_write_ndjson_path",
    "rust_write_parquet_path",
]
