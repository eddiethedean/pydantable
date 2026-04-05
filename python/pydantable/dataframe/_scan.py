"""Lazy scan-root detection and engine error parsing for optional-column recovery."""

from __future__ import annotations

import re


def _is_scan_file_root(obj: object) -> bool:
    t = type(obj)
    return (
        t.__name__ == "ScanFileRoot"
        and getattr(t, "__module__", "") == "pydantable_native._core"
    )


def _extract_missing_scan_column_from_engine_error(msg: str) -> str | None:
    """
    Best-effort parse of engine error strings for missing columns in Polars scans.

    Polars error messages vary across versions and execution paths. This parser is
    intentionally tolerant: it only runs on scan roots and only affects recovery
    for *optional* schema fields.
    """
    s = (msg or "").strip()
    if not s:
        return None
    patterns = (
        # Current/legacy Rust error string (pydantable-native tests); keep in sync
        # with tests/test_scan_column_error_parsing.py.
        r'not found: "([^"]+)" not found',
        # Common Polars Python error variants.
        r"ColumnNotFoundError: '([^']+)'",
        r'ColumnNotFoundError: "([^"]+)"',
        r"ColumnNotFoundError: ([A-Za-z_][A-Za-z0-9_]*)\b",
        r"column ['\"]([^'\"]+)['\"] not found",
    )
    for pat in patterns:
        m = re.search(pat, s)
        if m:
            return m.group(1)
    return None
