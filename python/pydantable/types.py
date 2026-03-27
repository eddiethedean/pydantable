"""Semantic marker types for pydantable columns (beyond plain ``bytes`` / ``str``)."""

from __future__ import annotations

from typing import Any

from pydantic_core import CoreSchema, core_schema


class WKB(bytes):
    """Well-Known Binary geometry (OGC SF); stored as Polars **Binary** like ``bytes``.

    Values are raw ``bytes`` at runtime; this subclass distinguishes **geospatial WKB**
    columns from opaque binary blobs in typing and the Rust dtype layer.
    """

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: Any) -> CoreSchema:
        def coerce(v: object) -> WKB:
            if isinstance(v, WKB):
                return v
            if isinstance(v, (bytes, bytearray)):
                return WKB(bytes(v))
            if isinstance(v, memoryview):
                return WKB(v.tobytes())
            raise TypeError(f"WKB expects bytes-like, got {type(v).__name__}")

        return core_schema.no_info_after_validator_function(
            coerce,
            core_schema.bytes_schema(),
        )

    def __repr__(self) -> str:
        return f"WKB({super().__repr__()})"
