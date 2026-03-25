"""Optional ``rapcsv`` / ``rapfiles`` async CSV path (``[rap]`` extra)."""

from __future__ import annotations

from typing import Any


def rap_csv_available() -> bool:
    """True when ``rapcsv`` and ``rapfiles`` are importable and ``rapfiles.open`` exists."""
    try:
        import rapcsv  # type: ignore[import-not-found,unused-ignore]  # noqa: F401
        import rapfiles  # type: ignore[import-not-found,unused-ignore]
    except ImportError:
        return False
    # rapfiles exposes aiofiles-compatible ``open``, not ``aopen`` (see rapfiles 0.2.x docs).
    return callable(getattr(rapfiles, "open", None))


async def aread_csv_rap(path: str) -> dict[str, list[Any]]:
    """
    Load a CSV file with ``rapcsv.AsyncDictReader`` (non-blocking I/O).

    Requires ``pip install 'pydantable[rap]'`` (``rapcsv`` + ``rapfiles``).
    """
    try:
        import rapcsv  # type: ignore[import-not-found]
        import rapfiles  # type: ignore[import-not-found]
    except ImportError as e:
        raise ImportError(
            "aread_csv_rap requires rapcsv and rapfiles (pip install 'pydantable[rap]')."
        ) from e

    # ``rapfiles.open`` matches aiofiles (there is no ``aopen`` on rapfiles 0.2.x).
    # Iterate with ``read_row()``: ``async for`` over ``AsyncDictReader`` can hang
    # in rapcsv 0.2.x when used with a rapfiles text handle.
    async with rapfiles.open(path, "r", encoding="utf-8", newline="") as fh:
        reader = rapcsv.AsyncDictReader(fh)
        rows: list[dict[str, Any]] = []
        while True:
            row = await reader.read_row()
            if not row:
                break
            rows.append(row)
    if not rows:
        return {}
    keys = list(rows[0].keys())
    return {k: [r.get(k) for r in rows] for k in keys}
