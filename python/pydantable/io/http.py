"""HTTP(S) and object-store style URLs (experimental; best-effort)."""

from __future__ import annotations

import os
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import urlparse

_EXPERIMENTAL_ENV = "PYDANTABLE_IO_EXPERIMENTAL"


def _require_experimental(experimental: bool) -> None:
    if experimental:
        return
    if os.environ.get(_EXPERIMENTAL_ENV, "").lower() in ("1", "true", "yes"):
        return
    raise ValueError(
        "URL / cloud-style ingestion is experimental. Pass experimental=True or set "
        f"{_EXPERIMENTAL_ENV}=1."
    )


def _read_limited(resp: Any, max_bytes: int | None) -> bytes:
    if max_bytes is None:
        return resp.read()
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = resp.read(65536)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise ValueError(
                f"download exceeds max_bytes={max_bytes}; refuse to buffer further"
            )
        chunks.append(chunk)
    return b"".join(chunks)


def fetch_bytes(
    url: str,
    *,
    experimental: bool = True,
    headers: dict[str, str] | None = None,
    timeout: float = 60.0,
    max_bytes: int | None = None,
) -> bytes:
    """Download ``url`` and return raw bytes (stdlib ``urllib``).

    If ``max_bytes`` is set, read in chunks and raise ``ValueError`` if the body
    would exceed that size (partial data is not returned).
    """
    _require_experimental(experimental)
    scheme = urlparse(url).scheme.lower()
    if scheme not in ("http", "https"):
        raise ValueError(
            f"fetch_bytes only supports http(s) URLs, got scheme={scheme!r}"
        )
    req = urllib.request.Request(url, headers=dict(headers or {}))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return _read_limited(resp, max_bytes)
    except urllib.error.URLError as e:
        raise OSError(f"failed to fetch {url!r}: {e}") from e


def _write_temp_suffix(suffix: str) -> tuple[Path, BinaryIO]:
    fd, name = tempfile.mkstemp(suffix=suffix)
    import os as _os

    f = _os.fdopen(fd, "wb")
    return Path(name), f


def fetch_parquet_url(
    url: str,
    *,
    experimental: bool = True,
    columns: list[str] | None = None,
    **kwargs: Any,
) -> dict[str, list[Any]]:
    """Download a Parquet file from ``url`` (HTTP(S) only) and materialize as ``dict[str, list]``.

    Extra ``kwargs`` are forwarded to :func:`fetch_bytes` (e.g. ``max_bytes``, ``timeout``).
    """
    from .arrow import read_parquet_pyarrow

    _require_experimental(experimental)
    data = fetch_bytes(url, experimental=True, **kwargs)
    return read_parquet_pyarrow(data, columns=columns)


def fetch_csv_url(
    url: str,
    *,
    experimental: bool = True,
    **kwargs: Any,
) -> dict[str, list[Any]]:
    """Download CSV from ``url`` to a temp file and read via the Rust CSV path when possible."""
    from pydantable_native.io_core import (  # type: ignore[import-not-found]
        rust_read_csv_path,
    )

    _require_experimental(experimental)
    data = fetch_bytes(url, experimental=True, **kwargs)
    path, f = _write_temp_suffix(".csv")
    try:
        f.write(data)
        f.close()
        try:
            return rust_read_csv_path(str(path))
        except Exception:
            # Native reader can surface PyO3-wrapped errors; stdlib csv is the fallback.
            import csv

            with path.open(newline="", encoding="utf-8") as fh:
                reader = csv.reader(fh)
                header = next(reader)
                cols: dict[str, list[Any]] = {h: [] for h in header}
                for row in reader:
                    for i, h in enumerate(header):
                        cols[h].append(row[i] if i < len(row) else None)
                return cols
    finally:
        path.unlink(missing_ok=True)


def fetch_ndjson_url(
    url: str,
    *,
    experimental: bool = True,
    **kwargs: Any,
) -> dict[str, list[Any]]:
    from pydantable_native.io_core import (  # type: ignore[import-not-found]
        rust_read_ndjson_path,
    )

    _require_experimental(experimental)
    data = fetch_bytes(url, experimental=True, **kwargs)
    path, f = _write_temp_suffix(".ndjson")
    try:
        f.write(data)
        f.close()
        return rust_read_ndjson_path(str(path))
    finally:
        path.unlink(missing_ok=True)


def read_from_object_store(
    uri: str,
    *,
    experimental: bool = True,
    format: str = "parquet",
    max_bytes: int | None = None,
    **kwargs: Any,
) -> dict[str, list[Any]]:
    """
    Read ``s3://``, ``gs://``, or ``az://`` style URIs via ``fsspec`` (optional dependency).

    *Experimental*: requires ``pip install 'pydantable[cloud]'`` (or ``fsspec`` + backend).

    ``max_bytes`` caps how much of the object is read into memory (streaming reads the
    remote file in chunks until the limit). Full streaming without a cap is not implemented.
    """
    _require_experimental(experimental)
    try:
        import fsspec  # type: ignore[import-not-found]
    except ImportError as e:
        raise ImportError(
            "object-store URIs require fsspec (pip install 'pydantable[cloud]'). "
            "If fsspec is installed, install a backend for your URI scheme (e.g. s3fs for s3://)."
        ) from e
    scheme = urlparse(uri).scheme.lower()
    if scheme in ("http", "https"):
        raise ValueError("use fetch_parquet_url / fetch_csv_url for http(s) URLs")
    fmt = format.lower()
    try:
        with fsspec.open(uri, "rb") as f:  # type: ignore[call-arg]
            raw = _read_limited(f, max_bytes)
    except OSError as e:
        raise OSError(
            f"failed to open or read {uri!r} via fsspec (check URI and backend drivers): {e}"
        ) from e
    from pydantable_native.io_core import (  # type: ignore[import-not-found]  # noqa: I001
        rust_read_csv_path,
        rust_read_ndjson_path,
    )
    from .arrow import read_parquet_pyarrow

    if fmt == "parquet":
        return read_parquet_pyarrow(raw)
    if fmt == "csv":
        path, out = _write_temp_suffix(".csv")
        try:
            out.write(raw)
            out.close()
            return rust_read_csv_path(str(path))
        finally:
            path.unlink(missing_ok=True)
    if fmt in ("ndjson", "jsonl"):
        path, out = _write_temp_suffix(".ndjson")
        try:
            out.write(raw)
            out.close()
            return rust_read_ndjson_path(str(path))
        finally:
            path.unlink(missing_ok=True)
    raise ValueError(f"unsupported format={format!r} (use parquet, csv, ndjson)")
