"""Tier-2/3 formats and SDK bridges (optional extras; many are **experimental**)."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, BinaryIO, TextIO, cast

_EXPERIMENTAL_ENV = "PYDANTABLE_IO_EXPERIMENTAL"


def _require_experimental(experimental: bool, feature: str) -> None:
    if experimental:
        return
    if os.environ.get(_EXPERIMENTAL_ENV, "").lower() in ("1", "true", "yes"):
        return
    raise ValueError(
        f"{feature} is experimental. Pass experimental=True or set {_EXPERIMENTAL_ENV}=1."
    )


def read_excel(
    path: str | Path,
    *,
    sheet_name: str | int = 0,
    experimental: bool = True,
) -> dict[str, list[Any]]:
    """Load the first sheet (or ``sheet_name``) from ``.xlsx`` via openpyxl → ``dict[str, list]``."""
    _require_experimental(experimental, "Excel ingestion")
    try:
        import openpyxl  # type: ignore[import-not-found]
    except ImportError as e:
        raise ImportError(
            "read_excel requires openpyxl (pip install 'pydantable[excel]')."
        ) from e
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.worksheets[sheet_name] if isinstance(sheet_name, int) else wb[sheet_name]
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    if not rows:
        return {}
    header = [str(h) if h is not None else f"col{i}" for i, h in enumerate(rows[0])]
    out: dict[str, list[Any]] = {h: [] for h in header}
    for row in rows[1:]:
        for i, h in enumerate(header):
            out[h].append(row[i] if i < len(row) else None)
    return out


def read_delta(
    path: str | Path,
    *,
    experimental: bool = True,
) -> dict[str, list[Any]]:
    """Read a Delta table directory via PyArrow dataset (``[arrow]`` extra)."""
    _require_experimental(experimental, "Delta Lake ingestion")
    try:
        import pyarrow.dataset as ds  # type: ignore[import-not-found]
    except ImportError as e:
        raise ImportError(
            "read_delta requires pyarrow with dataset support (pip install 'pydantable[arrow]')."
        ) from e
    from .arrow import arrow_table_to_column_dict

    dset = ds.dataset(path, format="parquet")
    table = dset.to_table()
    return arrow_table_to_column_dict(table)


def read_avro(
    path: str | Path,
    *,
    experimental: bool = True,
) -> dict[str, list[Any]]:
    _require_experimental(experimental, "Avro ingestion")
    try:
        import pyarrow as pa  # type: ignore[import-not-found]
    except ImportError as e:
        raise ImportError("read_avro requires pyarrow (pip install 'pydantable[arrow]').") from e
    from .arrow import arrow_table_to_column_dict

    try:
        table = pa.avro.read_table(str(path))
    except AttributeError as e:
        raise ImportError("pyarrow.avro is not available in this pyarrow build.") from e
    return arrow_table_to_column_dict(table)


def read_orc(
    path: str | Path,
    *,
    experimental: bool = True,
) -> dict[str, list[Any]]:
    _require_experimental(experimental, "ORC ingestion")
    try:
        import pyarrow.orc as orc  # type: ignore[import-not-found]
    except ImportError as e:
        raise ImportError("read_orc requires pyarrow.orc (pip install 'pydantable[arrow]').") from e
    from .arrow import arrow_table_to_column_dict

    with open(path, "rb") as f:
        table = orc.ORCFile(f).read()
    return arrow_table_to_column_dict(table)


def read_bigquery(
    query: str,
    *,
    project: str | None = None,
    experimental: bool = True,
    **kwargs: Any,
) -> dict[str, list[Any]]:
    """Run a BigQuery SQL string via ``google-cloud-bigquery`` → ``dict[str, list]``."""
    _require_experimental(experimental, "BigQuery ingestion")
    try:
        from google.cloud import bigquery  # type: ignore[import-not-found]
    except ImportError as e:
        raise ImportError(
            "read_bigquery requires google-cloud-bigquery (pip install 'pydantable[bq]')."
        ) from e
    from .arrow import arrow_table_to_column_dict

    client = bigquery.Client(project=project, **kwargs)
    rows = client.query(query).result()
    table = rows.to_arrow()
    return arrow_table_to_column_dict(table)


def read_snowflake(
    sql: str,
    *,
    experimental: bool = True,
    **connect_kwargs: Any,
) -> dict[str, list[Any]]:
    """Execute ``sql`` on Snowflake via ``snowflake-connector-python`` (experimental)."""
    _require_experimental(experimental, "Snowflake ingestion")
    try:
        import snowflake.connector  # type: ignore[import-not-found]
    except ImportError as e:
        raise ImportError(
            "read_snowflake requires snowflake-connector-python "
            "(pip install 'pydantable[snowflake]')."
        ) from e
    conn = snowflake.connector.connect(**connect_kwargs)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [c[0] for c in cur.description or []]
        fetched = cur.fetchall()
        if not cols:
            return {}
        return {cols[i]: [row[i] for row in fetched] for i in range(len(cols))}
    finally:
        conn.close()


def read_kafka_json_batch(
    topic: str,
    *,
    bootstrap_servers: str,
    max_messages: int = 100,
    experimental: bool = True,
    **consumer_config: Any,
) -> dict[str, list[Any]]:
    """
    Poll JSON payloads from ``topic`` into columns ``key``, ``value``, ``partition``, ``offset``.

    **At-least-once** delivery only; values must be JSON objects whose keys become columns when
    unioning (best-effort flatten).
    """
    _require_experimental(experimental, "Kafka ingestion")
    try:
        from kafka import KafkaConsumer  # type: ignore[import-not-found]
    except ImportError as e:
        raise ImportError(
            "read_kafka_json_batch requires kafka-python (pip install 'pydantable[kafka]')."
        ) from e
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        **consumer_config,
    )
    rows: list[dict[str, Any]] = []
    try:
        for _ in range(max_messages):
            pack = consumer.poll(timeout_ms=2000)
            if not pack:
                break
            for _tp, messages in pack.items():
                for msg in messages:
                    val = msg.value if isinstance(msg.value, dict) else {}
                    row = {
                        "key": msg.key.decode("utf-8") if msg.key else None,
                        "partition": msg.partition,
                        "offset": msg.offset,
                        **val,
                    }
                    rows.append(row)
                    if len(rows) >= max_messages:
                        break
                if len(rows) >= max_messages:
                    break
            if len(rows) >= max_messages:
                break
    finally:
        consumer.close()
    if not rows:
        return {}
    keys = sorted({k for r in rows for k in r})
    return {k: [r.get(k) for r in rows] for k in keys}


def read_csv_stdin(
    stream: TextIO | None = None,
    *,
    engine: str = "auto",
) -> dict[str, list[Any]]:
    """Read CSV from ``stdin`` (or ``stream``) via a temporary file + :func:`read_csv`."""
    import tempfile

    from . import read_csv

    fh = stream or sys.stdin
    data = fh.read()
    raw = data.encode("utf-8") if isinstance(data, str) else data
    path = Path(tempfile.mkstemp(suffix=".csv")[1])
    try:
        path.write_bytes(raw)
        return read_csv(str(path), engine=engine)
    finally:
        path.unlink(missing_ok=True)


def write_csv_stdout(
    data: dict[str, list[Any]],
    stream: TextIO | BinaryIO | None = None,
    *,
    engine: str = "auto",
) -> None:
    """Write ``data`` as CSV to ``stdout`` (or ``stream``) using :func:`write_csv` to a temp file."""
    import tempfile

    from . import write_csv

    path = Path(tempfile.mkstemp(suffix=".csv")[1])
    try:
        write_csv(str(path), data, engine=engine)
        out = path.read_bytes()
        if stream is None:
            sys.stdout.buffer.write(out)
        elif hasattr(stream, "buffer"):
            stream.buffer.write(out)
        else:
            cast("TextIO", stream).write(out.decode("utf-8"))
    finally:
        path.unlink(missing_ok=True)
