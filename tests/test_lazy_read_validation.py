from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler
from typing import TYPE_CHECKING

import pytest
from conftest import http_server_thread
from pydantable import DataFrame, DataFrameModel
from pydantable.io import export_ipc, export_parquet
from pydantic import BaseModel

if TYPE_CHECKING:
    from pathlib import Path


class _Row(BaseModel):
    id: int


class _RowWithDefault(BaseModel):
    id: int
    note: str | None = "n/a"


class _Model(DataFrameModel):
    id: int


def _write_bad_csv(path: Path) -> None:
    path.write_text("id\n1\nbad\n2\n", encoding="utf-8")


def _write_bad_ndjson(path: Path) -> None:
    path.write_text(
        json.dumps({"id": "1"})
        + "\n"
        + json.dumps({"id": "bad"})
        + "\n"
        + json.dumps({"id": "2"})
        + "\n",
        encoding="utf-8",
    )


def _write_bad_parquet(path: Path) -> None:
    # Persist as Utf8 so the scan yields strings; schema expects int and one row fails.
    export_parquet(path, {"id": ["1", "bad", "2"]})


def _write_bad_ipc(path: Path) -> None:
    export_ipc(path, {"id": ["1", "bad", "2"]})


@pytest.mark.parametrize(
    ("fmt", "writer", "df_read", "dfm_read"),
    [
        ("csv", _write_bad_csv, "read_csv", "read_csv"),
        ("ndjson", _write_bad_ndjson, "read_ndjson", "read_ndjson"),
        ("json", _write_bad_ndjson, "read_json", "read_json"),
        ("parquet", _write_bad_parquet, "read_parquet", "read_parquet"),
        ("ipc", _write_bad_ipc, "read_ipc", "read_ipc"),
    ],
)
def test_lazy_read_ignore_errors_applies_on_materialize_dataframe_and_model(
    tmp_path: Path,
    fmt: str,
    writer: object,
    df_read: str,
    dfm_read: str,
) -> None:
    pytest.importorskip("pydantable._core")

    path = tmp_path / f"in.{fmt}"
    writer(path)  # type: ignore[misc]

    failures_df: list[dict[str, object]] = []
    failures_dfm: list[dict[str, object]] = []

    def on_fail_df(items: list[dict[str, object]]) -> None:
        failures_df.extend(items)

    def on_fail_dfm(items: list[dict[str, object]]) -> None:
        failures_dfm.extend(items)

    df = getattr(DataFrame[_Row], df_read)(
        str(path),
        ignore_errors=True,
        on_validation_errors=on_fail_df,
    )
    dfm = getattr(_Model, dfm_read)(
        str(path),
        ignore_errors=True,
        on_validation_errors=on_fail_dfm,
    )

    assert df.to_dict() == {"id": [1, 2]}
    assert dfm.to_dict() == {"id": [1, 2]}
    assert [int(x["row_index"]) for x in failures_df] == [1]
    assert [int(x["row_index"]) for x in failures_dfm] == [1]


@pytest.mark.parametrize(
    ("fmt", "writer", "df_read"),
    [
        ("csv", _write_bad_csv, "read_csv"),
        ("ndjson", _write_bad_ndjson, "read_ndjson"),
        ("json", _write_bad_ndjson, "read_json"),
        ("parquet", _write_bad_parquet, "read_parquet"),
        ("ipc", _write_bad_ipc, "read_ipc"),
    ],
)
def test_lazy_read_strict_raises_on_bad_row(
    tmp_path: Path, fmt: str, writer: object, df_read: str
) -> None:
    pytest.importorskip("pydantable._core")

    path = tmp_path / f"strict.{fmt}"
    writer(path)  # type: ignore[misc]

    df = getattr(DataFrame[_Row], df_read)(str(path))
    with pytest.raises(ValueError):
        _ = df.to_dict()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fmt", "writer", "model_aread"),
    [
        ("csv", _write_bad_csv, "aread_csv"),
        ("ndjson", _write_bad_ndjson, "aread_ndjson"),
        ("json", _write_bad_ndjson, "aread_json"),
        ("parquet", _write_bad_parquet, "aread_parquet"),
        ("ipc", _write_bad_ipc, "aread_ipc"),
    ],
)
async def test_aread_lazy_read_ignore_errors_applies_on_materialize(
    tmp_path: Path, fmt: str, writer: object, model_aread: str
) -> None:
    pytest.importorskip("pydantable._core")

    path = tmp_path / f"ain.{fmt}"
    writer(path)  # type: ignore[misc]

    failures_dfm: list[dict[str, object]] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        failures_dfm.extend(items)

    dfm = await getattr(_Model, model_aread)(
        str(path),
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert dfm.to_dict() == {"id": [1, 2]}
    assert [int(x["row_index"]) for x in failures_dfm] == [1]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fmt", "writer", "df_aread"),
    [
        ("csv", _write_bad_csv, "aread_csv"),
        ("ndjson", _write_bad_ndjson, "aread_ndjson"),
        ("json", _write_bad_ndjson, "aread_json"),
        ("parquet", _write_bad_parquet, "aread_parquet"),
        ("ipc", _write_bad_ipc, "aread_ipc"),
    ],
)
async def test_dataframe_aread_ignore_errors_applies_on_materialize(
    tmp_path: Path,
    fmt: str,
    writer: object,
    df_aread: str,
) -> None:
    pytest.importorskip("pydantable._core")

    path = tmp_path / f"df_aread.{fmt}"
    writer(path)  # type: ignore[misc]

    failures: list[dict[str, object]] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        failures.extend(items)

    df = await getattr(DataFrame[_Row], df_aread)(
        str(path),
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert df.to_dict() == {"id": [1, 2]}
    assert [int(x["row_index"]) for x in failures] == [1]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fmt", "writer", "df_aread"),
    [
        ("csv", _write_bad_csv, "aread_csv"),
        ("ndjson", _write_bad_ndjson, "aread_ndjson"),
        ("json", _write_bad_ndjson, "aread_json"),
        ("parquet", _write_bad_parquet, "aread_parquet"),
        ("ipc", _write_bad_ipc, "aread_ipc"),
    ],
)
async def test_dataframe_aread_strict_raises_on_bad_row(
    tmp_path: Path,
    fmt: str,
    writer: object,
    df_aread: str,
) -> None:
    pytest.importorskip("pydantable._core")

    path = tmp_path / f"df_aread_strict.{fmt}"
    writer(path)  # type: ignore[misc]

    df = await getattr(DataFrame[_Row], df_aread)(str(path))
    with pytest.raises(ValueError):
        _ = df.to_dict()


def test_trusted_mode_shape_only_warns_and_does_not_filter(tmp_path: Path) -> None:
    """
    trusted_mode='shape_only' does not do per-element validation, but still enforces
    nullability/shape constraints.
    """
    pytest.importorskip("pydantable._core")

    path = tmp_path / "shape_only.parquet"
    _write_bad_parquet(path)

    df = DataFrame[_Row].read_parquet(str(path), trusted_mode="shape_only")
    with pytest.raises(ValueError, match=r"non-nullable.*null"):
        _ = df.to_dict()


def test_trusted_mode_strict_raises(tmp_path: Path) -> None:
    pytest.importorskip("pydantable._core")

    path = tmp_path / "strict_mode.parquet"
    _write_bad_parquet(path)
    df = DataFrame[_Row].read_parquet(str(path), trusted_mode="strict")
    # With schema typing applied during execution, bad parses may become nulls.
    with pytest.raises(ValueError, match=r"non-nullable.*null"):
        _ = df.to_dict()


def test_ignore_errors_is_only_effective_in_off_mode(tmp_path: Path) -> None:
    """
    ignore_errors/on_validation_errors are only honored when element validation runs
    (trusted_mode='off'). In trusted modes, we do not skip rows.
    """
    pytest.importorskip("pydantable._core")

    path = tmp_path / "trusted_ignore.parquet"
    _write_bad_parquet(path)

    called = False

    def on_fail(_items: list[dict[str, object]]) -> None:
        nonlocal called
        called = True

    df = DataFrame[_Row].read_parquet(
        str(path),
        trusted_mode="shape_only",
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    with pytest.raises(ValueError, match=r"non-nullable.*null"):
        _ = df.to_dict()
    assert called is False


def test_lazy_read_collect_honors_ignore_errors(tmp_path: Path) -> None:
    pytest.importorskip("pydantable._core")

    path = tmp_path / "collect.csv"
    _write_bad_csv(path)

    failures: list[dict[str, object]] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        failures.extend(items)

    df = DataFrame[_Row].read_csv(
        str(path),
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    rows = df.collect()
    assert [r.id for r in rows] == [1, 2]
    assert failures and set(failures[0].keys()) == {"row_index", "row", "errors"}
    assert failures[0]["row_index"] == 1
    # The execution engine may coerce unparseable cells to null (None) before
    # Python-side validation/skip runs.
    assert failures[0]["row"] == {"id": None}
    assert isinstance(failures[0]["errors"], list)
    first_err = failures[0]["errors"][0]
    assert isinstance(first_err, dict)
    assert {"type", "loc", "msg", "input"} <= set(first_err.keys())


def test_lazy_read_to_polars_and_to_arrow_reflect_filtered_output(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pydantable._core")

    path = tmp_path / "interop.csv"
    _write_bad_csv(path)

    df = DataFrame[_Row].read_csv(str(path), ignore_errors=True)

    pytest.importorskip("polars")
    pdf = df.to_polars()
    assert pdf.to_dict(as_series=False) == {"id": [1, 2]}

    pytest.importorskip("pyarrow")
    tbl = df.to_arrow()
    assert tbl.to_pydict() == {"id": [1, 2]}


@pytest.mark.network
def test_read_parquet_url_ignore_errors_applies_on_materialize(tmp_path: Path) -> None:
    pytest.importorskip("pydantable._core")
    pytest.importorskip("pyarrow")

    pq_path = tmp_path / "in.parquet"
    export_parquet(pq_path, {"id": ["1", "bad", "2"]})
    blob = pq_path.read_bytes()

    class H(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Length", str(len(blob)))
            self.end_headers()
            self.wfile.write(blob)

        def log_message(self, *args: object) -> None:
            return

    server, _ = http_server_thread(H)
    try:
        url = f"http://127.0.0.1:{server.server_port}/in.parquet"
        failures: list[dict[str, object]] = []

        def on_fail(items: list[dict[str, object]]) -> None:
            failures.extend(items)

        df = DataFrame[_Row].read_parquet_url(
            url, experimental=True, ignore_errors=True, on_validation_errors=on_fail
        )
        assert df.to_dict() == {"id": [1, 2]}
        assert [int(x["row_index"]) for x in failures] == [1]
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.network
def test_read_parquet_url_fill_missing_optional_false_with_explicit_default(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pydantable._core")
    pytest.importorskip("pyarrow")

    pq_path = tmp_path / "in_default.parquet"
    export_parquet(pq_path, {"id": [1, 2]})
    blob = pq_path.read_bytes()

    class H(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Length", str(len(blob)))
            self.end_headers()
            self.wfile.write(blob)

        def log_message(self, *args: object) -> None:
            return

    server, _ = http_server_thread(H)
    try:
        url = f"http://127.0.0.1:{server.server_port}/in_default.parquet"
        df = DataFrame[_RowWithDefault].read_parquet_url(
            url,
            experimental=True,
            fill_missing_optional=False,
        )
        assert df.to_dict() == {"id": [1, 2], "note": ["n/a", "n/a"]}
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.asyncio
async def test_dataframe_aread_csv_fill_missing_optional_false_with_explicit_default(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pydantable._core")
    path = tmp_path / "default_aread.csv"
    path.write_text("id\n1\n2\n", encoding="utf-8")

    df = await DataFrame[_RowWithDefault].aread_csv(
        str(path),
        fill_missing_optional=False,
    )
    assert df.to_dict() == {"id": [1, 2], "note": ["n/a", "n/a"]}


def test_fill_missing_optional_false_with_default_does_not_trigger_error_callback(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pydantable._core")
    path = tmp_path / "default_callback.csv"
    path.write_text("id\n1\n2\n", encoding="utf-8")

    called = False

    def on_fail(_items: list[dict[str, object]]) -> None:
        nonlocal called
        called = True

    df = DataFrame[_RowWithDefault].read_csv(
        str(path),
        fill_missing_optional=False,
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert df.to_dict() == {"id": [1, 2], "note": ["n/a", "n/a"]}
    assert called is False
