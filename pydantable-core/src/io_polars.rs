//! Columnar file I/O via Polars (Rust), materialized to Python ``dict[str, list]``.
//!
//! Writes from a column dict use a short Python ``polars.DataFrame`` → IPC → Rust path so
//! we reuse Polars' type inference without reimplementing every dtype in PyO3.

use std::fs::File;
use std::io::Cursor;

use polars::prelude::*;
use polars_io::csv::write::CsvWriter;
use polars_io::ipc::{IpcReader, IpcWriter};
use polars_io::json::JsonWriter;
use polars_io::prelude::{ParquetWriter, SerReader, SerWriter};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::plan::{dtype_from_polars, series_to_py_list};

fn polars_io_err(e: PolarsError) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Polars I/O error: {e}"))
}

pub fn dataframe_to_column_dict(py: Python<'_>, df: &DataFrame) -> PyResult<PyObject> {
    let d = PyDict::new(py);
    for col in df.columns() {
        let s = col.as_materialized_series();
        let dt = dtype_from_polars(s.dtype())?;
        let py_list = series_to_py_list(py, s, &dt)?;
        d.set_item(col.name().as_str(), py_list)?;
    }
    Ok(d.unbind().into())
}

/// Read Parquet at ``path`` into ``dict[str, list]`` (GIL released during scan/collect).
pub fn read_parquet_file(py: Python<'_>, path: String) -> PyResult<PyObject> {
    let df = py
        .allow_threads(move || {
            LazyFrame::scan_parquet(PlRefPath::new(path.as_str()), ScanArgsParquet::default())?
                .collect()
        })
        .map_err(polars_io_err)?;
    dataframe_to_column_dict(py, &df)
}

/// Read CSV at ``path`` into ``dict[str, list]``.
pub fn read_csv_file(py: Python<'_>, path: String) -> PyResult<PyObject> {
    let df = py
        .allow_threads(move || {
            LazyCsvReader::new(PlRefPath::new(path.as_str()))
                .finish()?
                .collect()
        })
        .map_err(polars_io_err)?;
    dataframe_to_column_dict(py, &df)
}

/// Read newline-delimited JSON at ``path`` into ``dict[str, list]``.
pub fn read_ndjson_file(py: Python<'_>, path: String) -> PyResult<PyObject> {
    let df = py
        .allow_threads(move || {
            LazyJsonLineReader::new(PlRefPath::new(path.as_str()))
                .finish()?
                .collect()
        })
        .map_err(polars_io_err)?;
    dataframe_to_column_dict(py, &df)
}

/// Read Arrow IPC *file* format at ``path`` into ``dict[str, list]``.
pub fn read_ipc_file(py: Python<'_>, path: String) -> PyResult<PyObject> {
    let bytes = std::fs::read(path.as_str())
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("read ipc file: {e}")))?;
    let df = py
        .allow_threads(move || IpcReader::new(Cursor::new(bytes)).finish())
        .map_err(polars_io_err)?;
    dataframe_to_column_dict(py, &df)
}

fn dict_to_dataframe_via_polars_py(
    py: Python<'_>,
    data: &Bound<'_, PyDict>,
) -> PyResult<DataFrame> {
    let io_mod = py.import("io")?;
    let buf = io_mod.call_method0("BytesIO")?;
    let polars = py.import("polars").map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyImportError, _>(
            "column-dict writes require the optional `polars` package \
             (pip install 'pydantable[polars]' or pip install polars).",
        )
    })?;
    let df_cls = polars.getattr("DataFrame")?;
    let df_py = df_cls.call1((data,))?;
    df_py.call_method1("write_ipc", (&buf,))?;
    let py_bytes: Vec<u8> = buf.call_method0("getvalue")?.extract()?;
    IpcReader::new(Cursor::new(py_bytes))
        .finish()
        .map_err(polars_io_err)
}

pub fn write_parquet_file(py: Python<'_>, path: String, data: &Bound<'_, PyDict>) -> PyResult<()> {
    let mut df = dict_to_dataframe_via_polars_py(py, data)?;
    py.allow_threads(move || {
        let mut file = File::create(path.as_str()).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("create parquet: {e}"))
        })?;
        ParquetWriter::new(&mut file)
            .finish(&mut df)
            .map_err(polars_io_err)
    })?;
    Ok(())
}

pub fn write_csv_file(py: Python<'_>, path: String, data: &Bound<'_, PyDict>) -> PyResult<()> {
    let mut df = dict_to_dataframe_via_polars_py(py, data)?;
    py.allow_threads(move || {
        let mut file = File::create(path.as_str()).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("create csv: {e}"))
        })?;
        CsvWriter::new(&mut file)
            .finish(&mut df)
            .map_err(polars_io_err)
    })?;
    Ok(())
}

pub fn write_ndjson_file(py: Python<'_>, path: String, data: &Bound<'_, PyDict>) -> PyResult<()> {
    let mut df = dict_to_dataframe_via_polars_py(py, data)?;
    py.allow_threads(move || {
        let mut file = File::create(path.as_str()).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("create ndjson: {e}"))
        })?;
        JsonWriter::new(&mut file)
            .with_json_format(JsonFormat::JsonLines)
            .finish(&mut df)
            .map_err(polars_io_err)
    })?;
    Ok(())
}

pub fn write_ipc_file(py: Python<'_>, path: String, data: &Bound<'_, PyDict>) -> PyResult<()> {
    let mut df = dict_to_dataframe_via_polars_py(py, data)?;
    py.allow_threads(move || {
        let mut file = File::create(path.as_str()).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("create ipc: {e}"))
        })?;
        IpcWriter::new(&mut file)
            .finish(&mut df)
            .map_err(polars_io_err)
    })?;
    Ok(())
}
