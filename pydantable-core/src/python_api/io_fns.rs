//! PyO3 exports for Rust-backed columnar file I/O.

use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyfunction]
fn io_read_parquet_path(py: Python<'_>, path: String) -> PyResult<PyObject> {
    #[cfg(feature = "polars_engine")]
    {
        crate::io_polars::read_parquet_file(py, path)
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Rust Parquet reads require pydantable-core built with `polars_engine`.",
        ))
    }
}

#[pyfunction]
fn io_read_csv_path(py: Python<'_>, path: String) -> PyResult<PyObject> {
    #[cfg(feature = "polars_engine")]
    {
        crate::io_polars::read_csv_file(py, path)
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Rust CSV reads require pydantable-core built with `polars_engine`.",
        ))
    }
}

#[pyfunction]
fn io_read_ndjson_path(py: Python<'_>, path: String) -> PyResult<PyObject> {
    #[cfg(feature = "polars_engine")]
    {
        crate::io_polars::read_ndjson_file(py, path)
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Rust NDJSON reads require pydantable-core built with `polars_engine`.",
        ))
    }
}

#[pyfunction]
fn io_read_ipc_path(py: Python<'_>, path: String) -> PyResult<PyObject> {
    #[cfg(feature = "polars_engine")]
    {
        crate::io_polars::read_ipc_file(py, path)
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Rust IPC reads require pydantable-core built with `polars_engine`.",
        ))
    }
}

#[pyfunction]
fn io_write_parquet_path(py: Python<'_>, path: String, data: &Bound<'_, PyDict>) -> PyResult<()> {
    #[cfg(feature = "polars_engine")]
    {
        crate::io_polars::write_parquet_file(py, path, data)
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Rust Parquet writes require pydantable-core built with `polars_engine`.",
        ))
    }
}

#[pyfunction]
fn io_write_csv_path(py: Python<'_>, path: String, data: &Bound<'_, PyDict>) -> PyResult<()> {
    #[cfg(feature = "polars_engine")]
    {
        crate::io_polars::write_csv_file(py, path, data)
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Rust CSV writes require pydantable-core built with `polars_engine`.",
        ))
    }
}

#[pyfunction]
fn io_write_ndjson_path(py: Python<'_>, path: String, data: &Bound<'_, PyDict>) -> PyResult<()> {
    #[cfg(feature = "polars_engine")]
    {
        crate::io_polars::write_ndjson_file(py, path, data)
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Rust NDJSON writes require pydantable-core built with `polars_engine`.",
        ))
    }
}

#[pyfunction]
fn io_write_ipc_path(py: Python<'_>, path: String, data: &Bound<'_, PyDict>) -> PyResult<()> {
    #[cfg(feature = "polars_engine")]
    {
        crate::io_polars::write_ipc_file(py, path, data)
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Rust IPC writes require pydantable-core built with `polars_engine`.",
        ))
    }
}

pub(super) fn register_functions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(io_read_parquet_path, m)?)?;
    m.add_function(wrap_pyfunction!(io_read_csv_path, m)?)?;
    m.add_function(wrap_pyfunction!(io_read_ndjson_path, m)?)?;
    m.add_function(wrap_pyfunction!(io_read_ipc_path, m)?)?;
    m.add_function(wrap_pyfunction!(io_write_parquet_path, m)?)?;
    m.add_function(wrap_pyfunction!(io_write_csv_path, m)?)?;
    m.add_function(wrap_pyfunction!(io_write_ndjson_path, m)?)?;
    m.add_function(wrap_pyfunction!(io_write_ipc_path, m)?)?;
    Ok(())
}
