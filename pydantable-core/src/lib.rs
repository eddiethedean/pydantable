use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;

/// Minimal Rust/PyO3 stub module for the `pydantable._core` extension.
///
/// This exists so the Python side can import the extension module via maturin.
/// Planner/execution logic will be filled in in later versions.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(execute_plan, m)?)?;
    m.add_function(wrap_pyfunction!(rust_version, m)?)?;
    Ok(())
}

#[pyfunction]
fn rust_version() -> &'static str {
    "0.4.0-skeleton"
}

#[pyfunction]
fn execute_plan(_plan: &Bound<'_, PyAny>, _data: &Bound<'_, PyAny>) -> PyResult<PyObject> {
    Err(PyNotImplementedError::new_err(
        "Rust execution is not implemented in the 0.4.0 skeleton.",
    ))
}

