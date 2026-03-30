//! Async materialization: Rust future bridged to a Python awaitable via `pyo3-async-runtimes`.

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::plan::execute_plan as execute_plan_inner;
use crate::python_api::types::PyPlan;

/// Run [`crate::plan::execute_plan`] on Tokio's blocking pool and await as a Python coroutine.
///
/// Work still happens in Rust with GIL released during Polars collect; this path avoids
/// Python's `asyncio.to_thread` for the engine call.
#[pyfunction]
#[pyo3(signature = (plan, root_data, as_python_lists=false, streaming=false))]
pub fn async_execute_plan<'py>(
    py: Python<'py>,
    plan: &PyPlan,
    root_data: Bound<'py, PyAny>,
    as_python_lists: bool,
    streaming: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let plan_inner = plan.inner.clone();
    let root_data = root_data.unbind();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let py_result = tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| {
                let root = root_data.bind(py);
                execute_plan_inner(py, &plan_inner, root, as_python_lists, streaming)
            })
        })
        .await
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "async_execute_plan blocking task join failed: {e}"
            ))
        })?;
        py_result
    })
}

#[cfg(feature = "polars_engine")]
use crate::plan::collect_plan_batches_polars;

/// Async wrapper around [`collect_plan_batches_polars`] (full collect then slice).
#[cfg(feature = "polars_engine")]
#[pyfunction]
#[pyo3(signature = (plan, root_data, batch_size=65_536, streaming=false))]
pub fn async_collect_plan_batches<'py>(
    py: Python<'py>,
    plan: &PyPlan,
    root_data: Bound<'py, PyAny>,
    batch_size: usize,
    streaming: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let plan_inner = plan.inner.clone();
    let root_data = root_data.unbind();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let py_result = tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| {
                collect_plan_batches_polars(
                    py,
                    &plan_inner,
                    root_data.bind(py),
                    batch_size,
                    streaming,
                )
            })
        })
        .await
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "async_collect_plan_batches blocking task join failed: {e}"
            ))
        })?;
        py_result
    })
}

#[cfg(not(feature = "polars_engine"))]
#[pyfunction]
#[pyo3(signature = (plan, root_data, batch_size=65_536, streaming=false))]
pub fn async_collect_plan_batches<'py>(
    _py: Python<'py>,
    plan: &PyPlan,
    root_data: Bound<'py, PyAny>,
    batch_size: usize,
    streaming: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let _ = (plan, root_data, batch_size, streaming);
    Err(pyo3::exceptions::PyRuntimeError::new_err(
        "async_collect_plan_batches requires pydantable-core built with the `polars_engine` feature.",
    ))
}

pub(super) fn register_functions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(async_execute_plan, m)?)?;
    m.add_function(wrap_pyfunction!(async_collect_plan_batches, m)?)?;
    Ok(())
}
