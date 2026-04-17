//! Lazy sinks and batched collection (`sink_*`, `collect_plan_batches`).

use pyo3::prelude::*;
use pyo3::types::PyAny;

#[cfg(feature = "polars_engine")]
use crate::plan::{
    collect_plan_batches_polars, sink_csv_polars, sink_ipc_polars, sink_ndjson_polars,
    sink_parquet_polars, PlanInner,
};

use crate::python_api::types::PyPlan;

/// Parameters for [`sink_parquet`](sink_parquet) after the plan/root handles are fixed.
#[cfg(feature = "polars_engine")]
struct ParquetSinkOptions<'py> {
    path: String,
    streaming: bool,
    write_kwargs: Option<&'py Bound<'py, PyAny>>,
    partition_by: Option<Vec<String>>,
    mkdir: bool,
}

#[cfg(feature = "polars_engine")]
fn sink_parquet_dispatch(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    opts: ParquetSinkOptions<'_>,
) -> PyResult<()> {
    sink_parquet_polars(
        py,
        plan,
        root_data,
        opts.path,
        opts.streaming,
        opts.write_kwargs,
        opts.partition_by,
        opts.mkdir,
    )
}

#[cfg(feature = "polars_engine")]
#[pyfunction]
#[pyo3(signature = (plan, root_data, path, streaming=false, write_kwargs=None, partition_by=None, mkdir=true))]
#[allow(clippy::too_many_arguments)]
pub(super) fn sink_parquet(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    path: String,
    streaming: bool,
    write_kwargs: Option<Bound<'_, PyAny>>,
    partition_by: Option<Vec<String>>,
    mkdir: bool,
) -> PyResult<()> {
    sink_parquet_dispatch(
        py,
        &plan.inner,
        root_data,
        ParquetSinkOptions {
            path,
            streaming,
            write_kwargs: write_kwargs.as_ref(),
            partition_by,
            mkdir,
        },
    )
}

#[cfg(not(feature = "polars_engine"))]
#[pyfunction]
#[pyo3(signature = (plan, root_data, path, streaming=false, write_kwargs=None, partition_by=None, mkdir=true))]
#[allow(clippy::too_many_arguments)]
pub(super) fn sink_parquet(
    _py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    path: String,
    streaming: bool,
    write_kwargs: Option<Bound<'_, PyAny>>,
    partition_by: Option<Vec<String>>,
    mkdir: bool,
) -> PyResult<()> {
    Err(pyo3::exceptions::PyRuntimeError::new_err(
        "sink_parquet requires pydantable-core built with the `polars_engine` feature.",
    ))
}

#[cfg(feature = "polars_engine")]
#[pyfunction]
#[pyo3(signature = (plan, root_data, path, streaming=false, separator=44, write_kwargs=None))]
pub(super) fn sink_csv(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    path: String,
    streaming: bool,
    separator: u8,
    write_kwargs: Option<Bound<'_, PyAny>>,
) -> PyResult<()> {
    sink_csv_polars(
        py,
        &plan.inner,
        root_data,
        path,
        streaming,
        separator,
        write_kwargs.as_ref(),
    )
}

#[cfg(not(feature = "polars_engine"))]
#[pyfunction]
#[pyo3(signature = (plan, root_data, path, streaming=false, separator=44, write_kwargs=None))]
pub(super) fn sink_csv(
    _py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    path: String,
    streaming: bool,
    separator: u8,
    write_kwargs: Option<Bound<'_, PyAny>>,
) -> PyResult<()> {
    Err(pyo3::exceptions::PyRuntimeError::new_err(
        "sink_csv requires pydantable-core built with the `polars_engine` feature.",
    ))
}

#[cfg(feature = "polars_engine")]
#[pyfunction]
#[pyo3(signature = (plan, root_data, path, streaming=false, compression=None, write_kwargs=None))]
pub(super) fn sink_ipc(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    path: String,
    streaming: bool,
    compression: Option<String>,
    write_kwargs: Option<Bound<'_, PyAny>>,
) -> PyResult<()> {
    sink_ipc_polars(
        py,
        &plan.inner,
        root_data,
        path,
        streaming,
        compression,
        write_kwargs.as_ref(),
    )
}

#[cfg(not(feature = "polars_engine"))]
#[pyfunction]
#[pyo3(signature = (plan, root_data, path, streaming=false, compression=None, write_kwargs=None))]
pub(super) fn sink_ipc(
    _py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    path: String,
    streaming: bool,
    compression: Option<String>,
    write_kwargs: Option<Bound<'_, PyAny>>,
) -> PyResult<()> {
    Err(pyo3::exceptions::PyRuntimeError::new_err(
        "sink_ipc requires pydantable-core built with the `polars_engine` feature.",
    ))
}

#[cfg(feature = "polars_engine")]
#[pyfunction]
#[pyo3(signature = (plan, root_data, path, streaming=false, write_kwargs=None))]
pub(super) fn sink_ndjson(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    path: String,
    streaming: bool,
    write_kwargs: Option<Bound<'_, PyAny>>,
) -> PyResult<()> {
    sink_ndjson_polars(
        py,
        &plan.inner,
        root_data,
        path,
        streaming,
        write_kwargs.as_ref(),
    )
}

#[cfg(not(feature = "polars_engine"))]
#[pyfunction]
#[pyo3(signature = (plan, root_data, path, streaming=false, write_kwargs=None))]
pub(super) fn sink_ndjson(
    _py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    path: String,
    streaming: bool,
    write_kwargs: Option<Bound<'_, PyAny>>,
) -> PyResult<()> {
    Err(pyo3::exceptions::PyRuntimeError::new_err(
        "sink_ndjson requires pydantable-core built with the `polars_engine` feature.",
    ))
}

#[cfg(feature = "polars_engine")]
#[pyfunction]
#[pyo3(signature = (plan, root_data, batch_size=65536, streaming=false))]
pub(super) fn collect_plan_batches(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    batch_size: usize,
    streaming: bool,
) -> PyResult<Vec<PyObject>> {
    collect_plan_batches_polars(py, &plan.inner, root_data, batch_size, streaming)
}

#[cfg(not(feature = "polars_engine"))]
#[pyfunction]
#[pyo3(signature = (plan, root_data, batch_size=65536, streaming=false))]
pub(super) fn collect_plan_batches(
    _py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    batch_size: usize,
    streaming: bool,
) -> PyResult<Vec<PyObject>> {
    Err(pyo3::exceptions::PyRuntimeError::new_err(
        "collect_plan_batches requires pydantable-core built with the `polars_engine` feature.",
    ))
}
