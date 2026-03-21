//! Dependency inversion: execution backends implement [`PhysicalPlanExecutor`].

use pyo3::prelude::*;
use pyo3::types::PyAny;

use super::ir::PlanInner;

/// Executes a logical [`PlanInner`] against root columnar Python data.
pub trait PhysicalPlanExecutor {
    fn execute_plan(
        &self,
        py: Python<'_>,
        plan: &PlanInner,
        root_data: &Bound<'_, PyAny>,
        as_python_lists: bool,
    ) -> PyResult<PyObject>;
}

#[cfg(feature = "polars_engine")]
pub struct PolarsExecutor;

#[cfg(feature = "polars_engine")]
impl PhysicalPlanExecutor for PolarsExecutor {
    fn execute_plan(
        &self,
        py: Python<'_>,
        plan: &PlanInner,
        root_data: &Bound<'_, PyAny>,
        as_python_lists: bool,
    ) -> PyResult<PyObject> {
        super::execute_polars::execute_plan_polars(py, plan, root_data, as_python_lists)
    }
}

#[cfg(not(feature = "polars_engine"))]
pub struct RowwiseExecutor;

#[cfg(not(feature = "polars_engine"))]
impl PhysicalPlanExecutor for RowwiseExecutor {
    fn execute_plan(
        &self,
        py: Python<'_>,
        plan: &PlanInner,
        root_data: &Bound<'_, PyAny>,
        _as_python_lists: bool,
    ) -> PyResult<PyObject> {
        super::execute_rowwise::execute_plan_rowwise(py, plan, root_data, _as_python_lists)
    }
}
