//! Full-plan execution (`execute_plan`).

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::plan::execute_polars;
use crate::plan::ir::PlanInner;

use super::super::PolarsExecutor;

impl PolarsExecutor {
    pub fn execute_plan(
        py: Python<'_>,
        plan: &PlanInner,
        root_data: &Bound<'_, PyAny>,
        as_python_lists: bool,
        streaming: bool,
    ) -> PyResult<PyObject> {
        execute_polars::execute_plan_polars(py, plan, root_data, as_python_lists, streaming)
    }
}
