//! `execute_plan` PyO3 binding.

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::plan::execute_plan as execute_plan_inner;

use crate::python_api::types::PyPlan;

#[pyfunction]
#[pyo3(signature = (plan, root_data, as_python_lists=false, streaming=false))]
pub(super) fn execute_plan(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    as_python_lists: bool,
    streaming: bool,
) -> PyResult<PyObject> {
    execute_plan_inner(py, &plan.inner, root_data, as_python_lists, streaming)
}
