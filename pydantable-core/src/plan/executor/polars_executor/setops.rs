//! Concatenation and set operations.

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::plan::execute_polars;
use crate::plan::ir::PlanInner;

use super::super::PolarsExecutor;

impl PolarsExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn concat(
        py: Python<'_>,
        left_plan: &PlanInner,
        left_root_data: &Bound<'_, PyAny>,
        right_plan: &PlanInner,
        right_root_data: &Bound<'_, PyAny>,
        how: String,
        as_python_lists: bool,
        streaming: bool,
    ) -> PyResult<(PyObject, PyObject)> {
        execute_polars::execute_concat_polars(
            py,
            left_plan,
            left_root_data,
            right_plan,
            right_root_data,
            how,
            as_python_lists,
            streaming,
        )
    }

    pub fn except_all(
        py: Python<'_>,
        left_plan: &PlanInner,
        left_root_data: &Bound<'_, PyAny>,
        right_plan: &PlanInner,
        right_root_data: &Bound<'_, PyAny>,
        as_python_lists: bool,
        streaming: bool,
    ) -> PyResult<(PyObject, PyObject)> {
        execute_polars::execute_except_all_polars(
            py,
            left_plan,
            left_root_data,
            right_plan,
            right_root_data,
            as_python_lists,
            streaming,
        )
    }

    pub fn intersect_all(
        py: Python<'_>,
        left_plan: &PlanInner,
        left_root_data: &Bound<'_, PyAny>,
        right_plan: &PlanInner,
        right_root_data: &Bound<'_, PyAny>,
        as_python_lists: bool,
        streaming: bool,
    ) -> PyResult<(PyObject, PyObject)> {
        execute_polars::execute_intersect_all_polars(
            py,
            left_plan,
            left_root_data,
            right_plan,
            right_root_data,
            as_python_lists,
            streaming,
        )
    }
}
