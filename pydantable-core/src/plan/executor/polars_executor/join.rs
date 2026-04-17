//! Multi-table join.

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::plan::execute_polars;
use crate::plan::ir::PlanInner;

use super::super::PolarsExecutor;

impl PolarsExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn join(
        py: Python<'_>,
        left_plan: &PlanInner,
        left_root_data: &Bound<'_, PyAny>,
        right_plan: &PlanInner,
        right_root_data: &Bound<'_, PyAny>,
        left_on: Vec<String>,
        right_on: Vec<String>,
        how: String,
        suffix: String,
        validate: Option<String>,
        coalesce: Option<bool>,
        join_nulls: Option<bool>,
        maintain_order: Option<String>,
        allow_parallel: Option<bool>,
        force_parallel: Option<bool>,
        as_python_lists: bool,
        streaming: bool,
    ) -> PyResult<(PyObject, PyObject)> {
        execute_polars::execute_join_polars(
            py,
            left_plan,
            left_root_data,
            right_plan,
            right_root_data,
            left_on,
            right_on,
            how,
            suffix,
            validate,
            coalesce,
            join_nulls,
            maintain_order,
            allow_parallel,
            force_parallel,
            as_python_lists,
            streaming,
        )
    }
}
