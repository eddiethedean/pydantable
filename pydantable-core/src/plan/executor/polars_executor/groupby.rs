//! Grouped aggregations (standard and time-based).

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::plan::execute_polars;
use crate::plan::ir::PlanInner;

use super::super::PolarsExecutor;

impl PolarsExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn groupby_agg(
        py: Python<'_>,
        plan: &PlanInner,
        root_data: &Bound<'_, PyAny>,
        by: Vec<String>,
        aggregations: Vec<(String, String, String)>,
        maintain_order: bool,
        drop_nulls: bool,
        as_python_lists: bool,
        streaming: bool,
    ) -> PyResult<(PyObject, PyObject)> {
        execute_polars::execute_groupby_agg_polars(
            py,
            plan,
            root_data,
            by,
            aggregations,
            maintain_order,
            drop_nulls,
            as_python_lists,
            streaming,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn groupby_dynamic_agg(
        py: Python<'_>,
        plan: &PlanInner,
        root_data: &Bound<'_, PyAny>,
        index_column: String,
        every: String,
        period: Option<String>,
        by: Option<Vec<String>>,
        aggregations: Vec<(String, String, String)>,
        as_python_lists: bool,
        streaming: bool,
    ) -> PyResult<(PyObject, PyObject)> {
        execute_polars::execute_groupby_dynamic_agg_polars(
            py,
            plan,
            root_data,
            index_column,
            every,
            period,
            by,
            aggregations,
            as_python_lists,
            streaming,
        )
    }
}
