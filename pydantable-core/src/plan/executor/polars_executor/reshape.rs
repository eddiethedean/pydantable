//! Reshape helpers (melt, pivot, explode, unnest).

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::plan::execute_polars;
use crate::plan::ir::PlanInner;

use super::super::PolarsExecutor;

impl PolarsExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn melt(
        py: Python<'_>,
        plan: &PlanInner,
        root_data: &Bound<'_, PyAny>,
        id_vars: Vec<String>,
        value_vars: Option<Vec<String>>,
        variable_name: String,
        value_name: String,
        as_python_lists: bool,
        streaming: bool,
    ) -> PyResult<(PyObject, PyObject)> {
        execute_polars::execute_melt_polars(
            py,
            plan,
            root_data,
            id_vars,
            value_vars,
            variable_name,
            value_name,
            as_python_lists,
            streaming,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn pivot(
        py: Python<'_>,
        plan: &PlanInner,
        root_data: &Bound<'_, PyAny>,
        index: Vec<String>,
        columns: String,
        values: Vec<String>,
        aggregate_function: String,
        pivot_values: Option<Vec<PyObject>>,
        sort_columns: bool,
        separator: String,
        as_python_lists: bool,
        streaming: bool,
    ) -> PyResult<(PyObject, PyObject)> {
        execute_polars::execute_pivot_polars(
            py,
            plan,
            root_data,
            index,
            columns,
            values,
            aggregate_function,
            pivot_values,
            sort_columns,
            separator,
            as_python_lists,
            streaming,
        )
    }

    pub fn explode(
        py: Python<'_>,
        plan: &PlanInner,
        root_data: &Bound<'_, PyAny>,
        columns: Vec<String>,
        streaming: bool,
        outer: bool,
    ) -> PyResult<(PyObject, PyObject)> {
        execute_polars::execute_explode_polars(py, plan, root_data, columns, streaming, outer)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn posexplode(
        py: Python<'_>,
        plan: &PlanInner,
        root_data: &Bound<'_, PyAny>,
        list_column: String,
        pos_name: String,
        value_name: String,
        streaming: bool,
        outer: bool,
    ) -> PyResult<(PyObject, PyObject)> {
        execute_polars::execute_posexplode_polars(
            py,
            plan,
            root_data,
            list_column,
            pos_name,
            value_name,
            streaming,
            outer,
        )
    }

    pub fn unnest(
        py: Python<'_>,
        plan: &PlanInner,
        root_data: &Bound<'_, PyAny>,
        columns: Vec<String>,
        streaming: bool,
    ) -> PyResult<(PyObject, PyObject)> {
        execute_polars::execute_unnest_polars(py, plan, root_data, columns, streaming)
    }
}
