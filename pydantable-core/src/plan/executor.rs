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
        streaming: bool,
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
        streaming: bool,
    ) -> PyResult<PyObject> {
        PolarsExecutor::execute_plan(py, plan, root_data, as_python_lists, streaming)
    }
}

/// Polars-backed plan fragments (join, groupby, reshape) for dependency inversion at call sites.
#[cfg(feature = "polars_engine")]
impl PolarsExecutor {
    pub fn execute_plan(
        py: Python<'_>,
        plan: &PlanInner,
        root_data: &Bound<'_, PyAny>,
        as_python_lists: bool,
        streaming: bool,
    ) -> PyResult<PyObject> {
        super::execute_polars::execute_plan_polars(py, plan, root_data, as_python_lists, streaming)
    }

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
        as_python_lists: bool,
        streaming: bool,
    ) -> PyResult<(PyObject, PyObject)> {
        super::execute_polars::execute_join_polars(
            py,
            left_plan,
            left_root_data,
            right_plan,
            right_root_data,
            left_on,
            right_on,
            how,
            suffix,
            as_python_lists,
            streaming,
        )
    }

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
        super::execute_polars::execute_groupby_agg_polars(
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
        super::execute_polars::execute_concat_polars(
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
        super::execute_polars::execute_melt_polars(
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
        sort_columns: bool,
        separator: String,
        as_python_lists: bool,
        streaming: bool,
    ) -> PyResult<(PyObject, PyObject)> {
        super::execute_polars::execute_pivot_polars(
            py,
            plan,
            root_data,
            index,
            columns,
            values,
            aggregate_function,
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
    ) -> PyResult<(PyObject, PyObject)> {
        super::execute_polars::execute_explode_polars(py, plan, root_data, columns, streaming)
    }

    pub fn unnest(
        py: Python<'_>,
        plan: &PlanInner,
        root_data: &Bound<'_, PyAny>,
        columns: Vec<String>,
        streaming: bool,
    ) -> PyResult<(PyObject, PyObject)> {
        super::execute_polars::execute_unnest_polars(py, plan, root_data, columns, streaming)
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
        super::execute_polars::execute_groupby_dynamic_agg_polars(
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
        _streaming: bool,
    ) -> PyResult<PyObject> {
        super::execute_rowwise::execute_plan_rowwise(py, plan, root_data, _as_python_lists)
    }
}
