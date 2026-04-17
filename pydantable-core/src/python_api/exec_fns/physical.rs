//! Physical plan helpers (`execute_join`, `execute_groupby_agg`, …).

use pyo3::prelude::*;
use pyo3::types::PyAny;

#[cfg(feature = "polars_engine")]
use crate::plan::PolarsExecutor;

use crate::python_api::types::PyPlan;

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (left_plan, left_root_data, right_plan, right_root_data, left_on, right_on, how, suffix, validate=None, coalesce=None, join_nulls=None, maintain_order=None, allow_parallel=None, force_parallel=None, as_python_lists=false, streaming=false))]
pub(super) fn execute_join(
    py: Python<'_>,
    left_plan: &PyPlan,
    left_root_data: &Bound<'_, PyAny>,
    right_plan: &PyPlan,
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
    #[cfg(feature = "polars_engine")]
    {
        PolarsExecutor::join(
            py,
            &left_plan.inner,
            left_root_data,
            &right_plan.inner,
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
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "join requires pydantable-core built with the `polars_engine` feature.",
        ))
    }
}

#[pyfunction]
#[pyo3(signature = (plan, root_data, by, aggregations, maintain_order=false, drop_nulls=true, as_python_lists=false, streaming=false))]
#[allow(clippy::too_many_arguments)]
pub(super) fn execute_groupby_agg(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    by: Vec<String>,
    aggregations: &Bound<'_, PyAny>,
    maintain_order: bool,
    drop_nulls: bool,
    as_python_lists: bool,
    streaming: bool,
) -> PyResult<(PyObject, PyObject)> {
    let dict: &Bound<'_, pyo3::types::PyDict> = aggregations.downcast()?;
    let mut aggs: Vec<(String, String, String)> = Vec::new();
    for (k, v) in dict.iter() {
        let out_name: String = k.extract()?;
        let spec: &Bound<'_, pyo3::types::PyTuple> = v.downcast()?;
        if spec.len() != 2 {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "Aggregation spec must be a tuple: (op, input_column).",
            ));
        }
        let op: String = spec.get_item(0)?.extract()?;
        let in_col: String = spec.get_item(1)?.extract()?;
        aggs.push((out_name, op, in_col));
    }
    #[cfg(feature = "polars_engine")]
    {
        PolarsExecutor::groupby_agg(
            py,
            &plan.inner,
            root_data,
            by,
            aggs,
            maintain_order,
            drop_nulls,
            as_python_lists,
            streaming,
        )
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "groupby aggregation requires pydantable-core built with the `polars_engine` feature.",
        ))
    }
}

#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (left_plan, left_root_data, right_plan, right_root_data, how, as_python_lists=false, streaming=false))]
pub(super) fn execute_concat(
    py: Python<'_>,
    left_plan: &PyPlan,
    left_root_data: &Bound<'_, PyAny>,
    right_plan: &PyPlan,
    right_root_data: &Bound<'_, PyAny>,
    how: String,
    as_python_lists: bool,
    streaming: bool,
) -> PyResult<(PyObject, PyObject)> {
    #[cfg(feature = "polars_engine")]
    {
        PolarsExecutor::concat(
            py,
            &left_plan.inner,
            left_root_data,
            &right_plan.inner,
            right_root_data,
            how,
            as_python_lists,
            streaming,
        )
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "concat requires pydantable-core built with the `polars_engine` feature.",
        ))
    }
}

#[pyfunction]
#[pyo3(signature = (left_plan, left_root_data, right_plan, right_root_data, as_python_lists=false, streaming=false))]
pub(super) fn execute_except_all(
    py: Python<'_>,
    left_plan: &PyPlan,
    left_root_data: &Bound<'_, PyAny>,
    right_plan: &PyPlan,
    right_root_data: &Bound<'_, PyAny>,
    as_python_lists: bool,
    streaming: bool,
) -> PyResult<(PyObject, PyObject)> {
    #[cfg(feature = "polars_engine")]
    {
        PolarsExecutor::except_all(
            py,
            &left_plan.inner,
            left_root_data,
            &right_plan.inner,
            right_root_data,
            as_python_lists,
            streaming,
        )
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "exceptAll requires pydantable-core built with the `polars_engine` feature.",
        ))
    }
}

#[pyfunction]
#[pyo3(signature = (left_plan, left_root_data, right_plan, right_root_data, as_python_lists=false, streaming=false))]
pub(super) fn execute_intersect_all(
    py: Python<'_>,
    left_plan: &PyPlan,
    left_root_data: &Bound<'_, PyAny>,
    right_plan: &PyPlan,
    right_root_data: &Bound<'_, PyAny>,
    as_python_lists: bool,
    streaming: bool,
) -> PyResult<(PyObject, PyObject)> {
    #[cfg(feature = "polars_engine")]
    {
        PolarsExecutor::intersect_all(
            py,
            &left_plan.inner,
            left_root_data,
            &right_plan.inner,
            right_root_data,
            as_python_lists,
            streaming,
        )
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "intersectAll requires pydantable-core built with the `polars_engine` feature.",
        ))
    }
}

#[pyfunction]
#[pyo3(signature = (plan, root_data, id_vars, value_vars, variable_name, value_name, as_python_lists=false, streaming=false))]
#[allow(clippy::too_many_arguments)]
pub(super) fn execute_melt(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    id_vars: Vec<String>,
    value_vars: Option<Vec<String>>,
    variable_name: String,
    value_name: String,
    as_python_lists: bool,
    streaming: bool,
) -> PyResult<(PyObject, PyObject)> {
    #[cfg(feature = "polars_engine")]
    {
        PolarsExecutor::melt(
            py,
            &plan.inner,
            root_data,
            id_vars,
            value_vars,
            variable_name,
            value_name,
            as_python_lists,
            streaming,
        )
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "melt requires pydantable-core built with the `polars_engine` feature.",
        ))
    }
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (plan, root_data, index, columns, values, aggregate_function, pivot_values=None, sort_columns=false, separator="_".to_string(), as_python_lists=false, streaming=false))]
pub(super) fn execute_pivot(
    py: Python<'_>,
    plan: &PyPlan,
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
    #[cfg(feature = "polars_engine")]
    {
        PolarsExecutor::pivot(
            py,
            &plan.inner,
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
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "pivot requires pydantable-core built with the `polars_engine` feature.",
        ))
    }
}

#[pyfunction]
#[pyo3(signature = (plan, root_data, columns, streaming=false, outer=false))]
pub(super) fn execute_explode(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    columns: Vec<String>,
    streaming: bool,
    outer: bool,
) -> PyResult<(PyObject, PyObject)> {
    #[cfg(feature = "polars_engine")]
    {
        PolarsExecutor::explode(py, &plan.inner, root_data, columns, streaming, outer)
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "explode requires pydantable-core built with the `polars_engine` feature.",
        ))
    }
}

#[pyfunction]
#[pyo3(signature = (plan, root_data, list_column, pos_name, value_name, streaming=false, outer=false))]
#[allow(clippy::too_many_arguments)]
pub(super) fn execute_posexplode(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    list_column: String,
    pos_name: String,
    value_name: String,
    streaming: bool,
    outer: bool,
) -> PyResult<(PyObject, PyObject)> {
    #[cfg(feature = "polars_engine")]
    {
        PolarsExecutor::posexplode(
            py,
            &plan.inner,
            root_data,
            list_column,
            pos_name,
            value_name,
            streaming,
            outer,
        )
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "posexplode requires pydantable-core built with the `polars_engine` feature.",
        ))
    }
}

#[pyfunction]
#[pyo3(signature = (plan, root_data, columns, streaming=false))]
pub(super) fn execute_unnest(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    columns: Vec<String>,
    streaming: bool,
) -> PyResult<(PyObject, PyObject)> {
    #[cfg(feature = "polars_engine")]
    {
        PolarsExecutor::unnest(py, &plan.inner, root_data, columns, streaming)
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "unnest requires pydantable-core built with the `polars_engine` feature.",
        ))
    }
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub(super) fn execute_rolling_agg(
    _py: Python<'_>,
    _plan: &PyPlan,
    _root_data: &Bound<'_, PyAny>,
    _on: String,
    _column: String,
    _window_size: &Bound<'_, PyAny>,
    _op: String,
    _out_name: String,
    _by: Option<Vec<String>>,
    _min_periods: usize,
) -> PyResult<(PyObject, PyObject)> {
    Err(pyo3::exceptions::PyNotImplementedError::new_err(
        "Rust execute_rolling_agg is not yet enabled; use Python DataFrame.rolling_agg implementation.",
    ))
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (plan, root_data, index_column, every, period, by, aggregations, as_python_lists=false, streaming=false))]
pub(super) fn execute_groupby_dynamic_agg(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    index_column: String,
    every: String,
    period: Option<String>,
    by: Option<Vec<String>>,
    aggregations: &Bound<'_, PyAny>,
    as_python_lists: bool,
    streaming: bool,
) -> PyResult<(PyObject, PyObject)> {
    let dict: &Bound<'_, pyo3::types::PyDict> = aggregations.downcast()?;
    let mut aggs: Vec<(String, String, String)> = Vec::new();
    for (k, v) in dict.iter() {
        let out_name: String = k.extract()?;
        let spec: &Bound<'_, pyo3::types::PyTuple> = v.downcast()?;
        if spec.len() != 2 {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "Aggregation spec must be a tuple: (op, input_column).",
            ));
        }
        let op: String = spec.get_item(0)?.extract()?;
        let in_col: String = spec.get_item(1)?.extract()?;
        aggs.push((out_name, op, in_col));
    }
    #[cfg(feature = "polars_engine")]
    {
        PolarsExecutor::groupby_dynamic_agg(
            py,
            &plan.inner,
            root_data,
            index_column,
            every,
            period,
            by,
            aggs,
            as_python_lists,
            streaming,
        )
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "group_by_dynamic requires pydantable-core built with the `polars_engine` feature.",
        ))
    }
}
