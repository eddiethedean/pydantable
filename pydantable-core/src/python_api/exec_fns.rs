#![cfg_attr(not(feature = "polars_engine"), allow(unused_variables))]

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::plan::execute_plan as execute_plan_inner;

#[cfg(feature = "polars_engine")]
use crate::plan::PolarsExecutor;
#[cfg(feature = "polars_engine")]
use crate::plan::{
    collect_plan_batches_polars, sink_csv_polars, sink_ipc_polars, sink_ndjson_polars,
    sink_parquet_polars,
};

use super::types::PyPlan;

#[pyfunction]
#[pyo3(signature = (plan, root_data, as_python_lists=false, streaming=false))]
fn execute_plan(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    as_python_lists: bool,
    streaming: bool,
) -> PyResult<PyObject> {
    execute_plan_inner(py, &plan.inner, root_data, as_python_lists, streaming)
}

#[cfg(feature = "polars_engine")]
#[pyfunction]
#[pyo3(signature = (plan, root_data, path, streaming=false, write_kwargs=None, partition_by=None, mkdir=true))]
#[allow(clippy::too_many_arguments)]
fn sink_parquet(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    path: String,
    streaming: bool,
    write_kwargs: Option<Bound<'_, PyAny>>,
    partition_by: Option<Vec<String>>,
    mkdir: bool,
) -> PyResult<()> {
    sink_parquet_polars(
        py,
        &plan.inner,
        root_data,
        path,
        streaming,
        write_kwargs.as_ref(),
        partition_by,
        mkdir,
    )
}

#[cfg(not(feature = "polars_engine"))]
#[pyfunction]
#[pyo3(signature = (plan, root_data, path, streaming=false, write_kwargs=None, partition_by=None, mkdir=true))]
#[allow(clippy::too_many_arguments)]
#[allow(unused_variables)]
fn sink_parquet(
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
fn sink_csv(
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
fn sink_csv(
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
fn sink_ipc(
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
fn sink_ipc(
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
fn sink_ndjson(
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
fn sink_ndjson(
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
fn collect_plan_batches(
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
fn collect_plan_batches(
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

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (left_plan, left_root_data, right_plan, right_root_data, left_on, right_on, how, suffix, validate=None, coalesce=None, join_nulls=None, maintain_order=None, allow_parallel=None, force_parallel=None, as_python_lists=false, streaming=false))]
fn execute_join(
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
fn execute_groupby_agg(
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
fn execute_concat(
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
fn execute_except_all(
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
fn execute_intersect_all(
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
fn execute_melt(
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
fn execute_pivot(
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
fn execute_explode(
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
fn execute_posexplode(
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
fn execute_unnest(
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
fn execute_rolling_agg(
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
fn execute_groupby_dynamic_agg(
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

pub(super) fn register_functions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(execute_plan, m)?)?;
    m.add_function(wrap_pyfunction!(sink_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(sink_csv, m)?)?;
    m.add_function(wrap_pyfunction!(sink_ipc, m)?)?;
    m.add_function(wrap_pyfunction!(sink_ndjson, m)?)?;
    m.add_function(wrap_pyfunction!(collect_plan_batches, m)?)?;
    m.add_function(wrap_pyfunction!(execute_join, m)?)?;
    m.add_function(wrap_pyfunction!(execute_groupby_agg, m)?)?;
    m.add_function(wrap_pyfunction!(execute_concat, m)?)?;
    m.add_function(wrap_pyfunction!(execute_except_all, m)?)?;
    m.add_function(wrap_pyfunction!(execute_intersect_all, m)?)?;
    m.add_function(wrap_pyfunction!(execute_melt, m)?)?;
    m.add_function(wrap_pyfunction!(execute_pivot, m)?)?;
    m.add_function(wrap_pyfunction!(execute_explode, m)?)?;
    m.add_function(wrap_pyfunction!(execute_posexplode, m)?)?;
    m.add_function(wrap_pyfunction!(execute_unnest, m)?)?;
    m.add_function(wrap_pyfunction!(execute_rolling_agg, m)?)?;
    m.add_function(wrap_pyfunction!(execute_groupby_dynamic_agg, m)?)?;
    Ok(())
}
