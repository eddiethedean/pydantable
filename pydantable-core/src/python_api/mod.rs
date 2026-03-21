//! PyO3 surface: `PyExpr`, `PyPlan`, and exported `#[pyfunction]`s.

#![cfg_attr(not(feature = "polars_engine"), allow(unused_variables))]

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::dtype::{dtype_to_python_type, py_annotation_to_dtype, DTypeDesc};
use crate::expr::{
    exprnode_to_serializable, op_symbol_to_arith, op_symbol_to_cmp, ArithOp, CmpOp, ExprHandle,
    ExprNode,
};
use crate::plan::{
    execute_plan as execute_plan_inner, make_plan as make_plan_inner, plan_drop as plan_drop_inner,
    plan_drop_nulls as plan_drop_nulls_inner, plan_fill_null as plan_fill_null_inner,
    plan_filter as plan_filter_inner, plan_rename as plan_rename_inner,
    plan_select as plan_select_inner, plan_slice as plan_slice_inner, plan_sort as plan_sort_inner,
    plan_unique as plan_unique_inner, plan_with_columns as plan_with_columns_inner,
    planinner_to_serializable, schema_descriptors_as_py, schema_fields_as_py, PlanInner,
};

#[cfg(feature = "polars_engine")]
use crate::plan::{
    execute_concat_polars as execute_concat_inner, execute_explode_polars as execute_explode_inner,
    execute_groupby_agg_polars as execute_groupby_agg_inner,
    execute_groupby_dynamic_agg_polars as execute_groupby_dynamic_agg_inner,
    execute_join_polars as execute_join_inner, execute_melt_polars as execute_melt_inner,
    execute_pivot_polars as execute_pivot_inner, execute_unnest_polars as execute_unnest_inner,
};

#[pyclass]
#[derive(Clone)]
pub struct PyExpr {
    node: ExprNode,
}

#[pymethods]
impl PyExpr {
    #[getter]
    fn dtype(&self, py: Python<'_>) -> PyResult<PyObject> {
        dtype_to_python_type(py, self.node.dtype())
    }

    fn referenced_columns(&self) -> Vec<String> {
        let referenced = self.node.referenced_columns();
        referenced.into_iter().collect()
    }

    fn to_serializable(&self, py: Python<'_>) -> PyResult<PyObject> {
        exprnode_to_serializable(py, &self.node)
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyPlan {
    inner: PlanInner,
}

#[pymethods]
impl PyPlan {
    fn schema_fields(&self, py: Python<'_>) -> PyResult<PyObject> {
        schema_fields_as_py(py, &self.inner.schema)
    }

    fn schema_descriptors(&self, py: Python<'_>) -> PyResult<PyObject> {
        schema_descriptors_as_py(py, &self.inner.schema)
    }

    fn to_serializable(&self, py: Python<'_>) -> PyResult<PyObject> {
        planinner_to_serializable(py, &self.inner)
    }

    #[pyo3(signature = (root_data, as_python_lists=false))]
    fn execute(
        &self,
        py: Python<'_>,
        root_data: &Bound<'_, PyAny>,
        as_python_lists: bool,
    ) -> PyResult<PyObject> {
        execute_plan_inner(py, &self.inner, root_data, as_python_lists)
    }
}

#[pyfunction]
fn rust_version() -> &'static str {
    "0.5.0"
}

#[pyfunction]
fn make_column_ref(
    py: Python<'_>,
    name: String,
    dtype_annotation: &Bound<'_, PyAny>,
) -> PyResult<PyExpr> {
    let dtype: DTypeDesc = py_annotation_to_dtype(py, dtype_annotation)?;
    if dtype.base.is_none() {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "ColumnRef dtype cannot have unknown base.",
        ));
    }
    Ok(PyExpr {
        node: ExprNode::make_column_ref(name, dtype)?,
    })
}

#[pyfunction]
fn make_literal(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<PyExpr> {
    let lit = ExprHandle::from_py_literal(py, value)?;
    Ok(PyExpr { node: lit.node })
}

#[pyfunction]
fn binary_op(op_symbol: String, left: &PyExpr, right: &PyExpr) -> PyResult<PyExpr> {
    let op: ArithOp = op_symbol_to_arith(&op_symbol)?;
    let node = ExprNode::make_binary_op(op, left.node.clone(), right.node.clone())?;
    Ok(PyExpr { node })
}

#[pyfunction]
fn compare_op(op_symbol: String, left: &PyExpr, right: &PyExpr) -> PyResult<PyExpr> {
    let op: CmpOp = op_symbol_to_cmp(&op_symbol)?;
    let node = ExprNode::make_compare_op(op, left.node.clone(), right.node.clone())?;
    Ok(PyExpr { node })
}

#[pyfunction]
fn cast_expr(
    py: Python<'_>,
    expr: &PyExpr,
    dtype_annotation: &Bound<'_, PyAny>,
) -> PyResult<PyExpr> {
    let target = py_annotation_to_dtype(py, dtype_annotation)?;
    let node = ExprNode::make_cast(expr.node.clone(), target)?;
    Ok(PyExpr { node })
}

#[pyfunction]
fn is_null_expr(expr: &PyExpr) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_is_null(expr.node.clone())?,
    })
}

#[pyfunction]
fn is_not_null_expr(expr: &PyExpr) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_is_not_null(expr.node.clone())?,
    })
}

#[pyfunction]
fn coalesce_exprs(exprs: Vec<Bound<'_, PyExpr>>) -> PyResult<PyExpr> {
    let nodes: Vec<ExprNode> = exprs.iter().map(|e| e.borrow().node.clone()).collect();
    Ok(PyExpr {
        node: ExprNode::make_coalesce(nodes)?,
    })
}

#[pyfunction]
fn expr_case_when(
    conditions: Vec<Bound<'_, PyExpr>>,
    thens: Vec<Bound<'_, PyExpr>>,
    else_expr: Bound<'_, PyExpr>,
) -> PyResult<PyExpr> {
    if conditions.len() != thens.len() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "case_when conditions/thens length mismatch",
        ));
    }
    let mut branches: Vec<(ExprNode, ExprNode)> = Vec::new();
    for (c, t) in conditions.iter().zip(thens.iter()) {
        branches.push((c.borrow().node.clone(), t.borrow().node.clone()));
    }
    Ok(PyExpr {
        node: ExprNode::make_case_when(branches, else_expr.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_in_list(
    py: Python<'_>,
    inner: Bound<'_, PyExpr>,
    values: Vec<Bound<'_, PyAny>>,
) -> PyResult<PyExpr> {
    let mut lits = Vec::new();
    for v in values {
        let lit = ExprHandle::from_py_literal(py, &v)?;
        match lit.node {
            ExprNode::Literal {
                value: Some(lv), ..
            } => lits.push(lv),
            ExprNode::Literal { value: None, .. } => {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "isin() value cannot be null.",
                ));
            }
            _ => {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "isin() values must be literals.",
                ));
            }
        }
    }
    Ok(PyExpr {
        node: ExprNode::make_in_list(inner.borrow().node.clone(), lits)?,
    })
}

#[pyfunction]
fn expr_between(
    inner: Bound<'_, PyExpr>,
    low: Bound<'_, PyExpr>,
    high: Bound<'_, PyExpr>,
) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_between(
            inner.borrow().node.clone(),
            low.borrow().node.clone(),
            high.borrow().node.clone(),
        )?,
    })
}

#[pyfunction]
fn expr_string_concat(exprs: Vec<Bound<'_, PyExpr>>) -> PyResult<PyExpr> {
    let nodes: Vec<ExprNode> = exprs.iter().map(|e| e.borrow().node.clone()).collect();
    Ok(PyExpr {
        node: ExprNode::make_string_concat(nodes)?,
    })
}

#[pyfunction]
fn expr_substring(
    inner: Bound<'_, PyExpr>,
    start: Bound<'_, PyExpr>,
    length: Option<Bound<'_, PyExpr>>,
) -> PyResult<PyExpr> {
    let len = length.map(|l| l.borrow().node.clone());
    Ok(PyExpr {
        node: ExprNode::make_substring(
            inner.borrow().node.clone(),
            start.borrow().node.clone(),
            len,
        )?,
    })
}

#[pyfunction]
fn expr_string_length(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_string_length(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn make_plan(py: Python<'_>, schema_fields: &Bound<'_, PyAny>) -> PyResult<PyPlan> {
    let dict: &Bound<'_, pyo3::types::PyDict> = schema_fields.downcast()?;
    let mut schema: std::collections::HashMap<String, DTypeDesc> = std::collections::HashMap::new();
    for (k, v) in dict.iter() {
        let name: String = k.extract()?;
        let dtype: DTypeDesc = py_annotation_to_dtype(py, &v)?;
        schema.insert(name, dtype);
    }
    Ok(PyPlan {
        inner: make_plan_inner(schema),
    })
}

#[pyfunction]
fn plan_select(plan: &PyPlan, columns: Vec<String>) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_select_inner(&plan.inner, columns)?,
    })
}

#[pyfunction]
fn plan_with_columns(
    py: Python<'_>,
    plan: &PyPlan,
    columns: &Bound<'_, PyAny>,
) -> PyResult<PyPlan> {
    let dict: &Bound<'_, pyo3::types::PyDict> = columns.downcast()?;
    let mut cols: std::collections::HashMap<String, ExprNode> = std::collections::HashMap::new();
    for (k, v) in dict.iter() {
        let name: String = k.extract()?;
        let expr_obj = v.downcast::<PyExpr>()?;
        cols.insert(name, expr_obj.borrow().node.clone());
    }
    let _ = py; // reserved
    Ok(PyPlan {
        inner: plan_with_columns_inner(&plan.inner, cols)?,
    })
}

#[pyfunction]
fn plan_filter(plan: &PyPlan, condition: &PyExpr) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_filter_inner(&plan.inner, condition.node.clone())?,
    })
}

#[pyfunction]
fn plan_sort(plan: &PyPlan, by: Vec<String>, descending: Vec<bool>) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_sort_inner(&plan.inner, by, descending)?,
    })
}

#[pyfunction]
fn plan_unique(plan: &PyPlan, subset: Option<Vec<String>>, keep: String) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_unique_inner(&plan.inner, subset, keep)?,
    })
}

#[pyfunction]
fn plan_drop(plan: &PyPlan, columns: Vec<String>) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_drop_inner(&plan.inner, columns)?,
    })
}

#[pyfunction]
fn plan_rename(_py: Python<'_>, plan: &PyPlan, columns: &Bound<'_, PyAny>) -> PyResult<PyPlan> {
    let dict: &Bound<'_, pyo3::types::PyDict> = columns.downcast()?;
    let mut cols: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for (k, v) in dict.iter() {
        let old: String = k.extract()?;
        let new: String = v.extract()?;
        cols.insert(old, new);
    }
    Ok(PyPlan {
        inner: plan_rename_inner(&plan.inner, cols)?,
    })
}

#[pyfunction]
fn plan_slice(plan: &PyPlan, offset: i64, length: usize) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_slice_inner(&plan.inner, offset, length)?,
    })
}

#[pyfunction]
fn plan_fill_null(
    _py: Python<'_>,
    plan: &PyPlan,
    subset: Option<Vec<String>>,
    value: Option<&Bound<'_, PyAny>>,
    strategy: Option<String>,
) -> PyResult<PyPlan> {
    let scalar = if let Some(v) = value {
        if v.is_none() {
            None
        } else if v.extract::<bool>().is_ok() {
            Some(crate::expr::LiteralValue::Bool(v.extract::<bool>()?))
        } else if v.extract::<i64>().is_ok() {
            Some(crate::expr::LiteralValue::Int(v.extract::<i64>()?))
        } else if v.extract::<f64>().is_ok() {
            Some(crate::expr::LiteralValue::Float(v.extract::<f64>()?))
        } else if v.extract::<String>().is_ok() {
            Some(crate::expr::LiteralValue::Str(v.extract::<String>()?))
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "fill_null(value=...) supports int/float/bool/str.",
            ));
        }
    } else {
        None
    };
    Ok(PyPlan {
        inner: plan_fill_null_inner(&plan.inner, subset, scalar, strategy)?,
    })
}

#[pyfunction]
fn plan_drop_nulls(plan: &PyPlan, subset: Option<Vec<String>>) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_drop_nulls_inner(&plan.inner, subset)?,
    })
}

#[pyfunction]
#[pyo3(signature = (plan, root_data, as_python_lists=false))]
fn execute_plan(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    as_python_lists: bool,
) -> PyResult<PyObject> {
    execute_plan_inner(py, &plan.inner, root_data, as_python_lists)
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (left_plan, left_root_data, right_plan, right_root_data, left_on, right_on, how, suffix, as_python_lists=false))]
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
    as_python_lists: bool,
) -> PyResult<(PyObject, PyObject)> {
    #[cfg(feature = "polars_engine")]
    {
        execute_join_inner(
            py,
            &left_plan.inner,
            left_root_data,
            &right_plan.inner,
            right_root_data,
            left_on,
            right_on,
            how,
            suffix,
            as_python_lists,
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
#[pyo3(signature = (plan, root_data, by, aggregations, as_python_lists=false))]
fn execute_groupby_agg(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    by: Vec<String>,
    aggregations: &Bound<'_, PyAny>,
    as_python_lists: bool,
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
        execute_groupby_agg_inner(py, &plan.inner, root_data, by, aggs, as_python_lists)
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "groupby aggregation requires pydantable-core built with the `polars_engine` feature.",
        ))
    }
}

#[pyfunction]
#[pyo3(signature = (left_plan, left_root_data, right_plan, right_root_data, how, as_python_lists=false))]
fn execute_concat(
    py: Python<'_>,
    left_plan: &PyPlan,
    left_root_data: &Bound<'_, PyAny>,
    right_plan: &PyPlan,
    right_root_data: &Bound<'_, PyAny>,
    how: String,
    as_python_lists: bool,
) -> PyResult<(PyObject, PyObject)> {
    #[cfg(feature = "polars_engine")]
    {
        execute_concat_inner(
            py,
            &left_plan.inner,
            left_root_data,
            &right_plan.inner,
            right_root_data,
            how,
            as_python_lists,
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
#[pyo3(signature = (plan, root_data, id_vars, value_vars, variable_name, value_name, as_python_lists=false))]
fn execute_melt(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    id_vars: Vec<String>,
    value_vars: Option<Vec<String>>,
    variable_name: String,
    value_name: String,
    as_python_lists: bool,
) -> PyResult<(PyObject, PyObject)> {
    #[cfg(feature = "polars_engine")]
    {
        execute_melt_inner(
            py,
            &plan.inner,
            root_data,
            id_vars,
            value_vars,
            variable_name,
            value_name,
            as_python_lists,
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
#[pyo3(signature = (plan, root_data, index, columns, values, aggregate_function, as_python_lists=false))]
fn execute_pivot(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    index: Vec<String>,
    columns: String,
    values: Vec<String>,
    aggregate_function: String,
    as_python_lists: bool,
) -> PyResult<(PyObject, PyObject)> {
    #[cfg(feature = "polars_engine")]
    {
        execute_pivot_inner(
            py,
            &plan.inner,
            root_data,
            index,
            columns,
            values,
            aggregate_function,
            as_python_lists,
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
fn execute_explode(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    columns: Vec<String>,
) -> PyResult<(PyObject, PyObject)> {
    #[cfg(feature = "polars_engine")]
    {
        execute_explode_inner(py, &plan.inner, root_data, columns)
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "explode requires pydantable-core built with the `polars_engine` feature.",
        ))
    }
}

#[pyfunction]
fn execute_unnest(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    columns: Vec<String>,
) -> PyResult<(PyObject, PyObject)> {
    #[cfg(feature = "polars_engine")]
    {
        execute_unnest_inner(py, &plan.inner, root_data, columns)
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
#[pyo3(signature = (plan, root_data, index_column, every, period, by, aggregations, as_python_lists=false))]
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
        execute_groupby_dynamic_agg_inner(
            py,
            &plan.inner,
            root_data,
            index_column,
            every,
            period,
            by,
            aggs,
            as_python_lists,
        )
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "group_by_dynamic requires pydantable-core built with the `polars_engine` feature.",
        ))
    }
}

/// Register all classes and functions on the `pydantable._core` extension module.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyExpr>()?;
    m.add_class::<PyPlan>()?;

    m.add_function(wrap_pyfunction!(execute_plan, m)?)?;
    m.add_function(wrap_pyfunction!(execute_join, m)?)?;
    m.add_function(wrap_pyfunction!(execute_groupby_agg, m)?)?;
    m.add_function(wrap_pyfunction!(execute_concat, m)?)?;
    m.add_function(wrap_pyfunction!(execute_melt, m)?)?;
    m.add_function(wrap_pyfunction!(execute_pivot, m)?)?;
    m.add_function(wrap_pyfunction!(execute_explode, m)?)?;
    m.add_function(wrap_pyfunction!(execute_unnest, m)?)?;
    m.add_function(wrap_pyfunction!(execute_rolling_agg, m)?)?;
    m.add_function(wrap_pyfunction!(execute_groupby_dynamic_agg, m)?)?;
    m.add_function(wrap_pyfunction!(rust_version, m)?)?;
    m.add_function(wrap_pyfunction!(make_column_ref, m)?)?;
    m.add_function(wrap_pyfunction!(make_literal, m)?)?;
    m.add_function(wrap_pyfunction!(binary_op, m)?)?;
    m.add_function(wrap_pyfunction!(compare_op, m)?)?;
    m.add_function(wrap_pyfunction!(cast_expr, m)?)?;
    m.add_function(wrap_pyfunction!(is_null_expr, m)?)?;
    m.add_function(wrap_pyfunction!(is_not_null_expr, m)?)?;
    m.add_function(wrap_pyfunction!(coalesce_exprs, m)?)?;
    m.add_function(wrap_pyfunction!(expr_case_when, m)?)?;
    m.add_function(wrap_pyfunction!(expr_in_list, m)?)?;
    m.add_function(wrap_pyfunction!(expr_between, m)?)?;
    m.add_function(wrap_pyfunction!(expr_string_concat, m)?)?;
    m.add_function(wrap_pyfunction!(expr_substring, m)?)?;
    m.add_function(wrap_pyfunction!(expr_string_length, m)?)?;
    m.add_function(wrap_pyfunction!(make_plan, m)?)?;
    m.add_function(wrap_pyfunction!(plan_select, m)?)?;
    m.add_function(wrap_pyfunction!(plan_with_columns, m)?)?;
    m.add_function(wrap_pyfunction!(plan_filter, m)?)?;
    m.add_function(wrap_pyfunction!(plan_sort, m)?)?;
    m.add_function(wrap_pyfunction!(plan_unique, m)?)?;
    m.add_function(wrap_pyfunction!(plan_drop, m)?)?;
    m.add_function(wrap_pyfunction!(plan_rename, m)?)?;
    m.add_function(wrap_pyfunction!(plan_slice, m)?)?;
    m.add_function(wrap_pyfunction!(plan_fill_null, m)?)?;
    m.add_function(wrap_pyfunction!(plan_drop_nulls, m)?)?;
    Ok(())
}
