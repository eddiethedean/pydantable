#![allow(deprecated)]

use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

mod dtype;
mod expr;
mod plan;

use crate::dtype::{dtype_to_python_type, py_annotation_to_dtype, py_value_to_dtype, BaseType, DTypeDesc};
use crate::expr::{
    exprnode_to_serializable, op_symbol_to_arith, op_symbol_to_cmp, ArithOp, CmpOp, ExprHandle,
    ExprNode, LiteralValue,
};
use crate::plan::{
    execute_groupby_agg_polars as execute_groupby_agg_inner,
    execute_join_polars as execute_join_inner, execute_plan as execute_plan_inner,
    make_plan as make_plan_inner, plan_distinct as plan_distinct_step,
    plan_drop as plan_drop_step, plan_filter as plan_filter_inner,
    plan_limit as plan_limit_step, plan_rename_column as plan_rename_step,
    plan_select as plan_select_inner, plan_sort as plan_sort_step,
    plan_with_columns as plan_with_columns_inner, planinner_to_serializable,
    schema_descriptors_as_py, schema_fields_as_py, PlanInner,
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

    fn execute(&self, py: Python<'_>, root_data: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        execute_plan_inner(py, &self.inner, root_data)
    }
}

#[pyfunction]
fn rust_version() -> &'static str {
    "0.4.0-skeleton-rust"
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
fn expr_is_null(expr: &PyExpr) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_is_null(expr.node.clone())?,
    })
}

#[pyfunction]
fn expr_is_not_null(expr: &PyExpr) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_is_not_null(expr.node.clone())?,
    })
}

#[pyfunction]
fn expr_coalesce(exprs: Vec<Bound<'_, PyExpr>>) -> PyResult<PyExpr> {
    let mut nodes = Vec::with_capacity(exprs.len());
    for e in exprs {
        nodes.push(e.borrow().node.clone());
    }
    Ok(PyExpr {
        node: ExprNode::make_coalesce(nodes)?,
    })
}

fn py_scalar_to_literal(py: Python<'_>, item: &Bound<'_, PyAny>) -> PyResult<LiteralValue> {
    if item.is_none() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "isin() list values cannot be None.",
        ));
    }
    let dt = py_value_to_dtype(py, item)?;
    match dt.base {
        Some(BaseType::Int) => Ok(LiteralValue::Int(item.extract::<i64>()?)),
        Some(BaseType::Float) => Ok(LiteralValue::Float(item.extract::<f64>()?)),
        Some(BaseType::Bool) => Ok(LiteralValue::Bool(item.extract::<bool>()?)),
        Some(BaseType::Str) => Ok(LiteralValue::Str(item.extract::<String>()?)),
        None => Err(pyo3::exceptions::PyTypeError::new_err(
            "isin() value must have a known scalar type.",
        )),
    }
}

fn py_annotation_to_base(py: Python<'_>, ann: &Bound<'_, PyAny>) -> PyResult<BaseType> {
    let d = py_annotation_to_dtype(py, ann)?;
    d.base.ok_or_else(|| {
        pyo3::exceptions::PyTypeError::new_err("cast target must be a concrete scalar type.")
    })
}

#[pyfunction]
fn expr_case_when(
    py: Python<'_>,
    branches: Bound<'_, PyList>,
    else_expr: &PyExpr,
) -> PyResult<PyExpr> {
    let mut v: Vec<(ExprNode, ExprNode)> = Vec::new();
    for item in branches.iter() {
        let t: &Bound<'_, PyTuple> = item.downcast()?;
        if t.len() != 2 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Each branch must be (condition_expr, value_expr).",
            ));
        }
        let c_any = t.get_item(0)?;
        let a_any = t.get_item(1)?;
        let c = c_any.downcast::<PyExpr>()?;
        let a = a_any.downcast::<PyExpr>()?;
        v.push((c.borrow().node.clone(), a.borrow().node.clone()));
    }
    let _ = py;
    Ok(PyExpr {
        node: ExprNode::make_case_when(v, else_expr.node.clone())?,
    })
}

#[pyfunction]
fn expr_cast(
    py: Python<'_>,
    inner: &PyExpr,
    dtype_annotation: &Bound<'_, PyAny>,
) -> PyResult<PyExpr> {
    let to = py_annotation_to_base(py, dtype_annotation)?;
    let _ = py;
    Ok(PyExpr {
        node: ExprNode::make_cast(inner.node.clone(), to)?,
    })
}

#[pyfunction]
fn expr_in_list(
    py: Python<'_>,
    inner: &PyExpr,
    values: Bound<'_, PyList>,
) -> PyResult<PyExpr> {
    let mut lits = Vec::new();
    for it in values.iter() {
        lits.push(py_scalar_to_literal(py, &it)?);
    }
    let _ = py;
    Ok(PyExpr {
        node: ExprNode::make_in_list(inner.node.clone(), lits)?,
    })
}

#[pyfunction]
fn expr_between(inner: &PyExpr, low: &PyExpr, high: &PyExpr) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_between(
            inner.node.clone(),
            low.node.clone(),
            high.node.clone(),
        )?,
    })
}

#[pyfunction]
fn expr_string_concat(exprs: Vec<Bound<'_, PyExpr>>) -> PyResult<PyExpr> {
    let mut nodes = Vec::with_capacity(exprs.len());
    for e in exprs {
        nodes.push(e.borrow().node.clone());
    }
    Ok(PyExpr {
        node: ExprNode::make_string_concat(nodes)?,
    })
}

#[pyfunction]
#[pyo3(signature = (inner, start, length=None))]
fn expr_substring(
    inner: &PyExpr,
    start: &PyExpr,
    length: Option<&PyExpr>,
) -> PyResult<PyExpr> {
    let len = length.map(|e| e.node.clone());
    Ok(PyExpr {
        node: ExprNode::make_substring(inner.node.clone(), start.node.clone(), len)?,
    })
}

#[pyfunction]
fn expr_string_length(inner: &PyExpr) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_string_length(inner.node.clone())?,
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
fn plan_limit(plan: &PyPlan, n: usize) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_limit_step(&plan.inner, n)?,
    })
}

#[pyfunction]
#[pyo3(signature = (plan, columns, ascending=None))]
fn plan_sort(
    plan: &PyPlan,
    columns: Vec<String>,
    ascending: Option<Vec<bool>>,
) -> PyResult<PyPlan> {
    let n = columns.len();
    if n == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "sort/order_by requires at least one column name.",
        ));
    }
    let asc = match ascending {
        None => vec![true; n],
        Some(v) if v.is_empty() => {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "ascending= must be non-empty when provided.",
            ));
        }
        Some(v) if v.len() == 1 => vec![v[0]; n],
        Some(v) if v.len() == n => v,
        Some(v) => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "ascending has length {} but {} columns were given.",
                v.len(),
                n
            )));
        }
    };
    let by: Vec<(String, bool)> = columns.into_iter().zip(asc.into_iter()).collect();
    Ok(PyPlan {
        inner: plan_sort_step(&plan.inner, by)?,
    })
}

#[pyfunction]
fn plan_drop(plan: &PyPlan, columns: Vec<String>) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_drop_step(&plan.inner, columns)?,
    })
}

#[pyfunction]
fn plan_distinct(plan: &PyPlan) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_distinct_step(&plan.inner)?,
    })
}

#[pyfunction]
fn plan_rename_column(plan: &PyPlan, from: String, to: String) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_rename_step(&plan.inner, from, to)?,
    })
}

#[pyfunction]
fn execute_plan(py: Python<'_>, plan: &PyPlan, root_data: &Bound<'_, PyAny>) -> PyResult<PyObject> {
    execute_plan_inner(py, &plan.inner, root_data)
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn execute_join(
    py: Python<'_>,
    left_plan: &PyPlan,
    left_root_data: &Bound<'_, PyAny>,
    right_plan: &PyPlan,
    right_root_data: &Bound<'_, PyAny>,
    on: Vec<String>,
    how: String,
    suffix: String,
) -> PyResult<(PyObject, PyObject)> {
    execute_join_inner(
        py,
        &left_plan.inner,
        left_root_data,
        &right_plan.inner,
        right_root_data,
        on,
        how,
        suffix,
    )
}

#[pyfunction]
fn plan_to_serializable(py: Python<'_>, plan: &PyPlan) -> PyResult<PyObject> {
    planinner_to_serializable(py, &plan.inner)
}

#[pyfunction]
fn execute_groupby_agg(
    py: Python<'_>,
    plan: &PyPlan,
    root_data: &Bound<'_, PyAny>,
    by: Vec<String>,
    aggregations: &Bound<'_, PyAny>,
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
    execute_groupby_agg_inner(py, &plan.inner, root_data, by, aggs)
}

/// Minimal Rust/PyO3 stub module for the `pydantable._core` extension.
///
/// This exists so the Python side can import the extension module via maturin.
/// Planner/execution logic will be filled in in later versions.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyExpr>()?;
    m.add_class::<PyPlan>()?;

    m.add_function(wrap_pyfunction!(execute_plan, m)?)?;
    m.add_function(wrap_pyfunction!(execute_join, m)?)?;
    m.add_function(wrap_pyfunction!(execute_groupby_agg, m)?)?;
    m.add_function(wrap_pyfunction!(rust_version, m)?)?;
    m.add_function(wrap_pyfunction!(make_column_ref, m)?)?;
    m.add_function(wrap_pyfunction!(make_literal, m)?)?;
    m.add_function(wrap_pyfunction!(binary_op, m)?)?;
    m.add_function(wrap_pyfunction!(compare_op, m)?)?;
    m.add_function(wrap_pyfunction!(make_plan, m)?)?;
    m.add_function(wrap_pyfunction!(plan_select, m)?)?;
    m.add_function(wrap_pyfunction!(plan_with_columns, m)?)?;
    m.add_function(wrap_pyfunction!(plan_filter, m)?)?;
    m.add_function(wrap_pyfunction!(plan_limit, m)?)?;
    m.add_function(wrap_pyfunction!(plan_sort, m)?)?;
    m.add_function(wrap_pyfunction!(plan_drop, m)?)?;
    m.add_function(wrap_pyfunction!(plan_distinct, m)?)?;
    m.add_function(wrap_pyfunction!(plan_rename_column, m)?)?;
    m.add_function(wrap_pyfunction!(expr_is_null, m)?)?;
    m.add_function(wrap_pyfunction!(expr_is_not_null, m)?)?;
    m.add_function(wrap_pyfunction!(expr_coalesce, m)?)?;
    m.add_function(wrap_pyfunction!(expr_case_when, m)?)?;
    m.add_function(wrap_pyfunction!(expr_cast, m)?)?;
    m.add_function(wrap_pyfunction!(expr_in_list, m)?)?;
    m.add_function(wrap_pyfunction!(expr_between, m)?)?;
    m.add_function(wrap_pyfunction!(expr_string_concat, m)?)?;
    m.add_function(wrap_pyfunction!(expr_substring, m)?)?;
    m.add_function(wrap_pyfunction!(expr_string_length, m)?)?;
    m.add_function(wrap_pyfunction!(plan_to_serializable, m)?)?;
    Ok(())
}
