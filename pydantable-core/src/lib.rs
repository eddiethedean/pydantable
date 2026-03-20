#![allow(deprecated)]

use pyo3::prelude::*;

mod dtype;
mod expr;
mod plan;

use crate::dtype::{dtype_to_python_type, py_annotation_to_dtype, DTypeDesc};
use crate::expr::{
    exprnode_to_serializable, op_symbol_to_arith, op_symbol_to_cmp, ArithOp, CmpOp, ExprHandle,
    ExprNode,
};
use crate::plan::{
    execute_concat_polars as execute_concat_inner,
    execute_groupby_agg_polars as execute_groupby_agg_inner,
    execute_join_polars as execute_join_inner, execute_plan as execute_plan_inner,
    make_plan as make_plan_inner, plan_drop as plan_drop_inner, plan_filter as plan_filter_inner,
    plan_rename as plan_rename_inner, plan_select as plan_select_inner,
    plan_slice as plan_slice_inner, plan_sort as plan_sort_inner, plan_unique as plan_unique_inner,
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

#[pyfunction]
fn execute_concat(
    py: Python<'_>,
    left_plan: &PyPlan,
    left_root_data: &Bound<'_, PyAny>,
    right_plan: &PyPlan,
    right_root_data: &Bound<'_, PyAny>,
    how: String,
) -> PyResult<(PyObject, PyObject)> {
    execute_concat_inner(
        py,
        &left_plan.inner,
        left_root_data,
        &right_plan.inner,
        right_root_data,
        how,
    )
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
    m.add_function(wrap_pyfunction!(execute_concat, m)?)?;
    m.add_function(wrap_pyfunction!(rust_version, m)?)?;
    m.add_function(wrap_pyfunction!(make_column_ref, m)?)?;
    m.add_function(wrap_pyfunction!(make_literal, m)?)?;
    m.add_function(wrap_pyfunction!(binary_op, m)?)?;
    m.add_function(wrap_pyfunction!(compare_op, m)?)?;
    m.add_function(wrap_pyfunction!(make_plan, m)?)?;
    m.add_function(wrap_pyfunction!(plan_select, m)?)?;
    m.add_function(wrap_pyfunction!(plan_with_columns, m)?)?;
    m.add_function(wrap_pyfunction!(plan_filter, m)?)?;
    m.add_function(wrap_pyfunction!(plan_sort, m)?)?;
    m.add_function(wrap_pyfunction!(plan_unique, m)?)?;
    m.add_function(wrap_pyfunction!(plan_drop, m)?)?;
    m.add_function(wrap_pyfunction!(plan_rename, m)?)?;
    m.add_function(wrap_pyfunction!(plan_slice, m)?)?;
    Ok(())
}
