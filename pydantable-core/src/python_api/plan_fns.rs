#![cfg_attr(not(feature = "polars_engine"), allow(unused_variables))]

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::dtype::{py_annotation_to_dtype, DTypeDesc};
use crate::expr::ExprNode;
use crate::plan::{
    make_plan as make_plan_inner, plan_drop as plan_drop_inner,
    plan_drop_duplicate_groups as plan_drop_duplicate_groups_inner,
    plan_drop_nulls as plan_drop_nulls_inner, plan_duplicate_mask as plan_duplicate_mask_inner,
    plan_fill_null as plan_fill_null_inner, plan_filter as plan_filter_inner,
    plan_global_select as build_plan_global_select, plan_melt as plan_melt_inner,
    plan_rename as plan_rename_inner, plan_rolling_agg as plan_rolling_agg_inner,
    plan_select as plan_select_inner, plan_slice as plan_slice_inner, plan_sort as plan_sort_inner,
    plan_unique as plan_unique_inner, plan_with_columns as plan_with_columns_inner,
    plan_with_row_count as plan_with_row_count_inner,
};

use super::types::{PyExpr, PyPlan};

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
fn plan_global_select(plan: &PyPlan, items: Vec<(String, Bound<'_, PyExpr>)>) -> PyResult<PyPlan> {
    let mut pairs: Vec<(String, ExprNode)> = Vec::with_capacity(items.len());
    for (name, e) in items {
        pairs.push((name, e.borrow().node.clone()));
    }
    Ok(PyPlan {
        inner: build_plan_global_select(&plan.inner, pairs)?,
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
fn plan_sort(
    plan: &PyPlan,
    by: Vec<String>,
    descending: Vec<bool>,
    nulls_last: Vec<bool>,
    maintain_order: bool,
) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_sort_inner(&plan.inner, by, descending, nulls_last, maintain_order)?,
    })
}

#[pyfunction]
fn plan_unique(
    plan: &PyPlan,
    subset: Option<Vec<String>>,
    keep: String,
    maintain_order: bool,
) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_unique_inner(&plan.inner, subset, keep, maintain_order)?,
    })
}

#[pyfunction]
fn plan_duplicate_mask(
    plan: &PyPlan,
    subset: Option<Vec<String>>,
    keep: String,
) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_duplicate_mask_inner(&plan.inner, subset, keep)?,
    })
}

#[pyfunction]
fn plan_drop_duplicate_groups(plan: &PyPlan, subset: Option<Vec<String>>) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_drop_duplicate_groups_inner(&plan.inner, subset)?,
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
#[pyo3(signature = (plan, name="row_nr".to_string(), offset=0))]
fn plan_with_row_count(plan: &PyPlan, name: String, offset: i64) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_with_row_count_inner(&plan.inner, name, offset)?,
    })
}

fn py_value_is_uuid(v: &Bound<'_, PyAny>) -> PyResult<bool> {
    let py = v.py();
    let builtins = py.import_bound("builtins")?;
    let isinstance = builtins.getattr("isinstance")?;
    let uuid_cls = py.import_bound("uuid")?.getattr("UUID")?;
    Ok(isinstance
        .call1((v, &uuid_cls))?
        .extract::<bool>()
        .unwrap_or(false))
}

fn py_value_is_decimal(v: &Bound<'_, PyAny>) -> PyResult<bool> {
    let py = v.py();
    let builtins = py.import_bound("builtins")?;
    let isinstance = builtins.getattr("isinstance")?;
    let dec_cls = py.import_bound("decimal")?.getattr("Decimal")?;
    Ok(isinstance
        .call1((v, &dec_cls))?
        .extract::<bool>()
        .unwrap_or(false))
}

fn py_value_is_enum(v: &Bound<'_, PyAny>) -> PyResult<bool> {
    let py = v.py();
    let builtins = py.import_bound("builtins")?;
    let isinstance = builtins.getattr("isinstance")?;
    let enum_cls = py.import_bound("enum")?.getattr("Enum")?;
    Ok(isinstance
        .call1((v, &enum_cls))?
        .extract::<bool>()
        .unwrap_or(false))
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
        } else if py_value_is_uuid(v)? {
            let s: String = v.str()?.extract()?;
            Some(crate::expr::LiteralValue::Uuid(s))
        } else if py_value_is_decimal(v)? {
            Some(crate::expr::LiteralValue::Decimal(
                crate::dtype::py_decimal_to_scaled_i128(v)?,
            ))
        } else if py_value_is_enum(v)? {
            Some(crate::expr::LiteralValue::EnumStr(
                crate::dtype::py_enum_to_wire_string(v)?,
            ))
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "fill_null(value=...) supports int/float/bool/str/uuid.UUID/decimal.Decimal/enum.Enum.",
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
#[pyo3(signature = (plan, subset=None, how="any".to_string(), threshold=None))]
fn plan_drop_nulls(
    plan: &PyPlan,
    subset: Option<Vec<String>>,
    how: String,
    threshold: Option<usize>,
) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_drop_nulls_inner(&plan.inner, subset, how, threshold)?,
    })
}

#[pyfunction]
#[pyo3(signature = (plan, id_vars, value_vars=None, variable_name="variable".to_string(), value_name="value".to_string()))]
fn plan_melt(
    plan: &PyPlan,
    id_vars: Vec<String>,
    value_vars: Option<Vec<String>>,
    variable_name: String,
    value_name: String,
) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_melt_inner(&plan.inner, id_vars, value_vars, variable_name, value_name)?,
    })
}

#[pyfunction]
#[pyo3(signature = (plan, column, window_size, min_periods, op, out_name, partition_by=None))]
fn plan_rolling_agg(
    plan: &PyPlan,
    column: String,
    window_size: usize,
    min_periods: usize,
    op: String,
    out_name: String,
    partition_by: Option<Vec<String>>,
) -> PyResult<PyPlan> {
    Ok(PyPlan {
        inner: plan_rolling_agg_inner(
            &plan.inner,
            column,
            window_size,
            min_periods,
            op,
            out_name,
            partition_by.unwrap_or_default(),
        )?,
    })
}

pub(super) fn register_functions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(make_plan, m)?)?;
    m.add_function(wrap_pyfunction!(plan_select, m)?)?;
    m.add_function(wrap_pyfunction!(plan_global_select, m)?)?;
    m.add_function(wrap_pyfunction!(plan_with_columns, m)?)?;
    m.add_function(wrap_pyfunction!(plan_filter, m)?)?;
    m.add_function(wrap_pyfunction!(plan_sort, m)?)?;
    m.add_function(wrap_pyfunction!(plan_unique, m)?)?;
    m.add_function(wrap_pyfunction!(plan_duplicate_mask, m)?)?;
    m.add_function(wrap_pyfunction!(plan_drop_duplicate_groups, m)?)?;
    m.add_function(wrap_pyfunction!(plan_drop, m)?)?;
    m.add_function(wrap_pyfunction!(plan_rename, m)?)?;
    m.add_function(wrap_pyfunction!(plan_slice, m)?)?;
    m.add_function(wrap_pyfunction!(plan_with_row_count, m)?)?;
    m.add_function(wrap_pyfunction!(plan_fill_null, m)?)?;
    m.add_function(wrap_pyfunction!(plan_drop_nulls, m)?)?;
    m.add_function(wrap_pyfunction!(plan_melt, m)?)?;
    m.add_function(wrap_pyfunction!(plan_rolling_agg, m)?)?;
    Ok(())
}
