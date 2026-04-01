#![cfg(feature = "polars_engine")]
#![allow(unused_imports)]

use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};

use crate::expr::LiteralValue;
use crate::plan::ir::PlanInner;
use crate::plan::schema_py::schema_descriptors_as_py;

use super::literal_agg::{literal_to_py, py_dict_to_literal_ctx};

fn validate_identical_schema(left: &PlanInner, right: &PlanInner, op: &str) -> PyResult<Vec<String>> {
    if left.schema.len() != right.schema.len() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "{op}() requires identical schemas."
        )));
    }
    for (name, ldt) in left.schema.iter() {
        let rdt = right.schema.get(name).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "{op}() requires identical schemas."
            ))
        })?;
        if ldt != rdt {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "{op}() requires identical schemas."
            )));
        }
    }
    let mut cols: Vec<String> = left.schema.keys().cloned().collect();
    cols.sort();
    Ok(cols)
}

fn signature_for_row(
    ctx: &HashMap<String, Vec<Option<LiteralValue>>>,
    cols: &[String],
    i: usize,
) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::new();
    for c in cols {
        let v = &ctx[c][i];
        match v {
            None => out.push(0),
            Some(LiteralValue::Int(x)) => {
                out.push(1);
                out.extend_from_slice(&x.to_le_bytes());
            }
            Some(LiteralValue::Float(x)) => {
                out.push(2);
                out.extend_from_slice(&x.to_bits().to_le_bytes());
            }
            Some(LiteralValue::Bool(x)) => {
                out.push(3);
                out.push(u8::from(*x));
            }
            Some(LiteralValue::Str(s))
            | Some(LiteralValue::Uuid(s))
            | Some(LiteralValue::EnumStr(s)) => {
                out.push(4);
                let b = s.as_bytes();
                out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                out.extend_from_slice(b);
            }
            Some(LiteralValue::Decimal(x)) => {
                out.push(5);
                out.extend_from_slice(&x.to_le_bytes());
            }
            Some(LiteralValue::DateTimeMicros(x))
            | Some(LiteralValue::DurationMicros(x))
            | Some(LiteralValue::TimeNanos(x)) => {
                out.push(6);
                out.extend_from_slice(&x.to_le_bytes());
            }
            Some(LiteralValue::DateDays(x)) => {
                out.push(7);
                out.extend_from_slice(&x.to_le_bytes());
            }
            Some(LiteralValue::Binary(b)) => {
                out.push(8);
                out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                out.extend_from_slice(b);
            }
        }
        // per-column separator
        out.push(255);
    }
    out
}

fn compute_counts(
    ctx: &HashMap<String, Vec<Option<LiteralValue>>>,
    cols: &[String],
) -> (HashMap<Vec<u8>, usize>, HashMap<Vec<u8>, Vec<Option<LiteralValue>>>) {
    let row_count = ctx.values().next().map_or(0, std::vec::Vec::len);
    let mut counts: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut exemplar: HashMap<Vec<u8>, Vec<Option<LiteralValue>>> = HashMap::new();
    for i in 0..row_count {
        let sig = signature_for_row(ctx, cols, i);
        *counts.entry(sig.clone()).or_insert(0) += 1;
        exemplar.entry(sig).or_insert_with(|| {
            cols.iter().map(|c| ctx[c][i].clone()).collect()
        });
    }
    (counts, exemplar)
}

fn multiset_emit<'py>(
    py: Python<'py>,
    cols: &[String],
    left_counts: HashMap<Vec<u8>, usize>,
    left_exemplar: HashMap<Vec<u8>, Vec<Option<LiteralValue>>>,
    right_counts: HashMap<Vec<u8>, usize>,
    op: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new_bound(py);
    for c in cols {
        out.set_item(c, Vec::<PyObject>::new())?;
    }
    // We append in deterministic signature order for stable tests/output.
    let mut sigs: Vec<Vec<u8>> = left_counts.keys().cloned().collect();
    sigs.sort();

    // Pre-extract mutable vectors from dict.
    let mut out_cols: HashMap<String, Vec<PyObject>> = HashMap::new();
    for c in cols {
        out_cols.insert(c.clone(), Vec::new());
    }

    for sig in sigs {
        let lc = *left_counts.get(&sig).unwrap_or(&0);
        let rc = *right_counts.get(&sig).unwrap_or(&0);
        let n = match op {
            "exceptAll" => lc.saturating_sub(rc),
            "intersectAll" => lc.min(rc),
            _ => 0,
        };
        if n == 0 {
            continue;
        }
        let row = left_exemplar.get(&sig).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "internal: missing exemplar row for signature",
            )
        })?;
        for _ in 0..n {
            for (c, cell) in cols.iter().zip(row.iter()) {
                let py_val = cell.as_ref().map_or(py.None(), |x| literal_to_py(py, x));
                out_cols.get_mut(c).unwrap().push(py_val);
            }
        }
    }

    for c in cols {
        out.set_item(c, out_cols.remove(c).unwrap())?;
    }
    Ok(out)
}

pub fn execute_except_all_polars(
    py: Python<'_>,
    left_plan: &PlanInner,
    left_root_data: &Bound<'_, PyAny>,
    right_plan: &PlanInner,
    right_root_data: &Bound<'_, PyAny>,
    as_python_lists: bool,
    streaming: bool,
) -> PyResult<(PyObject, PyObject)> {
    let cols = validate_identical_schema(left_plan, right_plan, "exceptAll")?;
    let left_obj = crate::plan::execute_plan(py, left_plan, left_root_data, true, streaming)?;
    let right_obj = crate::plan::execute_plan(py, right_plan, right_root_data, true, streaming)?;
    let left_ctx = py_dict_to_literal_ctx(&left_plan.schema, left_obj.bind(py))?;
    let right_ctx = py_dict_to_literal_ctx(&right_plan.schema, right_obj.bind(py))?;

    let (lc, lex) = compute_counts(&left_ctx, &cols);
    let (rc, _) = compute_counts(&right_ctx, &cols);
    let out_dict = multiset_emit(py, &cols, lc, lex, rc, "exceptAll")?;

    let desc = schema_descriptors_as_py(py, &left_plan.schema)?;
    if as_python_lists {
        return Ok((out_dict.into_py(py), desc));
    }
    let pl = py.import_bound("polars")?;
    let df_cls = pl.getattr("DataFrame")?;
    Ok((df_cls.call1((out_dict,))?.into_py(py), desc))
}

pub fn execute_intersect_all_polars(
    py: Python<'_>,
    left_plan: &PlanInner,
    left_root_data: &Bound<'_, PyAny>,
    right_plan: &PlanInner,
    right_root_data: &Bound<'_, PyAny>,
    as_python_lists: bool,
    streaming: bool,
) -> PyResult<(PyObject, PyObject)> {
    let cols = validate_identical_schema(left_plan, right_plan, "intersectAll")?;
    let left_obj = crate::plan::execute_plan(py, left_plan, left_root_data, true, streaming)?;
    let right_obj = crate::plan::execute_plan(py, right_plan, right_root_data, true, streaming)?;
    let left_ctx = py_dict_to_literal_ctx(&left_plan.schema, left_obj.bind(py))?;
    let right_ctx = py_dict_to_literal_ctx(&right_plan.schema, right_obj.bind(py))?;

    let (lc, lex) = compute_counts(&left_ctx, &cols);
    let (rc, _) = compute_counts(&right_ctx, &cols);
    let out_dict = multiset_emit(py, &cols, lc, lex, rc, "intersectAll")?;

    let desc = schema_descriptors_as_py(py, &left_plan.schema)?;
    if as_python_lists {
        return Ok((out_dict.into_py(py), desc));
    }
    let pl = py.import_bound("polars")?;
    let df_cls = pl.getattr("DataFrame")?;
    Ok((df_cls.call1((out_dict,))?.into_py(py), desc))
}

