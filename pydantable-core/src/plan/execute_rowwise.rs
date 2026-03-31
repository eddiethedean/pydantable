//! Row-wise plan execution when the Polars engine is disabled.

use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList};

use crate::dtype::scaled_i128_to_py_decimal;
use crate::expr::LiteralValue;

use super::context::{ctx_len, root_data_to_ctx};
use super::ir::{PlanInner, PlanStep};

fn micros_to_py_datetime(py: Python<'_>, micros: i64) -> PyResult<PyObject> {
    let dt_mod = py.import_bound("datetime")?;
    let dt = dt_mod.getattr("datetime")?;
    Ok(dt
        .call_method1("fromtimestamp", (micros as f64 / 1_000_000.0,))?
        .into_py(py))
}

fn days_to_py_date(py: Python<'_>, days: i32) -> PyResult<PyObject> {
    let dt_mod = py.import_bound("datetime")?;
    let date = dt_mod.getattr("date")?;
    Ok(date
        .call_method1("fromordinal", (days + 719_163,))?
        .into_py(py))
}

fn micros_to_py_timedelta(py: Python<'_>, micros: i64) -> PyResult<PyObject> {
    let dt_mod = py.import_bound("datetime")?;
    let td = dt_mod.getattr("timedelta")?;
    Ok(td.call1((0, 0, micros))?.into_py(py))
}

fn row_key_for_subset(
    ctx: &HashMap<String, Vec<Option<LiteralValue>>>,
    subset: &[String],
    i: usize,
) -> String {
    let mut sig = String::new();
    for key in subset {
        sig.push_str(&format!("{:?}|", ctx[key][i]));
    }
    sig
}

fn nanos_to_py_time(py: Python<'_>, ns: i64) -> PyResult<PyObject> {
    let dt_mod = py.import_bound("datetime")?;
    let time_cls = dt_mod.getattr("time")?;
    let nanos = ns.rem_euclid(86_400 * 1_000_000_000);
    let secs = nanos / 1_000_000_000;
    let nsub = nanos % 1_000_000_000;
    let micro = (nsub / 1000) as i32;
    let h = (secs / 3600) as i32;
    let m = ((secs % 3600) / 60) as i32;
    let s = (secs % 60) as i32;
    Ok(time_cls.call1((h, m, s, micro))?.into_py(py))
}

pub(crate) fn execute_plan_rowwise(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    _as_python_lists: bool,
) -> PyResult<PyObject> {
    let mut ctx = root_data_to_ctx(py, &plan.root_schema, root_data)?;
    let mut n = ctx_len(&ctx)?;

    for step in plan.steps.iter() {
        match step {
            PlanStep::Select { columns } => {
                ctx.retain(|k, _| columns.contains(k));
                n = ctx_len(&ctx)?;
            }
            PlanStep::GlobalSelect { .. } => {
                return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                    "Global aggregate select requires pydantable-core built with the Polars engine.",
                ));
            }
            PlanStep::WithColumns { columns } => {
                for (name, expr) in columns.iter() {
                    let out = expr.eval(&ctx, n)?;
                    ctx.insert(name.clone(), out);
                }
                n = ctx_len(&ctx)?;
            }
            PlanStep::Filter { condition } => {
                let mask = condition.eval(&ctx, n)?;
                let mut keep: Vec<usize> = Vec::new();
                for (i, m) in mask.into_iter().enumerate() {
                    if matches!(m, Some(LiteralValue::Bool(true))) {
                        keep.push(i);
                    }
                }

                for (_, col) in ctx.iter_mut() {
                    let new_col = keep.iter().map(|&i| col[i].clone()).collect();
                    *col = new_col;
                }
                n = ctx_len(&ctx)?;
            }
            PlanStep::Sort {
                by,
                descending,
                nulls_last,
                ..
            } => {
                let mut idx: Vec<usize> = (0..n).collect();
                let mut desc_flags = descending.clone();
                if desc_flags.is_empty() {
                    desc_flags = vec![false; by.len()];
                }
                let mut nl_flags = nulls_last.clone();
                if nl_flags.is_empty() {
                    nl_flags = vec![false; by.len()];
                }
                idx.sort_by(|a, b| {
                    use std::cmp::Ordering;
                    for (k_i, key) in by.iter().enumerate() {
                        let av = &ctx[key][*a];
                        let bv = &ctx[key][*b];
                        let ord = match (av, bv) {
                            (None, None) => Ordering::Equal,
                            (None, Some(_)) => {
                                if nl_flags[k_i] {
                                    Ordering::Greater
                                } else {
                                    Ordering::Less
                                }
                            }
                            (Some(_), None) => {
                                if nl_flags[k_i] {
                                    Ordering::Less
                                } else {
                                    Ordering::Greater
                                }
                            }
                            (Some(LiteralValue::Int(x)), Some(LiteralValue::Int(y))) => x.cmp(y),
                            (Some(LiteralValue::Float(x)), Some(LiteralValue::Float(y))) => {
                                x.partial_cmp(y).unwrap_or(Ordering::Equal)
                            }
                            (Some(LiteralValue::Bool(x)), Some(LiteralValue::Bool(y))) => x.cmp(y),
                            (Some(LiteralValue::Str(x)), Some(LiteralValue::Str(y))) => x.cmp(y),
                            (Some(LiteralValue::EnumStr(x)), Some(LiteralValue::EnumStr(y))) => {
                                x.cmp(y)
                            }
                            (Some(LiteralValue::Str(x)), Some(LiteralValue::EnumStr(y))) => {
                                x.as_str().cmp(y.as_str())
                            }
                            (Some(LiteralValue::EnumStr(x)), Some(LiteralValue::Str(y))) => {
                                x.as_str().cmp(y.as_str())
                            }
                            (Some(LiteralValue::Uuid(x)), Some(LiteralValue::Uuid(y))) => x.cmp(y),
                            _ => Ordering::Equal,
                        };
                        if ord != Ordering::Equal {
                            return if desc_flags[k_i] { ord.reverse() } else { ord };
                        }
                    }
                    Ordering::Equal
                });
                for (_, col) in ctx.iter_mut() {
                    let new_col = idx.iter().map(|i| col[*i].clone()).collect();
                    *col = new_col;
                }
                n = ctx_len(&ctx)?;
            }
            PlanStep::Unique {
                subset, keep: _, ..
            } => {
                use std::collections::HashSet;
                let keys = subset
                    .clone()
                    .unwrap_or_else(|| ctx.keys().cloned().collect());
                let mut seen: HashSet<String> = HashSet::new();
                let mut keep_idx: Vec<usize> = Vec::new();
                // Row index `i` is shared across all columns; no single slice to enumerate here.
                #[allow(clippy::needless_range_loop)]
                for i in 0..n {
                    let mut sig = String::new();
                    for key in keys.iter() {
                        sig.push_str(&format!("{:?}|", ctx[key][i]));
                    }
                    if !seen.contains(&sig) {
                        seen.insert(sig);
                        keep_idx.push(i);
                    }
                }
                for (_, col) in ctx.iter_mut() {
                    let new_col = keep_idx.iter().map(|&i| col[i].clone()).collect();
                    *col = new_col;
                }
                n = ctx_len(&ctx)?;
            }
            PlanStep::Rename { columns } => {
                for (old, new) in columns.iter() {
                    if let Some(values) = ctx.remove(old) {
                        ctx.insert(new.clone(), values);
                    }
                }
                n = ctx_len(&ctx)?;
            }
            PlanStep::Slice { offset, length } => {
                let start = if *offset < 0 {
                    n.saturating_sub(offset.unsigned_abs() as usize)
                } else {
                    (*offset) as usize
                };
                let end = std::cmp::min(start.saturating_add(*length), n);
                for (_, col) in ctx.iter_mut() {
                    *col = col[start..end].to_vec();
                }
                n = ctx_len(&ctx)?;
            }
            PlanStep::WithRowCount { name, offset } => {
                // Deterministic row number column; schema-first, non-null.
                let start = *offset;
                let out = (0..n)
                    .map(|i| Some(LiteralValue::Int(start + i as i64)))
                    .collect::<Vec<_>>();
                ctx.insert(name.clone(), out);
                n = ctx_len(&ctx)?;
            }
            PlanStep::FillNull {
                subset,
                value,
                strategy,
            } => {
                let targets = subset
                    .clone()
                    .unwrap_or_else(|| ctx.keys().cloned().collect());
                for name in targets.iter() {
                    if let Some(col) = ctx.get_mut(name) {
                        if let Some(v) = value.as_ref() {
                            for item in col.iter_mut() {
                                if item.is_none() {
                                    *item = Some(v.clone());
                                }
                            }
                            continue;
                        }
                        if let Some(s) = strategy.as_ref() {
                            match s.as_str() {
                                "forward" => {
                                    let mut last: Option<LiteralValue> = None;
                                    for item in col.iter_mut() {
                                        if item.is_none() {
                                            *item = last.clone();
                                        } else {
                                            last = item.clone();
                                        }
                                    }
                                }
                                "backward" => {
                                    let mut next: Option<LiteralValue> = None;
                                    for item in col.iter_mut().rev() {
                                        if item.is_none() {
                                            *item = next.clone();
                                        } else {
                                            next = item.clone();
                                        }
                                    }
                                }
                                "zero" => {
                                    for item in col.iter_mut() {
                                        if item.is_none() {
                                            *item = Some(LiteralValue::Int(0));
                                        }
                                    }
                                }
                                "one" => {
                                    for item in col.iter_mut() {
                                        if item.is_none() {
                                            *item = Some(LiteralValue::Int(1));
                                        }
                                    }
                                }
                                "min" | "max" | "mean" => {
                                    let nums: Vec<f64> = col
                                        .iter()
                                        .filter_map(|v| match v {
                                            Some(LiteralValue::Int(i)) => Some(*i as f64),
                                            Some(LiteralValue::Float(f)) => Some(*f),
                                            _ => None,
                                        })
                                        .collect();
                                    if nums.is_empty() {
                                        continue;
                                    }
                                    let fill = match s.as_str() {
                                        "min" => nums.iter().fold(f64::INFINITY, |a, b| a.min(*b)),
                                        "max" => {
                                            nums.iter().fold(f64::NEG_INFINITY, |a, b| a.max(*b))
                                        }
                                        _ => nums.iter().sum::<f64>() / nums.len() as f64,
                                    };
                                    for item in col.iter_mut() {
                                        if item.is_none() {
                                            *item = Some(LiteralValue::Float(fill));
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
                n = ctx_len(&ctx)?;
            }
            PlanStep::DropNulls {
                subset,
                how,
                threshold,
            } => {
                let targets: Vec<String> = subset
                    .clone()
                    .unwrap_or_else(|| ctx.keys().cloned().collect());
                if targets.is_empty() {
                    continue;
                }
                let keep_min = threshold.unwrap_or_else(|| match how.as_str() {
                    "all" => 1,
                    _ => targets.len(),
                });
                let keep = (0..n)
                    .filter(|i| {
                        let non_nulls = targets.iter().filter(|c| ctx[*c][*i].is_some()).count();
                        non_nulls >= keep_min
                    })
                    .collect::<Vec<_>>();
                for (_, col) in ctx.iter_mut() {
                    *col = keep.iter().map(|i| col[*i].clone()).collect();
                }
                n = ctx_len(&ctx)?;
            }
            PlanStep::Melt { .. } => {
                return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                    "melt requires pydantable-core built with the Polars engine.",
                ));
            }
            PlanStep::RollingAgg { .. } => {
                return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                    "rolling requires pydantable-core built with the Polars engine.",
                ));
            }
            PlanStep::DuplicateMask { subset, keep } => {
                let mut mask = vec![false; n];
                match keep.as_str() {
                    "first" => {
                        let mut seen = HashSet::new();
                        for i in 0..n {
                            let k = row_key_for_subset(&ctx, subset, i);
                            if seen.contains(&k) {
                                mask[i] = true;
                            } else {
                                seen.insert(k);
                            }
                        }
                    }
                    "last" => {
                        let mut seen = HashSet::new();
                        for i in (0..n).rev() {
                            let k = row_key_for_subset(&ctx, subset, i);
                            if seen.contains(&k) {
                                mask[i] = true;
                            } else {
                                seen.insert(k);
                            }
                        }
                    }
                    "none" => {
                        let mut counts: HashMap<String, usize> = HashMap::new();
                        for i in 0..n {
                            let k = row_key_for_subset(&ctx, subset, i);
                            *counts.entry(k).or_insert(0) += 1;
                        }
                        for i in 0..n {
                            let k = row_key_for_subset(&ctx, subset, i);
                            mask[i] = counts[&k] > 1;
                        }
                    }
                    _ => {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "internal: duplicate_mask keep",
                        ));
                    }
                }
                let dup_col: Vec<Option<LiteralValue>> = mask
                    .into_iter()
                    .map(|b| Some(LiteralValue::Bool(b)))
                    .collect();
                ctx.clear();
                ctx.insert("duplicated".to_string(), dup_col);
                n = ctx_len(&ctx)?;
            }
            PlanStep::DropDuplicateGroups { subset } => {
                let mut counts: HashMap<String, usize> = HashMap::new();
                for i in 0..n {
                    let k = row_key_for_subset(&ctx, subset, i);
                    *counts.entry(k).or_insert(0) += 1;
                }
                let keep_idx: Vec<usize> = (0..n)
                    .filter(|i| counts[&row_key_for_subset(&ctx, subset, *i)] == 1)
                    .collect();
                for (_, col) in ctx.iter_mut() {
                    *col = keep_idx.iter().map(|&i| col[i].clone()).collect();
                }
                n = ctx_len(&ctx)?;
            }
        }
    }

    // Convert context back to Python dict[str, list[Any]].
    let out_dict = PyDict::new_bound(py);
    for (name, col) in ctx.iter() {
        let mut values: Vec<PyObject> = Vec::with_capacity(col.len());
        for item in col.iter() {
            match item {
                None => values.push(py.None()),
                Some(LiteralValue::Int(i)) => values.push(i.into_py(py)),
                Some(LiteralValue::Float(f)) => values.push(f.into_py(py)),
                Some(LiteralValue::Bool(b)) => values.push(b.into_py(py)),
                Some(LiteralValue::Str(s)) => values.push(s.clone().into_py(py)),
                Some(LiteralValue::EnumStr(s)) => values.push(s.clone().into_py(py)),
                Some(LiteralValue::Uuid(s)) => {
                    let uuid_mod = py.import_bound("uuid")?;
                    let ctor = uuid_mod.getattr("UUID")?;
                    values.push(ctor.call1((s.as_str(),))?.into_py(py));
                }
                Some(LiteralValue::Decimal(d)) => {
                    values.push(scaled_i128_to_py_decimal(py, *d)?);
                }
                Some(LiteralValue::DateTimeMicros(v)) => {
                    values.push(micros_to_py_datetime(py, *v)?)
                }
                Some(LiteralValue::DateDays(v)) => values.push(days_to_py_date(py, *v)?),
                Some(LiteralValue::DurationMicros(v)) => {
                    values.push(micros_to_py_timedelta(py, *v)?)
                }
                Some(LiteralValue::TimeNanos(v)) => values.push(nanos_to_py_time(py, *v)?),
                Some(LiteralValue::Binary(b)) => {
                    values.push(PyBytes::new_bound(py, b.as_slice()).into_py(py));
                }
            }
        }
        let py_list = PyList::new_bound(py, values);
        out_dict.set_item(name, py_list)?;
    }

    Ok(out_dict.into_py(py))
}
