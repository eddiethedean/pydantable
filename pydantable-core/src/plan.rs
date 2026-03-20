use std::collections::{BTreeMap, HashMap};

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDate, PyDateTime, PyDelta, PyDict, PyList};

use crate::dtype::{dtype_to_descriptor_py, dtype_to_python_type, DTypeDesc};
use crate::expr::LiteralValue;
use crate::expr::{exprnode_to_serializable, ExprNode};

#[cfg(feature = "polars_engine")]
use polars::lazy::dsl::{col, lit, Expr as PolarsExpr};
#[cfg(feature = "polars_engine")]
use polars::prelude::{
    BooleanChunked, CrossJoin, DataFrame, DataType, FillNullStrategy, Float64Chunked, Int32Chunked,
    Int64Chunked, IntoColumn, IntoLazy, IntoSeries, JoinArgs, JoinType, LazyFrame,
    MaintainOrderJoin, NewChunkedArray, PolarsError, Series, SortMultipleOptions, StringChunked,
    UniqueKeepStrategy,
};

#[derive(Clone, Debug)]
pub enum PlanStep {
    Select {
        columns: Vec<String>,
    },
    WithColumns {
        columns: HashMap<String, ExprNode>,
    },
    Filter {
        condition: ExprNode,
    },
    Sort {
        by: Vec<String>,
        descending: Vec<bool>,
    },
    Unique {
        subset: Option<Vec<String>>,
        keep: String,
    },
    Rename {
        columns: HashMap<String, String>,
    },
    Slice {
        offset: i64,
        length: usize,
    },
    FillNull {
        subset: Option<Vec<String>>,
        value: Option<LiteralValue>,
        strategy: Option<String>,
    },
    DropNulls {
        subset: Option<Vec<String>>,
    },
}

#[derive(Clone, Debug)]
pub struct PlanInner {
    pub steps: Vec<PlanStep>,
    pub schema: HashMap<String, DTypeDesc>,
    pub root_schema: HashMap<String, DTypeDesc>,
}

pub fn planinner_to_serializable(py: Python<'_>, inner: &PlanInner) -> PyResult<PyObject> {
    let out = PyDict::new_bound(py);
    out.set_item("version", 1)?;

    out.set_item(
        "schema_descriptors",
        schema_descriptors_as_py(py, &inner.schema)?,
    )?;
    out.set_item(
        "root_schema_descriptors",
        schema_descriptors_as_py(py, &inner.root_schema)?,
    )?;

    let steps = PyList::empty_bound(py);
    for step in inner.steps.iter() {
        let step_out = PyDict::new_bound(py);
        match step {
            PlanStep::Select { columns } => {
                step_out.set_item("kind", "select")?;
                step_out.set_item("columns", columns)?;
            }
            PlanStep::WithColumns { columns } => {
                step_out.set_item("kind", "with_columns")?;
                let cols = PyDict::new_bound(py);
                for (name, expr) in columns.iter() {
                    cols.set_item(name, exprnode_to_serializable(py, expr)?)?;
                }
                step_out.set_item("columns", cols)?;
            }
            PlanStep::Filter { condition } => {
                step_out.set_item("kind", "filter")?;
                step_out.set_item("condition", exprnode_to_serializable(py, condition)?)?;
            }
            PlanStep::Sort { by, descending } => {
                step_out.set_item("kind", "sort")?;
                step_out.set_item("by", by)?;
                step_out.set_item("descending", descending)?;
            }
            PlanStep::Unique { subset, keep } => {
                step_out.set_item("kind", "unique")?;
                step_out.set_item("subset", subset)?;
                step_out.set_item("keep", keep)?;
            }
            PlanStep::Rename { columns } => {
                step_out.set_item("kind", "rename")?;
                step_out.set_item("columns", columns)?;
            }
            PlanStep::Slice { offset, length } => {
                step_out.set_item("kind", "slice")?;
                step_out.set_item("offset", offset)?;
                step_out.set_item("length", length)?;
            }
            PlanStep::FillNull {
                subset,
                value,
                strategy,
            } => {
                step_out.set_item("kind", "fill_null")?;
                step_out.set_item("subset", subset)?;
                let value_obj = match value {
                    None => py.None(),
                    Some(LiteralValue::Int(v)) => v.into_py(py),
                    Some(LiteralValue::Float(v)) => v.into_py(py),
                    Some(LiteralValue::Bool(v)) => v.into_py(py),
                    Some(LiteralValue::Str(v)) => v.clone().into_py(py),
                    Some(LiteralValue::DateTimeMicros(v)) => v.into_py(py),
                    Some(LiteralValue::DateDays(v)) => v.into_py(py),
                    Some(LiteralValue::DurationMicros(v)) => v.into_py(py),
                };
                step_out.set_item("value", value_obj)?;
                step_out.set_item("strategy", strategy)?;
            }
            PlanStep::DropNulls { subset } => {
                step_out.set_item("kind", "drop_nulls")?;
                step_out.set_item("subset", subset)?;
            }
        }
        steps.append(step_out)?;
    }

    out.set_item("steps", steps)?;
    Ok(out.into_py(py))
}

#[cfg(not(feature = "polars_engine"))]
fn ctx_len(ctx: &HashMap<String, Vec<Option<LiteralValue>>>) -> PyResult<usize> {
    ctx.values().next().map(|v| v.len()).ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Cannot execute plan with an empty input context.",
        )
    })
}

pub fn make_plan(schema: HashMap<String, DTypeDesc>) -> PlanInner {
    let root_schema = schema.clone();
    PlanInner {
        steps: Vec::new(),
        schema,
        root_schema,
    }
}

pub fn schema_fields_as_py(
    py: Python<'_>,
    schema: &HashMap<String, DTypeDesc>,
) -> PyResult<PyObject> {
    let dict = pyo3::types::PyDict::new_bound(py);
    for (name, dtype) in schema.iter() {
        let t = dtype_to_python_type(py, *dtype)?;
        dict.set_item(name, t)?;
    }
    Ok(dict.into_py(py))
}

pub fn schema_descriptors_as_py(
    py: Python<'_>,
    schema: &HashMap<String, DTypeDesc>,
) -> PyResult<PyObject> {
    let dict = pyo3::types::PyDict::new_bound(py);
    for (name, dtype) in schema.iter() {
        let d = dtype_to_descriptor_py(py, *dtype)?;
        dict.set_item(name, d)?;
    }
    Ok(dict.into_py(py))
}

#[cfg(not(feature = "polars_engine"))]
pub fn root_data_to_ctx(
    _py: Python<'_>,
    root_schema: &HashMap<String, DTypeDesc>,
    root_data: &Bound<'_, PyAny>,
) -> PyResult<HashMap<String, Vec<Option<LiteralValue>>>> {
    let dict: &Bound<'_, PyDict> = root_data.downcast()?;
    let mut ctx: HashMap<String, Vec<Option<LiteralValue>>> = HashMap::new();

    for (k, v) in dict.iter() {
        let name: String = k.extract()?;
        let values_any = v;
        let list: &Bound<'_, PyList> = values_any.downcast()?;

        let expected = root_schema.get(&name).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "Root data contains unknown column '{}'.",
                name
            ))
        })?;

        let mut out: Vec<Option<LiteralValue>> = Vec::with_capacity(list.len());
        for item in list.iter() {
            let item = item;
            if item.is_none() {
                out.push(None);
            } else {
                let lit = match expected.base {
                    Some(crate::dtype::BaseType::Int) => LiteralValue::Int(item.extract::<i64>()?),
                    Some(crate::dtype::BaseType::Float) => {
                        LiteralValue::Float(item.extract::<f64>()?)
                    }
                    Some(crate::dtype::BaseType::Bool) => {
                        LiteralValue::Bool(item.extract::<bool>()?)
                    }
                    Some(crate::dtype::BaseType::Str) => {
                        LiteralValue::Str(item.extract::<String>()?)
                    }
                    None => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Root schema cannot have unknown-base dtype.",
                        ))
                    }
                };
                out.push(Some(lit));
            }
        }
        ctx.insert(name, out);
    }

    Ok(ctx)
}

pub fn plan_select(plan: &PlanInner, columns: Vec<String>) -> PyResult<PlanInner> {
    if columns.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "select() requires at least one column.",
        ));
    }

    for c in columns.iter() {
        if !plan.schema.contains_key(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "select() unknown column '{}'.",
                c
            )));
        }
    }

    let mut new_schema = HashMap::new();
    for c in columns.iter() {
        new_schema.insert(c.clone(), *plan.schema.get(c).unwrap());
    }

    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::Select { columns });

    Ok(PlanInner {
        steps: new_steps,
        schema: new_schema,
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_with_columns(
    plan: &PlanInner,
    columns: HashMap<String, ExprNode>,
) -> PyResult<PlanInner> {
    let mut new_schema = plan.schema.clone();
    let mut new_steps = plan.steps.clone();

    // Type-check and compute derived schema.
    for (name, expr) in columns.iter() {
        let referenced = expr.referenced_columns();
        for c in referenced.iter() {
            if !plan.schema.contains_key(c) {
                let mut available: Vec<String> = plan.schema.keys().cloned().collect();
                available.sort();
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Expression for '{}' references unknown column '{}'. Available columns: [{}].",
                    name,
                    c,
                    available.join(", ")
                )));
            }
        }

        let mut expr_dtype = expr.dtype();
        if expr_dtype.base.is_none() {
            // Literal(None) assigned directly needs destination type inference.
            if let Some(dest) = plan.schema.get(name) {
                let base = dest.base.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Destination schema base type is unknown.",
                    )
                })?;
                expr_dtype = DTypeDesc {
                    base: Some(base),
                    nullable: true,
                };
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                    "with_columns({}=None) cannot infer destination type; combine None with a typed expression or replace an existing column.",
                    name
                )));
            }
        }

        new_schema.insert(name.clone(), expr_dtype);
    }

    new_steps.push(PlanStep::WithColumns { columns });

    Ok(PlanInner {
        steps: new_steps,
        schema: new_schema,
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_filter(plan: &PlanInner, condition: ExprNode) -> PyResult<PlanInner> {
    let cond_dtype = condition.dtype();
    if cond_dtype.base != Some(crate::dtype::BaseType::Bool) {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            format!(
                "filter(condition) expects condition typed as bool or Optional[bool]. Got base={:?} nullable={}.",
                cond_dtype.base, cond_dtype.nullable
            ),
        ));
    }

    // referenced column validation.
    let referenced = condition.referenced_columns();
    for c in referenced.iter() {
        if !plan.schema.contains_key(c) {
            let mut available: Vec<String> = plan.schema.keys().cloned().collect();
            available.sort();
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Filter expression references unknown column '{}'. Available columns: [{}].",
                c,
                available.join(", ")
            )));
        }
    }

    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::Filter { condition });

    Ok(PlanInner {
        steps: new_steps,
        schema: plan.schema.clone(),
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_sort(plan: &PlanInner, by: Vec<String>, descending: Vec<bool>) -> PyResult<PlanInner> {
    if by.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "sort(by=...) requires at least one key.",
        ));
    }
    if !descending.is_empty() && descending.len() != by.len() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "sort(descending=...) must be empty or the same length as by.",
        ));
    }
    for key in by.iter() {
        if !plan.schema.contains_key(key) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "sort() unknown key '{}'.",
                key
            )));
        }
    }
    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::Sort { by, descending });
    Ok(PlanInner {
        steps: new_steps,
        schema: plan.schema.clone(),
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_unique(
    plan: &PlanInner,
    subset: Option<Vec<String>>,
    keep: String,
) -> PyResult<PlanInner> {
    if let Some(keys) = subset.as_ref() {
        if keys.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "unique(subset=...) cannot be empty.",
            ));
        }
        for key in keys.iter() {
            if !plan.schema.contains_key(key) {
                return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                    "unique() unknown subset column '{}'.",
                    key
                )));
            }
        }
    }
    match keep.as_str() {
        "first" | "last" | "any" => {}
        other => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "unique(keep=...) unsupported value '{}'. Use one of: first, last, any.",
                other
            )))
        }
    }
    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::Unique { subset, keep });
    Ok(PlanInner {
        steps: new_steps,
        schema: plan.schema.clone(),
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_drop(plan: &PlanInner, columns: Vec<String>) -> PyResult<PlanInner> {
    if columns.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "drop(...) requires at least one column.",
        ));
    }
    let mut new_schema = plan.schema.clone();
    for col in columns.iter() {
        if !new_schema.contains_key(col) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "drop() unknown column '{}'.",
                col
            )));
        }
        new_schema.remove(col);
    }
    if new_schema.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "drop(...) cannot remove all columns.",
        ));
    }
    let mut new_steps = plan.steps.clone();
    let kept = new_schema.keys().cloned().collect::<Vec<_>>();
    new_steps.push(PlanStep::Select { columns: kept });
    Ok(PlanInner {
        steps: new_steps,
        schema: new_schema,
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_rename(plan: &PlanInner, columns: HashMap<String, String>) -> PyResult<PlanInner> {
    if columns.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "rename(...) requires at least one mapping.",
        ));
    }
    let mut new_schema = plan.schema.clone();
    for (old, new) in columns.iter() {
        if !plan.schema.contains_key(old) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "rename() unknown column '{}'.",
                old
            )));
        }
        if old != new && new_schema.contains_key(new) && !columns.contains_key(new) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "rename() target '{}' already exists.",
                new
            )));
        }
        let dtype = *plan.schema.get(old).unwrap();
        new_schema.remove(old);
        new_schema.insert(new.clone(), dtype);
    }
    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::Rename { columns });
    Ok(PlanInner {
        steps: new_steps,
        schema: new_schema,
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_slice(plan: &PlanInner, offset: i64, length: usize) -> PyResult<PlanInner> {
    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::Slice { offset, length });
    Ok(PlanInner {
        steps: new_steps,
        schema: plan.schema.clone(),
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_fill_null(
    plan: &PlanInner,
    subset: Option<Vec<String>>,
    value: Option<LiteralValue>,
    strategy: Option<String>,
) -> PyResult<PlanInner> {
    if value.is_none() && strategy.is_none() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "fill_null() requires either a scalar value or a strategy.",
        ));
    }
    if value.is_some() && strategy.is_some() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "fill_null() accepts either value or strategy, not both.",
        ));
    }
    if let Some(cols) = subset.as_ref() {
        if cols.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "fill_null(subset=...) cannot be empty.",
            ));
        }
        for c in cols.iter() {
            if !plan.schema.contains_key(c) {
                return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                    "fill_null() unknown subset column '{}'.",
                    c
                )));
            }
        }
    }
    if let Some(s) = strategy.as_ref() {
        match s.as_str() {
            "forward" | "backward" | "min" | "max" | "mean" | "zero" | "one" => {}
            other => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "fill_null(strategy=...) unsupported value '{}'.",
                    other
                )))
            }
        }
    }

    let mut new_schema = plan.schema.clone();
    if value.is_some() {
        let targets = subset
            .clone()
            .unwrap_or_else(|| new_schema.keys().cloned().collect());
        for c in targets.iter() {
            if let Some(mut d) = new_schema.get(c).copied() {
                d.nullable = false;
                new_schema.insert(c.clone(), d);
            }
        }
    }

    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::FillNull {
        subset,
        value,
        strategy,
    });
    Ok(PlanInner {
        steps: new_steps,
        schema: new_schema,
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_drop_nulls(plan: &PlanInner, subset: Option<Vec<String>>) -> PyResult<PlanInner> {
    if let Some(cols) = subset.as_ref() {
        if cols.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "drop_nulls(subset=...) cannot be empty.",
            ));
        }
        for c in cols.iter() {
            if !plan.schema.contains_key(c) {
                return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                    "drop_nulls() unknown subset column '{}'.",
                    c
                )));
            }
        }
    }
    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::DropNulls { subset });
    Ok(PlanInner {
        steps: new_steps,
        schema: plan.schema.clone(),
        root_schema: plan.root_schema.clone(),
    })
}

pub fn execute_plan(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    #[cfg(feature = "polars_engine")]
    {
        execute_plan_polars(py, plan, root_data)
    }

    #[cfg(not(feature = "polars_engine"))]
    {
        execute_plan_rowwise(py, plan, root_data)
    }
}

#[cfg(not(feature = "polars_engine"))]
fn execute_plan_rowwise(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let mut ctx = root_data_to_ctx(py, &plan.root_schema, root_data)?;
    let mut n = ctx_len(&ctx)?;

    for step in plan.steps.iter() {
        match step {
            PlanStep::Select { columns } => {
                ctx.retain(|k, _| columns.contains(k));
                n = ctx_len(&ctx)?;
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
            PlanStep::Sort { by, descending } => {
                let mut idx: Vec<usize> = (0..n).collect();
                let mut desc_flags = descending.clone();
                if desc_flags.is_empty() {
                    desc_flags = vec![false; by.len()];
                }
                idx.sort_by(|a, b| {
                    use std::cmp::Ordering;
                    for (k_i, key) in by.iter().enumerate() {
                        let av = &ctx[key][*a];
                        let bv = &ctx[key][*b];
                        let ord = match (av, bv) {
                            (None, None) => Ordering::Equal,
                            (None, Some(_)) => Ordering::Greater,
                            (Some(_), None) => Ordering::Less,
                            (Some(LiteralValue::Int(x)), Some(LiteralValue::Int(y))) => x.cmp(y),
                            (Some(LiteralValue::Float(x)), Some(LiteralValue::Float(y))) => {
                                x.partial_cmp(y).unwrap_or(Ordering::Equal)
                            }
                            (Some(LiteralValue::Bool(x)), Some(LiteralValue::Bool(y))) => x.cmp(y),
                            (Some(LiteralValue::Str(x)), Some(LiteralValue::Str(y))) => x.cmp(y),
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
            PlanStep::Unique { subset, keep: _ } => {
                use std::collections::HashSet;
                let keys = subset
                    .clone()
                    .unwrap_or_else(|| ctx.keys().cloned().collect());
                let mut seen: HashSet<String> = HashSet::new();
                let mut keep_idx: Vec<usize> = Vec::new();
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
            PlanStep::DropNulls { subset } => {
                let targets = subset
                    .clone()
                    .unwrap_or_else(|| ctx.keys().cloned().collect());
                let keep = (0..n)
                    .filter(|i| targets.iter().all(|c| ctx[c][*i].is_some()))
                    .collect::<Vec<_>>();
                for (_, col) in ctx.iter_mut() {
                    *col = keep.iter().map(|i| col[*i].clone()).collect();
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
            }
        }
        let py_list = PyList::new_bound(py, values);
        out_dict.set_item(name, py_list)?;
    }

    Ok(out_dict.into_py(py))
}

#[cfg(feature = "polars_engine")]
fn polars_err(e: PolarsError) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Polars execution error: {e}"))
}

#[cfg(feature = "polars_engine")]
fn py_datetime_to_micros(item: &Bound<'_, PyAny>) -> PyResult<i64> {
    let dt = item.downcast::<PyDateTime>()?;
    let secs: f64 = dt.call_method0("timestamp")?.extract()?;
    Ok((secs * 1_000_000.0).round() as i64)
}

#[cfg(feature = "polars_engine")]
fn py_date_to_days(item: &Bound<'_, PyAny>) -> PyResult<i32> {
    let d = item.downcast::<PyDate>()?;
    let ordinal: i32 = d.call_method0("toordinal")?.extract()?;
    Ok(ordinal - 719_163)
}

#[cfg(feature = "polars_engine")]
fn py_timedelta_to_micros(item: &Bound<'_, PyAny>) -> PyResult<i64> {
    let td = item.downcast::<PyDelta>()?;
    let secs: f64 = td.call_method0("total_seconds")?.extract()?;
    Ok((secs * 1_000_000.0).round() as i64)
}

#[cfg(feature = "polars_engine")]
fn micros_to_py_datetime(py: Python<'_>, micros: i64) -> PyResult<PyObject> {
    let dt_mod = py.import_bound("datetime")?;
    let dt = dt_mod.getattr("datetime")?;
    Ok(dt
        .call_method1("fromtimestamp", (micros as f64 / 1_000_000.0,))?
        .into_py(py))
}

#[cfg(feature = "polars_engine")]
fn days_to_py_date(py: Python<'_>, days: i32) -> PyResult<PyObject> {
    let dt_mod = py.import_bound("datetime")?;
    let date = dt_mod.getattr("date")?;
    Ok(date
        .call_method1("fromordinal", (days + 719_163,))?
        .into_py(py))
}

#[cfg(feature = "polars_engine")]
fn micros_to_py_timedelta(py: Python<'_>, micros: i64) -> PyResult<PyObject> {
    let dt_mod = py.import_bound("datetime")?;
    let td = dt_mod.getattr("timedelta")?;
    Ok(td.call1((0, 0, micros))?.into_py(py))
}

#[cfg(feature = "polars_engine")]
fn root_data_to_polars_df(
    root_schema: &HashMap<String, DTypeDesc>,
    root_data: &Bound<'_, PyAny>,
) -> PyResult<DataFrame> {
    let dict: &Bound<'_, PyDict> = root_data.downcast()?;

    let mut series_list: Vec<Series> = Vec::new();
    for (name, dtype) in root_schema.iter() {
        let values_any = dict.get_item(name)?.ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "Root data missing required column '{}'.",
                name
            ))
        })?;
        let list: &Bound<'_, PyList> = values_any.downcast()?;

        let s = match dtype.base {
            Some(crate::dtype::BaseType::Int) => {
                let mut v: Vec<Option<i64>> = Vec::with_capacity(list.len());
                for item in list.iter() {
                    if item.is_none() {
                        v.push(None);
                    } else {
                        v.push(Some(item.extract::<i64>()?));
                    }
                }
                let ca: Int64Chunked =
                    Int64Chunked::from_iter_options(name.as_str().into(), v.into_iter());
                ca.into_series()
            }
            Some(crate::dtype::BaseType::Float) => {
                let mut v: Vec<Option<f64>> = Vec::with_capacity(list.len());
                for item in list.iter() {
                    if item.is_none() {
                        v.push(None);
                    } else {
                        v.push(Some(item.extract::<f64>()?));
                    }
                }
                let ca: Float64Chunked =
                    Float64Chunked::from_iter_options(name.as_str().into(), v.into_iter());
                ca.into_series()
            }
            Some(crate::dtype::BaseType::Bool) => {
                let mut v: Vec<Option<bool>> = Vec::with_capacity(list.len());
                for item in list.iter() {
                    if item.is_none() {
                        v.push(None);
                    } else {
                        v.push(Some(item.extract::<bool>()?));
                    }
                }
                let ca: BooleanChunked =
                    BooleanChunked::from_iter_options(name.as_str().into(), v.into_iter());
                ca.into_series()
            }
            Some(crate::dtype::BaseType::Str) => {
                let mut v: Vec<Option<String>> = Vec::with_capacity(list.len());
                for item in list.iter() {
                    if item.is_none() {
                        v.push(None);
                    } else {
                        v.push(Some(item.extract::<String>()?));
                    }
                }
                let ca: StringChunked =
                    StringChunked::from_iter_options(name.as_str().into(), v.into_iter());
                ca.into_series()
            }
            Some(crate::dtype::BaseType::DateTime) => {
                let mut v: Vec<Option<i64>> = Vec::with_capacity(list.len());
                for item in list.iter() {
                    if item.is_none() {
                        v.push(None);
                    } else {
                        v.push(Some(py_datetime_to_micros(&item)?));
                    }
                }
                let base: Int64Chunked =
                    Int64Chunked::from_iter_options(name.as_str().into(), v.into_iter());
                base.into_series()
                    .cast(&DataType::Datetime(
                        polars::prelude::TimeUnit::Microseconds,
                        None,
                    ))
                    .map_err(polars_err)?
            }
            Some(crate::dtype::BaseType::Date) => {
                let mut v: Vec<Option<i32>> = Vec::with_capacity(list.len());
                for item in list.iter() {
                    if item.is_none() {
                        v.push(None);
                    } else {
                        v.push(Some(py_date_to_days(&item)?));
                    }
                }
                let base: Int32Chunked =
                    Int32Chunked::from_iter_options(name.as_str().into(), v.into_iter());
                base.into_series()
                    .cast(&DataType::Date)
                    .map_err(polars_err)?
            }
            Some(crate::dtype::BaseType::Duration) => {
                let mut v: Vec<Option<i64>> = Vec::with_capacity(list.len());
                for item in list.iter() {
                    if item.is_none() {
                        v.push(None);
                    } else {
                        v.push(Some(py_timedelta_to_micros(&item)?));
                    }
                }
                let base: Int64Chunked =
                    Int64Chunked::from_iter_options(name.as_str().into(), v.into_iter());
                base.into_series()
                    .cast(&DataType::Duration(polars::prelude::TimeUnit::Microseconds))
                    .map_err(polars_err)?
            }
            None => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Root schema cannot have unknown-base dtype.",
                ))
            }
        };
        series_list.push(s);
    }

    let columns = series_list
        .into_iter()
        .map(|s| s.into_column())
        .collect::<Vec<polars::prelude::Column>>();
    DataFrame::new_infer_height(columns).map_err(polars_err)
}

#[cfg(feature = "polars_engine")]
fn apply_steps_to_lazy(mut lf: LazyFrame, steps: &[PlanStep]) -> PyResult<LazyFrame> {
    for step in steps.iter() {
        match step {
            PlanStep::Select { columns } => {
                let exprs = columns.iter().map(col).collect::<Vec<_>>();
                lf = lf.select(exprs);
            }
            PlanStep::WithColumns { columns } => {
                let mut exprs = Vec::with_capacity(columns.len());
                for (name, expr) in columns.iter() {
                    let pe = expr.to_polars_expr()?.alias(name);
                    exprs.push(pe);
                }
                lf = lf.with_columns(exprs);
            }
            PlanStep::Filter { condition } => {
                // SQL-like null semantics for filter: keep exactly True; drop False/NULL.
                let cond = condition.to_polars_expr()?.fill_null(lit(false));
                lf = lf.filter(cond);
            }
            PlanStep::Sort { by, descending } => {
                let exprs = by.iter().map(col).collect::<Vec<PolarsExpr>>();
                let mut desc = descending.clone();
                if desc.is_empty() {
                    desc = vec![false; by.len()];
                }
                lf = lf.sort_by_exprs(
                    exprs,
                    SortMultipleOptions::new().with_order_descending_multi(desc),
                );
            }
            PlanStep::Unique { subset, keep } => {
                let keep_strategy = match keep.as_str() {
                    "first" => UniqueKeepStrategy::First,
                    "last" => UniqueKeepStrategy::Last,
                    _ => UniqueKeepStrategy::Any,
                };
                let subset_exprs = subset
                    .clone()
                    .map(|v| v.into_iter().map(col).collect::<Vec<PolarsExpr>>());
                lf = lf.unique_stable_generic(subset_exprs, keep_strategy);
            }
            PlanStep::Rename { columns } => {
                let old = columns.keys().cloned().collect::<Vec<_>>();
                let new = columns.values().cloned().collect::<Vec<_>>();
                lf = lf.rename(old, new, true);
            }
            PlanStep::Slice { offset, length } => {
                lf = lf.slice(*offset, *length as u32);
            }
            PlanStep::FillNull {
                subset,
                value,
                strategy,
            } => {
                let all_cols = lf
                    .collect_schema()
                    .map_err(polars_err)?
                    .iter_names_cloned()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                let targets = subset.clone().unwrap_or(all_cols);
                let exprs = targets
                    .into_iter()
                    .map(|name| {
                        let base = col(&name);
                        let filled = if let Some(v) = value.as_ref() {
                            match v {
                                LiteralValue::Int(i) => base.fill_null(lit(*i)),
                                LiteralValue::Float(f) => base.fill_null(lit(*f)),
                                LiteralValue::Bool(b) => base.fill_null(lit(*b)),
                                LiteralValue::Str(s) => base.fill_null(lit(s.clone())),
                                LiteralValue::DateTimeMicros(v) => {
                                    base.fill_null(lit(*v).cast(DataType::Datetime(
                                        polars::prelude::TimeUnit::Microseconds,
                                        None,
                                    )))
                                }
                                LiteralValue::DateDays(v) => {
                                    base.fill_null(lit(*v).cast(DataType::Date))
                                }
                                LiteralValue::DurationMicros(v) => base.fill_null(lit(*v).cast(
                                    DataType::Duration(polars::prelude::TimeUnit::Microseconds),
                                )),
                            }
                        } else {
                            match strategy.as_ref().expect("validated strategy").as_str() {
                                "forward" => {
                                    base.fill_null_with_strategy(FillNullStrategy::Forward(None))
                                }
                                "backward" => {
                                    base.fill_null_with_strategy(FillNullStrategy::Backward(None))
                                }
                                "min" => base.fill_null_with_strategy(FillNullStrategy::Min),
                                "max" => base.fill_null_with_strategy(FillNullStrategy::Max),
                                "mean" => base.fill_null_with_strategy(FillNullStrategy::Mean),
                                "zero" => base.fill_null_with_strategy(FillNullStrategy::Zero),
                                "one" => base.fill_null_with_strategy(FillNullStrategy::One),
                                _ => base,
                            }
                        };
                        filled.alias(&name)
                    })
                    .collect::<Vec<_>>();
                lf = lf.with_columns(exprs);
            }
            PlanStep::DropNulls { subset } => {
                let all_cols = lf
                    .collect_schema()
                    .map_err(polars_err)?
                    .iter_names_cloned()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                let targets = subset.clone().unwrap_or(all_cols);
                if let Some(first) = targets.first() {
                    let mut cond = col(first).is_not_null();
                    for c in targets.iter().skip(1) {
                        cond = cond.and(col(c).is_not_null());
                    }
                    lf = lf.filter(cond);
                }
            }
        }
    }
    Ok(lf)
}

#[cfg(feature = "polars_engine")]
fn series_to_py_list(py: Python<'_>, series: &Series, dtype: DTypeDesc) -> PyResult<PyObject> {
    let mut values: Vec<PyObject> = Vec::with_capacity(series.len());
    match dtype.base {
        Some(crate::dtype::BaseType::Int) => {
            let casted = series.cast(&DataType::Int64).map_err(polars_err)?;
            for item in casted.i64().map_err(polars_err)?.into_iter() {
                match item {
                    Some(v) => values.push(v.into_py(py)),
                    None => values.push(py.None()),
                }
            }
        }
        Some(crate::dtype::BaseType::Float) => {
            let casted = series.cast(&DataType::Float64).map_err(polars_err)?;
            for item in casted.f64().map_err(polars_err)?.into_iter() {
                match item {
                    Some(v) => values.push(v.into_py(py)),
                    None => values.push(py.None()),
                }
            }
        }
        Some(crate::dtype::BaseType::Bool) => {
            let casted = series.cast(&DataType::Boolean).map_err(polars_err)?;
            for item in casted.bool().map_err(polars_err)?.into_iter() {
                match item {
                    Some(v) => values.push(v.into_py(py)),
                    None => values.push(py.None()),
                }
            }
        }
        Some(crate::dtype::BaseType::Str) => {
            let casted = series.cast(&DataType::String).map_err(polars_err)?;
            for item in casted.str().map_err(polars_err)?.into_iter() {
                match item {
                    Some(v) => values.push(v.into_py(py)),
                    None => values.push(py.None()),
                }
            }
        }
        Some(crate::dtype::BaseType::DateTime) => {
            let casted = series.cast(&DataType::Int64).map_err(polars_err)?;
            for item in casted.i64().map_err(polars_err)?.into_iter() {
                match item {
                    Some(v) => values.push(micros_to_py_datetime(py, v)?),
                    None => values.push(py.None()),
                }
            }
        }
        Some(crate::dtype::BaseType::Date) => {
            let casted = series.cast(&DataType::Int32).map_err(polars_err)?;
            for item in casted.i32().map_err(polars_err)?.into_iter() {
                match item {
                    Some(v) => values.push(days_to_py_date(py, v)?),
                    None => values.push(py.None()),
                }
            }
        }
        Some(crate::dtype::BaseType::Duration) => {
            let casted = series.cast(&DataType::Int64).map_err(polars_err)?;
            for item in casted.i64().map_err(polars_err)?.into_iter() {
                match item {
                    Some(v) => values.push(micros_to_py_timedelta(py, v)?),
                    None => values.push(py.None()),
                }
            }
        }
        None => {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Output schema cannot have unknown-base dtype.",
            ))
        }
    }
    Ok(PyList::new_bound(py, values).into_py(py))
}

#[cfg(feature = "polars_engine")]
fn execute_plan_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let df = root_data_to_polars_df(&plan.root_schema, root_data)?;
    let lf = apply_steps_to_lazy(df.lazy(), &plan.steps)?;
    let out_df = lf.collect().map_err(polars_err)?;

    let out_dict = PyDict::new_bound(py);
    for (name, dtype) in plan.schema.iter() {
        let col = out_df
            .column(name)
            .map_err(polars_err)?
            .as_materialized_series()
            .clone();
        let py_list = series_to_py_list(py, &col, *dtype)?;
        out_dict.set_item(name, py_list)?;
    }
    Ok(out_dict.into_py(py))
}

#[cfg(feature = "polars_engine")]
fn dtype_from_polars(dt: &DataType) -> PyResult<DTypeDesc> {
    match dt {
        DataType::Int64 => Ok(DTypeDesc {
            base: Some(crate::dtype::BaseType::Int),
            nullable: true,
        }),
        DataType::Float64 => Ok(DTypeDesc {
            base: Some(crate::dtype::BaseType::Float),
            nullable: true,
        }),
        DataType::Boolean => Ok(DTypeDesc {
            base: Some(crate::dtype::BaseType::Bool),
            nullable: true,
        }),
        DataType::String => Ok(DTypeDesc {
            base: Some(crate::dtype::BaseType::Str),
            nullable: true,
        }),
        other => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
            "Unsupported Polars dtype in result schema: {other:?}"
        ))),
    }
}

#[cfg(feature = "polars_engine")]
#[allow(clippy::too_many_arguments)]
pub fn execute_join_polars(
    py: Python<'_>,
    left_plan: &PlanInner,
    left_root_data: &Bound<'_, PyAny>,
    right_plan: &PlanInner,
    right_root_data: &Bound<'_, PyAny>,
    left_on: Vec<String>,
    right_on: Vec<String>,
    how: String,
    suffix: String,
) -> PyResult<(PyObject, PyObject)> {
    let is_cross = how == "cross";
    let is_semi = how == "semi";
    let is_anti = how == "anti";
    if !is_cross && (left_on.is_empty() || right_on.is_empty()) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "join(on=...) requires at least one join key.",
        ));
    }
    if !is_cross && left_on.len() != right_on.len() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "join() left and right join key lists must have the same length.",
        ));
    }
    if is_cross && (!left_on.is_empty() || !right_on.is_empty()) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "cross join does not accept join keys.",
        ));
    }
    for key in left_on.iter() {
        if !left_plan.schema.contains_key(key) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "join() unknown left join key '{}'.",
                key
            )));
        }
    }
    for key in right_on.iter() {
        if !right_plan.schema.contains_key(key) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "join() unknown right join key '{}'.",
                key
            )));
        }
    }

    let join_type = match how.as_str() {
        "inner" => JoinType::Inner,
        "left" => JoinType::Left,
        "right" => JoinType::Right,
        "full" | "outer" => JoinType::Full,
        "semi" | "anti" => JoinType::Left,
        "cross" => JoinType::Cross,
        other => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Unsupported join how '{}'. Use one of: inner, left, full, right, semi, anti, cross.",
            other
        )))
        }
    };

    let left_df = root_data_to_polars_df(&left_plan.root_schema, left_root_data)?;
    let right_df = root_data_to_polars_df(&right_plan.root_schema, right_root_data)?;
    let left_lf = apply_steps_to_lazy(left_df.lazy(), &left_plan.steps)?;
    let mut right_lf = apply_steps_to_lazy(right_df.lazy(), &right_plan.steps)?;

    // Deterministic collision handling:
    // - keep left names unchanged
    // - for right non-key collisions, apply suffix
    // - right join keys are dropped (joined on same-name keys)
    let mut right_select = Vec::new();
    for name in right_plan.schema.keys() {
        if is_semi || is_anti {
            if right_on.contains(name) {
                right_select.push(col(name));
            }
            continue;
        }
        // The join operation needs the right-side join keys to still exist in the
        // LazyFrame at join time.
        if right_on.contains(name) {
            right_select.push(col(name));
            continue;
        }

        // Deterministic collision handling for non-key columns.
        if left_plan.schema.contains_key(name) {
            right_select.push(col(name).alias(format!("{}{}", name, suffix)));
        } else {
            right_select.push(col(name));
        }
    }
    if is_semi || is_anti {
        right_select.push(lit(1i64).alias("__pydantable_join_marker"));
    }
    if !right_select.is_empty() {
        right_lf = right_lf.select(right_select);
    } else if !is_cross && !right_on.is_empty() {
        right_lf = right_lf.select([col(right_on[0].as_str())]);
    }

    let out_df = if is_cross {
        let left_df = left_lf.collect().map_err(polars_err)?;
        let right_df = right_lf.collect().map_err(polars_err)?;
        left_df
            .cross_join(
                &right_df,
                Some(suffix.clone().into()),
                None,
                MaintainOrderJoin::Left,
            )
            .map_err(polars_err)?
    } else {
        let left_key_exprs = left_on.iter().map(col).collect::<Vec<_>>();
        let right_key_exprs = right_on.iter().map(col).collect::<Vec<_>>();
        let mut joined = left_lf.join(
            right_lf,
            left_key_exprs,
            right_key_exprs,
            JoinArgs::new(join_type.clone()),
        );
        if is_semi {
            joined = joined
                .filter(col("__pydantable_join_marker").is_not_null())
                .select(left_plan.schema.keys().map(col).collect::<Vec<_>>());
        } else if is_anti {
            joined = joined
                .filter(col("__pydantable_join_marker").is_null())
                .select(left_plan.schema.keys().map(col).collect::<Vec<_>>());
        }
        joined.collect().map_err(polars_err)?
    };

    // Build schema descriptors from actual output dtypes.
    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
    if is_semi || is_anti {
        out_schema = left_plan.schema.clone();
    }
    for col_name in out_df.get_column_names() {
        if is_semi || is_anti {
            continue;
        }
        let col_name_str = col_name.as_str();
        // Preserve nullable semantics from the input schemas instead of
        // inferring them from observed output nulls. This keeps
        // `Optional[T]` stable across joins even when the matched rows
        // happen to contain no nulls.
        let out_desc = if let Some(left_d) = left_plan.schema.get(col_name_str) {
            let mut d = *left_d;
            if matches!(join_type, JoinType::Right | JoinType::Full) {
                d.nullable = true;
            }
            d
        } else if let Some(stripped) = col_name_str.strip_suffix(suffix.as_str()) {
            // Collision columns from the right are renamed with the suffix.
            if let Some(right_d) = right_plan.schema.get(stripped) {
                let mut d = *right_d;
                if matches!(join_type, JoinType::Left | JoinType::Full) {
                    d.nullable = true;
                }
                d
            } else {
                let s = out_df
                    .column(col_name)
                    .map_err(polars_err)?
                    .as_materialized_series();
                dtype_from_polars(s.dtype())?
            }
        } else if let Some(right_d) = right_plan.schema.get(col_name_str) {
            let mut d = *right_d;
            if matches!(join_type, JoinType::Left | JoinType::Full) {
                d.nullable = true;
            }
            d
        } else {
            let s = out_df
                .column(col_name)
                .map_err(polars_err)?
                .as_materialized_series();
            dtype_from_polars(s.dtype())?
        };

        out_schema.insert(col_name.to_string(), out_desc);
    }

    let out_dict = PyDict::new_bound(py);
    for (name, dtype) in out_schema.iter() {
        let col = out_df
            .column(name)
            .map_err(polars_err)?
            .as_materialized_series()
            .clone();
        let py_list = series_to_py_list(py, &col, *dtype)?;
        out_dict.set_item(name, py_list)?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    Ok((out_dict.into_py(py), desc))
}

#[cfg(feature = "polars_engine")]
pub fn execute_groupby_agg_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    by: Vec<String>,
    aggregations: Vec<(String, String, String)>,
) -> PyResult<(PyObject, PyObject)> {
    if by.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "group_by(...) requires at least one key.",
        ));
    }
    for key in by.iter() {
        if !plan.schema.contains_key(key) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "group_by() unknown key '{}'.",
                key
            )));
        }
    }
    if aggregations.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "agg(...) requires at least one aggregation.",
        ));
    }

    let df = root_data_to_polars_df(&plan.root_schema, root_data)?;
    let lf = apply_steps_to_lazy(df.lazy(), &plan.steps)?;
    let by_exprs = by.iter().map(col).collect::<Vec<_>>();
    let mut agg_exprs = Vec::new();
    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
    let mut tmp_count_cols: HashMap<String, String> = HashMap::new();
    for key in by.iter() {
        out_schema.insert(key.clone(), *plan.schema.get(key).unwrap());
    }

    for (out_name, op, in_col) in aggregations.into_iter() {
        let in_dtype = *plan.schema.get(&in_col).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "agg() unknown input column '{}'.",
                in_col
            ))
        })?;
        match op.as_str() {
            "count" => {
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc {
                        base: Some(crate::dtype::BaseType::Int),
                        nullable: false,
                    },
                );
                agg_exprs.push(col(&in_col).count().alias(&out_name));
            }
            "sum" => {
                let base = in_dtype.base.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "sum() requires known-base numeric dtype.",
                    )
                })?;
                if !matches!(
                    base,
                    crate::dtype::BaseType::Int | crate::dtype::BaseType::Float
                ) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "sum() requires int or float input columns.",
                    ));
                }
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc {
                        base: Some(base),
                        nullable: true,
                    },
                );
                // Polars returns `0` for `sum` over all-null values.
                // For SQL-like semantics (and our contract), mask that case
                // to null by tracking non-null counts.
                let tmp_count_name = format!("__pydantable_tmp_count_sum_{out_name}");
                tmp_count_cols.insert(out_name.clone(), tmp_count_name.clone());
                agg_exprs.push(col(&in_col).count().alias(&tmp_count_name));
                agg_exprs.push(col(&in_col).sum().alias(&out_name));
            }
            "mean" => {
                let base = in_dtype.base.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "mean() requires known-base numeric dtype.",
                    )
                })?;
                if !matches!(
                    base,
                    crate::dtype::BaseType::Int | crate::dtype::BaseType::Float
                ) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "mean() requires int or float input columns.",
                    ));
                }
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc {
                        base: Some(crate::dtype::BaseType::Float),
                        nullable: true,
                    },
                );
                // Same masking approach as for `sum`: all-null -> None.
                let tmp_count_name = format!("__pydantable_tmp_count_mean_{out_name}");
                tmp_count_cols.insert(out_name.clone(), tmp_count_name.clone());
                agg_exprs.push(col(&in_col).count().alias(&tmp_count_name));
                agg_exprs.push(col(&in_col).mean().alias(&out_name));
            }
            "min" => {
                let base = in_dtype.base.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "min() requires known-base dtype.",
                    )
                })?;
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc {
                        base: Some(base),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).min().alias(&out_name));
            }
            "max" => {
                let base = in_dtype.base.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "max() requires known-base dtype.",
                    )
                })?;
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc {
                        base: Some(base),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).max().alias(&out_name));
            }
            "median" => {
                let base = in_dtype.base.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "median() requires known-base numeric dtype.",
                    )
                })?;
                if !matches!(
                    base,
                    crate::dtype::BaseType::Int | crate::dtype::BaseType::Float
                ) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "median() requires int or float input columns.",
                    ));
                }
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc {
                        base: Some(crate::dtype::BaseType::Float),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).median().alias(&out_name));
            }
            "std" => {
                let base = in_dtype.base.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "std() requires known-base numeric dtype.",
                    )
                })?;
                if !matches!(
                    base,
                    crate::dtype::BaseType::Int | crate::dtype::BaseType::Float
                ) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "std() requires int or float input columns.",
                    ));
                }
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc {
                        base: Some(crate::dtype::BaseType::Float),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).std(1).alias(&out_name));
            }
            "var" => {
                let base = in_dtype.base.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "var() requires known-base numeric dtype.",
                    )
                })?;
                if !matches!(
                    base,
                    crate::dtype::BaseType::Int | crate::dtype::BaseType::Float
                ) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "var() requires int or float input columns.",
                    ));
                }
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc {
                        base: Some(crate::dtype::BaseType::Float),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).var(1).alias(&out_name));
            }
            "first" => {
                let base = in_dtype.base.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "first() requires known-base dtype.",
                    )
                })?;
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc {
                        base: Some(base),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).first().alias(&out_name));
            }
            "last" => {
                let base = in_dtype.base.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "last() requires known-base dtype.",
                    )
                })?;
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc {
                        base: Some(base),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).last().alias(&out_name));
            }
            "n_unique" => {
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc {
                        base: Some(crate::dtype::BaseType::Int),
                        nullable: false,
                    },
                );
                // SQL-like behavior: distinct count ignores NULL values.
                agg_exprs.push(col(&in_col).drop_nulls().n_unique().alias(&out_name));
            }
            other => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Unsupported aggregation '{}'. Use one of: count, sum, mean, min, max, median, std, var, first, last, n_unique.",
                    other
                )))
            }
        }
    }

    let out_df = lf
        .group_by(by_exprs)
        .agg(agg_exprs)
        .collect()
        .map_err(polars_err)?;

    let out_dict = PyDict::new_bound(py);
    for (name, dtype) in out_schema.iter() {
        let col = out_df
            .column(name)
            .map_err(polars_err)?
            .as_materialized_series()
            .clone();

        let py_list = if let Some(tmp_count_col) = tmp_count_cols.get(name) {
            let count_series = out_df
                .column(tmp_count_col)
                .map_err(polars_err)?
                .as_materialized_series()
                .clone();
            // Mask all-null groups: when non-null count is 0, emit `None`.
            match dtype.base {
                Some(crate::dtype::BaseType::Int) => {
                    let casted = col.cast(&DataType::Int64).map_err(polars_err)?;
                    let counts = count_series.cast(&DataType::Int64).map_err(polars_err)?;
                    let mut values: Vec<PyObject> = Vec::with_capacity(casted.len());
                    for (v, c) in casted
                        .i64()
                        .map_err(polars_err)?
                        .into_iter()
                        .zip(counts.i64().map_err(polars_err)?.into_iter())
                    {
                        if c.unwrap_or(0) == 0 {
                            values.push(py.None());
                        } else {
                            match v {
                                Some(v) => values.push(v.into_py(py)),
                                None => values.push(py.None()),
                            }
                        }
                    }
                    PyList::new_bound(py, values).into_py(py)
                }
                Some(crate::dtype::BaseType::Float) => {
                    let casted = col.cast(&DataType::Float64).map_err(polars_err)?;
                    let counts = count_series.cast(&DataType::Int64).map_err(polars_err)?;
                    let mut values: Vec<PyObject> = Vec::with_capacity(casted.len());
                    for (v, c) in casted
                        .f64()
                        .map_err(polars_err)?
                        .into_iter()
                        .zip(counts.i64().map_err(polars_err)?.into_iter())
                    {
                        if c.unwrap_or(0) == 0 {
                            values.push(py.None());
                        } else {
                            match v {
                                Some(v) => values.push(v.into_py(py)),
                                None => values.push(py.None()),
                            }
                        }
                    }
                    PyList::new_bound(py, values).into_py(py)
                }
                _ => {
                    // Shouldn't happen for our current `sum`/`mean` contract.
                    series_to_py_list(py, &col, *dtype)?
                }
            }
        } else {
            series_to_py_list(py, &col, *dtype)?
        };

        out_dict.set_item(name, py_list)?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    Ok((out_dict.into_py(py), desc))
}

#[cfg(feature = "polars_engine")]
pub fn execute_concat_polars(
    py: Python<'_>,
    left_plan: &PlanInner,
    left_root_data: &Bound<'_, PyAny>,
    right_plan: &PlanInner,
    right_root_data: &Bound<'_, PyAny>,
    how: String,
) -> PyResult<(PyObject, PyObject)> {
    let left_df = root_data_to_polars_df(&left_plan.root_schema, left_root_data)?;
    let right_df = root_data_to_polars_df(&right_plan.root_schema, right_root_data)?;
    let left_out = apply_steps_to_lazy(left_df.lazy(), &left_plan.steps)?
        .collect()
        .map_err(polars_err)?;
    let right_out = apply_steps_to_lazy(right_df.lazy(), &right_plan.steps)?
        .collect()
        .map_err(polars_err)?;

    let out_df = match how.as_str() {
        "vertical" => {
            if left_out.get_column_names() != right_out.get_column_names() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "concat(vertical) requires both inputs to have identical columns.",
                ));
            }
            let mut df = left_out.clone();
            df.vstack_mut(&right_out).map_err(polars_err)?;
            df
        }
        "horizontal" => {
            for c in right_out.get_column_names_owned().iter() {
                if left_out.get_column_names_owned().contains(c) {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "concat(horizontal) duplicate column '{}' not supported.",
                        c
                    )));
                }
            }
            let mut df = left_out.clone();
            df.hstack_mut(right_out.columns()).map_err(polars_err)?;
            df
        }
        other => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unsupported concat how '{}'. Use one of: vertical, horizontal.",
                other
            )))
        }
    };

    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
    for name in out_df.get_column_names().iter() {
        if let Some(d) = left_plan.schema.get(name.as_str()) {
            out_schema.insert(name.to_string(), *d);
        } else if let Some(d) = right_plan.schema.get(name.as_str()) {
            out_schema.insert(name.to_string(), *d);
        } else {
            let s = out_df
                .column(name)
                .map_err(polars_err)?
                .as_materialized_series();
            out_schema.insert(name.to_string(), dtype_from_polars(s.dtype())?);
        }
    }

    let out_dict = PyDict::new_bound(py);
    for (name, dtype) in out_schema.iter() {
        let col = out_df
            .column(name)
            .map_err(polars_err)?
            .as_materialized_series()
            .clone();
        let py_list = series_to_py_list(py, &col, *dtype)?;
        out_dict.set_item(name, py_list)?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    Ok((out_dict.into_py(py), desc))
}

#[cfg(feature = "polars_engine")]
fn py_dict_to_literal_ctx(
    schema: &HashMap<String, DTypeDesc>,
    data_obj: &Bound<'_, PyAny>,
) -> PyResult<HashMap<String, Vec<Option<LiteralValue>>>> {
    let dict: &Bound<'_, PyDict> = data_obj.downcast()?;
    let mut out: HashMap<String, Vec<Option<LiteralValue>>> = HashMap::new();
    for (name, dtype) in schema.iter() {
        let col_any = dict.get_item(name)?.ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "Missing column '{}' in intermediate data.",
                name
            ))
        })?;
        let list: &Bound<'_, PyList> = col_any.downcast()?;
        let mut values: Vec<Option<LiteralValue>> = Vec::with_capacity(list.len());
        for item in list.iter() {
            if item.is_none() {
                values.push(None);
                continue;
            }
            let lit = match dtype.base {
                Some(crate::dtype::BaseType::Int) => LiteralValue::Int(item.extract::<i64>()?),
                Some(crate::dtype::BaseType::Float) => LiteralValue::Float(item.extract::<f64>()?),
                Some(crate::dtype::BaseType::Bool) => LiteralValue::Bool(item.extract::<bool>()?),
                Some(crate::dtype::BaseType::Str) => LiteralValue::Str(item.extract::<String>()?),
                Some(crate::dtype::BaseType::DateTime) => {
                    LiteralValue::DateTimeMicros(py_datetime_to_micros(&item)?)
                }
                Some(crate::dtype::BaseType::Date) => {
                    LiteralValue::DateDays(py_date_to_days(&item)?)
                }
                Some(crate::dtype::BaseType::Duration) => {
                    LiteralValue::DurationMicros(py_timedelta_to_micros(&item)?)
                }
                None => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Unsupported unknown-base dtype in reshape path.",
                    ))
                }
            };
            values.push(Some(lit));
        }
        out.insert(name.clone(), values);
    }
    Ok(out)
}

#[cfg(feature = "polars_engine")]
fn literal_to_py(py: Python<'_>, v: &LiteralValue) -> PyObject {
    match v {
        LiteralValue::Int(i) => i.into_py(py),
        LiteralValue::Float(f) => f.into_py(py),
        LiteralValue::Bool(b) => b.into_py(py),
        LiteralValue::Str(s) => s.clone().into_py(py),
        LiteralValue::DateTimeMicros(v) => {
            micros_to_py_datetime(py, *v).unwrap_or_else(|_| v.into_py(py))
        }
        LiteralValue::DateDays(v) => days_to_py_date(py, *v).unwrap_or_else(|_| v.into_py(py)),
        LiteralValue::DurationMicros(v) => {
            micros_to_py_timedelta(py, *v).unwrap_or_else(|_| v.into_py(py))
        }
    }
}

#[cfg(feature = "polars_engine")]
fn agg_literal(
    op: &str,
    vals: &[Option<LiteralValue>],
    base: crate::dtype::BaseType,
) -> PyResult<Option<LiteralValue>> {
    let non_null: Vec<&LiteralValue> = vals.iter().filter_map(|v| v.as_ref()).collect();
    match op {
        "count" => Ok(Some(LiteralValue::Int(non_null.len() as i64))),
        "n_unique" => {
            let mut uniq: std::collections::HashSet<String> = std::collections::HashSet::new();
            for v in non_null {
                uniq.insert(format!("{v:?}"));
            }
            Ok(Some(LiteralValue::Int(uniq.len() as i64)))
        }
        "first" => Ok(non_null.first().map(|v| (*v).clone())),
        "last" => Ok(non_null.last().map(|v| (*v).clone())),
        "min" | "max" | "sum" | "mean" | "median" | "std" | "var" => {
            let nums = non_null
                .iter()
                .filter_map(|v| match v {
                    LiteralValue::Int(i) => Some(*i as f64),
                    LiteralValue::Float(f) => Some(*f),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if nums.is_empty() {
                return Ok(None);
            }
            match op {
                "sum" => {
                    if matches!(base, crate::dtype::BaseType::Int) {
                        Ok(Some(LiteralValue::Int(nums.iter().sum::<f64>() as i64)))
                    } else {
                        Ok(Some(LiteralValue::Float(nums.iter().sum::<f64>())))
                    }
                }
                "mean" => Ok(Some(LiteralValue::Float(
                    nums.iter().sum::<f64>() / nums.len() as f64,
                ))),
                "median" => {
                    let mut v = nums;
                    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    let n = v.len();
                    let m = if n % 2 == 1 {
                        v[n / 2]
                    } else {
                        (v[n / 2 - 1] + v[n / 2]) / 2.0
                    };
                    Ok(Some(LiteralValue::Float(m)))
                }
                "std" | "var" => {
                    if nums.len() < 2 {
                        return Ok(None);
                    }
                    let mean = nums.iter().sum::<f64>() / nums.len() as f64;
                    let sq = nums.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>();
                    let var = sq / (nums.len() as f64 - 1.0);
                    if op == "var" {
                        Ok(Some(LiteralValue::Float(var)))
                    } else {
                        Ok(Some(LiteralValue::Float(var.sqrt())))
                    }
                }
                "min" => match base {
                    crate::dtype::BaseType::Int => Ok(Some(LiteralValue::Int(
                        nums.iter().fold(f64::INFINITY, |a, b| a.min(*b)) as i64,
                    ))),
                    _ => Ok(Some(LiteralValue::Float(
                        nums.iter().fold(f64::INFINITY, |a, b| a.min(*b)),
                    ))),
                },
                "max" => match base {
                    crate::dtype::BaseType::Int => Ok(Some(LiteralValue::Int(
                        nums.iter().fold(f64::NEG_INFINITY, |a, b| a.max(*b)) as i64,
                    ))),
                    _ => Ok(Some(LiteralValue::Float(
                        nums.iter().fold(f64::NEG_INFINITY, |a, b| a.max(*b)),
                    ))),
                },
                _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Unsupported numeric aggregation op.",
                )),
            }
        }
        _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Unsupported aggregation '{}'.",
            op
        ))),
    }
}

#[cfg(feature = "polars_engine")]
#[allow(clippy::needless_range_loop)]
pub fn execute_melt_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    id_vars: Vec<String>,
    value_vars: Option<Vec<String>>,
    variable_name: String,
    value_name: String,
) -> PyResult<(PyObject, PyObject)> {
    if variable_name == value_name {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "melt() variable_name and value_name must be different.",
        ));
    }
    if plan.schema.contains_key(&variable_name) || plan.schema.contains_key(&value_name) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "melt() output column names collide with existing schema columns.",
        ));
    }
    for c in id_vars.iter() {
        if !plan.schema.contains_key(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "melt() unknown id column '{}'.",
                c
            )));
        }
    }
    let mut values = value_vars.unwrap_or_else(|| {
        plan.schema
            .keys()
            .filter(|k| !id_vars.contains(k))
            .cloned()
            .collect::<Vec<_>>()
    });
    if values.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "melt() requires at least one value column.",
        ));
    }
    for c in values.iter() {
        if !plan.schema.contains_key(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "melt() unknown value column '{}'.",
                c
            )));
        }
        if id_vars.contains(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "melt() value_vars cannot overlap id_vars.",
            ));
        }
    }
    values.sort();
    let first_base = plan.schema.get(&values[0]).and_then(|d| d.base);
    for c in values.iter().skip(1) {
        if plan.schema.get(c).and_then(|d| d.base) != first_base {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "melt() requires all value columns to share the same base dtype.",
            ));
        }
    }
    let base = first_base.ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "melt() value columns must have known-base dtypes.",
        )
    })?;

    let data_obj = execute_plan(py, plan, root_data)?;
    let data_bound = data_obj.bind(py);
    let ctx = py_dict_to_literal_ctx(&plan.schema, data_bound)?;

    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
    for k in id_vars.iter() {
        out_schema.insert(k.clone(), *plan.schema.get(k).unwrap());
    }
    out_schema.insert(
        variable_name.clone(),
        DTypeDesc::non_nullable(crate::dtype::BaseType::Str),
    );
    let nullable = values
        .iter()
        .any(|c| plan.schema.get(c).map(|d| d.nullable).unwrap_or(true));
    out_schema.insert(
        value_name.clone(),
        DTypeDesc {
            base: Some(base),
            nullable,
        },
    );

    let mut out_cols: HashMap<String, Vec<PyObject>> = HashMap::new();
    for name in out_schema.keys() {
        out_cols.insert(name.clone(), Vec::new());
    }
    let row_count = ctx.values().next().map_or(0, std::vec::Vec::len);
    for i in 0..row_count {
        for vcol in values.iter() {
            for id in id_vars.iter() {
                let val = ctx[id][i]
                    .as_ref()
                    .map_or(py.None(), |x| literal_to_py(py, x));
                out_cols.get_mut(id).unwrap().push(val);
            }
            out_cols
                .get_mut(&variable_name)
                .unwrap()
                .push(vcol.clone().into_py(py));
            let vv = ctx[vcol][i]
                .as_ref()
                .map_or(py.None(), |x| literal_to_py(py, x));
            out_cols.get_mut(&value_name).unwrap().push(vv);
        }
    }

    let out_dict = PyDict::new_bound(py);
    for (k, v) in out_cols {
        out_dict.set_item(k, PyList::new_bound(py, v))?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    Ok((out_dict.into_py(py), desc))
}

#[cfg(feature = "polars_engine")]
#[allow(clippy::needless_range_loop)]
pub fn execute_pivot_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    index: Vec<String>,
    columns: String,
    values: Vec<String>,
    aggregate_function: String,
) -> PyResult<(PyObject, PyObject)> {
    if index.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "pivot() requires at least one index column.",
        ));
    }
    if values.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "pivot() requires at least one value column.",
        ));
    }
    for c in index.iter() {
        if !plan.schema.contains_key(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "pivot() unknown index column '{}'.",
                c
            )));
        }
    }
    if !plan.schema.contains_key(&columns) {
        return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
            "pivot() unknown columns argument '{}'.",
            columns
        )));
    }
    for v in values.iter() {
        if !plan.schema.contains_key(v) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "pivot() unknown value column '{}'.",
                v
            )));
        }
    }
    let supported = [
        "count", "sum", "mean", "min", "max", "median", "std", "var", "first", "last", "n_unique",
    ];
    if !supported.contains(&aggregate_function.as_str()) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "pivot() unsupported aggregate_function '{}'.",
            aggregate_function
        )));
    }

    let data_obj = execute_plan(py, plan, root_data)?;
    let data_bound = data_obj.bind(py);
    let ctx = py_dict_to_literal_ctx(&plan.schema, data_bound)?;

    let mut pivot_values: Vec<String> = Vec::new();
    let mut seen_pivot: std::collections::HashSet<String> = std::collections::HashSet::new();
    let pivot_col = ctx.get(&columns).ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
            "pivot() unknown columns argument '{}'.",
            columns
        ))
    })?;
    for item in pivot_col.iter() {
        let key = match item {
            Some(LiteralValue::Str(s)) => s.clone(),
            Some(LiteralValue::Int(v)) => v.to_string(),
            Some(LiteralValue::Float(v)) => v.to_string(),
            Some(LiteralValue::Bool(v)) => v.to_string(),
            Some(LiteralValue::DateTimeMicros(v)) => v.to_string(),
            Some(LiteralValue::DateDays(v)) => v.to_string(),
            Some(LiteralValue::DurationMicros(v)) => v.to_string(),
            None => "null".to_string(),
        };
        if seen_pivot.insert(key.clone()) {
            pivot_values.push(key);
        }
    }

    let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let row_count = ctx.values().next().map_or(0, std::vec::Vec::len);
    for i in 0..row_count {
        let mut sig = String::new();
        for c in index.iter() {
            sig.push_str(&format!("{:?}|", ctx[c][i]));
        }
        groups.entry(sig).or_default().push(i);
    }

    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
    for c in index.iter() {
        out_schema.insert(c.clone(), *plan.schema.get(c).unwrap());
    }
    let mut out_cols: HashMap<String, Vec<PyObject>> = HashMap::new();
    for c in index.iter() {
        out_cols.insert(c.clone(), Vec::new());
    }

    let mut generated_cols: Vec<(String, String, DTypeDesc)> = Vec::new();
    for pv in pivot_values.iter() {
        for v in values.iter() {
            let name = if values.len() == 1 {
                format!("{}_{}", pv, aggregate_function)
            } else {
                format!("{}_{}_{}", pv, v, aggregate_function)
            };
            if out_schema.contains_key(&name) {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "pivot() generated duplicate output column '{}'.",
                    name
                )));
            }
            let in_d = *plan.schema.get(v).unwrap();
            let base = in_d.base.ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "pivot() value columns must have known-base dtypes.",
                )
            })?;
            if matches!(
                aggregate_function.as_str(),
                "sum" | "mean" | "median" | "std" | "var"
            ) && !matches!(
                base,
                crate::dtype::BaseType::Int | crate::dtype::BaseType::Float
            ) {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "pivot() numeric aggregations require int or float value columns.",
                ));
            }
            let out_d = match aggregate_function.as_str() {
                "count" | "n_unique" => DTypeDesc::non_nullable(crate::dtype::BaseType::Int),
                "mean" | "median" | "std" | "var" => {
                    DTypeDesc::nullable(crate::dtype::BaseType::Float)
                }
                _ => DTypeDesc {
                    base: Some(base),
                    nullable: true,
                },
            };
            generated_cols.push((name.clone(), v.clone(), out_d));
            out_schema.insert(name.clone(), out_d);
            out_cols.insert(name, Vec::new());
        }
    }

    for row_idx in groups.values() {
        let first = row_idx[0];
        for c in index.iter() {
            let val = ctx[c][first]
                .as_ref()
                .map_or(py.None(), |x| literal_to_py(py, x));
            out_cols.get_mut(c).unwrap().push(val);
        }
        for pv in pivot_values.iter() {
            let matching = row_idx
                .iter()
                .copied()
                .filter(|i| {
                    let key = match &ctx[&columns][*i] {
                        Some(LiteralValue::Str(s)) => s.clone(),
                        Some(LiteralValue::Int(v)) => v.to_string(),
                        Some(LiteralValue::Float(v)) => v.to_string(),
                        Some(LiteralValue::Bool(v)) => v.to_string(),
                        Some(LiteralValue::DateTimeMicros(v)) => v.to_string(),
                        Some(LiteralValue::DateDays(v)) => v.to_string(),
                        Some(LiteralValue::DurationMicros(v)) => v.to_string(),
                        None => "null".to_string(),
                    };
                    &key == pv
                })
                .collect::<Vec<_>>();
            for (name, source_col, out_d) in generated_cols.iter() {
                let expected_name = if values.len() == 1 {
                    format!("{}_{}", pv, aggregate_function)
                } else {
                    format!("{}_{}_{}", pv, source_col, aggregate_function)
                };
                if &expected_name != name {
                    continue;
                }
                let vals = matching
                    .iter()
                    .map(|i| ctx[source_col][*i].clone())
                    .collect::<Vec<_>>();
                let lit = agg_literal(
                    &aggregate_function,
                    &vals,
                    out_d.base.unwrap_or(crate::dtype::BaseType::Float),
                )?;
                out_cols
                    .get_mut(name)
                    .unwrap()
                    .push(lit.as_ref().map_or(py.None(), |x| literal_to_py(py, x)));
            }
        }
    }

    let out_dict = PyDict::new_bound(py);
    for (k, v) in out_cols {
        out_dict.set_item(k, PyList::new_bound(py, v))?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    Ok((out_dict.into_py(py), desc))
}

#[cfg(feature = "polars_engine")]
pub fn execute_explode_polars(
    _py: Python<'_>,
    plan: &PlanInner,
    _root_data: &Bound<'_, PyAny>,
    columns: Vec<String>,
) -> PyResult<(PyObject, PyObject)> {
    for c in columns.iter() {
        if !plan.schema.contains_key(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "explode() unknown column '{}'.",
                c
            )));
        }
    }
    Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
        "explode() requires list-like typed columns, which are not yet supported by the current schema type system.",
    ))
}

#[cfg(feature = "polars_engine")]
pub fn execute_unnest_polars(
    _py: Python<'_>,
    plan: &PlanInner,
    _root_data: &Bound<'_, PyAny>,
    columns: Vec<String>,
) -> PyResult<(PyObject, PyObject)> {
    for c in columns.iter() {
        if !plan.schema.contains_key(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "unnest() unknown column '{}'.",
                c
            )));
        }
    }
    Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
        "unnest() requires struct-like typed columns, which are not yet supported by the current schema type system.",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dtype::{BaseType, DTypeDesc};
    use crate::expr::{ArithOp, CmpOp, LiteralValue};
    use pyo3::prepare_freethreaded_python;
    use pyo3::types::{PyDict, PyList};
    use std::sync::Once;

    static INIT_PYO3: Once = Once::new();

    fn ensure_python_initialized() {
        INIT_PYO3.call_once(|| {
            prepare_freethreaded_python();
        });
    }

    #[test]
    fn plan_select_rejects_empty_projection() {
        ensure_python_initialized();
        let mut schema = HashMap::new();
        schema.insert("id".to_string(), DTypeDesc::non_nullable(BaseType::Int));
        let plan = make_plan(schema);
        let err = plan_select(&plan, Vec::new()).unwrap_err();
        assert!(err.to_string().contains("requires at least one column"));
    }

    #[test]
    fn schema_descriptors_encode_base_and_nullable() {
        ensure_python_initialized();
        Python::with_gil(|py| {
            let mut schema = HashMap::new();
            schema.insert("id".to_string(), DTypeDesc::non_nullable(BaseType::Int));
            schema.insert("age".to_string(), DTypeDesc::nullable(BaseType::Int));
            let obj = schema_descriptors_as_py(py, &schema).unwrap();
            let dict = obj.bind(py).downcast::<PyDict>().unwrap();

            let id = dict.get_item("id").unwrap().unwrap();
            let age = dict.get_item("age").unwrap().unwrap();
            assert_eq!(
                id.get_item("base").unwrap().extract::<String>().unwrap(),
                "int"
            );
            assert!(!id.get_item("nullable").unwrap().extract::<bool>().unwrap());
            assert_eq!(
                age.get_item("base").unwrap().extract::<String>().unwrap(),
                "int"
            );
            assert!(age.get_item("nullable").unwrap().extract::<bool>().unwrap());
        });
    }

    #[test]
    fn planinner_to_serializable_smoke() {
        ensure_python_initialized();

        Python::with_gil(|py| {
            // Base schema
            let mut schema = HashMap::new();
            schema.insert("id".to_string(), DTypeDesc::non_nullable(BaseType::Int));
            schema.insert("age".to_string(), DTypeDesc::nullable(BaseType::Int));

            let plan0 = make_plan(schema);

            // select(id, age)
            let plan1 = plan_select(&plan0, vec!["id".to_string(), "age".to_string()])
                .expect("plan_select should succeed");

            // with_columns(age2 = age + 2)
            let age_ref =
                ExprNode::make_column_ref("age".to_string(), DTypeDesc::nullable(BaseType::Int))
                    .expect("age column ref");
            let lit_two = ExprNode::make_literal(
                Some(LiteralValue::Int(2)),
                DTypeDesc::non_nullable(BaseType::Int),
            )
            .expect("literal 2");
            let age_plus_two =
                ExprNode::make_binary_op(ArithOp::Add, age_ref, lit_two).expect("age + 2");

            let mut with_cols: HashMap<String, ExprNode> = HashMap::new();
            with_cols.insert("age2".to_string(), age_plus_two);
            let plan2 = plan_with_columns(&plan1, with_cols).expect("plan_with_columns");

            // filter(age2 > 10)
            let age2_dtype = plan2.schema.get("age2").expect("age2 in derived schema");
            let age2_ref = ExprNode::make_column_ref("age2".to_string(), *age2_dtype)
                .expect("age2 column ref");
            let lit_10 = ExprNode::make_literal(
                Some(LiteralValue::Int(10)),
                DTypeDesc::non_nullable(BaseType::Int),
            )
            .expect("literal 10");
            let cond = ExprNode::make_compare_op(CmpOp::Gt, age2_ref, lit_10).expect("age2 > 10");
            let plan3 = plan_filter(&plan2, cond).expect("plan_filter");

            let serial = planinner_to_serializable(py, &plan3).expect("serialize plan");
            let dict: &Bound<'_, PyDict> = serial
                .downcast_bound(py)
                .expect("plan serialization is a dict");

            assert_eq!(
                dict.get_item("version")
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                1
            );

            let steps_obj = dict.get_item("steps").unwrap().unwrap();
            let steps = steps_obj.downcast::<PyList>().unwrap();
            assert_eq!(steps.len(), 3);

            let step0_any = steps.get_item(0).unwrap();
            let step0 = step0_any.downcast::<PyDict>().unwrap();
            assert_eq!(
                step0
                    .get_item("kind")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "select"
            );

            let step1_any = steps.get_item(1).unwrap();
            let step1 = step1_any.downcast::<PyDict>().unwrap();
            assert_eq!(
                step1
                    .get_item("kind")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "with_columns"
            );

            let step2_any = steps.get_item(2).unwrap();
            let step2 = step2_any.downcast::<PyDict>().unwrap();
            assert_eq!(
                step2
                    .get_item("kind")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "filter"
            );
        });
    }
}
