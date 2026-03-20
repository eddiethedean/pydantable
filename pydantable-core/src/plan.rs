use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList};

use crate::dtype::{dtype_to_descriptor_py, dtype_to_python_type, DTypeDesc};
use crate::expr::{exprnode_to_serializable, ExprNode};

#[cfg(not(feature = "polars_engine"))]
use crate::expr::LiteralValue;

#[cfg(feature = "polars_engine")]
use polars::lazy::dsl::{col, lit, Expr as PolarsExpr};
#[cfg(feature = "polars_engine")]
use polars::prelude::{
    BooleanChunked, DataFrame, DataType, Float64Chunked, Int64Chunked, IntoColumn, IntoLazy,
    IntoSeries, JoinArgs, JoinType, LazyFrame, NewChunkedArray, PolarsError, Series,
    SortMultipleOptions, StringChunked, UniqueKeepStrategy,
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
    on: Vec<String>,
    how: String,
    suffix: String,
) -> PyResult<(PyObject, PyObject)> {
    if on.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "join(on=...) requires at least one join key.",
        ));
    }
    for key in on.iter() {
        if !left_plan.schema.contains_key(key) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "join() unknown left join key '{}'.",
                key
            )));
        }
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
        "full" | "outer" => JoinType::Full,
        other => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unsupported join how '{}'. Use one of: inner, left, full.",
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
        // The join operation needs the right-side join keys to still exist in the
        // LazyFrame at join time.
        if on.contains(name) {
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
    if !right_select.is_empty() {
        right_lf = right_lf.select(right_select);
    } else {
        right_lf = right_lf.select([col(on[0].as_str())]);
    }

    let key_exprs = on.iter().map(col).collect::<Vec<_>>();
    let joined = left_lf.join(
        right_lf,
        key_exprs.clone(),
        key_exprs,
        JoinArgs::new(join_type),
    );
    let out_df = joined.collect().map_err(polars_err)?;

    // Build schema descriptors from actual output dtypes.
    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
    for col_name in out_df.get_column_names() {
        let col_name_str = col_name.as_str();
        // Preserve nullable semantics from the input schemas instead of
        // inferring them from observed output nulls. This keeps
        // `Optional[T]` stable across joins even when the matched rows
        // happen to contain no nulls.
        let out_desc = if let Some(left_d) = left_plan.schema.get(col_name_str) {
            *left_d
        } else if let Some(stripped) = col_name_str.strip_suffix(suffix.as_str()) {
            // Collision columns from the right are renamed with the suffix.
            if let Some(right_d) = right_plan.schema.get(stripped) {
                *right_d
            } else {
                let s = out_df
                    .column(col_name)
                    .map_err(polars_err)?
                    .as_materialized_series();
                dtype_from_polars(s.dtype())?
            }
        } else if let Some(right_d) = right_plan.schema.get(col_name_str) {
            *right_d
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
            other => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Unsupported aggregation '{}'. Use one of: count, sum, mean.",
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
