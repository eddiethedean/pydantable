use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList};

use crate::dtype::{dtype_to_descriptor_py, dtype_to_python_type, DTypeDesc};
use crate::expr::{ExprNode, LiteralValue};

#[cfg(feature = "polars_engine")]
use polars::lazy::dsl::{col, lit};
#[cfg(feature = "polars_engine")]
use polars::prelude::{
    BooleanChunked, DataFrame, DataType, Float64Chunked, IntoLazy, Int64Chunked, JoinArgs,
    JoinType, IntoColumn, IntoSeries, LazyFrame, NewChunkedArray, PolarsError, Series,
    StringChunked,
};

#[derive(Clone, Debug)]
pub enum PlanStep {
    Select { columns: Vec<String> },
    WithColumns { columns: HashMap<String, ExprNode> },
    Filter { condition: ExprNode },
}

#[derive(Clone, Debug)]
pub struct PlanInner {
    pub steps: Vec<PlanStep>,
    pub schema: HashMap<String, DTypeDesc>,
    pub root_schema: HashMap<String, DTypeDesc>,
}

fn ctx_len(ctx: &HashMap<String, Vec<Option<LiteralValue>>>) -> PyResult<usize> {
    ctx.values()
        .next()
        .map(|v| v.len())
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Cannot execute plan with an empty input context.",
        ))
}

pub fn make_plan(schema: HashMap<String, DTypeDesc>) -> PlanInner {
    let root_schema = schema.clone();
    PlanInner {
        steps: Vec::new(),
        schema,
        root_schema,
    }
}

pub fn schema_fields_as_py(py: Python<'_>, schema: &HashMap<String, DTypeDesc>) -> PyResult<PyObject> {
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

pub fn root_data_to_ctx(
    py: Python<'_>,
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
                    Some(crate::dtype::BaseType::Bool) => LiteralValue::Bool(item.extract::<bool>()?),
                    Some(crate::dtype::BaseType::Str) => LiteralValue::Str(item.extract::<String>()?),
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
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Expression for '{}' references unknown column '{}'.",
                    name,
                    c
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
            "filter(condition) expects condition typed as bool or Optional[bool].",
        ));
    }

    // referenced column validation.
    let referenced = condition.referenced_columns();
    for c in referenced.iter() {
        if !plan.schema.contains_key(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Filter expression references unknown column '{}'.",
                c
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

pub fn execute_plan(py: Python<'_>, plan: &PlanInner, root_data: &Bound<'_, PyAny>) -> PyResult<PyObject> {
    #[cfg(feature = "polars_engine")]
    {
        return execute_plan_polars(py, plan, root_data);
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
                let ca: Float64Chunked = Float64Chunked::from_iter_options(
                    name.as_str().into(),
                    v.into_iter(),
                );
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
                let ca: StringChunked = StringChunked::from_iter_options(
                    name.as_str().into(),
                    v.into_iter(),
                );
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
                let exprs = columns.iter().map(|name| col(name)).collect::<Vec<_>>();
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
        }
    }
    Ok(lf)
}

#[cfg(feature = "polars_engine")]
fn series_to_py_list(
    py: Python<'_>,
    series: &Series,
    dtype: DTypeDesc,
) -> PyResult<PyObject> {
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
fn execute_plan_polars(py: Python<'_>, plan: &PlanInner, root_data: &Bound<'_, PyAny>) -> PyResult<PyObject> {
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
    let mut left_lf = apply_steps_to_lazy(left_df.lazy(), &left_plan.steps)?;
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

    let key_exprs = on.iter().map(|k| col(k)).collect::<Vec<_>>();
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
        let s = out_df.column(col_name).map_err(polars_err)?.as_materialized_series();
        let mut d = dtype_from_polars(s.dtype())?;
        d.nullable = s.null_count() > 0;
        out_schema.insert(col_name.to_string(), d);
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
    let by_exprs = by.iter().map(|k| col(k)).collect::<Vec<_>>();
    let mut agg_exprs = Vec::new();
    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
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
        let expr = match op.as_str() {
            "count" => {
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc {
                        base: Some(crate::dtype::BaseType::Int),
                        nullable: false,
                    },
                );
                col(&in_col).count().alias(&out_name)
            }
            "sum" => {
                let base = in_dtype.base.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "sum() requires known-base numeric dtype.",
                    )
                })?;
                if !matches!(base, crate::dtype::BaseType::Int | crate::dtype::BaseType::Float) {
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
                col(&in_col).sum().alias(&out_name)
            }
            "mean" => {
                let base = in_dtype.base.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "mean() requires known-base numeric dtype.",
                    )
                })?;
                if !matches!(base, crate::dtype::BaseType::Int | crate::dtype::BaseType::Float) {
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
                col(&in_col).mean().alias(&out_name)
            }
            other => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Unsupported aggregation '{}'. Use one of: count, sum, mean.",
                    other
                )))
            }
        };
        agg_exprs.push(expr);
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

    #[test]
    fn plan_select_rejects_empty_projection() {
        let mut schema = HashMap::new();
        schema.insert("id".to_string(), DTypeDesc::non_nullable(BaseType::Int));
        let plan = make_plan(schema);
        let err = plan_select(&plan, Vec::new()).unwrap_err();
        assert!(err.to_string().contains("requires at least one column"));
    }

    #[test]
    fn schema_descriptors_encode_base_and_nullable() {
        Python::with_gil(|py| {
            let mut schema = HashMap::new();
            schema.insert("id".to_string(), DTypeDesc::non_nullable(BaseType::Int));
            schema.insert("age".to_string(), DTypeDesc::nullable(BaseType::Int));
            let obj = schema_descriptors_as_py(py, &schema).unwrap();
            let dict = obj.bind(py).downcast::<PyDict>().unwrap();

            let id = dict.get_item("id").unwrap().unwrap();
            let age = dict.get_item("age").unwrap().unwrap();
            assert_eq!(id.get_item("base").unwrap().extract::<String>().unwrap(), "int");
            assert!(!id.get_item("nullable").unwrap().extract::<bool>().unwrap());
            assert_eq!(
                age.get_item("base").unwrap().extract::<String>().unwrap(),
                "int"
            );
            assert!(age.get_item("nullable").unwrap().extract::<bool>().unwrap());
        });
    }
}

