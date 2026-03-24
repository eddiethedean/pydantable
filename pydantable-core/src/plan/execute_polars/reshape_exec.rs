#![cfg(feature = "polars_engine")]
#![allow(unused_imports)]

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Cursor;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDate, PyDateTime, PyDelta, PyDict, PyList, PyTime};

use crate::dtype::{
    py_decimal_to_scaled_i128, py_enum_to_wire_string, scaled_i128_to_py_decimal, BaseType,
    DTypeDesc, DECIMAL_PRECISION, DECIMAL_SCALE,
};
use crate::expr::{ExprNode, LiteralValue, WindowFrame, WindowOp};

use crate::plan::ir::{PlanInner, PlanStep};
use crate::plan::schema_py::schema_descriptors_as_py;

use polars::chunked_array::builder::get_list_builder;
use polars::lazy::dsl::{col, cols, lit, when, Expr as PolarsExpr};
use polars::prelude::{
    AnyValue, BooleanChunked, CrossJoin, DataFrame, DataType, ExplodeOptions, Field,
    FillNullStrategy, Float64Chunked, Int128Chunked, Int32Chunked, Int64Chunked, IntoColumn,
    IntoLazy, IntoSeries, JoinArgs, JoinType, LazyFrame, Literal, MaintainOrderJoin, NamedFrom,
    NewChunkedArray, PlSmallStr, PolarsError, Scalar, Series, SortMultipleOptions, StringChunked,
    StructChunked, TimeUnit, UniqueKeepStrategy, UnpivotArgsDSL, NULL,
};
use polars_io::ipc::{IpcReader, IpcWriter};
use polars_io::prelude::{SerReader, SerWriter};

#[cfg(feature = "polars_engine")]
use numpy::PyReadonlyArray1;

use super::common::*;
use super::literal_agg::{agg_literal, literal_to_py, py_dict_to_literal_ctx};
use super::materialize::{dtype_from_polars, series_to_py_list};
use super::runner::PolarsPlanRunner;

pub fn execute_melt_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    id_vars: Vec<String>,
    value_vars: Option<Vec<String>>,
    variable_name: String,
    value_name: String,
    as_python_lists: bool,
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
    let first_base = plan
        .schema
        .get(&values[0])
        .and_then(|d| d.as_scalar_base_field().flatten());
    for c in values.iter().skip(1) {
        if plan
            .schema
            .get(c)
            .and_then(|d| d.as_scalar_base_field().flatten())
            != first_base
        {
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

    let df = root_data_to_polars_df(py, &plan.root_schema, root_data)?;
    let lf = PolarsPlanRunner::apply_steps(df.lazy(), &plan.steps)?;
    let args = UnpivotArgsDSL {
        on: Some(cols(values.iter().map(|s| s.as_str()))),
        index: cols(id_vars.iter().map(|s| s.as_str())),
        variable_name: Some(variable_name.clone().into()),
        value_name: Some(value_name.clone().into()),
    };
    let mut out_df = lf.unpivot(args).collect().map_err(polars_err)?;

    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
    for k in id_vars.iter() {
        out_schema.insert(k.clone(), plan.schema.get(k).unwrap().clone());
    }
    out_schema.insert(
        variable_name.clone(),
        DTypeDesc::non_nullable(crate::dtype::BaseType::Str),
    );
    let nullable = values.iter().any(|c| {
        plan.schema
            .get(c)
            .map(|d| d.nullable_flag())
            .unwrap_or(true)
    });
    out_schema.insert(
        value_name.clone(),
        DTypeDesc::Scalar {
            base: Some(base),
            nullable,
        },
    );

    if !as_python_lists {
        let py_df = polars_dataframe_to_python_via_ipc(py, &mut out_df)?;
        let desc = schema_descriptors_as_py(py, &out_schema)?;
        return Ok((py_df, desc));
    }

    let out_dict = PyDict::new_bound(py);
    for (name, dtype) in out_schema.iter() {
        let col = out_df
            .column(name)
            .map_err(polars_err)?
            .as_materialized_series()
            .clone();
        let py_list = series_to_py_list(py, &col, dtype)?;
        out_dict.set_item(name, py_list)?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    Ok((out_dict.into_py(py), desc))
}

#[cfg(feature = "polars_engine")]
#[allow(clippy::needless_range_loop)]
#[allow(clippy::too_many_arguments)]
pub fn execute_pivot_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    index: Vec<String>,
    columns: String,
    values: Vec<String>,
    aggregate_function: String,
    as_python_lists: bool,
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

    let data_obj = crate::plan::execute_plan(py, plan, root_data, true)?;
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
            Some(LiteralValue::EnumStr(s)) => s.clone(),
            Some(LiteralValue::Uuid(s)) => s.clone(),
            Some(LiteralValue::Decimal(v)) => v.to_string(),
            Some(LiteralValue::Int(v)) => v.to_string(),
            Some(LiteralValue::Float(v)) => v.to_string(),
            Some(LiteralValue::Bool(v)) => v.to_string(),
            Some(LiteralValue::DateTimeMicros(v)) => v.to_string(),
            Some(LiteralValue::DateDays(v)) => v.to_string(),
            Some(LiteralValue::DurationMicros(v)) => v.to_string(),
            Some(LiteralValue::TimeNanos(v)) => v.to_string(),
            Some(LiteralValue::Binary(b)) => format!("B:{}", b.len()),
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
        out_schema.insert(c.clone(), plan.schema.get(c).unwrap().clone());
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
            let in_d = plan.schema.get(v).unwrap().clone();
            let base = in_d.as_scalar_base_field().flatten().ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "pivot() value columns must have known-base dtypes.",
                )
            })?;
            if matches!(
                aggregate_function.as_str(),
                "sum" | "mean" | "median" | "std" | "var"
            ) && !matches!(
                base,
                crate::dtype::BaseType::Int
                    | crate::dtype::BaseType::Float
                    | crate::dtype::BaseType::Decimal
            ) {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "pivot() numeric aggregations require int, float, or decimal value columns.",
                ));
            }
            let out_d = match aggregate_function.as_str() {
                "count" | "n_unique" => DTypeDesc::non_nullable(crate::dtype::BaseType::Int),
                "mean" | "median" | "std" | "var" => {
                    DTypeDesc::scalar_nullable(crate::dtype::BaseType::Float)
                }
                _ => DTypeDesc::Scalar {
                    base: Some(base),
                    nullable: true,
                },
            };
            generated_cols.push((name.clone(), v.clone(), out_d.clone()));
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
                        Some(LiteralValue::EnumStr(s)) => s.clone(),
                        Some(LiteralValue::Uuid(s)) => s.clone(),
                        Some(LiteralValue::Decimal(v)) => v.to_string(),
                        Some(LiteralValue::Int(v)) => v.to_string(),
                        Some(LiteralValue::Float(v)) => v.to_string(),
                        Some(LiteralValue::Bool(v)) => v.to_string(),
                        Some(LiteralValue::DateTimeMicros(v)) => v.to_string(),
                        Some(LiteralValue::DateDays(v)) => v.to_string(),
                        Some(LiteralValue::DurationMicros(v)) => v.to_string(),
                        Some(LiteralValue::TimeNanos(v)) => v.to_string(),
                        Some(LiteralValue::Binary(b)) => format!("B:{}", b.len()),
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
                    out_d
                        .as_scalar_base_field()
                        .flatten()
                        .unwrap_or(crate::dtype::BaseType::Float),
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
    if !as_python_lists {
        let pl = py.import_bound("polars")?;
        let df_obj = pl.getattr("DataFrame")?.call1((out_dict.as_ref(),))?;
        return Ok((df_obj.into_py(py), desc));
    }
    Ok((out_dict.into_py(py), desc))
}

#[cfg(feature = "polars_engine")]
fn py_index_value_to_seconds(item: &Bound<'_, PyAny>) -> PyResult<f64> {
    if item.is_none() {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "group_by_dynamic index column must be time-like or numeric.",
        ));
    }
    if let Ok(dt) = item.downcast::<PyDateTime>() {
        let secs: f64 = dt.call_method0("timestamp")?.extract()?;
        return Ok(secs);
    }
    if let Ok(d) = item.downcast::<PyDate>() {
        let py = item.py();
        let dt_mod = py.import_bound("datetime")?;
        let datetime = dt_mod.getattr("datetime")?;
        let combine = datetime.getattr("combine")?;
        let min_time = datetime.getattr("min")?.getattr("time")?;
        let dt_obj = combine.call1((d, min_time))?;
        let secs: f64 = dt_obj.call_method0("timestamp")?.extract()?;
        return Ok(secs);
    }
    if let Ok(td) = item.downcast::<PyDelta>() {
        let secs: f64 = td.call_method0("total_seconds")?.extract()?;
        return Ok(secs);
    }
    if let Ok(i) = item.extract::<i64>() {
        return Ok(i as f64);
    }
    if let Ok(f) = item.extract::<f64>() {
        return Ok(f);
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "group_by_dynamic index column must be time-like or numeric.",
    ))
}

#[cfg(feature = "polars_engine")]
fn parse_duration_seconds_strict(text: &str) -> PyResult<f64> {
    let text = text.trim();
    if text.len() < 2 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "invalid duration string",
        ));
    }
    let unit = text.chars().last().unwrap();
    let num: f64 = text[..text.len() - 1].parse().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("invalid duration {text:?}"))
    })?;
    let factor = match unit {
        's' => 1.0,
        'm' => 60.0,
        'h' => 3600.0,
        'd' => 86400.0,
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Duration supports s/m/h/d suffixes.",
            ))
        }
    };
    Ok(num * factor)
}

#[cfg(feature = "polars_engine")]
fn dynamic_group_key_fragment(value: &Option<LiteralValue>) -> String {
    match value {
        None => "N".to_string(),
        Some(LiteralValue::Int(i)) => format!("I:{i}"),
        Some(LiteralValue::Float(f)) => format!("F:{f:?}"),
        Some(LiteralValue::Bool(b)) => format!("B:{b}"),
        Some(LiteralValue::Str(s)) => format!("S:{s}"),
        Some(LiteralValue::EnumStr(s)) => format!("E:{s}"),
        Some(LiteralValue::Uuid(s)) => format!("U:{s}"),
        Some(LiteralValue::Decimal(v)) => format!("DEC:{v}"),
        Some(LiteralValue::DateTimeMicros(v)) => format!("DT:{v}"),
        Some(LiteralValue::DateDays(v)) => format!("D:{v}"),
        Some(LiteralValue::DurationMicros(v)) => format!("TD:{v}"),
        Some(LiteralValue::TimeNanos(v)) => format!("T:{v}"),
        Some(LiteralValue::Binary(b)) => format!("BIN:{}", b.len()),
    }
}

#[cfg(feature = "polars_engine")]
fn dynamic_row_group_key(
    ctx: &HashMap<String, Vec<Option<LiteralValue>>>,
    by: &[String],
    row: usize,
) -> String {
    let mut s = String::new();
    for c in by {
        s.push('|');
        s.push_str(&dynamic_group_key_fragment(&ctx[c][row]));
    }
    s
}

#[cfg(feature = "polars_engine")]
#[allow(clippy::too_many_arguments)]
pub fn execute_groupby_dynamic_agg_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    index_column: String,
    every: String,
    period: Option<String>,
    by: Option<Vec<String>>,
    aggregations: Vec<(String, String, String)>,
    as_python_lists: bool,
) -> PyResult<(PyObject, PyObject)> {
    let by = by.unwrap_or_default();
    if !plan.schema.contains_key(&index_column) {
        return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
            "group_by_dynamic() unknown index column '{index_column}'.",
        )));
    }
    for c in by.iter() {
        if !plan.schema.contains_key(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "group_by_dynamic() unknown by column '{c}'.",
            )));
        }
    }
    if aggregations.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "agg(...) requires at least one aggregation.",
        ));
    }
    for (_, op, _) in aggregations.iter() {
        if !matches!(op.as_str(), "count" | "sum" | "mean" | "min" | "max") {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unsupported dynamic aggregation '{op}'.",
            )));
        }
    }

    let data_obj = crate::plan::execute_plan(py, plan, root_data, true)?;
    let data_bound = data_obj.bind(py);
    let ctx = py_dict_to_literal_ctx(&plan.schema, data_bound)?;

    let dict: &Bound<'_, PyDict> = data_bound.downcast()?;
    let index_list_any = dict.get_item(&index_column)?.ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
            "group_by_dynamic() unknown index column '{index_column}'.",
        ))
    })?;
    let index_list: &Bound<'_, PyList> = index_list_any.downcast()?;
    let n = index_list.len();
    let mut times: Vec<f64> = Vec::with_capacity(n);
    for item in index_list.iter() {
        times.push(py_index_value_to_seconds(&item)?);
    }

    let every_s = parse_duration_seconds_strict(&every)?;
    let period_str = period.unwrap_or_else(|| every.clone());
    let period_s = parse_duration_seconds_strict(&period_str)?;
    if every_s <= 0.0 || period_s <= 0.0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "group_by_dynamic() requires positive every= and period= durations \
             (got every={every:?} -> {every_s}, period={period_str:?} -> {period_s}).",
        )));
    }

    let (t_min, t_max) = if times.is_empty() {
        (0.0, 0.0)
    } else {
        (
            times.iter().copied().fold(f64::INFINITY, f64::min),
            times.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        )
    };

    let mut start = t_min;
    let mut out_cols: HashMap<String, Vec<PyObject>> = HashMap::new();
    out_cols.insert(index_column.clone(), Vec::new());
    for c in by.iter() {
        out_cols.insert(c.clone(), Vec::new());
    }
    for (name, _, _) in aggregations.iter() {
        out_cols.insert(name.clone(), Vec::new());
    }

    while start <= t_max {
        let end = start + period_s;
        let win_rows: Vec<usize> = times
            .iter()
            .enumerate()
            .filter_map(|(i, t)| {
                if *t >= start && *t < end {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        let mut group_order: Vec<String> = Vec::new();
        let mut group_rows: HashMap<String, Vec<usize>> = HashMap::new();

        if by.is_empty() {
            group_order.push(String::new());
            group_rows.insert(String::new(), win_rows);
        } else {
            for &i in &win_rows {
                let key = dynamic_row_group_key(&ctx, &by, i);
                if !group_rows.contains_key(&key) {
                    group_order.push(key.clone());
                }
                group_rows.entry(key).or_default().push(i);
            }
        }

        for key in group_order {
            let rows = group_rows.get(&key).unwrap();
            if rows.is_empty() {
                continue;
            }
            let first = rows[0];
            let idx_val = index_list.get_item(first)?;
            out_cols
                .get_mut(&index_column)
                .unwrap()
                .push(idx_val.into_py(py));

            for c in by.iter() {
                let v = ctx[c][first]
                    .as_ref()
                    .map_or(py.None(), |x| literal_to_py(py, x));
                out_cols.get_mut(c).unwrap().push(v);
            }

            for (out_name, op, in_col) in aggregations.iter() {
                let in_dtype = plan.schema.get(in_col).cloned().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                        "agg() unknown input column '{in_col}'.",
                    ))
                })?;
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "aggregation requires known-base dtype.",
                    )
                })?;
                let vals: Vec<Option<LiteralValue>> =
                    rows.iter().map(|&i| ctx[in_col][i].clone()).collect();
                let lit = agg_literal(op, &vals, base)?;
                out_cols
                    .get_mut(out_name)
                    .unwrap()
                    .push(lit.as_ref().map_or(py.None(), |x| literal_to_py(py, x)));
            }
        }

        start += every_s;
    }

    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
    out_schema.insert(
        index_column.clone(),
        plan.schema.get(&index_column).unwrap().clone(),
    );
    for c in by.iter() {
        out_schema.insert(c.clone(), plan.schema.get(c).unwrap().clone());
    }
    for (out_name, op, in_col) in aggregations.iter() {
        let in_dtype = plan.schema.get(in_col).unwrap().clone();
        let out_d = match op.as_str() {
            "count" => DTypeDesc::Scalar {
                base: Some(crate::dtype::BaseType::Int),
                nullable: false,
            },
            "mean" => DTypeDesc::Scalar {
                base: Some(crate::dtype::BaseType::Float),
                nullable: true,
            },
            "sum" | "min" | "max" => in_dtype.clone(),
            _ => unreachable!(),
        };
        out_schema.insert(out_name.clone(), out_d);
    }

    let out_dict = PyDict::new_bound(py);
    for (k, v) in out_cols {
        out_dict.set_item(k, PyList::new_bound(py, v))?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    if !as_python_lists {
        let pl = py.import_bound("polars")?;
        let df_obj = pl.getattr("DataFrame")?.call1((out_dict.as_ref(),))?;
        return Ok((df_obj.into_py(py), desc));
    }
    Ok((out_dict.into_py(py), desc))
}

#[cfg(feature = "polars_engine")]
fn dtype_after_explode(inner: &DTypeDesc) -> DTypeDesc {
    match inner {
        DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
            base: *base,
            nullable: true,
        },
        DTypeDesc::Struct { fields, .. } => DTypeDesc::Struct {
            fields: fields.clone(),
            nullable: true,
        },
        DTypeDesc::List {
            inner: i,
            nullable: _,
        } => DTypeDesc::List {
            inner: i.clone(),
            nullable: true,
        },
        DTypeDesc::Map { value, .. } => DTypeDesc::Map {
            value: value.clone(),
            nullable: true,
        },
    }
}

#[cfg(feature = "polars_engine")]
pub fn execute_explode_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    columns: Vec<String>,
) -> PyResult<(PyObject, PyObject)> {
    for c in columns.iter() {
        let dt = plan.schema.get(c).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "explode() unknown column '{}'.",
                c
            ))
        })?;
        if !matches!(dt, DTypeDesc::List { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "explode() column '{}' must have list dtype.",
                c
            )));
        }
    }

    let df = root_data_to_polars_df(py, &plan.root_schema, root_data)?;
    let mut lf = PolarsPlanRunner::apply_steps(df.lazy(), &plan.steps)?;
    lf = lf.explode(
        cols(columns.iter().map(|c| c.as_str())),
        ExplodeOptions {
            empty_as_null: false,
            keep_nulls: true,
        },
    );
    let out_df = lf.collect().map_err(polars_err)?;

    let mut out_schema: HashMap<String, DTypeDesc> = plan.schema.clone();
    for c in &columns {
        if let Some(DTypeDesc::List { inner, .. }) = out_schema.get(c) {
            let new_d = dtype_after_explode(inner);
            out_schema.insert(c.clone(), new_d);
        }
    }

    let out_dict = PyDict::new_bound(py);
    for (name, dtype) in out_schema.iter() {
        let s = out_df
            .column(name)
            .map_err(polars_err)?
            .as_materialized_series()
            .clone();
        let py_list = series_to_py_list(py, &s, dtype)?;
        out_dict.set_item(name, py_list)?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    Ok((out_dict.into_py(py), desc))
}

#[cfg(feature = "polars_engine")]
fn dtype_for_unnest_output_column(
    name: &str,
    plan_schema: &HashMap<String, DTypeDesc>,
    unnest_parents: &[String],
) -> Option<DTypeDesc> {
    for parent in unnest_parents {
        if let Some(DTypeDesc::Struct {
            fields,
            nullable: struct_nullable,
        }) = plan_schema.get(parent)
        {
            for (fname, fdt) in fields {
                let composed = format!("{parent}_{fname}");
                if composed == name {
                    let mut out = fdt.clone();
                    if *struct_nullable {
                        out = out.with_assigned_none_nullability();
                    }
                    return Some(out);
                }
            }
        }
    }
    plan_schema.get(name).cloned()
}

#[cfg(feature = "polars_engine")]
pub fn execute_unnest_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    columns: Vec<String>,
) -> PyResult<(PyObject, PyObject)> {
    for c in columns.iter() {
        let dt = plan.schema.get(c).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "unnest() unknown column '{}'.",
                c
            ))
        })?;
        if !matches!(dt, DTypeDesc::Struct { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "unnest() column '{}' must have struct dtype (nested model column).",
                c
            )));
        }
    }

    let df = root_data_to_polars_df(py, &plan.root_schema, root_data)?;
    let mut lf = PolarsPlanRunner::apply_steps(df.lazy(), &plan.steps)?;
    let sep: PlSmallStr = "_".into();
    lf = lf.unnest(cols(columns.iter().map(|s| s.as_str())), Some(sep));
    let out_df = lf.collect().map_err(polars_err)?;

    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
    for col_name in out_df.get_column_names() {
        let col_name_str = col_name.as_str();
        let out_desc =
            if let Some(d) = dtype_for_unnest_output_column(col_name_str, &plan.schema, &columns) {
                d
            } else {
                let s = out_df
                    .column(col_name)
                    .map_err(polars_err)?
                    .as_materialized_series();
                dtype_from_polars(s.dtype())?
            };
        out_schema.insert(col_name_str.to_string(), out_desc);
    }

    let out_dict = PyDict::new_bound(py);
    for (name, dtype) in out_schema.iter() {
        let s = out_df
            .column(name)
            .map_err(polars_err)?
            .as_materialized_series()
            .clone();
        let py_list = series_to_py_list(py, &s, dtype)?;
        out_dict.set_item(name, py_list)?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    Ok((out_dict.into_py(py), desc))
}
