//! Polars-backed physical execution for logical plans.

use std::collections::{BTreeMap, HashMap};

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDate, PyDateTime, PyDelta, PyDict, PyList};

use crate::dtype::DTypeDesc;
use crate::expr::LiteralValue;

use super::ir::{PlanInner, PlanStep};
use super::schema_py::schema_descriptors_as_py;

use polars::lazy::dsl::{col, lit, Expr as PolarsExpr};
use polars::prelude::{
    BooleanChunked, CrossJoin, DataFrame, DataType, FillNullStrategy, Float64Chunked, Int32Chunked,
    Int64Chunked, IntoColumn, IntoLazy, IntoSeries, JoinArgs, JoinType, LazyFrame,
    MaintainOrderJoin, NewChunkedArray, PolarsError, Series, SortMultipleOptions, StringChunked,
    UniqueKeepStrategy,
};

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

/// Polars physical plan runner: one step at a time for easier extension.
#[cfg(feature = "polars_engine")]
pub struct PolarsPlanRunner;

#[cfg(feature = "polars_engine")]
impl PolarsPlanRunner {
    pub fn apply_steps(mut lf: LazyFrame, steps: &[PlanStep]) -> PyResult<LazyFrame> {
        for step in steps.iter() {
            lf = Self::apply_step(lf, step)?;
        }
        Ok(lf)
    }

    fn apply_step(mut lf: LazyFrame, step: &PlanStep) -> PyResult<LazyFrame> {
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
        Ok(lf)
    }
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
pub(crate) fn execute_plan_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let df = root_data_to_polars_df(&plan.root_schema, root_data)?;
    let lf = PolarsPlanRunner::apply_steps(df.lazy(), &plan.steps)?;
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
    let left_lf = PolarsPlanRunner::apply_steps(left_df.lazy(), &left_plan.steps)?;
    let mut right_lf = PolarsPlanRunner::apply_steps(right_df.lazy(), &right_plan.steps)?;

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
    let lf = PolarsPlanRunner::apply_steps(df.lazy(), &plan.steps)?;
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
    let left_out = PolarsPlanRunner::apply_steps(left_df.lazy(), &left_plan.steps)?
        .collect()
        .map_err(polars_err)?;
    let right_out = PolarsPlanRunner::apply_steps(right_df.lazy(), &right_plan.steps)?
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

    let data_obj = super::execute_plan(py, plan, root_data)?;
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

    let data_obj = super::execute_plan(py, plan, root_data)?;
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
