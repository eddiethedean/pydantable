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
use super::materialize::series_to_py_list;
use super::runner::PolarsPlanRunner;

fn mask_groupby_sum_mean_columns(
    mut df: DataFrame,
    tmp_count_cols: &HashMap<String, String>,
    out_schema: &HashMap<String, DTypeDesc>,
) -> PyResult<DataFrame> {
    if tmp_count_cols.is_empty() {
        let names: Vec<&str> = out_schema.keys().map(|s| s.as_str()).collect();
        return df.select(&names).map_err(polars_err);
    }
    let mut lf = df.lazy();
    for (out_name, tmp_name) in tmp_count_cols.iter() {
        let dtype = out_schema.get(out_name).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "Internal error: missing schema for masked column '{out_name}'."
            ))
        })?;
        let expr = match dtype.as_scalar_base_field().flatten() {
            Some(crate::dtype::BaseType::Int) => when(col(tmp_name).eq(lit(0i64)))
                .then(lit(NULL))
                .otherwise(col(out_name))
                .alias(out_name),
            Some(crate::dtype::BaseType::Float) => when(col(tmp_name).eq(lit(0i64)))
                .then(lit(NULL))
                .otherwise(col(out_name))
                .alias(out_name),
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "sum/mean masking expects int or float output dtypes.",
                ))
            }
        };
        lf = lf.with_columns([expr]);
    }
    df = lf.collect().map_err(polars_err)?;
    let names: Vec<&str> = out_schema.keys().map(|s| s.as_str()).collect();
    df.select(&names).map_err(polars_err)
}

#[cfg(feature = "polars_engine")]
pub fn execute_groupby_agg_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    by: Vec<String>,
    aggregations: Vec<(String, String, String)>,
    as_python_lists: bool,
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

    let df = root_data_to_polars_df(py, &plan.root_schema, root_data)?;
    let lf = PolarsPlanRunner::apply_steps(df.lazy(), &plan.steps)?;
    let by_exprs = by.iter().map(col).collect::<Vec<_>>();
    let mut agg_exprs = Vec::new();
    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
    let mut tmp_count_cols: HashMap<String, String> = HashMap::new();
    for key in by.iter() {
        let dt = plan.schema.get(key).cloned().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "group_by() unknown key column '{key}'.",
            ))
        })?;
        out_schema.insert(key.clone(), dt);
    }

    for (out_name, op, in_col) in aggregations.into_iter() {
        let in_dtype = plan.schema.get(&in_col).cloned().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "agg() unknown input column '{}'.",
                in_col
            ))
        })?;
        match op.as_str() {
            "count" => {
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc::Scalar {
                        base: Some(crate::dtype::BaseType::Int),
                        nullable: false,
                    },
                );
                agg_exprs.push(col(&in_col).count().alias(&out_name));
            }
            "sum" => {
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
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
                    DTypeDesc::Scalar {
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
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
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
                    DTypeDesc::Scalar {
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
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "min() requires known-base dtype.",
                    )
                })?;
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc::Scalar {
                        base: Some(base),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).min().alias(&out_name));
            }
            "max" => {
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "max() requires known-base dtype.",
                    )
                })?;
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc::Scalar {
                        base: Some(base),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).max().alias(&out_name));
            }
            "median" => {
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
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
                    DTypeDesc::Scalar {
                        base: Some(crate::dtype::BaseType::Float),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).median().alias(&out_name));
            }
            "std" => {
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
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
                    DTypeDesc::Scalar {
                        base: Some(crate::dtype::BaseType::Float),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).std(1).alias(&out_name));
            }
            "var" => {
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
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
                    DTypeDesc::Scalar {
                        base: Some(crate::dtype::BaseType::Float),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).var(1).alias(&out_name));
            }
            "first" => {
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "first() requires known-base dtype.",
                    )
                })?;
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc::Scalar {
                        base: Some(base),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).first().alias(&out_name));
            }
            "last" => {
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "last() requires known-base dtype.",
                    )
                })?;
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc::Scalar {
                        base: Some(base),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).last().alias(&out_name));
            }
            "n_unique" => {
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc::Scalar {
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

    let mut out_df = lf
        .group_by(by_exprs)
        .agg(agg_exprs)
        .collect()
        .map_err(|e| super::common::polars_err_ctx("group_by().agg()", e))?;

    out_df = mask_groupby_sum_mean_columns(out_df, &tmp_count_cols, &out_schema)?;

    if !as_python_lists {
        let mut out_only = out_df;
        let py_df = polars_dataframe_to_python_via_ipc(py, &mut out_only)?;
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
