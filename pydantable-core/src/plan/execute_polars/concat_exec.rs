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
use super::materialize::{dtype_from_polars, series_to_py_list};
use super::root_lazy::{collect_lazyframe, plan_to_lazyframe};

#[allow(clippy::too_many_arguments)]
pub fn execute_concat_polars(
    py: Python<'_>,
    left_plan: &PlanInner,
    left_root_data: &Bound<'_, PyAny>,
    right_plan: &PlanInner,
    right_root_data: &Bound<'_, PyAny>,
    how: String,
    as_python_lists: bool,
    streaming: bool,
) -> PyResult<(PyObject, PyObject)> {
    let left_out = collect_lazyframe(
        py,
        plan_to_lazyframe(py, left_plan, left_root_data)?,
        streaming,
    )?;
    let right_out = collect_lazyframe(
        py,
        plan_to_lazyframe(py, right_plan, right_root_data)?,
        streaming,
    )?;

    let out_df = match how.as_str() {
        "vertical" => {
            let left_names = left_out.get_column_names();
            let right_names = right_out.get_column_names();
            let right_aligned = if left_names == right_names {
                right_out
            } else {
                let ls: HashSet<_> = left_names.iter().collect();
                let rs: HashSet<_> = right_names.iter().collect();
                if ls != rs {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "concat(vertical) requires both inputs to have identical columns.",
                    ));
                }
                let names: Vec<&str> = left_names.iter().map(|n| n.as_str()).collect();
                right_out.select(&names).map_err(polars_err)?
            };
            let mut df = left_out.clone();
            df.vstack_mut(&right_aligned).map_err(polars_err)?;
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
            out_schema.insert(name.to_string(), d.clone());
        } else if let Some(d) = right_plan.schema.get(name.as_str()) {
            out_schema.insert(name.to_string(), d.clone());
        } else {
            let s = out_df
                .column(name)
                .map_err(polars_err)?
                .as_materialized_series();
            out_schema.insert(name.to_string(), dtype_from_polars(s.dtype())?);
        }
    }

    if !as_python_lists {
        let mut out_only = out_df;
        let names: Vec<&str> = out_schema.keys().map(|s| s.as_str()).collect();
        out_only = out_only.select(&names).map_err(polars_err)?;
        let py_df = polars_dataframe_to_python_via_ipc(py, &mut out_only)?;
        let desc = schema_descriptors_as_py(py, &out_schema)?;
        return Ok((py_df, desc));
    }

    let out_dict = PyDict::new(py);
    for (name, dtype) in out_schema.iter() {
        let col = out_df
            .column(name)
            .map_err(polars_err)?
            .as_materialized_series();
        let py_list = series_to_py_list(py, col, dtype)?;
        out_dict.set_item(name, py_list)?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    Ok((out_dict.unbind().into(), desc))
}
