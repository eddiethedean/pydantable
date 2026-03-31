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
use super::literal_agg::py_dict_to_literal_ctx;
use super::materialize::{dtype_from_polars, series_to_py_list};
use super::root_lazy::{collect_lazyframe, plan_to_lazyframe};
use super::runner::PolarsPlanRunner;

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
    validate: Option<String>,
    as_python_lists: bool,
    streaming: bool,
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

    if validate.is_some() && is_cross {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "cross join does not support validate=...; remove validate or use a keyed join.",
        ));
    }

    let left_lf = plan_to_lazyframe(py, left_plan, left_root_data)?;
    let mut right_lf = plan_to_lazyframe(py, right_plan, right_root_data)?;

    if let Some(v) = validate.as_deref() {
        fn key_fragment(value: &Option<LiteralValue>) -> String {
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

        fn keys_unique(
            py: Python<'_>,
            plan: &PlanInner,
            root_data: &Bound<'_, PyAny>,
            keys: &[String],
            streaming: bool,
        ) -> PyResult<bool> {
            if keys.is_empty() {
                return Ok(true);
            }
            let data_obj = crate::plan::execute_plan(py, plan, root_data, true, streaming)?;
            let data_bound = data_obj.bind(py);
            let ctx = py_dict_to_literal_ctx(&plan.schema, data_bound)?;
            let row_count = ctx.values().next().map_or(0, std::vec::Vec::len);
            let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
            for i in 0..row_count {
                let mut sig = String::new();
                for k in keys {
                    sig.push('|');
                    sig.push_str(&key_fragment(&ctx[k][i]));
                }
                if !seen.insert(sig) {
                    return Ok(false);
                }
            }
            Ok(true)
        }

        let left_unique = keys_unique(py, left_plan, left_root_data, &left_on, streaming)?;
        let right_unique = keys_unique(py, right_plan, right_root_data, &right_on, streaming)?;
        match v {
            "one_to_one" => {
                if !(left_unique && right_unique) {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "join(validate='one_to_one') failed: keys not unique.",
                    ));
                }
            }
            "one_to_many" => {
                if !left_unique {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "join(validate='one_to_many') failed: left keys not unique.",
                    ));
                }
            }
            "many_to_one" => {
                if !right_unique {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "join(validate='many_to_one') failed: right keys not unique.",
                    ));
                }
            }
            "many_to_many" => {}
            other => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "join(validate=...) must be one of one_to_one, one_to_many, many_to_one, many_to_many (got {other:?}).",
                )));
            }
        }
    }

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
        let left_df = collect_lazyframe(py, left_lf, streaming)?;
        let right_df = collect_lazyframe(py, right_lf, streaming)?;
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
        collect_lazyframe(py, joined, streaming)?
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
            let mut d = left_d.clone();
            if matches!(join_type, JoinType::Right | JoinType::Full) {
                d = d.with_assigned_none_nullability();
            }
            d
        } else if let Some(stripped) = col_name_str.strip_suffix(suffix.as_str()) {
            // Collision columns from the right are renamed with the suffix.
            if let Some(right_d) = right_plan.schema.get(stripped) {
                let mut d = right_d.clone();
                if matches!(join_type, JoinType::Left | JoinType::Full) {
                    d = d.with_assigned_none_nullability();
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
            let mut d = right_d.clone();
            if matches!(join_type, JoinType::Left | JoinType::Full) {
                d = d.with_assigned_none_nullability();
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

    if !as_python_lists {
        let mut out_only = out_df;
        let names: Vec<&str> = out_schema.keys().map(|s| s.as_str()).collect();
        out_only = out_only.select(&names).map_err(polars_err)?;
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
