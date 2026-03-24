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

use super::common::polars_err;

pub struct PolarsPlanRunner;

#[cfg(feature = "polars_engine")]
impl PolarsPlanRunner {
    fn expr_has_framed_window(expr: &ExprNode) -> bool {
        match expr {
            ExprNode::Window { frame: Some(_), .. } => true,
            ExprNode::BinaryOp { left, right, .. }
            | ExprNode::CompareOp { left, right, .. }
            | ExprNode::LogicalBinary { left, right, .. } => {
                Self::expr_has_framed_window(left) || Self::expr_has_framed_window(right)
            }
            ExprNode::Cast { input, .. }
            | ExprNode::IsNull { input, .. }
            | ExprNode::IsNotNull { input, .. } => Self::expr_has_framed_window(input),
            ExprNode::Coalesce { exprs, .. } | ExprNode::StringConcat { parts: exprs, .. } => {
                exprs.iter().any(Self::expr_has_framed_window)
            }
            ExprNode::CaseWhen {
                branches, else_, ..
            } => {
                branches.iter().any(|(a, b)| {
                    Self::expr_has_framed_window(a) || Self::expr_has_framed_window(b)
                }) || Self::expr_has_framed_window(else_)
            }
            ExprNode::InList { inner, .. }
            | ExprNode::StringLength { inner, .. }
            | ExprNode::StringReplace { inner, .. }
            | ExprNode::ListLen { inner, .. }
            | ExprNode::ListMin { inner, .. }
            | ExprNode::ListMax { inner, .. }
            | ExprNode::ListSum { inner, .. }
            | ExprNode::DatetimeToDate { inner, .. }
            | ExprNode::Strptime { inner, .. }
            | ExprNode::UnixTimestamp { inner, .. }
            | ExprNode::BinaryLength { inner, .. }
            | ExprNode::MapLen { inner, .. }
            | ExprNode::MapGet { inner, .. }
            | ExprNode::MapContainsKey { inner, .. }
            | ExprNode::MapKeys { inner, .. }
            | ExprNode::MapValues { inner, .. }
            | ExprNode::MapEntries { inner, .. }
            | ExprNode::MapFromEntries { inner, .. }
            | ExprNode::LogicalNot { inner, .. }
            | ExprNode::UnaryNumeric { inner, .. }
            | ExprNode::StringUnary { inner, .. }
            | ExprNode::TemporalPart { inner, .. }
            | ExprNode::GlobalAgg { inner, .. } => Self::expr_has_framed_window(inner),
            ExprNode::Between {
                inner, low, high, ..
            } => {
                Self::expr_has_framed_window(inner)
                    || Self::expr_has_framed_window(low)
                    || Self::expr_has_framed_window(high)
            }
            ExprNode::Substring {
                inner,
                start,
                length,
                ..
            } => {
                Self::expr_has_framed_window(inner)
                    || Self::expr_has_framed_window(start)
                    || length
                        .as_ref()
                        .map(|e| Self::expr_has_framed_window(e))
                        .unwrap_or(false)
            }
            ExprNode::StructField { base, .. } => Self::expr_has_framed_window(base),
            ExprNode::ListGet { inner, index, .. }
            | ExprNode::ListContains {
                inner,
                value: index,
                ..
            } => Self::expr_has_framed_window(inner) || Self::expr_has_framed_window(index),
            ExprNode::Window { operand, .. } => operand
                .as_ref()
                .map(|e| Self::expr_has_framed_window(e))
                .unwrap_or(false),
            ExprNode::ColumnRef { .. }
            | ExprNode::Literal { .. }
            | ExprNode::GlobalRowCount { .. } => false,
        }
    }

    fn anyvalue_sort_cmp_nulls(a: AnyValue<'_>, b: AnyValue<'_>, nulls_last: bool) -> Ordering {
        match (a.clone(), b.clone()) {
            (AnyValue::Null, AnyValue::Null) => Ordering::Equal,
            (AnyValue::Null, _) => {
                if nulls_last {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (_, AnyValue::Null) => {
                if nulls_last {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (AnyValue::Int64(x), AnyValue::Int64(y)) => x.cmp(&y),
            (AnyValue::Int32(x), AnyValue::Int32(y)) => x.cmp(&y),
            (AnyValue::UInt64(x), AnyValue::UInt64(y)) => x.cmp(&y),
            (AnyValue::UInt32(x), AnyValue::UInt32(y)) => x.cmp(&y),
            (AnyValue::Float64(x), AnyValue::Float64(y)) => {
                x.partial_cmp(&y).unwrap_or(Ordering::Equal)
            }
            (AnyValue::Float32(x), AnyValue::Float32(y)) => {
                x.partial_cmp(&y).unwrap_or(Ordering::Equal)
            }
            (AnyValue::String(x), AnyValue::String(y)) => x.cmp(y),
            (AnyValue::StringOwned(x), AnyValue::StringOwned(y)) => x.as_str().cmp(y.as_str()),
            _ => format!("{a:?}").cmp(&format!("{b:?}")),
        }
    }

    fn anyvalue_as_i64(av: AnyValue<'_>) -> Option<i64> {
        match av {
            AnyValue::Int64(v) => Some(v),
            AnyValue::Int32(v) => Some(v as i64),
            AnyValue::UInt64(v) => Some(v as i64),
            AnyValue::UInt32(v) => Some(v as i64),
            _ => None,
        }
    }

    fn anyvalue_as_f64(av: AnyValue<'_>) -> Option<f64> {
        match av {
            AnyValue::Float64(v) => Some(v),
            AnyValue::Float32(v) => Some(v as f64),
            AnyValue::Int64(v) => Some(v as f64),
            AnyValue::Int32(v) => Some(v as f64),
            AnyValue::UInt64(v) => Some(v as f64),
            AnyValue::UInt32(v) => Some(v as f64),
            _ => None,
        }
    }

    fn anyvalue_as_range_ord(av: AnyValue<'_>) -> Option<(bool, i128, f64)> {
        match av {
            AnyValue::Int64(v) => Some((false, v as i128, 0.0)),
            AnyValue::Int32(v) => Some((false, v as i128, 0.0)),
            AnyValue::UInt64(v) => Some((false, v as i128, 0.0)),
            AnyValue::UInt32(v) => Some((false, v as i128, 0.0)),
            AnyValue::Date(v) => Some((false, v as i128, 0.0)),
            AnyValue::Datetime(v, tu, _) => {
                let micros = match tu {
                    TimeUnit::Microseconds => v,
                    TimeUnit::Milliseconds => v.saturating_mul(1000),
                    TimeUnit::Nanoseconds => v / 1000,
                };
                Some((false, micros as i128, 0.0))
            }
            AnyValue::Duration(v, tu) => {
                let micros = match tu {
                    TimeUnit::Microseconds => v,
                    TimeUnit::Milliseconds => v.saturating_mul(1000),
                    TimeUnit::Nanoseconds => v / 1000,
                };
                Some((false, micros as i128, 0.0))
            }
            AnyValue::Float64(v) => Some((true, 0, v)),
            AnyValue::Float32(v) => Some((true, 0, v as f64)),
            _ => None,
        }
    }

    fn eval_framed_window_expr(df: &DataFrame, name: &str, expr: &ExprNode) -> PyResult<Series> {
        let ExprNode::Window {
            op,
            operand,
            partition_by,
            order_by,
            frame: Some(frame),
            ..
        } = expr
        else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "internal: expected framed window expression.",
            ));
        };

        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Framed windows require order_by columns.",
            ));
        }

        let n = df.height();
        let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for idx in 0..n {
            let mut key = String::new();
            for p in partition_by.iter() {
                let s = df.column(p).map_err(polars_err)?.as_materialized_series();
                let av = s.get(idx).map_err(polars_err)?;
                key.push_str(&format!("{av:?}|"));
            }
            groups.entry(key).or_default().push(idx);
        }

        let mut out_i64: Vec<Option<i64>> = vec![None; n];
        let mut out_f64: Vec<Option<f64>> = vec![None; n];

        let value_col_name = match op {
            WindowOp::Sum
            | WindowOp::Mean
            | WindowOp::Min
            | WindowOp::Max
            | WindowOp::Lag { .. }
            | WindowOp::Lead { .. } => {
                let Some(opnd) = operand.as_ref() else {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "window operation missing operand.",
                    ));
                };
                let ExprNode::ColumnRef { name, .. } = opnd.as_ref() else {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "framed window operations currently support column operands only.",
                    ));
                };
                Some(name.clone())
            }
            WindowOp::RowNumber | WindowOp::Rank | WindowOp::DenseRank => None,
        };

        for (c, _, _) in order_by.iter() {
            df.column(c).map_err(polars_err)?;
        }

        for (_pk, mut idxs) in groups {
            idxs.sort_by(|a, b| {
                for (c, asc, nulls_last) in order_by.iter() {
                    let s = df
                        .column(c)
                        .map_err(polars_err)
                        .expect("order column validated above")
                        .as_materialized_series();
                    let av_a = s
                        .get(*a)
                        .map_err(polars_err)
                        .expect("window sort row index");
                    let av_b = s
                        .get(*b)
                        .map_err(polars_err)
                        .expect("window sort row index");
                    let cmp = Self::anyvalue_sort_cmp_nulls(av_a, av_b, *nulls_last);
                    if cmp != Ordering::Equal {
                        return if *asc { cmp } else { cmp.reverse() };
                    }
                }
                a.cmp(b)
            });

            if matches!(op, WindowOp::RowNumber) {
                for (pos, idx) in idxs.iter().enumerate() {
                    out_i64[*idx] = Some((pos + 1) as i64);
                }
                continue;
            }
            if matches!(op, WindowOp::Rank) {
                let mut rank = 1_i64;
                for (pos, idx) in idxs.iter().enumerate() {
                    if pos > 0 {
                        let prev = idxs[pos - 1];
                        let mut all_equal = true;
                        for (c, _asc, nl) in order_by.iter() {
                            let s = df.column(c).map_err(polars_err)?.as_materialized_series();
                            let a = s.get(prev).map_err(polars_err)?;
                            let b = s.get(*idx).map_err(polars_err)?;
                            if Self::anyvalue_sort_cmp_nulls(a, b, *nl) != Ordering::Equal {
                                all_equal = false;
                                break;
                            }
                        }
                        if !all_equal {
                            rank = (pos + 1) as i64;
                        }
                    }
                    out_i64[*idx] = Some(rank);
                }
                continue;
            }
            if matches!(op, WindowOp::DenseRank) {
                let mut dense = 1_i64;
                for (pos, idx) in idxs.iter().enumerate() {
                    if pos > 0 {
                        let prev = idxs[pos - 1];
                        let mut all_equal = true;
                        for (c, _asc, nl) in order_by.iter() {
                            let s = df.column(c).map_err(polars_err)?.as_materialized_series();
                            let a = s.get(prev).map_err(polars_err)?;
                            let b = s.get(*idx).map_err(polars_err)?;
                            if Self::anyvalue_sort_cmp_nulls(a, b, *nl) != Ordering::Equal {
                                all_equal = false;
                                break;
                            }
                        }
                        if !all_equal {
                            dense += 1;
                        }
                    }
                    out_i64[*idx] = Some(dense);
                }
                continue;
            }

            let val_col = df
                .column(value_col_name.as_deref().unwrap_or(""))
                .map_err(polars_err)?
                .as_materialized_series()
                .clone();
            // RANGE frame: sort uses all order_by keys (lexicographic); start/end offsets
            // compare only the first order column (same rule as PostgreSQL multi-column RANGE).
            let ord_series = df
                .column(order_by[0].0.as_str())
                .map_err(polars_err)?
                .as_materialized_series()
                .clone();

            for (pos, idx) in idxs.iter().enumerate() {
                let mut frame_members: Vec<usize> = Vec::new();
                match frame {
                    WindowFrame::Rows { start, end } => {
                        let lo = (pos as i64 + *start).max(0) as usize;
                        let hi = (pos as i64 + *end).min((idxs.len() as i64) - 1) as usize;
                        if lo <= hi {
                            frame_members.extend(idxs.iter().take(hi + 1).skip(lo).copied());
                        }
                    }
                    WindowFrame::Range { start, end } => {
                        let cur = ord_series.get(*idx).map_err(polars_err)?;
                        let Some((is_float, cur_i, cur_f)) = Self::anyvalue_as_range_ord(cur)
                        else {
                            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                "rangeBetween supports numeric/date/datetime/duration order columns.",
                            ));
                        };
                        for cand_idx in idxs.iter() {
                            let ord = ord_series.get(*cand_idx).map_err(polars_err)?;
                            if let Some((cand_is_float, cand_i, cand_f)) =
                                Self::anyvalue_as_range_ord(ord)
                            {
                                let in_frame = if is_float && cand_is_float {
                                    let delta = cand_f - cur_f;
                                    let lo = *start as f64;
                                    let hi = *end as f64;
                                    delta >= lo && delta <= hi
                                } else if !is_float && !cand_is_float {
                                    let delta = cand_i - cur_i;
                                    delta >= *start as i128 && delta <= *end as i128
                                } else {
                                    false
                                };
                                if in_frame {
                                    frame_members.push(*cand_idx);
                                }
                            }
                        }
                    }
                }

                match op {
                    WindowOp::Sum => {
                        let mut acc: i64 = 0;
                        let mut saw = false;
                        for cand_idx in frame_members.iter() {
                            let av = val_col.get(*cand_idx).map_err(polars_err)?;
                            if let Some(v) = Self::anyvalue_as_i64(av) {
                                acc += v;
                                saw = true;
                            }
                        }
                        out_i64[*idx] = if saw { Some(acc) } else { None };
                    }
                    WindowOp::Mean => {
                        let mut acc: f64 = 0.0;
                        let mut cnt: usize = 0;
                        for cand_idx in frame_members.iter() {
                            let av = val_col.get(*cand_idx).map_err(polars_err)?;
                            if let Some(v) = Self::anyvalue_as_f64(av) {
                                acc += v;
                                cnt += 1;
                            }
                        }
                        out_f64[*idx] = if cnt > 0 {
                            Some(acc / (cnt as f64))
                        } else {
                            None
                        };
                    }
                    WindowOp::Min => {
                        let mut best: Option<i64> = None;
                        for cand_idx in frame_members.iter() {
                            let av = val_col.get(*cand_idx).map_err(polars_err)?;
                            if let Some(v) = Self::anyvalue_as_i64(av) {
                                best = Some(best.map_or(v, |b| b.min(v)));
                            }
                        }
                        out_i64[*idx] = best;
                    }
                    WindowOp::Max => {
                        let mut best: Option<i64> = None;
                        for cand_idx in frame_members.iter() {
                            let av = val_col.get(*cand_idx).map_err(polars_err)?;
                            if let Some(v) = Self::anyvalue_as_i64(av) {
                                best = Some(best.map_or(v, |b| b.max(v)));
                            }
                        }
                        out_i64[*idx] = best;
                    }
                    WindowOp::Lag { n } => {
                        let target = pos as i64 - (*n as i64);
                        if target >= 0 {
                            let av = val_col.get(idxs[target as usize]).map_err(polars_err)?;
                            out_i64[*idx] = Self::anyvalue_as_i64(av);
                        } else {
                            out_i64[*idx] = None;
                        }
                    }
                    WindowOp::Lead { n } => {
                        let target = pos + (*n as usize);
                        if target < idxs.len() {
                            let av = val_col.get(idxs[target]).map_err(polars_err)?;
                            out_i64[*idx] = Self::anyvalue_as_i64(av);
                        } else {
                            out_i64[*idx] = None;
                        }
                    }
                    WindowOp::RowNumber | WindowOp::Rank | WindowOp::DenseRank => {}
                }
            }
        }

        match op {
            WindowOp::Mean => Ok(Series::new(name.into(), out_f64)),
            _ => Ok(Series::new(name.into(), out_i64)),
        }
    }

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
            PlanStep::GlobalSelect { items } => {
                let mut exprs = Vec::with_capacity(items.len());
                for (name, expr) in items.iter() {
                    let pe = expr.to_polars_expr()?.alias(name.as_str());
                    exprs.push(pe);
                }
                lf = lf.select(exprs);
            }
            PlanStep::WithColumns { columns } => {
                let has_framed = columns.values().any(Self::expr_has_framed_window);
                if has_framed {
                    let mut df = lf.collect().map_err(polars_err)?;
                    let mut regular_exprs = Vec::new();
                    for (name, expr) in columns.iter() {
                        if Self::expr_has_framed_window(expr) {
                            continue;
                        }
                        regular_exprs.push(expr.to_polars_expr()?.alias(name));
                    }
                    if !regular_exprs.is_empty() {
                        df = df
                            .lazy()
                            .with_columns(regular_exprs)
                            .collect()
                            .map_err(polars_err)?;
                    }
                    for (name, expr) in columns.iter() {
                        if !Self::expr_has_framed_window(expr) {
                            continue;
                        }
                        let s = Self::eval_framed_window_expr(&df, name, expr)?;
                        df.with_column(s.into_column()).map_err(polars_err)?;
                    }
                    lf = df.lazy();
                } else {
                    let mut exprs = Vec::with_capacity(columns.len());
                    for (name, expr) in columns.iter() {
                        let pe = expr.to_polars_expr()?.alias(name);
                        exprs.push(pe);
                    }
                    lf = lf.with_columns(exprs);
                }
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
                                LiteralValue::EnumStr(s) => base.fill_null(lit(s.clone())),
                                LiteralValue::Uuid(s) => base.fill_null(lit(s.clone())),
                                LiteralValue::Decimal(v) => base.fill_null(
                                    Scalar::new_decimal(*v, DECIMAL_PRECISION, DECIMAL_SCALE).lit(),
                                ),
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
                                LiteralValue::TimeNanos(ns) => {
                                    base.fill_null(lit(*ns).cast(DataType::Time))
                                }
                                LiteralValue::Binary(b) => {
                                    base.fill_null(lit(b.as_slice()).cast(DataType::Binary))
                                }
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
