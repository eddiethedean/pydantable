#![cfg_attr(not(feature = "polars_engine"), allow(unused_variables))]

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::dtype::{py_annotation_to_dtype, DTypeDesc};
use crate::expr::{
    op_symbol_to_arith, op_symbol_to_cmp, ArithOp, CmpOp, ExprHandle, ExprNode, LogicalOp,
    RowAccumOp, StringPredicateKind, StringUnaryOp, TemporalPart, UnaryNumericOp,
    UnixTimestampUnit,
};

use super::types::PyExpr;

#[pyfunction]
fn rust_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pyfunction]
fn make_column_ref(
    py: Python<'_>,
    name: String,
    dtype_annotation: &Bound<'_, PyAny>,
) -> PyResult<PyExpr> {
    let dtype: DTypeDesc = py_annotation_to_dtype(py, dtype_annotation)?;
    if dtype.is_scalar_unknown_nullable() {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "ColumnRef dtype cannot have unknown scalar base.",
        ));
    }
    Ok(PyExpr {
        node: ExprNode::make_column_ref(name, dtype)?,
    })
}

#[pyfunction]
fn make_literal(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<PyExpr> {
    let lit = ExprHandle::from_py_literal(py, value)?;
    Ok(PyExpr { node: lit.node })
}

#[pyfunction]
fn binary_op(op_symbol: String, left: &PyExpr, right: &PyExpr) -> PyResult<PyExpr> {
    let op: ArithOp = op_symbol_to_arith(&op_symbol)?;
    let node = ExprNode::make_binary_op(op, left.node.clone(), right.node.clone())?;
    Ok(PyExpr { node })
}

#[pyfunction]
fn compare_op(op_symbol: String, left: &PyExpr, right: &PyExpr) -> PyResult<PyExpr> {
    let op: CmpOp = op_symbol_to_cmp(&op_symbol)?;
    let node = ExprNode::make_compare_op(op, left.node.clone(), right.node.clone())?;
    Ok(PyExpr { node })
}

#[pyfunction]
fn cast_expr(
    py: Python<'_>,
    expr: &PyExpr,
    dtype_annotation: &Bound<'_, PyAny>,
) -> PyResult<PyExpr> {
    let target = py_annotation_to_dtype(py, dtype_annotation)?;
    let node = ExprNode::make_cast(expr.node.clone(), target)?;
    Ok(PyExpr { node })
}

#[pyfunction]
fn is_null_expr(expr: &PyExpr) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_is_null(expr.node.clone())?,
    })
}

#[pyfunction]
fn is_not_null_expr(expr: &PyExpr) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_is_not_null(expr.node.clone())?,
    })
}

#[pyfunction]
fn coalesce_exprs(exprs: Vec<Bound<'_, PyExpr>>) -> PyResult<PyExpr> {
    let nodes: Vec<ExprNode> = exprs.iter().map(|e| e.borrow().node.clone()).collect();
    Ok(PyExpr {
        node: ExprNode::make_coalesce(nodes)?,
    })
}

#[pyfunction]
fn expr_case_when(
    conditions: Vec<Bound<'_, PyExpr>>,
    thens: Vec<Bound<'_, PyExpr>>,
    else_expr: Bound<'_, PyExpr>,
) -> PyResult<PyExpr> {
    if conditions.len() != thens.len() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "case_when conditions/thens length mismatch",
        ));
    }
    let mut branches: Vec<(ExprNode, ExprNode)> = Vec::new();
    for (c, t) in conditions.iter().zip(thens.iter()) {
        branches.push((c.borrow().node.clone(), t.borrow().node.clone()));
    }
    Ok(PyExpr {
        node: ExprNode::make_case_when(branches, else_expr.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_in_list(
    py: Python<'_>,
    inner: Bound<'_, PyExpr>,
    values: Vec<Bound<'_, PyAny>>,
) -> PyResult<PyExpr> {
    let mut lits = Vec::new();
    for v in values {
        let lit = ExprHandle::from_py_literal(py, &v)?;
        match lit.node {
            ExprNode::Literal {
                value: Some(lv), ..
            } => lits.push(lv),
            ExprNode::Literal { value: None, .. } => {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "isin() value cannot be null.",
                ));
            }
            _ => {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "isin() values must be literals.",
                ));
            }
        }
    }
    Ok(PyExpr {
        node: ExprNode::make_in_list(inner.borrow().node.clone(), lits)?,
    })
}

#[pyfunction]
fn expr_between(
    inner: Bound<'_, PyExpr>,
    low: Bound<'_, PyExpr>,
    high: Bound<'_, PyExpr>,
) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_between(
            inner.borrow().node.clone(),
            low.borrow().node.clone(),
            high.borrow().node.clone(),
        )?,
    })
}

#[pyfunction]
fn expr_string_concat(exprs: Vec<Bound<'_, PyExpr>>) -> PyResult<PyExpr> {
    let nodes: Vec<ExprNode> = exprs.iter().map(|e| e.borrow().node.clone()).collect();
    Ok(PyExpr {
        node: ExprNode::make_string_concat(nodes)?,
    })
}

#[pyfunction]
fn expr_substring(
    inner: Bound<'_, PyExpr>,
    start: Bound<'_, PyExpr>,
    length: Option<Bound<'_, PyExpr>>,
) -> PyResult<PyExpr> {
    let len = length.map(|l| l.borrow().node.clone());
    Ok(PyExpr {
        node: ExprNode::make_substring(
            inner.borrow().node.clone(),
            start.borrow().node.clone(),
            len,
        )?,
    })
}

#[pyfunction]
fn expr_string_length(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_string_length(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
#[pyo3(signature = (inner, pattern, replacement, *, literal = true))]
fn expr_string_replace(
    inner: Bound<'_, PyExpr>,
    pattern: String,
    replacement: String,
    literal: bool,
) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_string_replace(
            inner.borrow().node.clone(),
            pattern,
            replacement,
            literal,
        )?,
    })
}

#[pyfunction]
#[pyo3(signature = (inner, op, pattern, *, literal = true))]
fn expr_string_predicate(
    inner: Bound<'_, PyExpr>,
    op: String,
    pattern: String,
    literal: bool,
) -> PyResult<PyExpr> {
    let kind = match op.as_str() {
        "starts_with" => StringPredicateKind::StartsWith,
        "ends_with" => StringPredicateKind::EndsWith,
        "contains" => StringPredicateKind::Contains { literal },
        other => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "string_predicate op must be starts_with|ends_with|contains, got {other:?}",
            )));
        }
    };
    Ok(PyExpr {
        node: ExprNode::make_string_predicate(inner.borrow().node.clone(), kind, pattern)?,
    })
}

#[pyfunction]
fn expr_struct_field(inner: Bound<'_, PyExpr>, field: String) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_struct_field(inner.borrow().node.clone(), field)?,
    })
}

#[pyfunction]
fn expr_abs(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_unary_numeric(inner.borrow().node.clone(), UnaryNumericOp::Abs)?,
    })
}

#[pyfunction]
fn expr_round(inner: Bound<'_, PyExpr>, decimals: u32) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_unary_numeric(
            inner.borrow().node.clone(),
            UnaryNumericOp::Round { decimals },
        )?,
    })
}

#[pyfunction]
fn expr_floor(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_unary_numeric(inner.borrow().node.clone(), UnaryNumericOp::Floor)?,
    })
}

#[pyfunction]
fn expr_ceil(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_unary_numeric(inner.borrow().node.clone(), UnaryNumericOp::Ceil)?,
    })
}

#[pyfunction]
#[pyo3(signature = (inner, op, arg=None))]
fn expr_string_unary(
    inner: Bound<'_, PyExpr>,
    op: String,
    arg: Option<String>,
) -> PyResult<PyExpr> {
    let uop = match (op.as_str(), arg.as_deref()) {
        ("strip", None) => StringUnaryOp::Strip,
        ("upper", None) => StringUnaryOp::Upper,
        ("lower", None) => StringUnaryOp::Lower,
        ("reverse", None) => StringUnaryOp::Reverse,
        ("strip_prefix", Some(s)) => StringUnaryOp::StripPrefix(s.to_string()),
        ("strip_suffix", Some(s)) => StringUnaryOp::StripSuffix(s.to_string()),
        ("strip_chars", Some(s)) => StringUnaryOp::StripChars(s.to_string()),
        _ => {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "string_unary: use op 'strip'|'upper'|'lower'|'reverse' (no arg), or \
                 'strip_prefix'|'strip_suffix'|'strip_chars' with a string arg.",
            ));
        }
    };
    Ok(PyExpr {
        node: ExprNode::make_string_unary(inner.borrow().node.clone(), uop)?,
    })
}

#[pyfunction]
fn expr_logical_and(left: Bound<'_, PyExpr>, right: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_logical_binary(
            LogicalOp::And,
            left.borrow().node.clone(),
            right.borrow().node.clone(),
        )?,
    })
}

#[pyfunction]
fn expr_logical_or(left: Bound<'_, PyExpr>, right: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_logical_binary(
            LogicalOp::Or,
            left.borrow().node.clone(),
            right.borrow().node.clone(),
        )?,
    })
}

#[pyfunction]
fn expr_logical_not(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_logical_not(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_temporal_part(inner: Bound<'_, PyExpr>, part: String) -> PyResult<PyExpr> {
    let p = match part.as_str() {
        "year" => TemporalPart::Year,
        "month" => TemporalPart::Month,
        "day" => TemporalPart::Day,
        "hour" => TemporalPart::Hour,
        "minute" => TemporalPart::Minute,
        "second" => TemporalPart::Second,
        "nanosecond" => TemporalPart::Nanosecond,
        "weekday" => TemporalPart::Weekday,
        "quarter" => TemporalPart::Quarter,
        "week" => TemporalPart::Week,
        _ => {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "temporal part must be year|month|day|hour|minute|second|nanosecond|weekday|quarter|week",
            ));
        }
    };
    Ok(PyExpr {
        node: ExprNode::make_temporal_part(inner.borrow().node.clone(), p)?,
    })
}

#[pyfunction]
fn expr_list_len(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_list_len(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_list_get(inner: Bound<'_, PyExpr>, index: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_list_get(inner.borrow().node.clone(), index.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_list_contains(inner: Bound<'_, PyExpr>, value: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_list_contains(
            inner.borrow().node.clone(),
            value.borrow().node.clone(),
        )?,
    })
}

#[pyfunction]
fn expr_list_min(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_list_min(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_list_max(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_list_max(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_list_sum(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_list_sum(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_list_mean(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_list_mean(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
#[pyo3(signature = (inner, separator, *, ignore_nulls = false))]
fn expr_list_join(
    inner: Bound<'_, PyExpr>,
    separator: String,
    ignore_nulls: bool,
) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_list_join(inner.borrow().node.clone(), separator, ignore_nulls)?,
    })
}

#[pyfunction]
#[pyo3(signature = (inner, *, descending = false, nulls_last = false, maintain_order = false))]
fn expr_list_sort(
    inner: Bound<'_, PyExpr>,
    descending: bool,
    nulls_last: bool,
    maintain_order: bool,
) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_list_sort(
            inner.borrow().node.clone(),
            descending,
            nulls_last,
            maintain_order,
        )?,
    })
}

#[pyfunction]
#[pyo3(signature = (inner, *, stable = false))]
fn expr_list_unique(inner: Bound<'_, PyExpr>, stable: bool) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_list_unique(inner.borrow().node.clone(), stable)?,
    })
}

#[pyfunction]
fn expr_str_reverse(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_str_reverse(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_str_pad_start(
    inner: Bound<'_, PyExpr>,
    length: u32,
    fill_char: String,
) -> PyResult<PyExpr> {
    let mut it = fill_char.chars();
    let ch = it
        .next()
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("fill_char must not be empty."))?;
    if it.next().is_some() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "fill_char must be a single Unicode character.",
        ));
    }
    Ok(PyExpr {
        node: ExprNode::make_str_pad_start(inner.borrow().node.clone(), length, ch)?,
    })
}

#[pyfunction]
fn expr_str_pad_end(inner: Bound<'_, PyExpr>, length: u32, fill_char: String) -> PyResult<PyExpr> {
    let mut it = fill_char.chars();
    let ch = it
        .next()
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("fill_char must not be empty."))?;
    if it.next().is_some() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "fill_char must be a single Unicode character.",
        ));
    }
    Ok(PyExpr {
        node: ExprNode::make_str_pad_end(inner.borrow().node.clone(), length, ch)?,
    })
}

#[pyfunction]
fn expr_str_zfill(inner: Bound<'_, PyExpr>, length: u32) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_str_zfill(inner.borrow().node.clone(), length)?,
    })
}

#[pyfunction]
fn expr_str_extract_regex(
    inner: Bound<'_, PyExpr>,
    pattern: String,
    group_index: usize,
) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_string_extract(inner.borrow().node.clone(), pattern, group_index)?,
    })
}

#[pyfunction]
fn expr_str_json_path_match(inner: Bound<'_, PyExpr>, path: String) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_string_json_path_match(inner.borrow().node.clone(), path)?,
    })
}

#[pyfunction]
fn expr_string_split(inner: Bound<'_, PyExpr>, delimiter: String) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_string_split(inner.borrow().node.clone(), delimiter)?,
    })
}

#[pyfunction]
fn expr_datetime_to_date(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_datetime_to_date(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_window_row_number(
    partition_by: Vec<String>,
    order_by: Vec<(String, bool, bool)>,
    frame_kind: Option<String>,
    frame_start: Option<i64>,
    frame_end: Option<i64>,
) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_window_row_number(
            partition_by,
            order_by,
            frame_kind,
            frame_start,
            frame_end,
        )?,
    })
}

#[pyfunction]
fn expr_window_rank(
    dense: bool,
    partition_by: Vec<String>,
    order_by: Vec<(String, bool, bool)>,
    frame_kind: Option<String>,
    frame_start: Option<i64>,
    frame_end: Option<i64>,
) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_window_rank(
            dense,
            partition_by,
            order_by,
            frame_kind,
            frame_start,
            frame_end,
        )?,
    })
}

#[pyfunction]
fn expr_window_sum(
    inner: Bound<'_, PyExpr>,
    partition_by: Vec<String>,
    order_by: Vec<(String, bool, bool)>,
    frame_kind: Option<String>,
    frame_start: Option<i64>,
    frame_end: Option<i64>,
) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_window_sum(
            inner.borrow().node.clone(),
            partition_by,
            order_by,
            frame_kind,
            frame_start,
            frame_end,
        )?,
    })
}

#[pyfunction]
fn expr_window_mean(
    inner: Bound<'_, PyExpr>,
    partition_by: Vec<String>,
    order_by: Vec<(String, bool, bool)>,
    frame_kind: Option<String>,
    frame_start: Option<i64>,
    frame_end: Option<i64>,
) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_window_mean(
            inner.borrow().node.clone(),
            partition_by,
            order_by,
            frame_kind,
            frame_start,
            frame_end,
        )?,
    })
}

#[pyfunction]
fn expr_window_min(
    inner: Bound<'_, PyExpr>,
    partition_by: Vec<String>,
    order_by: Vec<(String, bool, bool)>,
    frame_kind: Option<String>,
    frame_start: Option<i64>,
    frame_end: Option<i64>,
) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_window_min(
            inner.borrow().node.clone(),
            partition_by,
            order_by,
            frame_kind,
            frame_start,
            frame_end,
        )?,
    })
}

#[pyfunction]
fn expr_window_max(
    inner: Bound<'_, PyExpr>,
    partition_by: Vec<String>,
    order_by: Vec<(String, bool, bool)>,
    frame_kind: Option<String>,
    frame_start: Option<i64>,
    frame_end: Option<i64>,
) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_window_max(
            inner.borrow().node.clone(),
            partition_by,
            order_by,
            frame_kind,
            frame_start,
            frame_end,
        )?,
    })
}

#[pyfunction]
fn expr_global_sum(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_global_sum(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_global_mean(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_global_mean(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_strptime(inner: Bound<'_, PyExpr>, format: String, to_datetime: bool) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_strptime(inner.borrow().node.clone(), format, to_datetime)?,
    })
}

#[pyfunction]
fn expr_unix_timestamp(inner: Bound<'_, PyExpr>, unit: String) -> PyResult<PyExpr> {
    let u = match unit.as_str() {
        "seconds" | "s" => UnixTimestampUnit::Seconds,
        "milliseconds" | "ms" => UnixTimestampUnit::Milliseconds,
        _ => {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "unix_timestamp unit must be 'seconds' or 'milliseconds'.",
            ));
        }
    };
    Ok(PyExpr {
        node: ExprNode::make_unix_timestamp(inner.borrow().node.clone(), u)?,
    })
}

#[pyfunction]
fn expr_binary_length(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_binary_length(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_map_len(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_map_len(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_map_get(inner: Bound<'_, PyExpr>, key: String) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_map_get(inner.borrow().node.clone(), key)?,
    })
}

#[pyfunction]
fn expr_map_contains_key(inner: Bound<'_, PyExpr>, key: String) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_map_contains_key(inner.borrow().node.clone(), key)?,
    })
}

#[pyfunction]
fn expr_map_keys(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_map_keys(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_map_values(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_map_values(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_map_entries(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_map_entries(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_map_from_entries(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_map_from_entries(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_global_count(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_global_count(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_global_min(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_global_min(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_global_max(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_global_max(inner.borrow().node.clone())?,
    })
}

#[pyfunction]
fn expr_row_accum_cum_sum(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_row_accum(inner.borrow().node.clone(), RowAccumOp::CumSum)?,
    })
}

#[pyfunction]
fn expr_row_accum_cum_prod(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_row_accum(inner.borrow().node.clone(), RowAccumOp::CumProd)?,
    })
}

#[pyfunction]
fn expr_row_accum_cum_min(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_row_accum(inner.borrow().node.clone(), RowAccumOp::CumMin)?,
    })
}

#[pyfunction]
fn expr_row_accum_cum_max(inner: Bound<'_, PyExpr>) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_row_accum(inner.borrow().node.clone(), RowAccumOp::CumMax)?,
    })
}

#[pyfunction]
fn expr_row_accum_diff(inner: Bound<'_, PyExpr>, periods: i64) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_row_accum(inner.borrow().node.clone(), RowAccumOp::Diff { periods })?,
    })
}

#[pyfunction]
fn expr_row_accum_pct_change(inner: Bound<'_, PyExpr>, periods: i64) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_row_accum(
            inner.borrow().node.clone(),
            RowAccumOp::PctChange { periods },
        )?,
    })
}

#[pyfunction]
fn expr_window_lag(
    inner: Bound<'_, PyExpr>,
    n: u32,
    partition_by: Vec<String>,
    order_by: Vec<(String, bool, bool)>,
    frame_kind: Option<String>,
    frame_start: Option<i64>,
    frame_end: Option<i64>,
) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_window_lag(
            inner.borrow().node.clone(),
            n,
            partition_by,
            order_by,
            frame_kind,
            frame_start,
            frame_end,
        )?,
    })
}

#[pyfunction]
fn expr_window_lead(
    inner: Bound<'_, PyExpr>,
    n: u32,
    partition_by: Vec<String>,
    order_by: Vec<(String, bool, bool)>,
    frame_kind: Option<String>,
    frame_start: Option<i64>,
    frame_end: Option<i64>,
) -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_window_lead(
            inner.borrow().node.clone(),
            n,
            partition_by,
            order_by,
            frame_kind,
            frame_start,
            frame_end,
        )?,
    })
}

#[pyfunction]
fn expr_global_default_alias(expr: &Bound<'_, PyExpr>) -> Option<String> {
    expr.borrow().node.global_agg_default_alias()
}

#[pyfunction]
fn expr_is_global_agg(expr: &Bound<'_, PyExpr>) -> bool {
    matches!(
        expr.borrow().node,
        ExprNode::GlobalAgg { .. } | ExprNode::GlobalRowCount { .. }
    )
}

#[pyfunction]
fn expr_global_row_count() -> PyResult<PyExpr> {
    Ok(PyExpr {
        node: ExprNode::make_global_row_count(),
    })
}

pub(super) fn register_functions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rust_version, m)?)?;
    m.add_function(wrap_pyfunction!(make_column_ref, m)?)?;
    m.add_function(wrap_pyfunction!(make_literal, m)?)?;
    m.add_function(wrap_pyfunction!(binary_op, m)?)?;
    m.add_function(wrap_pyfunction!(compare_op, m)?)?;
    m.add_function(wrap_pyfunction!(cast_expr, m)?)?;
    m.add_function(wrap_pyfunction!(is_null_expr, m)?)?;
    m.add_function(wrap_pyfunction!(is_not_null_expr, m)?)?;
    m.add_function(wrap_pyfunction!(coalesce_exprs, m)?)?;
    m.add_function(wrap_pyfunction!(expr_case_when, m)?)?;
    m.add_function(wrap_pyfunction!(expr_in_list, m)?)?;
    m.add_function(wrap_pyfunction!(expr_between, m)?)?;
    m.add_function(wrap_pyfunction!(expr_string_concat, m)?)?;
    m.add_function(wrap_pyfunction!(expr_substring, m)?)?;
    m.add_function(wrap_pyfunction!(expr_string_length, m)?)?;
    m.add_function(wrap_pyfunction!(expr_string_replace, m)?)?;
    m.add_function(wrap_pyfunction!(expr_string_predicate, m)?)?;
    m.add_function(wrap_pyfunction!(expr_struct_field, m)?)?;
    m.add_function(wrap_pyfunction!(expr_abs, m)?)?;
    m.add_function(wrap_pyfunction!(expr_round, m)?)?;
    m.add_function(wrap_pyfunction!(expr_floor, m)?)?;
    m.add_function(wrap_pyfunction!(expr_ceil, m)?)?;
    m.add_function(wrap_pyfunction!(expr_string_unary, m)?)?;
    m.add_function(wrap_pyfunction!(expr_logical_and, m)?)?;
    m.add_function(wrap_pyfunction!(expr_logical_or, m)?)?;
    m.add_function(wrap_pyfunction!(expr_logical_not, m)?)?;
    m.add_function(wrap_pyfunction!(expr_temporal_part, m)?)?;
    m.add_function(wrap_pyfunction!(expr_list_len, m)?)?;
    m.add_function(wrap_pyfunction!(expr_list_get, m)?)?;
    m.add_function(wrap_pyfunction!(expr_list_contains, m)?)?;
    m.add_function(wrap_pyfunction!(expr_list_min, m)?)?;
    m.add_function(wrap_pyfunction!(expr_list_max, m)?)?;
    m.add_function(wrap_pyfunction!(expr_list_sum, m)?)?;
    m.add_function(wrap_pyfunction!(expr_list_mean, m)?)?;
    m.add_function(wrap_pyfunction!(expr_list_join, m)?)?;
    m.add_function(wrap_pyfunction!(expr_list_sort, m)?)?;
    m.add_function(wrap_pyfunction!(expr_list_unique, m)?)?;
    m.add_function(wrap_pyfunction!(expr_str_reverse, m)?)?;
    m.add_function(wrap_pyfunction!(expr_str_pad_start, m)?)?;
    m.add_function(wrap_pyfunction!(expr_str_pad_end, m)?)?;
    m.add_function(wrap_pyfunction!(expr_str_zfill, m)?)?;
    m.add_function(wrap_pyfunction!(expr_str_extract_regex, m)?)?;
    m.add_function(wrap_pyfunction!(expr_str_json_path_match, m)?)?;
    m.add_function(wrap_pyfunction!(expr_string_split, m)?)?;
    m.add_function(wrap_pyfunction!(expr_datetime_to_date, m)?)?;
    m.add_function(wrap_pyfunction!(expr_window_row_number, m)?)?;
    m.add_function(wrap_pyfunction!(expr_window_rank, m)?)?;
    m.add_function(wrap_pyfunction!(expr_window_sum, m)?)?;
    m.add_function(wrap_pyfunction!(expr_window_mean, m)?)?;
    m.add_function(wrap_pyfunction!(expr_window_min, m)?)?;
    m.add_function(wrap_pyfunction!(expr_window_max, m)?)?;
    m.add_function(wrap_pyfunction!(expr_global_sum, m)?)?;
    m.add_function(wrap_pyfunction!(expr_global_mean, m)?)?;
    m.add_function(wrap_pyfunction!(expr_strptime, m)?)?;
    m.add_function(wrap_pyfunction!(expr_unix_timestamp, m)?)?;
    m.add_function(wrap_pyfunction!(expr_binary_length, m)?)?;
    m.add_function(wrap_pyfunction!(expr_map_len, m)?)?;
    m.add_function(wrap_pyfunction!(expr_map_get, m)?)?;
    m.add_function(wrap_pyfunction!(expr_map_contains_key, m)?)?;
    m.add_function(wrap_pyfunction!(expr_map_keys, m)?)?;
    m.add_function(wrap_pyfunction!(expr_map_values, m)?)?;
    m.add_function(wrap_pyfunction!(expr_map_entries, m)?)?;
    m.add_function(wrap_pyfunction!(expr_map_from_entries, m)?)?;
    m.add_function(wrap_pyfunction!(expr_global_count, m)?)?;
    m.add_function(wrap_pyfunction!(expr_global_min, m)?)?;
    m.add_function(wrap_pyfunction!(expr_global_max, m)?)?;
    m.add_function(wrap_pyfunction!(expr_row_accum_cum_sum, m)?)?;
    m.add_function(wrap_pyfunction!(expr_row_accum_cum_prod, m)?)?;
    m.add_function(wrap_pyfunction!(expr_row_accum_cum_min, m)?)?;
    m.add_function(wrap_pyfunction!(expr_row_accum_cum_max, m)?)?;
    m.add_function(wrap_pyfunction!(expr_row_accum_diff, m)?)?;
    m.add_function(wrap_pyfunction!(expr_row_accum_pct_change, m)?)?;
    m.add_function(wrap_pyfunction!(expr_global_row_count, m)?)?;
    m.add_function(wrap_pyfunction!(expr_window_lag, m)?)?;
    m.add_function(wrap_pyfunction!(expr_window_lead, m)?)?;
    m.add_function(wrap_pyfunction!(expr_global_default_alias, m)?)?;
    m.add_function(wrap_pyfunction!(expr_is_global_agg, m)?)?;
    Ok(())
}
