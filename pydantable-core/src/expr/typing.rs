//! Typing rules, `ExprNode::make_*`, [`ExprNode::eval`], and [`ExprNode::referenced_columns`].

#[cfg(not(feature = "polars_engine"))]
use std::collections::HashMap;
use std::collections::HashSet;

use pyo3::prelude::*;

use crate::dtype::{dtype_structural_eq, BaseType, DTypeDesc};

use super::ir::{
    ArithOp, CmpOp, ExprNode, GlobalAggOp, LiteralValue, LogicalOp, StringUnaryOp, TemporalPart,
    UnaryNumericOp, UnixTimestampUnit, WindowFrame, WindowOp,
};

enum ListAggKind {
    Min,
    Max,
    Sum,
}

fn dtype_is_string_like(dtype: &DTypeDesc) -> bool {
    matches!(
        dtype.as_scalar_base_field().flatten(),
        Some(BaseType::Str | BaseType::Enum)
    )
}

#[cfg(not(feature = "polars_engine"))]
fn trim_matches_char_set(s: &str, pat: &str) -> String {
    s.trim_matches(|c| pat.chars().any(|m| m == c)).to_string()
}

#[cfg(not(feature = "polars_engine"))]
fn wire_str<'a>(v: &'a LiteralValue) -> Option<&'a str> {
    match v {
        LiteralValue::Str(s) | LiteralValue::EnumStr(s) => Some(s.as_str()),
        _ => None,
    }
}

/// Proleptic Gregorian calendar from Python / Polars `Date` days since 1970-01-01.
#[cfg(not(feature = "polars_engine"))]
fn ordinal_to_ymd(ordinal: i32) -> (i32, u32, u32) {
    let a = ordinal + 32044;
    let b = (4 * a + 3) / 146_097;
    let c = a - (146_097 * b) / 4;
    let d = (4 * c + 3) / 1461;
    let e = c - (1461 * d) / 4;
    let m = (5 * e + 2) / 153;
    let day = e - (153 * m + 2) / 5 + 1;
    let month = m + 3 - 12 * (m / 10);
    let year = b * 100 + d - 4800 + m / 10;
    (year, month as u32, day as u32)
}

#[cfg(not(feature = "polars_engine"))]
fn utc_calendar_from_epoch_days(days: i32) -> (i32, u32, u32) {
    ordinal_to_ymd(days + 719_163)
}

/// UTC wall time from Unix epoch microseconds (matches naive `datetime.fromtimestamp` semantics).
#[cfg(not(feature = "polars_engine"))]
fn utc_ymdhms_from_unix_micros(us: i64) -> (i32, u32, u32, u32, u32, u32) {
    let secs = us.div_euclid(1_000_000);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, mo, d) = utc_calendar_from_epoch_days(days as i32);
    let h = (sod / 3600) as u32;
    let mi = ((sod % 3600) / 60) as u32;
    let s = (sod % 60) as u32;
    (y, mo, d, h, mi, s)
}

#[cfg(not(feature = "polars_engine"))]
fn literal_between_inclusive(x: &LiteralValue, lo: &LiteralValue, hi: &LiteralValue) -> bool {
    if let (Some(xw), Some(lw), Some(hw)) = (wire_str(x), wire_str(lo), wire_str(hi)) {
        return xw >= lw && xw <= hw;
    }
    match (x, lo, hi) {
        (LiteralValue::Int(a), LiteralValue::Int(b), LiteralValue::Int(c)) => *a >= *b && *a <= *c,
        (LiteralValue::Float(a), LiteralValue::Float(b), LiteralValue::Float(c)) => {
            *a >= *b && *a <= *c
        }
        (LiteralValue::Int(a), LiteralValue::Int(b), LiteralValue::Float(c)) => {
            *a as f64 >= *b as f64 && *a as f64 <= *c
        }
        (LiteralValue::Int(a), LiteralValue::Float(b), LiteralValue::Float(c)) => {
            *a as f64 >= *b && *a as f64 <= *c
        }
        (LiteralValue::Float(a), LiteralValue::Int(b), LiteralValue::Int(c)) => {
            *a >= *b as f64 && *a <= *c as f64
        }
        (LiteralValue::Str(a), LiteralValue::Str(b), LiteralValue::Str(c)) => a >= b && a <= c,
        (LiteralValue::Uuid(a), LiteralValue::Uuid(b), LiteralValue::Uuid(c)) => a >= b && a <= c,
        (LiteralValue::Decimal(a), LiteralValue::Decimal(b), LiteralValue::Decimal(c)) => {
            a >= b && a <= c
        }
        _ => false,
    }
}

impl ExprNode {
    pub fn dtype(&self) -> DTypeDesc {
        match self {
            ExprNode::ColumnRef { dtype, .. } => dtype.clone(),
            ExprNode::Literal { dtype, .. } => dtype.clone(),
            ExprNode::BinaryOp { dtype, .. } => dtype.clone(),
            ExprNode::CompareOp { dtype, .. } => dtype.clone(),
            ExprNode::Cast { dtype, .. } => dtype.clone(),
            ExprNode::IsNull { dtype, .. } => dtype.clone(),
            ExprNode::IsNotNull { dtype, .. } => dtype.clone(),
            ExprNode::Coalesce { dtype, .. }
            | ExprNode::CaseWhen { dtype, .. }
            | ExprNode::InList { dtype, .. }
            | ExprNode::Between { dtype, .. }
            | ExprNode::StringConcat { dtype, .. }
            | ExprNode::Substring { dtype, .. }
            | ExprNode::StringLength { dtype, .. }
            | ExprNode::StringReplace { dtype, .. }
            | ExprNode::StructField { dtype, .. }
            | ExprNode::UnaryNumeric { dtype, .. }
            | ExprNode::StringUnary { dtype, .. }
            | ExprNode::LogicalBinary { dtype, .. }
            | ExprNode::LogicalNot { dtype, .. }
            | ExprNode::TemporalPart { dtype, .. }
            | ExprNode::ListLen { dtype, .. }
            | ExprNode::ListGet { dtype, .. }
            | ExprNode::ListContains { dtype, .. }
            | ExprNode::ListMin { dtype, .. }
            | ExprNode::ListMax { dtype, .. }
            | ExprNode::ListSum { dtype, .. }
            | ExprNode::DatetimeToDate { dtype, .. }
            | ExprNode::Strptime { dtype, .. }
            | ExprNode::UnixTimestamp { dtype, .. }
            | ExprNode::BinaryLength { dtype, .. }
            | ExprNode::MapLen { dtype, .. }
            | ExprNode::MapGet { dtype, .. }
            | ExprNode::MapContainsKey { dtype, .. }
            | ExprNode::MapKeys { dtype, .. }
            | ExprNode::MapValues { dtype, .. }
            | ExprNode::Window { dtype, .. }
            | ExprNode::GlobalAgg { dtype, .. }
            | ExprNode::GlobalRowCount { dtype, .. } => dtype.clone(),
        }
    }

    pub fn referenced_columns(&self) -> HashSet<String> {
        match self {
            ExprNode::ColumnRef { name, .. } => HashSet::from([name.clone()]),
            ExprNode::Literal { .. } => HashSet::new(),
            ExprNode::BinaryOp { left, right, .. } => {
                let mut out = left.referenced_columns();
                out.extend(right.referenced_columns());
                out
            }
            ExprNode::CompareOp { left, right, .. } => {
                let mut out = left.referenced_columns();
                out.extend(right.referenced_columns());
                out
            }
            ExprNode::Cast { input, .. }
            | ExprNode::IsNull { input, .. }
            | ExprNode::IsNotNull { input, .. } => input.referenced_columns(),
            ExprNode::Coalesce { exprs, .. } => {
                let mut out = HashSet::new();
                for e in exprs {
                    out.extend(e.referenced_columns());
                }
                out
            }
            ExprNode::CaseWhen {
                branches, else_, ..
            } => {
                let mut out = else_.referenced_columns();
                for (c, t) in branches {
                    out.extend(c.referenced_columns());
                    out.extend(t.referenced_columns());
                }
                out
            }
            ExprNode::InList { inner, .. } => inner.referenced_columns(),
            ExprNode::Between {
                inner, low, high, ..
            } => {
                let mut out = inner.referenced_columns();
                out.extend(low.referenced_columns());
                out.extend(high.referenced_columns());
                out
            }
            ExprNode::StringConcat { parts, .. } => {
                let mut out = HashSet::new();
                for p in parts {
                    out.extend(p.referenced_columns());
                }
                out
            }
            ExprNode::Substring {
                inner,
                start,
                length,
                ..
            } => {
                let mut out = inner.referenced_columns();
                out.extend(start.referenced_columns());
                if let Some(l) = length {
                    out.extend(l.referenced_columns());
                }
                out
            }
            ExprNode::StringLength { inner, .. }
            | ExprNode::StringReplace { inner, .. }
            | ExprNode::UnaryNumeric { inner, .. }
            | ExprNode::StringUnary { inner, .. }
            | ExprNode::LogicalNot { inner, .. }
            | ExprNode::TemporalPart { inner, .. }
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
            | ExprNode::MapValues { inner, .. } => inner.referenced_columns(),
            ExprNode::ListGet { inner, index, .. } => {
                let mut out = inner.referenced_columns();
                out.extend(index.referenced_columns());
                out
            }
            ExprNode::ListContains { inner, value, .. } => {
                let mut out = inner.referenced_columns();
                out.extend(value.referenced_columns());
                out
            }
            ExprNode::LogicalBinary { left, right, .. } => {
                let mut out = left.referenced_columns();
                out.extend(right.referenced_columns());
                out
            }
            ExprNode::StructField { base, .. } => base.referenced_columns(),
            ExprNode::GlobalAgg { inner, .. } => inner.referenced_columns(),
            ExprNode::GlobalRowCount { .. } => HashSet::new(),
            ExprNode::Window {
                operand,
                partition_by,
                order_by,
                ..
            } => {
                let mut out = HashSet::new();
                for n in partition_by {
                    out.insert(n.clone());
                }
                for (n, _) in order_by {
                    out.insert(n.clone());
                }
                if let Some(op) = operand {
                    out.extend(op.referenced_columns());
                }
                out
            }
        }
    }

    fn infer_arith_dtype(op: ArithOp, left: DTypeDesc, right: DTypeDesc) -> PyResult<DTypeDesc> {
        if left.is_struct() || right.is_struct() || left.is_list() || right.is_list() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Arithmetic operators do not support struct- or list-typed columns.",
            ));
        }
        let nullable = left.nullable_flag() || right.nullable_flag();
        let left_b = left.as_scalar_base_field().unwrap();
        let right_b = right.as_scalar_base_field().unwrap();

        if let (Some(a), Some(b)) = (left_b, right_b) {
            use ArithOp::*;
            match (a, b, op) {
                (BaseType::DateTime, BaseType::Duration, Add)
                | (BaseType::Duration, BaseType::DateTime, Add) => {
                    return Ok(DTypeDesc::Scalar {
                        base: Some(BaseType::DateTime),
                        nullable,
                    });
                }
                (BaseType::DateTime, BaseType::Duration, Sub) => {
                    return Ok(DTypeDesc::Scalar {
                        base: Some(BaseType::DateTime),
                        nullable,
                    });
                }
                (BaseType::Date, BaseType::Duration, Add)
                | (BaseType::Duration, BaseType::Date, Add) => {
                    return Ok(DTypeDesc::Scalar {
                        base: Some(BaseType::Date),
                        nullable,
                    });
                }
                (BaseType::Date, BaseType::Duration, Sub) => {
                    return Ok(DTypeDesc::Scalar {
                        base: Some(BaseType::Date),
                        nullable,
                    });
                }
                _ => {}
            }
        }

        let inferred_base = match (left_b, right_b) {
            (Some(a), Some(b)) => {
                let valid = matches!(
                    (a, b),
                    (BaseType::Int, BaseType::Int | BaseType::Float)
                        | (BaseType::Float, BaseType::Int | BaseType::Float)
                        | (BaseType::Decimal, BaseType::Decimal)
                );
                if !valid {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "Arithmetic operator requires numeric operands; got {:?} and {:?}.",
                        a, b
                    )));
                }
                match (a, b) {
                    (BaseType::Int, BaseType::Int) => Some(BaseType::Int),
                    (BaseType::Decimal, BaseType::Decimal) => Some(BaseType::Decimal),
                    _ => Some(BaseType::Float),
                }
            }
            (None, Some(b)) => match b {
                BaseType::Int => Some(BaseType::Int),
                BaseType::Float => Some(BaseType::Float),
                BaseType::Decimal => Some(BaseType::Decimal),
                _ => None,
            },
            (Some(a), None) => match a {
                BaseType::Int => Some(BaseType::Int),
                BaseType::Float => Some(BaseType::Float),
                BaseType::Decimal => Some(BaseType::Decimal),
                _ => None,
            },
            (None, None) => None,
        };

        let inferred_base = inferred_base.ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Cannot infer arithmetic result type from unknown/None base alone.",
            )
        })?;

        if op == ArithOp::Div {
            return Ok(DTypeDesc::Scalar {
                base: Some(BaseType::Float),
                nullable,
            });
        }

        Ok(DTypeDesc::Scalar {
            base: Some(inferred_base),
            nullable,
        })
    }

    fn infer_compare_dtype(op: CmpOp, left: DTypeDesc, right: DTypeDesc) -> PyResult<DTypeDesc> {
        let nullable = left.nullable_flag() || right.nullable_flag();

        match op {
            CmpOp::Eq | CmpOp::Ne => {
                if left.is_struct() && right.is_struct() {
                    if !dtype_structural_eq(&left, &right) {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Equality requires structurally equal struct dtypes.",
                        ));
                    }
                    return Ok(DTypeDesc::Scalar {
                        base: Some(BaseType::Bool),
                        nullable,
                    });
                }
                if left.is_struct() || right.is_struct() {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Cannot compare struct columns to scalar columns.",
                    ));
                }
                if left.is_list() || right.is_list() {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Equality comparisons for list columns are not supported yet.",
                    ));
                }
                if left.is_map() || right.is_map() {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Equality comparisons for map columns are not supported yet.",
                    ));
                }

                let lb = left.as_scalar_base_field().unwrap();
                let rb = right.as_scalar_base_field().unwrap();
                let inferred_left_base = lb.or(rb);
                let inferred_right_base = rb.or(lb);
                let (lb, rb) = match (inferred_left_base, inferred_right_base) {
                    (Some(a), Some(b)) => (a, b),
                    (None, None) => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Cannot infer equality from Literal(None) alone.",
                        ))
                    }
                    _ => unreachable!(),
                };

                let allowed = (matches!(
                    (lb, rb),
                    (BaseType::Int, BaseType::Int | BaseType::Float)
                        | (BaseType::Float, BaseType::Int | BaseType::Float)
                ) || (lb == BaseType::Bool && rb == BaseType::Bool)
                    || (lb == BaseType::Str && rb == BaseType::Str)
                    || (lb == BaseType::Enum && rb == BaseType::Enum)
                    || (lb == BaseType::Str && rb == BaseType::Enum)
                    || (lb == BaseType::Enum && rb == BaseType::Str)
                    || (lb == BaseType::Uuid && rb == BaseType::Uuid)
                    || (lb == BaseType::Decimal && rb == BaseType::Decimal)
                    || (lb == BaseType::DateTime && rb == BaseType::DateTime)
                    || (lb == BaseType::Date && rb == BaseType::Date)
                    || (lb == BaseType::Duration && rb == BaseType::Duration)
                    || (lb == BaseType::Time && rb == BaseType::Time)
                    || (lb == BaseType::Binary && rb == BaseType::Binary));

                if !allowed {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Equality requires compatible scalar operands or matching struct dtypes.",
                    ));
                }

                Ok(DTypeDesc::Scalar {
                    base: Some(BaseType::Bool),
                    nullable,
                })
            }
            CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge => {
                if left.is_struct()
                    || right.is_struct()
                    || left.is_list()
                    || right.is_list()
                    || left.is_map()
                    || right.is_map()
                {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Ordering comparisons do not support struct-, list-, or map-typed columns.",
                    ));
                }

                let lb = left.as_scalar_base_field().unwrap();
                let rb = right.as_scalar_base_field().unwrap();
                let inferred_left_base = lb.or(rb);
                let inferred_right_base = rb.or(lb);
                let (lb, rb) = match (inferred_left_base, inferred_right_base) {
                    (Some(a), Some(b)) => (a, b),
                    (None, None) => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Cannot infer ordering from Literal(None) alone.",
                        ))
                    }
                    _ => unreachable!(),
                };

                let allowed_numeric = matches!(
                    (lb, rb),
                    (BaseType::Int, BaseType::Int | BaseType::Float)
                        | (BaseType::Float, BaseType::Int | BaseType::Float)
                );
                let allowed_str = lb == BaseType::Str && rb == BaseType::Str;
                let allowed_enum = ((lb == BaseType::Str || lb == BaseType::Enum)
                    && rb == BaseType::Enum)
                    || (lb == BaseType::Enum && rb == BaseType::Str);
                let allowed_uuid = lb == BaseType::Uuid && rb == BaseType::Uuid;
                let allowed_decimal = lb == BaseType::Decimal && rb == BaseType::Decimal;
                let allowed_temporal = (lb == BaseType::DateTime && rb == BaseType::DateTime)
                    || (lb == BaseType::Date && rb == BaseType::Date)
                    || (lb == BaseType::Duration && rb == BaseType::Duration)
                    || (lb == BaseType::Time && rb == BaseType::Time);

                if !(allowed_numeric
                    || allowed_str
                    || allowed_enum
                    || allowed_uuid
                    || allowed_decimal
                    || allowed_temporal)
                {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Ordering comparisons require numeric-numeric, str/str-like, uuid-uuid, decimal-decimal, or same temporal operands.",
                    ));
                }

                Ok(DTypeDesc::Scalar {
                    base: Some(BaseType::Bool),
                    nullable,
                })
            }
        }
    }

    pub fn make_column_ref(name: String, dtype: DTypeDesc) -> PyResult<Self> {
        if dtype.is_scalar_unknown_nullable() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "ColumnRef dtype cannot have unknown scalar base.",
            ));
        }
        Ok(ExprNode::ColumnRef { name, dtype })
    }

    pub fn make_literal(value: Option<LiteralValue>, dtype: DTypeDesc) -> PyResult<Self> {
        match (&value, &dtype) {
            (None, DTypeDesc::Scalar { base: None, .. }) => {}
            (None, DTypeDesc::Scalar { base: Some(_), .. }) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Literal(None) must use unknown-base nullable dtype or a nullable struct column type.",
                ));
            }
            (None, DTypeDesc::Struct { nullable: true, .. }) => {}
            (
                None,
                DTypeDesc::Struct {
                    nullable: false, ..
                },
            ) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Literal(None) cannot target a non-nullable struct column.",
                ));
            }
            (None, DTypeDesc::List { nullable: true, .. }) => {}
            (
                None,
                DTypeDesc::List {
                    nullable: false, ..
                },
            ) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Literal(None) cannot target a non-nullable list column.",
                ));
            }
            (None, DTypeDesc::Map { nullable: true, .. }) => {}
            (
                None,
                DTypeDesc::Map {
                    nullable: false, ..
                },
            ) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Literal(None) cannot target a non-nullable map column.",
                ));
            }
            (Some(_), DTypeDesc::Scalar { base: None, .. }) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Non-None Literal must have a known scalar base dtype.",
                ));
            }
            (Some(_), DTypeDesc::Struct { .. }) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Struct literals are not supported; use column references.",
                ));
            }
            (Some(_), DTypeDesc::List { .. }) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "List literals are not supported; use column references.",
                ));
            }
            (Some(_), DTypeDesc::Map { .. }) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Map literals are not supported; use column references.",
                ));
            }
            (Some(_), _) => {}
        }
        Ok(ExprNode::Literal { value, dtype })
    }

    pub fn make_binary_op(op: ArithOp, left: ExprNode, right: ExprNode) -> PyResult<Self> {
        let dtype = Self::infer_arith_dtype(op, left.dtype(), right.dtype())?;
        Ok(ExprNode::BinaryOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
            dtype,
        })
    }

    pub fn make_compare_op(op: CmpOp, left: ExprNode, right: ExprNode) -> PyResult<Self> {
        let dtype = Self::infer_compare_dtype(op, left.dtype(), right.dtype())?;
        Ok(ExprNode::CompareOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
            dtype,
        })
    }

    pub fn make_cast(input: ExprNode, target: DTypeDesc) -> PyResult<Self> {
        if target.is_struct()
            || input.dtype().is_struct()
            || target.is_list()
            || input.dtype().is_list()
            || target.is_map()
            || input.dtype().is_map()
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() does not support struct, list, or map dtypes.",
            ));
        }
        let base = match &target {
            DTypeDesc::Scalar { base: Some(b), .. } => *b,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "cast() target dtype must have known base.",
                ));
            }
        };
        let in_base = match input.dtype() {
            DTypeDesc::Scalar { base: Some(b), .. } => b,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "cast() input dtype must have known base.",
                ));
            }
        };
        let allowed = matches!(
            (in_base, base),
            (
                BaseType::Int,
                BaseType::Int | BaseType::Float | BaseType::Bool | BaseType::Str
            ) | (
                BaseType::Float,
                BaseType::Int | BaseType::Float | BaseType::Bool | BaseType::Str
            ) | (
                BaseType::Bool,
                BaseType::Int | BaseType::Float | BaseType::Bool | BaseType::Str
            ) | (
                BaseType::Str,
                BaseType::Int | BaseType::Float | BaseType::Bool | BaseType::Str
            ) | (BaseType::Uuid, BaseType::Str | BaseType::Uuid)
                | (BaseType::Str, BaseType::Uuid)
                | (BaseType::Decimal, BaseType::Str | BaseType::Decimal)
                | (BaseType::Str, BaseType::Decimal)
                | (BaseType::Enum, BaseType::Str | BaseType::Enum)
                | (BaseType::Str, BaseType::Enum)
                | (BaseType::DateTime, BaseType::Date)
                | (BaseType::DateTime, BaseType::Str)
                | (BaseType::Date, BaseType::Str)
                | (BaseType::Str, BaseType::Date)
                | (BaseType::Str, BaseType::DateTime)
        );
        if !allowed {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() unsupported primitive conversion.",
            ));
        }
        let input_nullable = input.dtype().nullable_flag();
        Ok(ExprNode::Cast {
            input: Box::new(input),
            dtype: DTypeDesc::Scalar {
                base: Some(base),
                nullable: input_nullable || target.nullable_flag(),
            },
        })
    }

    pub fn make_is_null(input: ExprNode) -> PyResult<Self> {
        Ok(ExprNode::IsNull {
            input: Box::new(input),
            dtype: DTypeDesc::non_nullable(BaseType::Bool),
        })
    }

    pub fn make_is_not_null(input: ExprNode) -> PyResult<Self> {
        Ok(ExprNode::IsNotNull {
            input: Box::new(input),
            dtype: DTypeDesc::non_nullable(BaseType::Bool),
        })
    }

    pub fn make_coalesce(exprs: Vec<ExprNode>) -> PyResult<Self> {
        if exprs.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "coalesce() requires at least one expression.",
            ));
        }
        let mut nullable = false;
        if exprs
            .iter()
            .any(|e| e.dtype().is_struct() || e.dtype().is_list())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "coalesce() does not support struct or list dtypes.",
            ));
        }
        let first_base = exprs[0]
            .dtype()
            .as_scalar_base_field()
            .flatten()
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "coalesce() requires expressions with known dtypes.",
                )
            })?;
        for e in &exprs {
            if e.dtype().as_scalar_base_field().flatten() != Some(first_base) {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "coalesce() requires compatible scalar dtypes.",
                ));
            }
            nullable |= e.dtype().nullable_flag();
        }
        Ok(ExprNode::Coalesce {
            exprs,
            dtype: DTypeDesc::Scalar {
                base: Some(first_base),
                nullable,
            },
        })
    }

    pub fn make_case_when(branches: Vec<(ExprNode, ExprNode)>, else_: ExprNode) -> PyResult<Self> {
        if branches.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "case_when() requires at least one branch.",
            ));
        }
        if else_.dtype().is_struct()
            || else_.dtype().is_list()
            || branches
                .iter()
                .any(|(_, t)| t.dtype().is_struct() || t.dtype().is_list())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "case_when() does not support struct or list dtypes.",
            ));
        }
        let mut nullable = else_.dtype().nullable_flag();
        let first_base = branches[0]
            .1
            .dtype()
            .as_scalar_base_field()
            .flatten()
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "case_when() requires then-branches with known dtypes.",
                )
            })?;
        for (_, t) in &branches {
            if t.dtype().as_scalar_base_field().flatten() != Some(first_base) {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "case_when() requires compatible then dtypes.",
                ));
            }
            nullable |= t.dtype().nullable_flag();
        }
        if else_.dtype().as_scalar_base_field().flatten() != Some(first_base) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "case_when() else branch dtype must match then branches.",
            ));
        }
        Ok(ExprNode::CaseWhen {
            branches,
            else_: Box::new(else_),
            dtype: DTypeDesc::Scalar {
                base: Some(first_base),
                nullable,
            },
        })
    }

    pub fn make_in_list(inner: ExprNode, values: Vec<LiteralValue>) -> PyResult<Self> {
        if inner.dtype().is_struct() || inner.dtype().is_list() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "isin() does not support struct or list dtypes.",
            ));
        }
        let ib = inner
            .dtype()
            .as_scalar_base_field()
            .flatten()
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "isin() requires a column expression with known dtype.",
                )
            })?;
        if values.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "isin() requires at least one value.",
            ));
        }
        for v in &values {
            let vb = match v {
                LiteralValue::Int(_) => Some(BaseType::Int),
                LiteralValue::Float(_) => Some(BaseType::Float),
                LiteralValue::Bool(_) => Some(BaseType::Bool),
                LiteralValue::Str(_) => Some(BaseType::Str),
                LiteralValue::Uuid(_) => Some(BaseType::Uuid),
                LiteralValue::Decimal(_) => Some(BaseType::Decimal),
                LiteralValue::EnumStr(_) => Some(BaseType::Enum),
                LiteralValue::DateTimeMicros(_)
                | LiteralValue::DateDays(_)
                | LiteralValue::DurationMicros(_) => None,
                LiteralValue::TimeNanos(_) => Some(BaseType::Time),
                LiteralValue::Binary(_) => Some(BaseType::Binary),
            };
            let ok = match (ib, vb) {
                (a, Some(b)) if a == b => true,
                (BaseType::Enum, Some(BaseType::Str)) => true,
                (BaseType::Str, Some(BaseType::Enum)) => true,
                _ => false,
            };
            if !ok {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "isin() value types must match column dtype.",
                ));
            }
        }
        Ok(ExprNode::InList {
            inner: Box::new(inner),
            values,
            dtype: DTypeDesc::non_nullable(BaseType::Bool),
        })
    }

    pub fn make_between(inner: ExprNode, low: ExprNode, high: ExprNode) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || low.dtype().is_struct()
            || low.dtype().is_list()
            || high.dtype().is_struct()
            || high.dtype().is_list()
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "between() does not support struct or list dtypes.",
            ));
        }
        let ib = inner
            .dtype()
            .as_scalar_base_field()
            .flatten()
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "between() requires known inner dtype.",
                )
            })?;
        if low.dtype().as_scalar_base_field().flatten() != Some(ib)
            || high.dtype().as_scalar_base_field().flatten() != Some(ib)
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "between() bounds must match inner dtype.",
            ));
        }
        Ok(ExprNode::Between {
            inner: Box::new(inner),
            low: Box::new(low),
            high: Box::new(high),
            dtype: DTypeDesc::non_nullable(BaseType::Bool),
        })
    }

    pub fn make_string_concat(parts: Vec<ExprNode>) -> PyResult<Self> {
        if parts.len() < 2 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "concat() requires at least two expressions.",
            ));
        }
        for p in &parts {
            if p.dtype().is_struct() || p.dtype().is_list() || !dtype_is_string_like(&p.dtype()) {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "concat() requires string-typed expressions.",
                ));
            }
        }
        let nullable = parts.iter().any(|p| p.dtype().nullable_flag());
        Ok(ExprNode::StringConcat {
            parts,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
            },
        })
    }

    pub fn make_substring(
        inner: ExprNode,
        start: ExprNode,
        length: Option<ExprNode>,
    ) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "substring() requires a string column.",
            ));
        }
        if start.dtype().is_struct()
            || start.dtype().is_list()
            || start.dtype().as_scalar_base_field().flatten() != Some(BaseType::Int)
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "substring() start must be int.",
            ));
        }
        if let Some(l) = &length {
            if l.dtype().is_struct()
                || l.dtype().is_list()
                || l.dtype().as_scalar_base_field().flatten() != Some(BaseType::Int)
            {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "substring() length must be int.",
                ));
            }
        }
        let nullable = inner.dtype().nullable_flag()
            || start.dtype().nullable_flag()
            || length
                .as_ref()
                .map(|l| l.dtype().nullable_flag())
                .unwrap_or(false);
        Ok(ExprNode::Substring {
            inner: Box::new(inner),
            start: Box::new(start),
            length: length.map(Box::new),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
            },
        })
    }

    pub fn make_string_length(inner: ExprNode) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "length() requires a string column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StringLength {
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable,
            },
        })
    }

    pub fn make_string_replace(
        inner: ExprNode,
        pattern: String,
        replacement: String,
    ) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "str_replace() requires a string-like column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StringReplace {
            inner: Box::new(inner),
            pattern,
            replacement,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
            },
        })
    }

    pub fn make_struct_field(inner: ExprNode, field: String) -> PyResult<Self> {
        let dtype = match inner.dtype() {
            DTypeDesc::Struct {
                fields,
                nullable: struct_nullable,
            } => {
                let (_, fd) = fields.iter().find(|(n, _)| n == &field).ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "Unknown struct field {:?}.",
                        field
                    ))
                })?;
                let mut out = fd.clone();
                if struct_nullable {
                    out = match out {
                        DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                            base,
                            nullable: true,
                        },
                        DTypeDesc::Struct { fields, .. } => DTypeDesc::Struct {
                            fields,
                            nullable: true,
                        },
                        DTypeDesc::List { inner, .. } => DTypeDesc::List {
                            inner,
                            nullable: true,
                        },
                        DTypeDesc::Map { value, .. } => DTypeDesc::Map {
                            value,
                            nullable: true,
                        },
                    };
                }
                out
            }
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "struct_field() requires a struct-typed expression.",
                ));
            }
        };
        Ok(ExprNode::StructField {
            base: Box::new(inner),
            field,
            dtype,
        })
    }

    fn numeric_inner_dtype(inner: &DTypeDesc) -> PyResult<(BaseType, bool)> {
        if inner.is_struct() || inner.is_list() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Numeric unary operations do not support struct- or list-typed columns.",
            ));
        }
        let nullable = inner.nullable_flag();
        match inner.as_scalar_base_field().flatten() {
            Some(b @ BaseType::Int) | Some(b @ BaseType::Float) => Ok((b, nullable)),
            Some(other) => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Numeric unary operation requires int or float column; got {:?}.",
                other
            ))),
            None => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Numeric unary operation requires a column with known numeric dtype.",
            )),
        }
    }

    pub fn make_unary_numeric(inner: ExprNode, op: UnaryNumericOp) -> PyResult<Self> {
        let (in_base, nullable) = Self::numeric_inner_dtype(&inner.dtype())?;
        let dtype = match op {
            UnaryNumericOp::Abs | UnaryNumericOp::Round { .. } => DTypeDesc::Scalar {
                base: Some(in_base),
                nullable,
            },
            UnaryNumericOp::Floor | UnaryNumericOp::Ceil => DTypeDesc::Scalar {
                base: Some(BaseType::Float),
                nullable,
            },
        };
        Ok(ExprNode::UnaryNumeric {
            op,
            inner: Box::new(inner),
            dtype,
        })
    }

    pub fn make_string_unary(inner: ExprNode, op: StringUnaryOp) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "String operation requires a string column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StringUnary {
            op,
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
            },
        })
    }

    pub fn make_logical_binary(op: LogicalOp, left: ExprNode, right: ExprNode) -> PyResult<Self> {
        let is_bool = |d: &DTypeDesc| {
            matches!(
                d,
                DTypeDesc::Scalar {
                    base: Some(BaseType::Bool),
                    ..
                }
            )
        };
        if !is_bool(&left.dtype()) || !is_bool(&right.dtype()) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Logical operations require boolean expressions.",
            ));
        }
        let nullable = left.dtype().nullable_flag() || right.dtype().nullable_flag();
        Ok(ExprNode::LogicalBinary {
            op,
            left: Box::new(left),
            right: Box::new(right),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Bool),
                nullable,
            },
        })
    }

    pub fn make_logical_not(inner: ExprNode) -> PyResult<Self> {
        let is_bool = matches!(
            inner.dtype(),
            DTypeDesc::Scalar {
                base: Some(BaseType::Bool),
                ..
            }
        );
        if !is_bool {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Logical not requires a boolean expression.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::LogicalNot {
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Bool),
                nullable,
            },
        })
    }

    pub fn make_temporal_part(inner: ExprNode, part: TemporalPart) -> PyResult<Self> {
        let d = inner.dtype();
        if d.is_struct() || d.is_list() || d.is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Temporal extraction requires a datetime, date, or time column.",
            ));
        }
        let base = d.as_scalar_base_field().flatten();
        let is_dt = base == Some(BaseType::DateTime);
        let is_date = base == Some(BaseType::Date);
        let is_time = base == Some(BaseType::Time);
        match part {
            TemporalPart::Year | TemporalPart::Month | TemporalPart::Day => {
                if !(is_dt || is_date) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "year(), month(), day() require a datetime or date column.",
                    ));
                }
            }
            TemporalPart::Hour
            | TemporalPart::Minute
            | TemporalPart::Second
            | TemporalPart::Nanosecond => {
                if !(is_dt || is_time) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "hour(), minute(), second(), nanosecond() require a datetime or time column.",
                    ));
                }
            }
        }
        let nullable = d.nullable_flag();
        Ok(ExprNode::TemporalPart {
            part,
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable,
            },
        })
    }

    pub fn make_list_len(inner: ExprNode) -> PyResult<Self> {
        if !inner.dtype().is_list() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "list_len() requires a list-typed column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::ListLen {
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable,
            },
        })
    }

    pub fn make_list_get(inner: ExprNode, index: ExprNode) -> PyResult<Self> {
        let element = match inner.dtype() {
            DTypeDesc::List { inner: e, .. } => e.as_ref().clone(),
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "list_get() requires a list-typed column.",
                ));
            }
        };
        if matches!(&element, DTypeDesc::List { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "list_get() does not support nested list columns.",
            ));
        }
        if matches!(&element, DTypeDesc::Map { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "list_get() does not support list columns whose element type is a map.",
            ));
        }
        if index.dtype().is_struct()
            || index.dtype().is_list()
            || index.dtype().as_scalar_base_field().flatten() != Some(BaseType::Int)
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "list_get() index must be an int expression.",
            ));
        }
        let dtype = match element {
            DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                base,
                nullable: true,
            },
            DTypeDesc::Struct { fields, .. } => DTypeDesc::Struct {
                fields,
                nullable: true,
            },
            DTypeDesc::Map { .. } => unreachable!(),
            DTypeDesc::List { .. } => unreachable!(),
        };
        Ok(ExprNode::ListGet {
            inner: Box::new(inner),
            index: Box::new(index),
            dtype,
        })
    }

    pub fn make_list_contains(inner: ExprNode, value: ExprNode) -> PyResult<Self> {
        let elt = match inner.dtype() {
            DTypeDesc::List { inner: e, .. } => e.as_ref().clone(),
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "list_contains() requires a list-typed column.",
                ));
            }
        };
        if matches!(&elt, DTypeDesc::Map { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "list_contains() does not support list columns whose element type is a map.",
            ));
        }
        if !dtype_structural_eq(&elt, &value.dtype()) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "list_contains() value dtype must match the list element dtype.",
            ));
        }
        let nullable = inner.dtype().nullable_flag() || value.dtype().nullable_flag();
        Ok(ExprNode::ListContains {
            inner: Box::new(inner),
            value: Box::new(value),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Bool),
                nullable,
            },
        })
    }

    fn make_list_numeric_agg(inner: ExprNode, kind: ListAggKind) -> PyResult<Self> {
        let (base, list_nullable) = match inner.dtype() {
            DTypeDesc::List {
                inner: e, nullable, ..
            } => match e.as_ref() {
                DTypeDesc::Scalar {
                    base: Some(BaseType::Int),
                    ..
                } => (BaseType::Int, nullable),
                DTypeDesc::Scalar {
                    base: Some(BaseType::Float),
                    ..
                } => (BaseType::Float, nullable),
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "list_min/list_max/list_sum require list[int] or list[float].",
                    ));
                }
            },
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "list aggregation requires a list-typed column.",
                ));
            }
        };
        let dtype = DTypeDesc::Scalar {
            base: Some(base),
            nullable: list_nullable,
        };
        let inner = Box::new(inner);
        Ok(match kind {
            ListAggKind::Min => ExprNode::ListMin { inner, dtype },
            ListAggKind::Max => ExprNode::ListMax { inner, dtype },
            ListAggKind::Sum => ExprNode::ListSum { inner, dtype },
        })
    }

    pub fn make_list_min(inner: ExprNode) -> PyResult<Self> {
        Self::make_list_numeric_agg(inner, ListAggKind::Min)
    }

    pub fn make_list_max(inner: ExprNode) -> PyResult<Self> {
        Self::make_list_numeric_agg(inner, ListAggKind::Max)
    }

    pub fn make_list_sum(inner: ExprNode) -> PyResult<Self> {
        Self::make_list_numeric_agg(inner, ListAggKind::Sum)
    }

    pub fn make_datetime_to_date(inner: ExprNode) -> PyResult<Self> {
        let d = inner.dtype();
        if d.is_struct() || d.is_list() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "dt_date() requires a datetime column.",
            ));
        }
        if d.as_scalar_base_field().flatten() != Some(BaseType::DateTime) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "dt_date() requires a datetime column.",
            ));
        }
        let nullable = d.nullable_flag();
        Ok(ExprNode::DatetimeToDate {
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Date),
                nullable,
            },
        })
    }

    fn parse_window_frame(
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Option<WindowFrame>> {
        match (frame_kind, frame_start, frame_end) {
            (None, None, None) => Ok(None),
            (Some(kind), Some(start), Some(end)) => {
                if start > end {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Window frame requires start <= end.",
                    ));
                }
                match kind.as_str() {
                    "rows" => Ok(Some(WindowFrame::Rows { start, end })),
                    "range" => Ok(Some(WindowFrame::Range { start, end })),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Window frame kind must be 'rows' or 'range'.",
                    )),
                }
            }
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Window frame requires kind/start/end together.",
            )),
        }
    }

    fn reject_range_frame(frame: &Option<WindowFrame>, op_name: &str) -> PyResult<()> {
        if matches!(frame, Some(WindowFrame::Range { .. })) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "{op_name}() does not support rangeBetween frames."
            )));
        }
        Ok(())
    }

    pub fn make_window_row_number(
        partition_by: Vec<String>,
        order_by: Vec<(String, bool)>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if partition_by.is_empty() && order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "window expression requires at least one partition_by or order_by column.",
            ));
        }
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "row_number() requires at least one order_by column.",
            ));
        }
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "row_number")?;
        Ok(ExprNode::Window {
            op: WindowOp::RowNumber,
            operand: None,
            partition_by,
            order_by,
            frame,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable: true,
            },
        })
    }

    pub fn make_window_rank(
        dense: bool,
        partition_by: Vec<String>,
        order_by: Vec<(String, bool)>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "rank() and dense_rank() require at least one order_by column.",
            ));
        }
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "rank")?;
        Ok(ExprNode::Window {
            op: if dense {
                WindowOp::DenseRank
            } else {
                WindowOp::Rank
            },
            operand: None,
            partition_by,
            order_by,
            frame,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable: true,
            },
        })
    }

    fn infer_window_sum_mean_dtype(inner: &ExprNode, mean: bool) -> PyResult<DTypeDesc> {
        let d = inner.dtype();
        if d.is_struct() || d.is_list() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "window sum/mean expect a numeric scalar column.",
            ));
        }
        let b = d.as_scalar_base_field().flatten().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "window sum/mean require a column with known scalar dtype.",
            )
        })?;
        match b {
            BaseType::Int => Ok(DTypeDesc::Scalar {
                base: Some(if mean { BaseType::Float } else { BaseType::Int }),
                nullable: true,
            }),
            BaseType::Float => Ok(DTypeDesc::Scalar {
                base: Some(BaseType::Float),
                nullable: true,
            }),
            BaseType::Decimal => Ok(DTypeDesc::Scalar {
                base: Some(if mean {
                    BaseType::Float
                } else {
                    BaseType::Decimal
                }),
                nullable: true,
            }),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "window sum/mean require int, float, or decimal column.",
            )),
        }
    }

    pub fn make_window_sum(
        inner: ExprNode,
        partition_by: Vec<String>,
        order_by: Vec<(String, bool)>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if partition_by.is_empty() && order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "window expression requires at least one partition_by or order_by column.",
            ));
        }
        let dtype = Self::infer_window_sum_mean_dtype(&inner, false)?;
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Ok(ExprNode::Window {
            op: WindowOp::Sum,
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn make_window_mean(
        inner: ExprNode,
        partition_by: Vec<String>,
        order_by: Vec<(String, bool)>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if partition_by.is_empty() && order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "window expression requires at least one partition_by or order_by column.",
            ));
        }
        let dtype = Self::infer_window_sum_mean_dtype(&inner, true)?;
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Ok(ExprNode::Window {
            op: WindowOp::Mean,
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn make_window_min(
        inner: ExprNode,
        partition_by: Vec<String>,
        order_by: Vec<(String, bool)>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if partition_by.is_empty() && order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "window expression requires at least one partition_by or order_by column.",
            ));
        }
        let dtype = Self::infer_global_min_max_dtype(&inner)?;
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Ok(ExprNode::Window {
            op: WindowOp::Min,
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn make_window_max(
        inner: ExprNode,
        partition_by: Vec<String>,
        order_by: Vec<(String, bool)>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if partition_by.is_empty() && order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "window expression requires at least one partition_by or order_by column.",
            ));
        }
        let dtype = Self::infer_global_min_max_dtype(&inner)?;
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Ok(ExprNode::Window {
            op: WindowOp::Max,
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn make_global_sum(inner: ExprNode) -> PyResult<Self> {
        if !matches!(&inner, ExprNode::ColumnRef { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "global sum() expects a column reference expression.",
            ));
        }
        let dtype = Self::infer_window_sum_mean_dtype(&inner, false)?;
        Ok(ExprNode::GlobalAgg {
            op: GlobalAggOp::Sum,
            inner: Box::new(inner),
            dtype,
        })
    }

    pub fn make_global_mean(inner: ExprNode) -> PyResult<Self> {
        if !matches!(&inner, ExprNode::ColumnRef { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "global mean() expects a column reference expression.",
            ));
        }
        let dtype = Self::infer_window_sum_mean_dtype(&inner, true)?;
        Ok(ExprNode::GlobalAgg {
            op: GlobalAggOp::Mean,
            inner: Box::new(inner),
            dtype,
        })
    }

    pub fn make_global_count(inner: ExprNode) -> PyResult<Self> {
        if !matches!(&inner, ExprNode::ColumnRef { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "global count() expects a column reference expression.",
            ));
        }
        Ok(ExprNode::GlobalAgg {
            op: GlobalAggOp::Count,
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable: true,
            },
        })
    }

    fn infer_global_min_max_dtype(inner: &ExprNode) -> PyResult<DTypeDesc> {
        let d = inner.dtype();
        if d.is_struct() || d.is_list() || d.is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "global min/max expect a numeric scalar column.",
            ));
        }
        let b = d.as_scalar_base_field().flatten().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "global min/max require a column with known scalar dtype.",
            )
        })?;
        match b {
            BaseType::Int | BaseType::Float | BaseType::Decimal => Ok(DTypeDesc::Scalar {
                base: Some(b),
                nullable: true,
            }),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "global min/max require int, float, or decimal column.",
            )),
        }
    }

    pub fn make_global_min(inner: ExprNode) -> PyResult<Self> {
        if !matches!(&inner, ExprNode::ColumnRef { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "global min() expects a column reference expression.",
            ));
        }
        let dtype = Self::infer_global_min_max_dtype(&inner)?;
        Ok(ExprNode::GlobalAgg {
            op: GlobalAggOp::Min,
            inner: Box::new(inner),
            dtype,
        })
    }

    pub fn make_global_max(inner: ExprNode) -> PyResult<Self> {
        if !matches!(&inner, ExprNode::ColumnRef { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "global max() expects a column reference expression.",
            ));
        }
        let dtype = Self::infer_global_min_max_dtype(&inner)?;
        Ok(ExprNode::GlobalAgg {
            op: GlobalAggOp::Max,
            inner: Box::new(inner),
            dtype,
        })
    }

    /// Whole-frame row count (Spark `count(*)`-style), for [`DataFrame.select`] only.
    pub fn make_global_row_count() -> Self {
        ExprNode::GlobalRowCount {
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable: false,
            },
        }
    }

    pub fn make_strptime(inner: ExprNode, format: String, to_datetime: bool) -> PyResult<Self> {
        if format.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "strptime format must be non-empty.",
            ));
        }
        if inner.dtype().as_scalar_base_field().flatten() != Some(BaseType::Str) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "strptime() requires a string column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        let dtype = if to_datetime {
            DTypeDesc::Scalar {
                base: Some(BaseType::DateTime),
                nullable,
            }
        } else {
            DTypeDesc::Scalar {
                base: Some(BaseType::Date),
                nullable,
            }
        };
        Ok(ExprNode::Strptime {
            inner: Box::new(inner),
            format,
            to_datetime,
            dtype,
        })
    }

    pub fn make_unix_timestamp(inner: ExprNode, unit: UnixTimestampUnit) -> PyResult<Self> {
        let b = inner.dtype().as_scalar_base_field().flatten();
        if b != Some(BaseType::Date) && b != Some(BaseType::DateTime) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "unix_timestamp() requires a date or datetime column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::UnixTimestamp {
            inner: Box::new(inner),
            unit,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable,
            },
        })
    }

    pub fn make_binary_length(inner: ExprNode) -> PyResult<Self> {
        if inner.dtype().as_scalar_base_field().flatten() != Some(BaseType::Binary) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "binary_len() requires a bytes column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::BinaryLength {
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable,
            },
        })
    }

    pub fn make_map_len(inner: ExprNode) -> PyResult<Self> {
        if !inner.dtype().is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_len() requires a map column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::MapLen {
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable,
            },
        })
    }

    pub fn make_map_get(inner: ExprNode, key: String) -> PyResult<Self> {
        if !inner.dtype().is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_get() requires a map column.",
            ));
        }
        let DTypeDesc::Map { value, .. } = inner.dtype() else {
            unreachable!("is_map checked");
        };
        let dtype = match value.as_ref().clone() {
            DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                base,
                nullable: true,
            },
            DTypeDesc::Struct { fields, .. } => DTypeDesc::Struct {
                fields,
                nullable: true,
            },
            DTypeDesc::List { inner: li, .. } => DTypeDesc::List {
                inner: li,
                nullable: true,
            },
            DTypeDesc::Map { value: v, .. } => DTypeDesc::Map {
                value: v,
                nullable: true,
            },
        };
        Ok(ExprNode::MapGet {
            inner: Box::new(inner),
            key,
            dtype,
        })
    }

    pub fn make_map_contains_key(inner: ExprNode, key: String) -> PyResult<Self> {
        if !inner.dtype().is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_contains_key() requires a map column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::MapContainsKey {
            inner: Box::new(inner),
            key,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Bool),
                nullable,
            },
        })
    }

    pub fn make_map_keys(inner: ExprNode) -> PyResult<Self> {
        if !inner.dtype().is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_keys() requires a map column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::MapKeys {
            inner: Box::new(inner),
            dtype: DTypeDesc::List {
                inner: Box::new(DTypeDesc::Scalar {
                    base: Some(BaseType::Str),
                    nullable: false,
                }),
                nullable,
            },
        })
    }

    pub fn make_map_values(inner: ExprNode) -> PyResult<Self> {
        if !inner.dtype().is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_values() requires a map column.",
            ));
        }
        let DTypeDesc::Map { value, .. } = inner.dtype() else {
            unreachable!("is_map checked");
        };
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::MapValues {
            inner: Box::new(inner),
            dtype: DTypeDesc::List {
                inner: Box::new(value.as_ref().clone()),
                nullable,
            },
        })
    }

    fn window_shift_operand_dtype(inner: &ExprNode) -> PyResult<DTypeDesc> {
        let d = inner.dtype();
        if d.is_struct() || d.is_list() || d.is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "lag/lead expect a scalar column expression.",
            ));
        }
        let base = d.as_scalar_base_field().flatten().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "lag/lead require a column with known scalar dtype.",
            )
        })?;
        Ok(DTypeDesc::Scalar {
            base: Some(base),
            nullable: true,
        })
    }

    pub fn make_window_lag(
        inner: ExprNode,
        n: u32,
        partition_by: Vec<String>,
        order_by: Vec<(String, bool)>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if partition_by.is_empty() && order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "window expression requires at least one partition_by or order_by column.",
            ));
        }
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "lag() requires order_by columns.",
            ));
        }
        let dtype = Self::window_shift_operand_dtype(&inner)?;
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "lag")?;
        Ok(ExprNode::Window {
            op: WindowOp::Lag { n },
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn make_window_lead(
        inner: ExprNode,
        n: u32,
        partition_by: Vec<String>,
        order_by: Vec<(String, bool)>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if partition_by.is_empty() && order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "window expression requires at least one partition_by or order_by column.",
            ));
        }
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "lead() requires order_by columns.",
            ));
        }
        let dtype = Self::window_shift_operand_dtype(&inner)?;
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "lead")?;
        Ok(ExprNode::Window {
            op: WindowOp::Lead { n },
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn global_agg_default_alias(&self) -> Option<String> {
        match self {
            ExprNode::GlobalAgg { op, inner, .. } => {
                let ExprNode::ColumnRef { name, .. } = inner.as_ref() else {
                    return None;
                };
                Some(match op {
                    GlobalAggOp::Sum => format!("sum_{name}"),
                    GlobalAggOp::Mean => format!("mean_{name}"),
                    GlobalAggOp::Count => format!("count_{name}"),
                    GlobalAggOp::Min => format!("min_{name}"),
                    GlobalAggOp::Max => format!("max_{name}"),
                })
            }
            ExprNode::GlobalRowCount { .. } => Some("row_count".to_string()),
            _ => None,
        }
    }

    #[cfg(not(feature = "polars_engine"))]
    pub fn eval(
        &self,
        ctx: &HashMap<String, Vec<Option<LiteralValue>>>,
        n: usize,
    ) -> PyResult<Vec<Option<LiteralValue>>> {
        match self {
            ExprNode::ColumnRef { name, .. } => {
                let col = ctx.get(name).ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                        "Unknown column '{}' during expression evaluation.",
                        name
                    ))
                })?;
                Ok(col.clone())
            }
            ExprNode::Literal { value, .. } => {
                // Materialize literal into a vector. This avoids needing per-row context.
                if value.is_none() {
                    Ok(vec![None; n])
                } else {
                    let lit = value.as_ref().unwrap();
                    Ok((0..n).map(|_| Some(lit.clone())).collect())
                }
            }
            ExprNode::BinaryOp {
                op,
                left,
                right,
                dtype,
            } => {
                let lvals = left.eval(ctx, n)?;
                let rvals = right.eval(ctx, n)?;
                let mut out: Vec<Option<LiteralValue>> = Vec::with_capacity(n);

                let result_base = match dtype {
                    DTypeDesc::Scalar { base: Some(b), .. } => *b,
                    _ => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "BinaryOp result dtype base cannot be unknown.",
                        ));
                    }
                };

                for (a, b) in lvals.into_iter().zip(rvals.into_iter()) {
                    match (a, b) {
                        (None, _) | (_, None) => out.push(None),
                        (Some(va), Some(vb)) => match result_base {
                            BaseType::Int => {
                                if *op == ArithOp::Div {
                                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                        "Unexpected division producing int result.",
                                    ));
                                }

                                let (ai, bi) = match (va, vb) {
                                    (LiteralValue::Int(ai), LiteralValue::Int(bi)) => (ai, bi),
                                    _ => {
                                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                            "Typed arithmetic expected int operands for int result.",
                                        ));
                                    }
                                };

                                let res_i = match op {
                                    ArithOp::Add => ai + bi,
                                    ArithOp::Sub => ai - bi,
                                    ArithOp::Mul => ai * bi,
                                    ArithOp::Div => unreachable!(),
                                };
                                out.push(Some(LiteralValue::Int(res_i)));
                            }
                            BaseType::Float => {
                                let (af, bf) = match (va, vb) {
                                    (LiteralValue::Int(ai), LiteralValue::Int(bi)) => {
                                        (ai as f64, bi as f64)
                                    }
                                    (LiteralValue::Float(af), LiteralValue::Float(bf)) => (af, bf),
                                    (LiteralValue::Int(ai), LiteralValue::Float(bf)) => {
                                        (ai as f64, bf)
                                    }
                                    (LiteralValue::Float(af), LiteralValue::Int(bi)) => {
                                        (af, bi as f64)
                                    }
                                    _ => {
                                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                            "Typed arithmetic expected numeric operands for float result.",
                                        ));
                                    }
                                };

                                let res_f = match op {
                                    ArithOp::Add => af + bf,
                                    ArithOp::Sub => af - bf,
                                    ArithOp::Mul => af * bf,
                                    ArithOp::Div => af / bf,
                                };
                                out.push(Some(LiteralValue::Float(res_f)));
                            }
                            BaseType::DateTime => match (op, va, vb) {
                                (
                                    ArithOp::Add,
                                    LiteralValue::DateTimeMicros(dt),
                                    LiteralValue::DurationMicros(d),
                                )
                                | (
                                    ArithOp::Add,
                                    LiteralValue::DurationMicros(d),
                                    LiteralValue::DateTimeMicros(dt),
                                ) => {
                                    out.push(Some(LiteralValue::DateTimeMicros(dt + d)));
                                }
                                (
                                    ArithOp::Sub,
                                    LiteralValue::DateTimeMicros(dt),
                                    LiteralValue::DurationMicros(d),
                                ) => {
                                    out.push(Some(LiteralValue::DateTimeMicros(dt - d)));
                                }
                                _ => {
                                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                        "datetime arithmetic expects datetime ± duration.",
                                    ));
                                }
                            },
                            BaseType::Date => {
                                const US_PER_DAY: i64 = 86_400_000_000;
                                match (op, va, vb) {
                                    (
                                        ArithOp::Add,
                                        LiteralValue::DateDays(d),
                                        LiteralValue::DurationMicros(us),
                                    )
                                    | (
                                        ArithOp::Add,
                                        LiteralValue::DurationMicros(us),
                                        LiteralValue::DateDays(d),
                                    ) => {
                                        out.push(Some(LiteralValue::DateDays(
                                            d + (us / US_PER_DAY) as i32,
                                        )));
                                    }
                                    (
                                        ArithOp::Sub,
                                        LiteralValue::DateDays(d),
                                        LiteralValue::DurationMicros(us),
                                    ) => {
                                        out.push(Some(LiteralValue::DateDays(
                                            d - (us / US_PER_DAY) as i32,
                                        )));
                                    }
                                    _ => {
                                        return Err(
                                            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                                "date arithmetic expects date ± duration.",
                                            ),
                                        );
                                    }
                                }
                            }
                            _ => {
                                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                    "Unsupported BinaryOp result base.",
                                ))
                            }
                        },
                    }
                }

                Ok(out)
            }
            ExprNode::CompareOp {
                op, left, right, ..
            } => {
                let lvals = left.eval(ctx, n)?;
                let rvals = right.eval(ctx, n)?;
                let mut out: Vec<Option<LiteralValue>> = Vec::with_capacity(n);

                // Decide comparison mode based on operand base types from child dtypes.
                let left_base = left.dtype().as_scalar_base_field().flatten();
                let right_base = right.dtype().as_scalar_base_field().flatten();
                let effective_base = left_base.or(right_base).ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Comparison operand base cannot be unknown.",
                    )
                })?;

                for (a, b) in lvals.into_iter().zip(rvals.into_iter()) {
                    match (a, b) {
                        (None, _) | (_, None) => out.push(None),
                        (Some(va), Some(vb)) => {
                            let res_bool = match op {
                                CmpOp::Eq | CmpOp::Ne => {
                                    let eq = match effective_base {
                                        BaseType::Int | BaseType::Float => {
                                            let af = match va {
                                                LiteralValue::Int(i) => i as f64,
                                                LiteralValue::Float(f) => f,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed equality expected numeric operands.",
                                                    ));
                                                }
                                            };
                                            let bf = match vb {
                                                LiteralValue::Int(i) => i as f64,
                                                LiteralValue::Float(f) => f,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed equality expected numeric operands.",
                                                    ));
                                                }
                                            };
                                            af == bf
                                        }
                                        BaseType::Bool => {
                                            let ab = match va {
                                                LiteralValue::Bool(b) => b,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed equality expected bool operands.",
                                                    ));
                                                }
                                            };
                                            let bb = match vb {
                                                LiteralValue::Bool(b) => b,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed equality expected bool operands.",
                                                    ));
                                                }
                                            };
                                            ab == bb
                                        }
                                        BaseType::Str | BaseType::Enum => {
                                            let as_ = match &va {
                                                LiteralValue::Str(s) | LiteralValue::EnumStr(s) => {
                                                    s.as_str()
                                                }
                                                _ => {
                                                    return Err(PyErr::new::<
                                                            pyo3::exceptions::PyTypeError,
                                                            _,
                                                        >(
                                                            "Typed equality expected str-like operands.",
                                                        ));
                                                }
                                            };
                                            let bs_ = match &vb {
                                                LiteralValue::Str(s) | LiteralValue::EnumStr(s) => {
                                                    s.as_str()
                                                }
                                                _ => {
                                                    return Err(PyErr::new::<
                                                            pyo3::exceptions::PyTypeError,
                                                            _,
                                                        >(
                                                            "Typed equality expected str-like operands.",
                                                        ));
                                                }
                                            };
                                            as_ == bs_
                                        }
                                        BaseType::Uuid => {
                                            let as_ = match va {
                                                LiteralValue::Uuid(s) => s,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed equality expected uuid operands.",
                                                    ));
                                                }
                                            };
                                            let bs_ = match vb {
                                                LiteralValue::Uuid(s) => s,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed equality expected uuid operands.",
                                                    ));
                                                }
                                            };
                                            as_ == bs_
                                        }
                                        BaseType::Decimal => {
                                            let ai = match va {
                                                LiteralValue::Decimal(i) => *i,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed equality expected decimal operands.",
                                                    ));
                                                }
                                            };
                                            let bi = match vb {
                                                LiteralValue::Decimal(i) => *i,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed equality expected decimal operands.",
                                                    ));
                                                }
                                            };
                                            ai == bi
                                        }
                                        BaseType::DateTime => {
                                            match (va, vb) {
                                                (
                                                    LiteralValue::DateTimeMicros(a),
                                                    LiteralValue::DateTimeMicros(b),
                                                ) => a == b,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                    pyo3::exceptions::PyTypeError,
                                                    _,
                                                >("Typed equality expected datetime operands."));
                                                }
                                            }
                                        }
                                        BaseType::Date => match (va, vb) {
                                            (
                                                LiteralValue::DateDays(a),
                                                LiteralValue::DateDays(b),
                                            ) => a == b,
                                            _ => {
                                                return Err(PyErr::new::<
                                                    pyo3::exceptions::PyTypeError,
                                                    _,
                                                >(
                                                    "Typed equality expected date operands.",
                                                ));
                                            }
                                        },
                                        BaseType::Duration => {
                                            match (va, vb) {
                                                (
                                                    LiteralValue::DurationMicros(a),
                                                    LiteralValue::DurationMicros(b),
                                                ) => a == b,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                    pyo3::exceptions::PyTypeError,
                                                    _,
                                                >("Typed equality expected duration operands."));
                                                }
                                            }
                                        }
                                    };
                                    if *op == CmpOp::Eq {
                                        eq
                                    } else {
                                        !eq
                                    }
                                }
                                CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge => {
                                    match effective_base {
                                        BaseType::Int | BaseType::Float => {
                                            let af = match va {
                                                LiteralValue::Int(i) => i as f64,
                                                LiteralValue::Float(f) => f,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed ordering expected numeric operands.",
                                                    ));
                                                }
                                            };
                                            let bf = match vb {
                                                LiteralValue::Int(i) => i as f64,
                                                LiteralValue::Float(f) => f,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed ordering expected numeric operands.",
                                                    ));
                                                }
                                            };
                                            match op {
                                                CmpOp::Lt => af < bf,
                                                CmpOp::Le => af <= bf,
                                                CmpOp::Gt => af > bf,
                                                CmpOp::Ge => af >= bf,
                                                CmpOp::Eq | CmpOp::Ne => unreachable!(),
                                            }
                                        }
                                        BaseType::Str | BaseType::Enum => {
                                            let as_ = match &va {
                                                LiteralValue::Str(s) | LiteralValue::EnumStr(s) => {
                                                    s.as_str()
                                                }
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed ordering expected str-like operands.",
                                                    ));
                                                }
                                            };
                                            let bs_ = match &vb {
                                                LiteralValue::Str(s) | LiteralValue::EnumStr(s) => {
                                                    s.as_str()
                                                }
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed ordering expected str-like operands.",
                                                    ));
                                                }
                                            };
                                            match op {
                                                CmpOp::Lt => as_ < bs_,
                                                CmpOp::Le => as_ <= bs_,
                                                CmpOp::Gt => as_ > bs_,
                                                CmpOp::Ge => as_ >= bs_,
                                                CmpOp::Eq | CmpOp::Ne => unreachable!(),
                                            }
                                        }
                                        BaseType::Uuid => {
                                            let as_ = match va {
                                                LiteralValue::Uuid(s) => s,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed ordering expected uuid operands.",
                                                    ));
                                                }
                                            };
                                            let bs_ = match vb {
                                                LiteralValue::Uuid(s) => s,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed ordering expected uuid operands.",
                                                    ));
                                                }
                                            };
                                            match op {
                                                CmpOp::Lt => as_ < bs_,
                                                CmpOp::Le => as_ <= bs_,
                                                CmpOp::Gt => as_ > bs_,
                                                CmpOp::Ge => as_ >= bs_,
                                                CmpOp::Eq | CmpOp::Ne => unreachable!(),
                                            }
                                        }
                                        BaseType::Decimal => {
                                            let ai = match va {
                                                LiteralValue::Decimal(i) => *i,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed ordering expected decimal operands.",
                                                    ));
                                                }
                                            };
                                            let bi = match vb {
                                                LiteralValue::Decimal(i) => *i,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed ordering expected decimal operands.",
                                                    ));
                                                }
                                            };
                                            match op {
                                                CmpOp::Lt => ai < bi,
                                                CmpOp::Le => ai <= bi,
                                                CmpOp::Gt => ai > bi,
                                                CmpOp::Ge => ai >= bi,
                                                CmpOp::Eq | CmpOp::Ne => unreachable!(),
                                            }
                                        }
                                        BaseType::DateTime => {
                                            let (a, b) = match (va, vb) {
                                                (
                                                    LiteralValue::DateTimeMicros(a),
                                                    LiteralValue::DateTimeMicros(b),
                                                ) => (a, b),
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed ordering expected datetime operands.",
                                                    ));
                                                }
                                            };
                                            match op {
                                                CmpOp::Lt => a < b,
                                                CmpOp::Le => a <= b,
                                                CmpOp::Gt => a > b,
                                                CmpOp::Ge => a >= b,
                                                _ => false,
                                            }
                                        }
                                        BaseType::Date => {
                                            let (a, b) =
                                                match (va, vb) {
                                                    (
                                                        LiteralValue::DateDays(a),
                                                        LiteralValue::DateDays(b),
                                                    ) => (a, b),
                                                    _ => {
                                                        return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >("Typed ordering expected date operands."));
                                                    }
                                                };
                                            match op {
                                                CmpOp::Lt => a < b,
                                                CmpOp::Le => a <= b,
                                                CmpOp::Gt => a > b,
                                                CmpOp::Ge => a >= b,
                                                _ => false,
                                            }
                                        }
                                        BaseType::Duration => {
                                            let (a, b) = match (va, vb) {
                                                (
                                                    LiteralValue::DurationMicros(a),
                                                    LiteralValue::DurationMicros(b),
                                                ) => (a, b),
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed ordering expected duration operands.",
                                                    ));
                                                }
                                            };
                                            match op {
                                                CmpOp::Lt => a < b,
                                                CmpOp::Le => a <= b,
                                                CmpOp::Gt => a > b,
                                                CmpOp::Ge => a >= b,
                                                _ => false,
                                            }
                                        }
                                        _ => {
                                            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                            "Ordering operand types not supported by typed skeleton.",
                                        ));
                                        }
                                    }
                                }
                            };

                            out.push(Some(LiteralValue::Bool(res_bool)));
                        }
                    }
                }

                Ok(out)
            }
            ExprNode::Cast { input, dtype } => {
                let vals = input.eval(ctx, n)?;
                let mut out = Vec::with_capacity(n);
                let target = match dtype {
                    DTypeDesc::Scalar { base: Some(b), .. } => *b,
                    _ => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "cast() target dtype must have known base.",
                        ));
                    }
                };
                for v in vals.into_iter() {
                    match v {
                        None => out.push(None),
                        Some(v) => out.push(Some(cast_literal_value(v, target)?)),
                    }
                }
                Ok(out)
            }
            ExprNode::IsNull { input, .. } => {
                let vals = input.eval(ctx, n)?;
                Ok(vals
                    .into_iter()
                    .map(|v| Some(LiteralValue::Bool(v.is_none())))
                    .collect())
            }
            ExprNode::IsNotNull { input, .. } => {
                let vals = input.eval(ctx, n)?;
                Ok(vals
                    .into_iter()
                    .map(|v| Some(LiteralValue::Bool(v.is_some())))
                    .collect())
            }
            ExprNode::Coalesce { exprs, .. } => {
                let mut cols: Vec<Vec<Option<LiteralValue>>> = Vec::new();
                for e in exprs {
                    cols.push(e.eval(ctx, n)?);
                }
                let mut out: Vec<Option<LiteralValue>> = vec![None; n];
                for (i, out_slot) in out.iter_mut().enumerate() {
                    for c in &cols {
                        if let Some(Some(lv)) = c.get(i) {
                            *out_slot = Some(lv.clone());
                            break;
                        }
                    }
                }
                Ok(out)
            }
            ExprNode::CaseWhen {
                branches, else_, ..
            } => {
                let mut cond_cols: Vec<Vec<Option<LiteralValue>>> = Vec::new();
                let mut then_cols: Vec<Vec<Option<LiteralValue>>> = Vec::new();
                for (c, t) in branches {
                    cond_cols.push(c.eval(ctx, n)?);
                    then_cols.push(t.eval(ctx, n)?);
                }
                let else_v = else_.eval(ctx, n)?;
                let mut out: Vec<Option<LiteralValue>> = Vec::with_capacity(n);
                for i in 0..n {
                    let mut picked: Option<LiteralValue> = None;
                    for (cc, tc) in cond_cols.iter().zip(then_cols.iter()) {
                        let cond = cc.get(i).and_then(|x| x.as_ref());
                        match cond {
                            Some(LiteralValue::Bool(true)) => {
                                picked = tc.get(i).and_then(|x| x.clone());
                                break;
                            }
                            Some(LiteralValue::Bool(false)) | None => {}
                            _ => {
                                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                    "CASE WHEN expects boolean conditions.",
                                ));
                            }
                        }
                    }
                    if picked.is_none() {
                        picked = else_v.get(i).and_then(|x| x.clone());
                    }
                    out.push(picked);
                }
                Ok(out)
            }
            ExprNode::InList { inner, values, .. } => {
                let vals = inner.eval(ctx, n)?;
                Ok(vals
                    .into_iter()
                    .map(|v| v.map(|x| LiteralValue::Bool(values.iter().any(|u| u == &x))))
                    .collect())
            }
            ExprNode::Between {
                inner, low, high, ..
            } => {
                let iv = inner.eval(ctx, n)?;
                let lv = low.eval(ctx, n)?;
                let hv = high.eval(ctx, n)?;
                let mut out = Vec::with_capacity(n);
                for i in 0..n {
                    let tri = match (
                        iv.get(i).and_then(|x| x.as_ref()),
                        lv.get(i).and_then(|x| x.as_ref()),
                        hv.get(i).and_then(|x| x.as_ref()),
                    ) {
                        (Some(a), Some(b), Some(c)) => Some(literal_between_inclusive(a, b, c)),
                        _ => None,
                    };
                    out.push(tri.map(LiteralValue::Bool));
                }
                Ok(out)
            }
            ExprNode::StringConcat { parts, .. } => {
                let part_vals: Vec<Vec<Option<LiteralValue>>> = parts
                    .iter()
                    .map(|p| p.eval(ctx, n))
                    .collect::<PyResult<_>>()?;
                let mut out: Vec<Option<LiteralValue>> = Vec::with_capacity(n);
                for i in 0..n {
                    let mut buf = String::new();
                    let mut miss = false;
                    for col in &part_vals {
                        match col.get(i).and_then(|x| x.as_ref()) {
                            None => {
                                miss = true;
                                break;
                            }
                            Some(LiteralValue::Str(s)) | Some(LiteralValue::EnumStr(s)) => {
                                buf.push_str(s)
                            }
                            Some(LiteralValue::Uuid(s)) => buf.push_str(s),
                            Some(LiteralValue::Decimal(d)) => {
                                buf.push_str(&crate::dtype::scaled_i128_to_decimal_string(*d))
                            }
                            Some(LiteralValue::Int(v)) => buf.push_str(&v.to_string()),
                            Some(LiteralValue::Float(v)) => buf.push_str(&v.to_string()),
                            Some(LiteralValue::Bool(v)) => buf.push_str(&v.to_string()),
                            _ => {
                                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                    "concat() expected string or coercible scalar parts.",
                                ));
                            }
                        }
                    }
                    if miss {
                        out.push(None);
                    } else {
                        out.push(Some(LiteralValue::Str(buf)));
                    }
                }
                Ok(out)
            }
            ExprNode::Substring {
                inner,
                start,
                length,
                ..
            } => {
                let svals = inner.eval(ctx, n)?;
                let stvals = start.eval(ctx, n)?;
                let lnvals = if let Some(l) = length {
                    Some(l.eval(ctx, n)?)
                } else {
                    None
                };
                let mut out: Vec<Option<LiteralValue>> = Vec::with_capacity(n);
                for i in 0..n {
                    let res = match (
                        svals.get(i).and_then(|x| x.as_ref()),
                        stvals.get(i).and_then(|x| x.as_ref()),
                    ) {
                        (
                            Some(LiteralValue::Str(s)) | Some(LiteralValue::EnumStr(s)),
                            Some(LiteralValue::Int(st)),
                        ) => {
                            let ln = lnvals
                                .as_ref()
                                .and_then(|c| c.get(i).and_then(|x| x.as_ref()));
                            let ln_i = match ln {
                                Some(LiteralValue::Int(v)) => Some(*v),
                                None => None,
                                _ => {
                                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                        "substring length must be int.",
                                    ));
                                }
                            };
                            let slice_spark = |text: &str, start1: i64, len: Option<i64>| {
                                if start1 < 1 {
                                    return String::new();
                                }
                                let idx = (start1 - 1) as usize;
                                if idx >= text.len() {
                                    return String::new();
                                }
                                let rest = &text[idx..];
                                match len {
                                    None => rest.to_string(),
                                    Some(l) if l <= 0 => String::new(),
                                    Some(l) => rest.chars().take(l as usize).collect(),
                                }
                            };
                            Some(LiteralValue::Str(slice_spark(s, *st, ln_i)))
                        }
                        (None, _) | (_, None) => None,
                        _ => {
                            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                "substring expects string and int start.",
                            ));
                        }
                    };
                    out.push(res);
                }
                Ok(out)
            }
            ExprNode::StringLength { inner, .. } => {
                let vals = inner.eval(ctx, n)?;
                Ok(vals
                    .into_iter()
                    .map(|v| match v {
                        None => None,
                        Some(LiteralValue::Str(s)) | Some(LiteralValue::EnumStr(s)) => {
                            Some(LiteralValue::Int(s.chars().count() as i64))
                        }
                        Some(LiteralValue::Uuid(s)) => {
                            Some(LiteralValue::Int(s.chars().count() as i64))
                        }
                        Some(LiteralValue::Decimal(d)) => Some(LiteralValue::Int(
                            crate::dtype::scaled_i128_to_decimal_string(*d)
                                .chars()
                                .count() as i64,
                        )),
                        _ => None,
                    })
                    .collect())
            }
            ExprNode::StringReplace {
                inner,
                pattern,
                replacement,
                ..
            } => {
                let vals = inner.eval(ctx, n)?;
                let pat = pattern.as_str();
                let rep = replacement.as_str();
                Ok(vals
                    .into_iter()
                    .map(|v| match v {
                        None => None,
                        Some(LiteralValue::Str(s)) | Some(LiteralValue::EnumStr(s)) => {
                            Some(LiteralValue::Str(s.replace(pat, rep)))
                        }
                        _ => None,
                    })
                    .collect())
            }
            ExprNode::UnaryNumeric { op, inner, .. } => {
                let vals = inner.eval(ctx, n)?;
                let round_f = |f: f64, decimals: u32| {
                    let m = 10f64.powi(decimals as i32);
                    (f * m).round() / m
                };
                Ok(vals
                    .into_iter()
                    .map(|v| match (op, v) {
                        (UnaryNumericOp::Abs, Some(LiteralValue::Int(i))) => {
                            Some(LiteralValue::Int(i.abs()))
                        }
                        (UnaryNumericOp::Abs, Some(LiteralValue::Float(f))) => {
                            Some(LiteralValue::Float(f.abs()))
                        }
                        (UnaryNumericOp::Round { decimals }, Some(LiteralValue::Int(i))) => {
                            Some(LiteralValue::Int(round_f(i as f64, *decimals) as i64))
                        }
                        (UnaryNumericOp::Round { decimals }, Some(LiteralValue::Float(f))) => {
                            Some(LiteralValue::Float(round_f(f, *decimals)))
                        }
                        (UnaryNumericOp::Floor, Some(LiteralValue::Int(i))) => {
                            Some(LiteralValue::Float((i as f64).floor()))
                        }
                        (UnaryNumericOp::Floor, Some(LiteralValue::Float(f))) => {
                            Some(LiteralValue::Float(f.floor()))
                        }
                        (UnaryNumericOp::Ceil, Some(LiteralValue::Int(i))) => {
                            Some(LiteralValue::Float((i as f64).ceil()))
                        }
                        (UnaryNumericOp::Ceil, Some(LiteralValue::Float(f))) => {
                            Some(LiteralValue::Float(f.ceil()))
                        }
                        _ => None,
                    })
                    .collect())
            }
            ExprNode::StringUnary { op, inner, .. } => {
                let vals = inner.eval(ctx, n)?;
                Ok(vals
                    .into_iter()
                    .map(|v| {
                        let str_like = |s: &str| match op {
                            StringUnaryOp::Strip => Some(LiteralValue::Str(s.trim().to_string())),
                            StringUnaryOp::Upper => Some(LiteralValue::Str(s.to_uppercase())),
                            StringUnaryOp::Lower => Some(LiteralValue::Str(s.to_lowercase())),
                            StringUnaryOp::StripPrefix(ref p) => Some(LiteralValue::Str(
                                s.strip_prefix(p.as_str()).unwrap_or(s).to_string(),
                            )),
                            StringUnaryOp::StripSuffix(ref suf) => Some(LiteralValue::Str(
                                s.strip_suffix(suf.as_str()).unwrap_or(s).to_string(),
                            )),
                            StringUnaryOp::StripChars(ref c) => {
                                Some(LiteralValue::Str(trim_matches_char_set(s, c)))
                            }
                        };
                        match v {
                            Some(LiteralValue::Str(s)) => str_like(s.as_str()),
                            Some(LiteralValue::EnumStr(s)) => str_like(s.as_str()),
                            _ => None,
                        }
                    })
                    .collect())
            }
            ExprNode::LogicalBinary {
                op, left, right, ..
            } => {
                let lv = left.eval(ctx, n)?;
                let rv = right.eval(ctx, n)?;
                let mut out = Vec::with_capacity(n);
                for i in 0..n {
                    let lb = lv.get(i).and_then(|x| x.as_ref());
                    let rb = rv.get(i).and_then(|x| x.as_ref());
                    let res = match (op, lb, rb) {
                        (
                            LogicalOp::And,
                            Some(LiteralValue::Bool(a)),
                            Some(LiteralValue::Bool(b)),
                        ) => Some(LiteralValue::Bool(*a && *b)),
                        (
                            LogicalOp::Or,
                            Some(LiteralValue::Bool(a)),
                            Some(LiteralValue::Bool(b)),
                        ) => Some(LiteralValue::Bool(*a || *b)),
                        _ => None,
                    };
                    out.push(res);
                }
                Ok(out)
            }
            ExprNode::LogicalNot { inner, .. } => {
                let vals = inner.eval(ctx, n)?;
                Ok(vals
                    .into_iter()
                    .map(|v| match v {
                        Some(LiteralValue::Bool(b)) => Some(LiteralValue::Bool(!b)),
                        _ => None,
                    })
                    .collect())
            }
            ExprNode::DatetimeToDate { inner, .. } => {
                let vals = inner.eval(ctx, n)?;
                Ok(vals
                    .into_iter()
                    .map(|v| match v {
                        None => None,
                        Some(LiteralValue::DateTimeMicros(us)) => {
                            Some(LiteralValue::DateDays((us / 86_400_000_000) as i32))
                        }
                        _ => None,
                    })
                    .collect())
            }
            ExprNode::TemporalPart { part, inner, .. } => {
                let vals = inner.eval(ctx, n)?;
                let is_date =
                    inner.dtype().as_scalar_base_field().flatten() == Some(BaseType::Date);
                let is_time =
                    inner.dtype().as_scalar_base_field().flatten() == Some(BaseType::Time);
                const NS_PER_HOUR: i64 = 3_600_000_000_000;
                const NS_PER_MIN: i64 = 60_000_000_000;
                const NS_PER_SEC: i64 = 1_000_000_000;
                Ok(vals
                    .into_iter()
                    .map(|v| match v {
                        None => None,
                        Some(LiteralValue::DateTimeMicros(us)) if !is_date && !is_time => {
                            let (y, mo, d, h, mi, s) = utc_ymdhms_from_unix_micros(us);
                            let i = match part {
                                TemporalPart::Year => i64::from(y),
                                TemporalPart::Month => i64::from(mo),
                                TemporalPart::Day => i64::from(d),
                                TemporalPart::Hour => i64::from(h),
                                TemporalPart::Minute => i64::from(mi),
                                TemporalPart::Second => i64::from(s),
                                TemporalPart::Nanosecond => {
                                    let sub_us = us.rem_euclid(1_000_000);
                                    i64::from(sub_us) * 1000
                                }
                            };
                            Some(LiteralValue::Int(i))
                        }
                        Some(LiteralValue::DateDays(days)) if is_date => match part {
                            TemporalPart::Hour
                            | TemporalPart::Minute
                            | TemporalPart::Second
                            | TemporalPart::Nanosecond => None,
                            TemporalPart::Year | TemporalPart::Month | TemporalPart::Day => {
                                let (y, mo, d) = utc_calendar_from_epoch_days(days);
                                let i = match part {
                                    TemporalPart::Year => i64::from(y),
                                    TemporalPart::Month => i64::from(mo),
                                    TemporalPart::Day => i64::from(d),
                                    _ => unreachable!(),
                                };
                                Some(LiteralValue::Int(i))
                            }
                        },
                        Some(LiteralValue::TimeNanos(ns)) if is_time => match part {
                            TemporalPart::Year | TemporalPart::Month | TemporalPart::Day => None,
                            TemporalPart::Hour => {
                                Some(LiteralValue::Int((ns / NS_PER_HOUR).rem_euclid(24)))
                            }
                            TemporalPart::Minute => {
                                Some(LiteralValue::Int((ns / NS_PER_MIN).rem_euclid(60)))
                            }
                            TemporalPart::Second => {
                                Some(LiteralValue::Int((ns / NS_PER_SEC).rem_euclid(60)))
                            }
                            TemporalPart::Nanosecond => {
                                Some(LiteralValue::Int(ns.rem_euclid(NS_PER_SEC)))
                            }
                        },
                        _ => None,
                    })
                    .collect())
            }
            ExprNode::ListLen { .. }
            | ExprNode::ListGet { .. }
            | ExprNode::ListContains { .. }
            | ExprNode::ListMin { .. }
            | ExprNode::ListMax { .. }
            | ExprNode::ListSum { .. }
            | ExprNode::Strptime { .. }
            | ExprNode::UnixTimestamp { .. }
            | ExprNode::BinaryLength { .. }
            | ExprNode::MapLen { .. }
            | ExprNode::MapGet { .. }
            | ExprNode::MapContainsKey { .. }
            | ExprNode::MapKeys { .. }
            | ExprNode::MapValues { .. } => {
                Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                    "This expression is only supported with the Polars execution engine.",
                ))
            }
            ExprNode::StructField { .. } => {
                Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                    "Struct field access is only supported with the Polars execution engine.",
                ))
            }
            ExprNode::Window { .. } => {
                Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                    "Window expressions are only supported with the Polars execution engine.",
                ))
            }
            ExprNode::GlobalAgg { .. } | ExprNode::GlobalRowCount { .. } => Err(PyErr::new::<
                pyo3::exceptions::PyNotImplementedError,
                _,
            >(
                "Global aggregate expressions are only supported with the Polars execution engine.",
            )),
        }
    }
}

/// Gregorian calendar date to Julian day number (proleptic), matching Python/Wikipedia formula.
#[cfg(not(feature = "polars_engine"))]
fn civil_to_jdn(y: i32, m: i32, d: i32) -> i32 {
    let a = (14 - m) / 12;
    let y = y + 4800 - a;
    let m = m + 12 * a - 3;
    d + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045
}

/// [`LiteralValue::DateDays`]: days since Unix epoch (1970-01-01).
#[cfg(not(feature = "polars_engine"))]
fn parse_iso8601_date_str_to_unix_days(s: &str) -> PyResult<i32> {
    let mut parts = s.trim().split('-');
    let y: i32 = parts
        .next()
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() str to date: expected YYYY-MM-DD.",
            )
        })?
        .parse()
        .map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() str to date: invalid year in YYYY-MM-DD.",
            )
        })?;
    let mo: i32 = parts
        .next()
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() str to date: expected YYYY-MM-DD.",
            )
        })?
        .parse()
        .map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() str to date: invalid month in YYYY-MM-DD.",
            )
        })?;
    let d: i32 = parts
        .next()
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() str to date: expected YYYY-MM-DD.",
            )
        })?
        .parse()
        .map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() str to date: invalid day in YYYY-MM-DD.",
            )
        })?;
    if parts.next().is_some() {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "cast() str to date: trailing text in date string.",
        ));
    }
    const UNIX_EPOCH_JDN: i32 = 2_440_588;
    Ok(civil_to_jdn(y, mo, d) - UNIX_EPOCH_JDN)
}

/// Time-of-day to microseconds since midnight; supports optional fractional seconds (up to 6 digits = µs).
#[cfg(not(feature = "polars_engine"))]
fn parse_hms_str_to_micros_in_day(time_s: &str) -> PyResult<i64> {
    let time_s = time_s.trim().trim_end_matches('Z');
    // Strip trailing `+hh:mm` / `-hh:mm` offsets; naive strings are the common case.
    let time_s = time_s
        .split_once('+')
        .map(|(a, _)| a)
        .unwrap_or(time_s)
        .trim();
    let (base, frac) = match time_s.split_once('.') {
        Some((b, f)) => (b, Some(f)),
        None => (time_s, None),
    };
    let mut iter = base.split(':');
    let h: i64 = iter
        .next()
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() str to datetime: expected HH:MM:SS time.",
            )
        })?
        .parse()
        .map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("cast() str to datetime: invalid hour.")
        })?;
    let mi: i64 = iter
        .next()
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() str to datetime: expected HH:MM:SS time.",
            )
        })?
        .parse()
        .map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() str to datetime: invalid minute.",
            )
        })?;
    let sec: i64 = iter
        .next()
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() str to datetime: expected HH:MM:SS time.",
            )
        })?
        .parse()
        .map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() str to datetime: invalid second.",
            )
        })?;
    if iter.next().is_some() {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "cast() str to datetime: invalid time (extra segments).",
        ));
    }
    let mut micros = h * 3_600_000_000 + mi * 60_000_000 + sec * 1_000_000;
    if let Some(f) = frac {
        let digits: String = f.chars().take_while(|c| c.is_ascii_digit()).collect();
        if digits.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() str to datetime: invalid fractional seconds.",
            ));
        }
        let mut padded = digits;
        if padded.len() > 6 {
            padded.truncate(6);
        }
        while padded.len() < 6 {
            padded.push('0');
        }
        let frac_us: i64 = padded.parse().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() str to datetime: invalid fractional seconds.",
            )
        })?;
        micros += frac_us;
    }
    Ok(micros)
}

#[cfg(not(feature = "polars_engine"))]
fn parse_iso8601_datetime_str_to_unix_micros(s: &str) -> PyResult<i64> {
    let s = s.trim();
    let (date_part, time_part) = if let Some(i) = s.find('T') {
        (&s[..i], &s[i + 1..])
    } else if let Some(i) = s.find(' ') {
        (&s[..i], &s[i + 1..])
    } else {
        let days = parse_iso8601_date_str_to_unix_days(s)?;
        return Ok(i64::from(days) * 86_400_000_000);
    };
    let days = parse_iso8601_date_str_to_unix_days(date_part)?;
    let micros_day = parse_hms_str_to_micros_in_day(time_part)?;
    Ok(i64::from(days) * 86_400_000_000 + micros_day)
}

#[cfg(not(feature = "polars_engine"))]
fn cast_literal_value(v: LiteralValue, target: BaseType) -> PyResult<LiteralValue> {
    match target {
        BaseType::Int => match v {
            LiteralValue::Int(i) => Ok(LiteralValue::Int(i)),
            LiteralValue::Float(f) => Ok(LiteralValue::Int(f as i64)),
            LiteralValue::Bool(b) => Ok(LiteralValue::Int(if b { 1 } else { 0 })),
            LiteralValue::Str(s) => s.parse::<i64>().map(LiteralValue::Int).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("Cannot cast str to int.")
            }),
            LiteralValue::DateTimeMicros(_)
            | LiteralValue::DateDays(_)
            | LiteralValue::DurationMicros(_)
            | LiteralValue::Uuid(_)
            | LiteralValue::Decimal(_)
            | LiteralValue::EnumStr(_) => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Cannot cast temporal literal to int.",
            )),
        },
        BaseType::Float => match v {
            LiteralValue::Int(i) => Ok(LiteralValue::Float(i as f64)),
            LiteralValue::Float(f) => Ok(LiteralValue::Float(f)),
            LiteralValue::Bool(b) => Ok(LiteralValue::Float(if b { 1.0 } else { 0.0 })),
            LiteralValue::Str(s) => s.parse::<f64>().map(LiteralValue::Float).map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("Cannot cast str to float.")
            }),
            LiteralValue::DateTimeMicros(_)
            | LiteralValue::DateDays(_)
            | LiteralValue::DurationMicros(_)
            | LiteralValue::Uuid(_)
            | LiteralValue::Decimal(_)
            | LiteralValue::EnumStr(_) => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Cannot cast temporal literal to float.",
            )),
        },
        BaseType::Bool => match v {
            LiteralValue::Int(i) => Ok(LiteralValue::Bool(i != 0)),
            LiteralValue::Float(f) => Ok(LiteralValue::Bool(f != 0.0)),
            LiteralValue::Bool(b) => Ok(LiteralValue::Bool(b)),
            LiteralValue::Str(s) => match s.to_lowercase().as_str() {
                "true" | "1" => Ok(LiteralValue::Bool(true)),
                "false" | "0" => Ok(LiteralValue::Bool(false)),
                _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Cannot cast str to bool.",
                )),
            },
            LiteralValue::DateTimeMicros(_)
            | LiteralValue::DateDays(_)
            | LiteralValue::DurationMicros(_)
            | LiteralValue::Uuid(_)
            | LiteralValue::Decimal(_)
            | LiteralValue::EnumStr(_) => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Cannot cast temporal literal to bool.",
            )),
        },
        BaseType::Str => match v {
            LiteralValue::Int(i) => Ok(LiteralValue::Str(i.to_string())),
            LiteralValue::Float(f) => Ok(LiteralValue::Str(f.to_string())),
            LiteralValue::Bool(b) => Ok(LiteralValue::Str(b.to_string())),
            LiteralValue::Str(s) => Ok(LiteralValue::Str(s)),
            LiteralValue::EnumStr(s) => Ok(LiteralValue::Str(s)),
            LiteralValue::Uuid(s) => Ok(LiteralValue::Str(s)),
            LiteralValue::Decimal(d) => Ok(LiteralValue::Str(
                crate::dtype::scaled_i128_to_decimal_string(d),
            )),
            LiteralValue::DateTimeMicros(us) => Ok(LiteralValue::Str(format!("{us}"))),
            LiteralValue::DateDays(d) => Ok(LiteralValue::Str(format!("{d}"))),
            LiteralValue::DurationMicros(us) => Ok(LiteralValue::Str(format!("{us}"))),
        },
        BaseType::Enum => match v {
            LiteralValue::EnumStr(s) => Ok(LiteralValue::EnumStr(s)),
            LiteralValue::Str(s) => Ok(LiteralValue::EnumStr(s)),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() to enum supports str-like literals only.",
            )),
        },
        BaseType::Uuid => match v {
            LiteralValue::Uuid(s) => Ok(LiteralValue::Uuid(s)),
            LiteralValue::Str(s) => Ok(LiteralValue::Uuid(s)),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() to uuid supports uuid or str literals only.",
            )),
        },
        BaseType::Decimal => match v {
            LiteralValue::Decimal(d) => Ok(LiteralValue::Decimal(d)),
            LiteralValue::Int(i) => Ok(LiteralValue::Decimal(
                i as i128 * 10_i128.pow(crate::dtype::DECIMAL_SCALE as u32),
            )),
            LiteralValue::Float(f) => Ok(LiteralValue::Decimal(
                (f * 10_f64.powi(crate::dtype::DECIMAL_SCALE as i32)).round() as i128,
            )),
            LiteralValue::Str(s) => {
                let t = s.trim();
                if t.is_empty() {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Cannot cast empty str to decimal.",
                    ));
                }
                let neg = t.starts_with('-');
                let t = t.trim_start_matches('-');
                let (whole_s, frac_s) = match t.split_once('.') {
                    Some((w, f)) => (w, f),
                    None => (t, ""),
                };
                let whole: i128 = whole_s.parse().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>("Cannot cast str to decimal.")
                })?;
                let mut frac = frac_s
                    .chars()
                    .take(crate::dtype::DECIMAL_SCALE)
                    .collect::<String>();
                while frac.len() < crate::dtype::DECIMAL_SCALE {
                    frac.push('0');
                }
                let frac_v: i128 = frac.parse().unwrap_or(0);
                let p = 10_i128.pow(crate::dtype::DECIMAL_SCALE as u32);
                let mut v = whole * p + frac_v;
                if neg {
                    v = -v;
                }
                Ok(LiteralValue::Decimal(v))
            }
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() to decimal supports decimal, int, float, or str literals only.",
            )),
        },
        BaseType::Date => match v {
            LiteralValue::DateDays(d) => Ok(LiteralValue::DateDays(d)),
            LiteralValue::DateTimeMicros(us) => {
                let days = (us / 86_400_000_000) as i32;
                Ok(LiteralValue::DateDays(days))
            }
            LiteralValue::Str(s) => Ok(LiteralValue::DateDays(
                parse_iso8601_date_str_to_unix_days(s.trim())?,
            )),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() to date supports date, datetime, or ISO-8601 date str literals.",
            )),
        },
        BaseType::DateTime => match v {
            LiteralValue::DateTimeMicros(us) => Ok(LiteralValue::DateTimeMicros(us)),
            LiteralValue::Str(s) => Ok(LiteralValue::DateTimeMicros(
                parse_iso8601_datetime_str_to_unix_micros(s.trim())?,
            )),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() to datetime supports datetime or ISO-8601 datetime str literals.",
            )),
        },
        BaseType::Duration => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Row-wise cast() for duration target is not supported.",
        )),
    }
}
