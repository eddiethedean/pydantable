//! Typing rules, `ExprNode::make_*`, [`ExprNode::eval`], and [`ExprNode::referenced_columns`].

#[cfg(not(feature = "polars_engine"))]
use std::collections::HashMap;
use std::collections::HashSet;

use pyo3::prelude::*;

use crate::dtype::{
    dtype_structural_eq, widen_scalar_drop_literals, BaseType, DTypeDesc, LiteralSet,
};

use super::ir::{
    ArithOp, CmpOp, ExprNode, GlobalAggOp, LiteralValue, LogicalOp, RowAccumOp,
    StringPredicateKind, StringUnaryOp, TemporalPart, UnaryNumericOp, UnixTimestampUnit,
    WindowFrame, WindowOp, WindowOrderKey,
};

#[cfg(not(feature = "polars_engine"))]
#[path = "rowwise_support.rs"]
mod rowwise_support;

#[cfg(not(feature = "polars_engine"))]
use rowwise_support::*;

#[cfg(not(feature = "polars_engine"))]
#[path = "eval_rowwise.rs"]
mod eval_rowwise;

enum ListAggKind {
    Min,
    Max,
    Sum,
}

fn dtype_is_string_like(dtype: &DTypeDesc) -> bool {
    matches!(
        dtype.as_scalar_base_field().flatten(),
        Some(BaseType::Str | BaseType::Enum | BaseType::Uuid | BaseType::Ipv4 | BaseType::Ipv6)
    )
}

fn literal_set_contains(ls: &LiteralSet, v: &LiteralValue) -> bool {
    match (ls, v) {
        (LiteralSet::Str(vals), LiteralValue::Str(s)) => vals.iter().any(|x| x == s),
        (LiteralSet::Str(vals), LiteralValue::EnumStr(s)) => vals.iter().any(|x| x == s),
        (LiteralSet::Int(vals), LiteralValue::Int(i)) => vals.contains(i),
        (LiteralSet::Bool(vals), LiteralValue::Bool(b)) => vals.contains(b),
        _ => false,
    }
}

fn validate_literal_membership_compare(
    left: &ExprNode,
    right: &ExprNode,
    op: CmpOp,
) -> PyResult<()> {
    if !matches!(op, CmpOp::Eq | CmpOp::Ne) {
        return Ok(());
    }
    for (col_side, lit_side) in [(left, right), (right, left)] {
        if let ExprNode::ColumnRef { dtype, .. } = col_side {
            if let Some(ls) = dtype.literals() {
                if let ExprNode::Literal { value: Some(v), .. } = lit_side {
                    if !literal_set_contains(ls, v) {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Comparison literal is not in the column typing.Literal[...] value set.",
                        ));
                    }
                }
            }
        }
    }
    Ok(())
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
            | ExprNode::StringPredicate { dtype, .. }
            | ExprNode::StructField { dtype, .. }
            | ExprNode::StructJsonEncode { dtype, .. }
            | ExprNode::StructJsonPathMatch { dtype, .. }
            | ExprNode::StructRenameFields { dtype, .. }
            | ExprNode::StructWithFields { dtype, .. }
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
            | ExprNode::ListMean { dtype, .. }
            | ExprNode::ListJoin { dtype, .. }
            | ExprNode::ListSort { dtype, .. }
            | ExprNode::ListUnique { dtype, .. }
            | ExprNode::StringSplit { dtype, .. }
            | ExprNode::StringExtract { dtype, .. }
            | ExprNode::StringJsonPathMatch { dtype, .. }
            | ExprNode::StringJsonDecode { dtype, .. }
            | ExprNode::DatetimeToDate { dtype, .. }
            | ExprNode::Strptime { dtype, .. }
            | ExprNode::UnixTimestamp { dtype, .. }
            | ExprNode::FromUnixTime { dtype, .. }
            | ExprNode::BinaryLength { dtype, .. }
            | ExprNode::MapLen { dtype, .. }
            | ExprNode::MapGet { dtype, .. }
            | ExprNode::MapContainsKey { dtype, .. }
            | ExprNode::MapKeys { dtype, .. }
            | ExprNode::MapValues { dtype, .. }
            | ExprNode::MapEntries { dtype, .. }
            | ExprNode::MapFromEntries { dtype, .. }
            | ExprNode::Window { dtype, .. }
            | ExprNode::RowAccum { dtype, .. }
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
            | ExprNode::StringPredicate { inner, .. }
            | ExprNode::UnaryNumeric { inner, .. }
            | ExprNode::StringUnary { inner, .. }
            | ExprNode::LogicalNot { inner, .. }
            | ExprNode::TemporalPart { inner, .. }
            | ExprNode::ListLen { inner, .. }
            | ExprNode::ListMin { inner, .. }
            | ExprNode::ListMax { inner, .. }
            | ExprNode::ListSum { inner, .. }
            | ExprNode::ListMean { inner, .. }
            | ExprNode::ListJoin { inner, .. }
            | ExprNode::ListSort { inner, .. }
            | ExprNode::ListUnique { inner, .. }
            | ExprNode::StringSplit { inner, .. }
            | ExprNode::StringExtract { inner, .. }
            | ExprNode::StringJsonPathMatch { inner, .. }
            | ExprNode::StringJsonDecode { inner, .. }
            | ExprNode::DatetimeToDate { inner, .. }
            | ExprNode::Strptime { inner, .. }
            | ExprNode::UnixTimestamp { inner, .. }
            | ExprNode::FromUnixTime { inner, .. }
            | ExprNode::BinaryLength { inner, .. }
            | ExprNode::MapLen { inner, .. }
            | ExprNode::MapGet { inner, .. }
            | ExprNode::MapContainsKey { inner, .. }
            | ExprNode::MapKeys { inner, .. }
            | ExprNode::MapValues { inner, .. }
            | ExprNode::MapEntries { inner, .. }
            | ExprNode::MapFromEntries { inner, .. }
            | ExprNode::RowAccum { inner, .. } => inner.referenced_columns(),
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
            ExprNode::StructField { base, .. }
            | ExprNode::StructJsonEncode { base, .. }
            | ExprNode::StructJsonPathMatch { base, .. }
            | ExprNode::StructRenameFields { base, .. } => base.referenced_columns(),
            ExprNode::StructWithFields { base, updates, .. } => {
                let mut out = base.referenced_columns();
                for (_, e) in updates {
                    out.extend(e.referenced_columns());
                }
                out
            }
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
                for (n, _, _) in order_by {
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
        if left.is_struct()
            || right.is_struct()
            || left.is_list()
            || right.is_list()
            || left.is_map()
            || right.is_map()
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Arithmetic operators do not support struct-, list-, or map-typed columns.",
            ));
        }
        let nullable = left.nullable_flag() || right.nullable_flag();
        let left_b = left.as_scalar_base_field().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Arithmetic operators do not support struct-, list-, or map-typed columns.",
            )
        })?;
        let right_b = right.as_scalar_base_field().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Arithmetic operators do not support struct-, list-, or map-typed columns.",
            )
        })?;

        if let (Some(a), Some(b)) = (left_b, right_b) {
            use ArithOp::*;
            match (a, b, op) {
                (BaseType::DateTime, BaseType::Duration, Add)
                | (BaseType::Duration, BaseType::DateTime, Add) => {
                    return Ok(DTypeDesc::Scalar {
                        base: Some(BaseType::DateTime),
                        nullable,
                        literals: None,
                    });
                }
                (BaseType::DateTime, BaseType::Duration, Sub) => {
                    return Ok(DTypeDesc::Scalar {
                        base: Some(BaseType::DateTime),
                        nullable,
                        literals: None,
                    });
                }
                (BaseType::Date, BaseType::Duration, Add)
                | (BaseType::Duration, BaseType::Date, Add) => {
                    return Ok(DTypeDesc::Scalar {
                        base: Some(BaseType::Date),
                        nullable,
                        literals: None,
                    });
                }
                (BaseType::Date, BaseType::Duration, Sub) => {
                    return Ok(DTypeDesc::Scalar {
                        base: Some(BaseType::Date),
                        nullable,
                        literals: None,
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
                literals: None,
            });
        }

        Ok(DTypeDesc::Scalar {
            base: Some(inferred_base),
            nullable,
            literals: None,
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
                        literals: None,
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

                let lb = left.as_scalar_base_field().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Equality comparisons do not support struct-, list-, or map-typed columns.",
                    )
                })?;
                let rb = right.as_scalar_base_field().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Equality comparisons do not support struct-, list-, or map-typed columns.",
                    )
                })?;
                let inferred_left_base = lb.or(rb);
                let inferred_right_base = rb.or(lb);
                let (lb, rb) = match (inferred_left_base, inferred_right_base) {
                    (Some(a), Some(b)) => (a, b),
                    (None, None) => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Cannot infer equality from Literal(None) alone.",
                        ));
                    }
                    _ => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Cannot infer equality from a typed operand and an unknown-base literal.",
                        ));
                    }
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
                    || (lb == BaseType::Ipv4 && rb == BaseType::Ipv4)
                    || (lb == BaseType::Ipv6 && rb == BaseType::Ipv6)
                    || (lb == BaseType::Wkb && rb == BaseType::Wkb)
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
                    literals: None,
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

                let lb = left.as_scalar_base_field().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Ordering comparisons do not support struct-, list-, or map-typed columns.",
                    )
                })?;
                let rb = right.as_scalar_base_field().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Ordering comparisons do not support struct-, list-, or map-typed columns.",
                    )
                })?;
                let inferred_left_base = lb.or(rb);
                let inferred_right_base = rb.or(lb);
                let (lb, rb) = match (inferred_left_base, inferred_right_base) {
                    (Some(a), Some(b)) => (a, b),
                    (None, None) => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Cannot infer ordering from Literal(None) alone.",
                        ));
                    }
                    _ => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Cannot infer ordering from a typed operand and an unknown-base literal.",
                        ));
                    }
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
                let allowed_ip = (lb == BaseType::Ipv4 && rb == BaseType::Ipv4)
                    || (lb == BaseType::Ipv6 && rb == BaseType::Ipv6);
                let allowed_wkb = lb == BaseType::Wkb && rb == BaseType::Wkb;

                if !(allowed_numeric
                    || allowed_str
                    || allowed_enum
                    || allowed_uuid
                    || allowed_decimal
                    || allowed_temporal
                    || allowed_ip
                    || allowed_wkb)
                {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Ordering comparisons require numeric-numeric, str/str-like, uuid-uuid, decimal-decimal, or same temporal operands.",
                    ));
                }

                Ok(DTypeDesc::Scalar {
                    base: Some(BaseType::Bool),
                    nullable,
                    literals: None,
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
        let dtype =
            widen_scalar_drop_literals(Self::infer_arith_dtype(op, left.dtype(), right.dtype())?);
        Ok(ExprNode::BinaryOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
            dtype,
        })
    }

    pub fn make_compare_op(op: CmpOp, left: ExprNode, right: ExprNode) -> PyResult<Self> {
        validate_literal_membership_compare(&left, &right, op)?;
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
        // SQL NULL (`Literal(None)` with unknown-base dtype): cast to a concrete nullable scalar.
        if input.dtype().is_scalar_unknown_nullable() {
            return Ok(ExprNode::Cast {
                input: Box::new(input),
                dtype: DTypeDesc::Scalar {
                    base: Some(base),
                    nullable: true,
                    literals: None,
                },
            });
        }
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
                | (BaseType::Ipv4, BaseType::Str | BaseType::Ipv4)
                | (BaseType::Str, BaseType::Ipv4)
                | (BaseType::Ipv6, BaseType::Str | BaseType::Ipv6)
                | (BaseType::Str, BaseType::Ipv6)
                | (BaseType::Wkb, BaseType::Binary | BaseType::Wkb)
                | (BaseType::Binary, BaseType::Wkb)
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
                literals: None,
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
                literals: None,
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
                literals: None,
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
                literals: None,
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
                literals: None,
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
                literals: None,
            },
        })
    }

    pub fn make_string_replace(
        inner: ExprNode,
        pattern: String,
        replacement: String,
        literal: bool,
    ) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "str_replace() requires a string-like column.",
            ));
        }
        if !literal && pattern.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Regex pattern must not be empty.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StringReplace {
            inner: Box::new(inner),
            pattern,
            replacement,
            literal,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_string_predicate(
        inner: ExprNode,
        kind: StringPredicateKind,
        pattern: String,
    ) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "String predicate requires a string-like column.",
            ));
        }
        if let StringPredicateKind::Contains { literal: false } = &kind {
            if pattern.is_empty() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Regex pattern must not be empty.",
                ));
            }
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StringPredicate {
            inner: Box::new(inner),
            kind,
            pattern,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Bool),
                nullable,
                literals: None,
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
                            literals: None,
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

    fn dtype_under_nullable_struct(struct_nullable: bool, d: DTypeDesc) -> DTypeDesc {
        if !struct_nullable {
            return d;
        }
        match d {
            DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                base,
                nullable: true,
                literals: None,
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
        }
    }

    pub fn make_struct_json_encode(inner: ExprNode) -> PyResult<Self> {
        if !inner.dtype().is_struct() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "struct_json_encode() requires a struct-typed expression.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StructJsonEncode {
            base: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_struct_json_path_match(inner: ExprNode, path: String) -> PyResult<Self> {
        if !inner.dtype().is_struct() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "struct_json_path_match() requires a struct-typed expression.",
            ));
        }
        if path.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "JSONPath pattern must not be empty.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StructJsonPathMatch {
            base: Box::new(inner),
            path,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_struct_rename_fields(inner: ExprNode, names: Vec<String>) -> PyResult<Self> {
        let DTypeDesc::Struct {
            fields,
            nullable: struct_nullable,
        } = inner.dtype()
        else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "struct_rename_fields() requires a struct-typed expression.",
            ));
        };
        if names.len() != fields.len() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "struct_rename_fields() expected {} names (one per struct field), got {}.",
                fields.len(),
                names.len()
            )));
        }
        let mut seen = std::collections::HashSet::new();
        for n in &names {
            if !seen.insert(n.as_str()) {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "struct_rename_fields() duplicate new field name {:?}.",
                    n
                )));
            }
        }
        let new_fields: Vec<(String, DTypeDesc)> = names
            .iter()
            .zip(fields.iter())
            .map(|(n, (_, dt))| (n.clone(), dt.clone()))
            .collect();
        let dtype = DTypeDesc::Struct {
            fields: new_fields,
            nullable: struct_nullable,
        };
        Ok(ExprNode::StructRenameFields {
            base: Box::new(inner),
            names,
            dtype,
        })
    }

    pub fn make_struct_with_fields(
        inner: ExprNode,
        updates: Vec<(String, ExprNode)>,
    ) -> PyResult<Self> {
        let DTypeDesc::Struct {
            fields: base_fields,
            nullable: struct_nullable,
        } = inner.dtype()
        else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "struct_with_fields() requires a struct-typed expression.",
            ));
        };
        if updates.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "struct_with_fields() requires at least one field expression.",
            ));
        }
        let mut seen_update_keys = std::collections::HashSet::new();
        for (name, _) in &updates {
            if !seen_update_keys.insert(name.as_str()) {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "struct_with_fields() duplicate field name {:?} in updates.",
                    name
                )));
            }
        }
        let mut out_fields: Vec<(String, DTypeDesc)> = base_fields.clone();
        let mut boxed_updates: Vec<(String, Box<ExprNode>)> = Vec::with_capacity(updates.len());
        for (name, expr) in updates {
            let fd = Self::dtype_under_nullable_struct(struct_nullable, expr.dtype());
            if let Some(pos) = out_fields.iter().position(|(n, _)| n == &name) {
                out_fields[pos] = (name.clone(), fd);
            } else {
                out_fields.push((name.clone(), fd));
            }
            boxed_updates.push((name, Box::new(expr)));
        }
        let dtype = DTypeDesc::Struct {
            fields: out_fields,
            nullable: struct_nullable,
        };
        Ok(ExprNode::StructWithFields {
            base: Box::new(inner),
            updates: boxed_updates,
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
                literals: None,
            },
            UnaryNumericOp::Floor | UnaryNumericOp::Ceil => DTypeDesc::Scalar {
                base: Some(BaseType::Float),
                nullable,
                literals: None,
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
                literals: None,
            },
        })
    }

    pub fn make_str_reverse(inner: ExprNode) -> PyResult<Self> {
        Self::make_string_unary(inner, StringUnaryOp::Reverse)
    }

    pub fn make_str_pad_start(inner: ExprNode, length: u32, fill_char: char) -> PyResult<Self> {
        Self::make_string_unary(inner, StringUnaryOp::PadStart { length, fill_char })
    }

    pub fn make_str_pad_end(inner: ExprNode, length: u32, fill_char: char) -> PyResult<Self> {
        Self::make_string_unary(inner, StringUnaryOp::PadEnd { length, fill_char })
    }

    pub fn make_str_zfill(inner: ExprNode, length: u32) -> PyResult<Self> {
        Self::make_string_unary(inner, StringUnaryOp::ZFill { length })
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
                literals: None,
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
                literals: None,
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
            TemporalPart::Year
            | TemporalPart::Month
            | TemporalPart::Day
            | TemporalPart::Weekday
            | TemporalPart::Quarter
            | TemporalPart::Week
            | TemporalPart::DayOfYear => {
                if !(is_dt || is_date) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "That temporal part requires a datetime or date column.",
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
                literals: None,
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
                literals: None,
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
                literals: None,
            },
            DTypeDesc::Struct { fields, .. } => DTypeDesc::Struct {
                fields,
                nullable: true,
            },
            DTypeDesc::Map { .. } => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "internal: list_get() does not support map element dtypes.",
                ));
            }
            DTypeDesc::List { .. } => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "internal: list_get() does not support nested list element dtypes.",
                ));
            }
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
                literals: None,
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
            literals: None,
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

    pub fn make_list_mean(inner: ExprNode) -> PyResult<Self> {
        let list_nullable = match inner.dtype() {
            DTypeDesc::List {
                inner: e, nullable, ..
            } => match e.as_ref() {
                DTypeDesc::Scalar {
                    base: Some(BaseType::Int | BaseType::Float),
                    ..
                } => nullable,
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "list_mean() requires list[int] or list[float].",
                    ));
                }
            },
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "list_mean() requires a list-typed column.",
                ));
            }
        };
        Ok(ExprNode::ListMean {
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Float),
                nullable: list_nullable,
                literals: None,
            },
        })
    }

    pub fn make_string_split(inner: ExprNode, delimiter: String) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "str_split() requires a string-like column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StringSplit {
            inner: Box::new(inner),
            delimiter,
            dtype: DTypeDesc::List {
                inner: Box::new(DTypeDesc::Scalar {
                    base: Some(BaseType::Str),
                    nullable: true,
                    literals: None,
                }),
                nullable,
            },
        })
    }

    fn dtype_is_sortable_list_element(d: &DTypeDesc) -> bool {
        matches!(
            d,
            DTypeDesc::Scalar {
                base: Some(
                    BaseType::Int
                        | BaseType::Float
                        | BaseType::Bool
                        | BaseType::Str
                        | BaseType::Date
                        | BaseType::DateTime
                        | BaseType::Time
                        | BaseType::Enum
                        | BaseType::Uuid
                ),
                ..
            }
        )
    }

    pub fn make_list_join(
        inner: ExprNode,
        separator: String,
        ignore_nulls: bool,
    ) -> PyResult<Self> {
        let nullable = match inner.dtype() {
            DTypeDesc::List {
                inner: e, nullable, ..
            } => match e.as_ref() {
                DTypeDesc::Scalar {
                    base: Some(BaseType::Str),
                    ..
                } => nullable,
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "list_join() requires list[str].",
                    ));
                }
            },
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "list_join() requires a list-typed column.",
                ));
            }
        };
        Ok(ExprNode::ListJoin {
            inner: Box::new(inner),
            separator,
            ignore_nulls,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_list_sort(
        inner: ExprNode,
        descending: bool,
        nulls_last: bool,
        maintain_order: bool,
    ) -> PyResult<Self> {
        let dtype = match &inner.dtype() {
            DTypeDesc::List { inner: elt, .. } => {
                if !Self::dtype_is_sortable_list_element(elt.as_ref()) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "list_sort() requires list elements that are int, float, bool, str, date, datetime, time, enum, or uuid.",
                    ));
                }
                inner.dtype().clone()
            }
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "list_sort() requires a list-typed column.",
                ));
            }
        };
        Ok(ExprNode::ListSort {
            inner: Box::new(inner),
            descending,
            nulls_last,
            maintain_order,
            dtype,
        })
    }

    pub fn make_list_unique(inner: ExprNode, stable: bool) -> PyResult<Self> {
        let dtype = match &inner.dtype() {
            DTypeDesc::List { inner: elt, .. } => {
                if !Self::dtype_is_sortable_list_element(elt.as_ref()) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "list_unique() requires list elements that are int, float, bool, str, date, datetime, time, enum, or uuid.",
                    ));
                }
                inner.dtype().clone()
            }
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "list_unique() requires a list-typed column.",
                ));
            }
        };
        Ok(ExprNode::ListUnique {
            inner: Box::new(inner),
            stable,
            dtype,
        })
    }

    pub fn make_string_extract(
        inner: ExprNode,
        pattern: String,
        group_index: usize,
    ) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "str_extract_regex() requires a string-like column.",
            ));
        }
        if pattern.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Regex pattern must not be empty.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StringExtract {
            inner: Box::new(inner),
            pattern,
            group_index,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_string_json_path_match(inner: ExprNode, path: String) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "str_json_path_match() requires a string-like column.",
            ));
        }
        if path.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "JSONPath pattern must not be empty.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StringJsonPathMatch {
            inner: Box::new(inner),
            path,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_str_json_decode(inner: ExprNode, target: DTypeDesc) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "str_json_decode() requires a string-like column.",
            ));
        }
        let dtype = match &target {
            DTypeDesc::Struct { fields, .. } => DTypeDesc::Struct {
                fields: fields.clone(),
                nullable: true,
            },
            DTypeDesc::Map { value, .. } => DTypeDesc::Map {
                value: value.clone(),
                nullable: true,
            },
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "str_json_decode() target dtype must be a struct (nested model) or dict[str, T] map.",
                ));
            }
        };
        Ok(ExprNode::StringJsonDecode {
            inner: Box::new(inner),
            target,
            dtype,
        })
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
                literals: None,
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

    fn validate_range_frame_order_keys(
        frame: &Option<WindowFrame>,
        order_by: &[WindowOrderKey],
        op_name: &str,
    ) -> PyResult<()> {
        if matches!(frame, Some(WindowFrame::Range { .. })) && order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "{op_name}() with rangeBetween requires at least one order_by column; \
                 range bounds apply to the first order column (PostgreSQL-style)."
            )));
        }
        Ok(())
    }

    pub fn make_window_row_number(
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
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
                literals: None,
            },
        })
    }

    pub fn make_window_rank(
        dense: bool,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
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
                literals: None,
            },
        })
    }

    pub fn make_window_first_value(
        inner: ExprNode,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "first_value() requires at least one order_by column.",
            ));
        }
        let dtype = match inner.dtype() {
            DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                base,
                nullable: true,
                literals: None,
            },
            other => other,
        };
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "first_value")?;
        Ok(ExprNode::Window {
            op: WindowOp::FirstValue,
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn make_window_last_value(
        inner: ExprNode,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "last_value() requires at least one order_by column.",
            ));
        }
        let dtype = match inner.dtype() {
            DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                base,
                nullable: true,
                literals: None,
            },
            other => other,
        };
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "last_value")?;
        Ok(ExprNode::Window {
            op: WindowOp::LastValue,
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn make_window_nth_value(
        inner: ExprNode,
        n: u32,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if n == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "nth_value() n must be >= 1.",
            ));
        }
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "nth_value() requires at least one order_by column.",
            ));
        }
        let dtype = match inner.dtype() {
            DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                base,
                nullable: true,
                literals: None,
            },
            other => other,
        };
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "nth_value")?;
        Ok(ExprNode::Window {
            op: WindowOp::NthValue { n },
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn make_window_ntile(
        ntiles: u32,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if ntiles == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "ntile() n must be >= 1.",
            ));
        }
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "ntile() requires at least one order_by column.",
            ));
        }
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "ntile")?;
        Ok(ExprNode::Window {
            op: WindowOp::NTile { n: ntiles },
            operand: None,
            partition_by,
            order_by,
            frame,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable: true,
                literals: None,
            },
        })
    }

    pub fn make_window_percent_rank(
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "percent_rank() requires at least one order_by column.",
            ));
        }
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "percent_rank")?;
        Ok(ExprNode::Window {
            op: WindowOp::PercentRank,
            operand: None,
            partition_by,
            order_by,
            frame,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Float),
                nullable: true,
                literals: None,
            },
        })
    }

    pub fn make_window_cume_dist(
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "cume_dist() requires at least one order_by column.",
            ));
        }
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "cume_dist")?;
        Ok(ExprNode::Window {
            op: WindowOp::CumeDist,
            operand: None,
            partition_by,
            order_by,
            frame,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Float),
                nullable: true,
                literals: None,
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
                literals: None,
            }),
            BaseType::Float => Ok(DTypeDesc::Scalar {
                base: Some(BaseType::Float),
                nullable: true,
                literals: None,
            }),
            BaseType::Decimal => Ok(DTypeDesc::Scalar {
                base: Some(if mean {
                    BaseType::Float
                } else {
                    BaseType::Decimal
                }),
                nullable: true,
                literals: None,
            }),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "window sum/mean require int, float, or decimal column.",
            )),
        }
    }

    pub fn make_window_sum(
        inner: ExprNode,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
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
        Self::validate_range_frame_order_keys(&frame, &order_by, "window_sum")?;
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
        order_by: Vec<WindowOrderKey>,
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
        Self::validate_range_frame_order_keys(&frame, &order_by, "window_mean")?;
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
        order_by: Vec<WindowOrderKey>,
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
        Self::validate_range_frame_order_keys(&frame, &order_by, "window_min")?;
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
        order_by: Vec<WindowOrderKey>,
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
        Self::validate_range_frame_order_keys(&frame, &order_by, "window_max")?;
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
                literals: None,
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
                literals: None,
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
                literals: None,
            },
        }
    }

    fn assert_row_accum_numeric_inner(inner: &ExprNode) -> PyResult<()> {
        let d = inner.dtype();
        if d.is_struct() || d.is_list() || d.is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "row accumulations expect a numeric scalar expression.",
            ));
        }
        let b = d.as_scalar_base_field().flatten().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "row accumulations require a column or expression with known scalar dtype.",
            )
        })?;
        match b {
            BaseType::Int | BaseType::Float | BaseType::Decimal => Ok(()),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "row accumulations require int, float, or decimal values.",
            )),
        }
    }

    /// Cumulative / `diff` / `pct_change` along scan order (no partition).
    pub fn make_row_accum(inner: ExprNode, op: RowAccumOp) -> PyResult<Self> {
        Self::assert_row_accum_numeric_inner(&inner)?;
        let dtype = match op {
            RowAccumOp::CumSum | RowAccumOp::CumProd => {
                Self::infer_window_sum_mean_dtype(&inner, false)?
            }
            RowAccumOp::CumMin | RowAccumOp::CumMax => Self::infer_global_min_max_dtype(&inner)?,
            RowAccumOp::Diff { periods } | RowAccumOp::PctChange { periods } => {
                if periods <= 0 {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "diff and pct_change require a positive periods value.",
                    ));
                }
                DTypeDesc::Scalar {
                    base: Some(BaseType::Float),
                    nullable: true,
                    literals: None,
                }
            }
        };
        Ok(ExprNode::RowAccum {
            op,
            inner: Box::new(inner),
            dtype,
        })
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
                literals: None,
            }
        } else {
            DTypeDesc::Scalar {
                base: Some(BaseType::Date),
                nullable,
                literals: None,
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
                literals: None,
            },
        })
    }

    pub fn make_from_unix_time(inner: ExprNode, unit: UnixTimestampUnit) -> PyResult<Self> {
        let b = inner.dtype().as_scalar_base_field().flatten();
        if b != Some(BaseType::Int) && b != Some(BaseType::Float) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "from_unix_time() requires an int or float column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::FromUnixTime {
            inner: Box::new(inner),
            unit,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::DateTime),
                nullable,
                literals: None,
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
                literals: None,
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
                literals: None,
            },
        })
    }

    pub fn make_map_get(inner: ExprNode, key: String) -> PyResult<Self> {
        if !inner.dtype().is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_get() requires a map column.",
            ));
        }
        let dtype_src = inner.dtype();
        let DTypeDesc::Map { value, .. } = dtype_src else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "internal error: expected map dtype after is_map() check",
            ));
        };
        let dtype = match value.as_ref().clone() {
            DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                base,
                nullable: true,
                literals: None,
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
                literals: None,
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
                    literals: None,
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
        let dtype_src = inner.dtype();
        let DTypeDesc::Map { value, .. } = dtype_src else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "internal error: expected map dtype after is_map() check",
            ));
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

    pub fn make_map_entries(inner: ExprNode) -> PyResult<Self> {
        if !inner.dtype().is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_entries() requires a map column.",
            ));
        }
        let dtype_src = inner.dtype();
        let DTypeDesc::Map { value, .. } = dtype_src else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "internal error: expected map dtype after is_map() check",
            ));
        };
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::MapEntries {
            inner: Box::new(inner),
            dtype: DTypeDesc::List {
                inner: Box::new(DTypeDesc::Struct {
                    fields: vec![
                        (
                            "key".to_string(),
                            DTypeDesc::Scalar {
                                base: Some(BaseType::Str),
                                nullable: false,
                                literals: None,
                            },
                        ),
                        ("value".to_string(), value.as_ref().clone()),
                    ],
                    nullable: false,
                }),
                nullable,
            },
        })
    }

    pub fn make_map_from_entries(inner: ExprNode) -> PyResult<Self> {
        let DTypeDesc::List {
            inner: entry_dtype, ..
        } = inner.dtype()
        else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_from_entries() requires a list of entry structs.",
            ));
        };
        let DTypeDesc::Struct { fields, .. } = entry_dtype.as_ref() else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_from_entries() requires a list of entry structs.",
            ));
        };
        if fields.len() != 2 || fields[0].0 != "key" || fields[1].0 != "value" {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_from_entries() requires struct fields ['key', 'value'] in order.",
            ));
        }
        let key_dtype = &fields[0].1;
        if key_dtype.as_scalar_base_field().flatten() != Some(BaseType::Str) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_from_entries() requires a string key field.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::MapFromEntries {
            inner: Box::new(inner),
            dtype: DTypeDesc::Map {
                value: Box::new(fields[1].1.clone()),
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
            literals: None,
        })
    }

    pub fn make_window_lag(
        inner: ExprNode,
        n: u32,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
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
        order_by: Vec<WindowOrderKey>,
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
        eval_rowwise::eval_expr_node(self, ctx, n)
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
            | LiteralValue::EnumStr(_)
            | LiteralValue::TimeNanos(_)
            | LiteralValue::Binary(_) => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
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
            | LiteralValue::EnumStr(_)
            | LiteralValue::TimeNanos(_)
            | LiteralValue::Binary(_) => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
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
            | LiteralValue::EnumStr(_)
            | LiteralValue::TimeNanos(_)
            | LiteralValue::Binary(_) => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
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
            LiteralValue::TimeNanos(_) | LiteralValue::Binary(_) => {
                Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Row-wise cast() to str does not support time or binary literals.",
                ))
            }
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
        BaseType::Time => match v {
            LiteralValue::TimeNanos(ns) => Ok(LiteralValue::TimeNanos(ns)),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() to time supports time literal (nanoseconds) only.",
            )),
        },
        BaseType::Binary => match v {
            LiteralValue::Binary(b) => Ok(LiteralValue::Binary(b)),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() to binary supports bytes literal only.",
            )),
        },
        BaseType::Ipv4 | BaseType::Ipv6 => match v {
            LiteralValue::Str(s) => Ok(LiteralValue::Str(s)),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() to ip address supports str literal only.",
            )),
        },
        BaseType::Wkb => match v {
            LiteralValue::Binary(b) => Ok(LiteralValue::Binary(b)),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() to WKB supports bytes literal only.",
            )),
        },
        BaseType::Duration => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Row-wise cast() for duration target is not supported.",
        )),
    }
}
