//! Typing rules, `ExprNode::make_*`, [`ExprNode::eval`], and [`ExprNode::referenced_columns`].

#[cfg(not(feature = "polars_engine"))]
use std::collections::HashMap;
use std::collections::HashSet;

use pyo3::prelude::*;

use crate::dtype::{dtype_structural_eq, BaseType, DTypeDesc};

use super::ir::{ArithOp, CmpOp, ExprNode, LiteralValue};

#[cfg(not(feature = "polars_engine"))]
fn literal_between_inclusive(x: &LiteralValue, lo: &LiteralValue, hi: &LiteralValue) -> bool {
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
            | ExprNode::StructField { dtype, .. } => dtype.clone(),
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
            ExprNode::StringLength { inner, .. } => inner.referenced_columns(),
            ExprNode::StructField { base, .. } => base.referenced_columns(),
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

        let inferred_base = match (left_b, right_b) {
            (Some(a), Some(b)) => {
                let valid = matches!(
                    (a, b),
                    (BaseType::Int, BaseType::Int | BaseType::Float)
                        | (BaseType::Float, BaseType::Int | BaseType::Float)
                );
                if !valid {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "Arithmetic operator requires numeric operands; got {:?} and {:?}.",
                        a, b
                    )));
                }
                match (a, b) {
                    (BaseType::Int, BaseType::Int) => Some(BaseType::Int),
                    _ => Some(BaseType::Float),
                }
            }
            (None, Some(b)) => match b {
                BaseType::Int => Some(BaseType::Int),
                BaseType::Float => Some(BaseType::Float),
                _ => None,
            },
            (Some(a), None) => match a {
                BaseType::Int => Some(BaseType::Int),
                BaseType::Float => Some(BaseType::Float),
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
                    || (lb == BaseType::DateTime && rb == BaseType::DateTime)
                    || (lb == BaseType::Date && rb == BaseType::Date)
                    || (lb == BaseType::Duration && rb == BaseType::Duration));

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
                if left.is_struct() || right.is_struct() || left.is_list() || right.is_list() {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Ordering comparisons do not support struct- or list-typed columns.",
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
                let allowed_temporal = (lb == BaseType::DateTime && rb == BaseType::DateTime)
                    || (lb == BaseType::Date && rb == BaseType::Date)
                    || (lb == BaseType::Duration && rb == BaseType::Duration);

                if !(allowed_numeric || allowed_str || allowed_temporal) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Ordering comparisons require numeric-numeric, str-str, or same temporal operands.",
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
            (None, DTypeDesc::Struct { nullable: false, .. }) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Literal(None) cannot target a non-nullable struct column.",
                ));
            }
            (None, DTypeDesc::List { nullable: true, .. }) => {}
            (None, DTypeDesc::List { nullable: false, .. }) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Literal(None) cannot target a non-nullable list column.",
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
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() does not support struct or list dtypes.",
            ));
        }
        let base = match &target {
            DTypeDesc::Scalar {
                base: Some(b), ..
            } => *b,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "cast() target dtype must have known base.",
                ));
            }
        };
        let in_base = match input.dtype() {
            DTypeDesc::Scalar {
                base: Some(b), ..
            } => b,
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
            )
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
        if exprs.iter().any(|e| e.dtype().is_struct() || e.dtype().is_list()) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "coalesce() does not support struct or list dtypes.",
            ));
        }
        let first_base = exprs[0].dtype().as_scalar_base_field().flatten().ok_or_else(|| {
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
        let first_base = branches[0].1.dtype().as_scalar_base_field().flatten().ok_or_else(|| {
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
        let ib = inner.dtype().as_scalar_base_field().flatten().ok_or_else(|| {
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
                LiteralValue::DateTimeMicros(_)
                | LiteralValue::DateDays(_)
                | LiteralValue::DurationMicros(_) => None,
            };
            if vb != Some(ib) {
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
        let ib = inner.dtype().as_scalar_base_field().flatten().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("between() requires known inner dtype.")
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
            if p.dtype().is_struct()
                || p.dtype().is_list()
                || p.dtype().as_scalar_base_field().flatten() != Some(BaseType::Str)
            {
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
            || inner.dtype().as_scalar_base_field().flatten() != Some(BaseType::Str)
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
            || inner.dtype().as_scalar_base_field().flatten() != Some(BaseType::Str)
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
                    DTypeDesc::Scalar {
                        base: Some(b), ..
                    } => *b,
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
                                    let eq =
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
                                            BaseType::Str => {
                                                let as_ = match va {
                                                    LiteralValue::Str(s) => s,
                                                    _ => {
                                                        return Err(PyErr::new::<
                                                            pyo3::exceptions::PyTypeError,
                                                            _,
                                                        >(
                                                            "Typed equality expected str operands.",
                                                        ));
                                                    }
                                                };
                                                let bs_ = match vb {
                                                    LiteralValue::Str(s) => s,
                                                    _ => {
                                                        return Err(PyErr::new::<
                                                            pyo3::exceptions::PyTypeError,
                                                            _,
                                                        >(
                                                            "Typed equality expected str operands.",
                                                        ));
                                                    }
                                                };
                                                as_ == bs_
                                            }
                                            BaseType::DateTime => match (va, vb) {
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
                                            },
                                            BaseType::Date => {
                                                match (va, vb) {
                                                    (
                                                        LiteralValue::DateDays(a),
                                                        LiteralValue::DateDays(b),
                                                    ) => a == b,
                                                    _ => {
                                                        return Err(PyErr::new::<
                                                    pyo3::exceptions::PyTypeError,
                                                    _,
                                                >("Typed equality expected date operands."));
                                                    }
                                                }
                                            }
                                            BaseType::Duration => match (va, vb) {
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
                                            },
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
                                        BaseType::Str => {
                                            let as_ = match va {
                                                LiteralValue::Str(s) => s,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed ordering expected str operands.",
                                                    ));
                                                }
                                            };
                                            let bs_ = match vb {
                                                LiteralValue::Str(s) => s,
                                                _ => {
                                                    return Err(PyErr::new::<
                                                        pyo3::exceptions::PyTypeError,
                                                        _,
                                                    >(
                                                        "Typed ordering expected str operands.",
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
                    DTypeDesc::Scalar {
                        base: Some(b), ..
                    } => *b,
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
                            Some(LiteralValue::Str(s)) => buf.push_str(s),
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
                        (Some(LiteralValue::Str(s)), Some(LiteralValue::Int(st))) => {
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
                        Some(LiteralValue::Str(s)) => {
                            Some(LiteralValue::Int(s.chars().count() as i64))
                        }
                        _ => None,
                    })
                    .collect())
            }
            ExprNode::StructField { .. } => Err(PyErr::new::<
                pyo3::exceptions::PyNotImplementedError,
                _,
            >(
                "Struct field access is only supported with the Polars execution engine.",
            )),
        }
    }
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
            | LiteralValue::DurationMicros(_) => {
                Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Cannot cast temporal literal to int.",
                ))
            }
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
            | LiteralValue::DurationMicros(_) => {
                Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Cannot cast temporal literal to float.",
                ))
            }
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
            | LiteralValue::DurationMicros(_) => {
                Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Cannot cast temporal literal to bool.",
                ))
            }
        },
        BaseType::Str => match v {
            LiteralValue::Int(i) => Ok(LiteralValue::Str(i.to_string())),
            LiteralValue::Float(f) => Ok(LiteralValue::Str(f.to_string())),
            LiteralValue::Bool(b) => Ok(LiteralValue::Str(b.to_string())),
            LiteralValue::Str(s) => Ok(LiteralValue::Str(s)),
            LiteralValue::DateTimeMicros(_)
            | LiteralValue::DateDays(_)
            | LiteralValue::DurationMicros(_) => {
                Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Cannot cast temporal literal to str.",
                ))
            }
        },
        BaseType::DateTime | BaseType::Date | BaseType::Duration => {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Row-wise cast() for temporal types is not supported.",
            ))
        }
    }
}
