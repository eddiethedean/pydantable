use std::collections::HashSet;

#[cfg(not(feature = "polars_engine"))]
use std::cmp::Ordering;
#[cfg(not(feature = "polars_engine"))]
use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};

use crate::dtype::{dtype_to_descriptor_py, py_value_to_dtype, BaseType, DTypeDesc};

#[cfg(feature = "polars_engine")]
use polars::lazy::dsl::{coalesce as pl_coalesce, col, lit, Expr as PolarsExpr};
#[cfg(feature = "polars_engine")]
use polars::prelude::Literal;
#[cfg(feature = "polars_engine")]
use polars::prelude::{
    concat_str, when as pl_when, DataType, NamedFrom, Null, PlSmallStr, Series,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug)]
pub enum LiteralValue {
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
}

#[derive(Clone, Debug)]
pub enum ExprNode {
    ColumnRef {
        name: String,
        dtype: DTypeDesc,
    },
    Literal {
        value: Option<LiteralValue>,
        dtype: DTypeDesc,
    },
    BinaryOp {
        op: ArithOp,
        left: Box<ExprNode>,
        right: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    CompareOp {
        op: CmpOp,
        left: Box<ExprNode>,
        right: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    IsNull {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    IsNotNull {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    Coalesce {
        exprs: Vec<ExprNode>,
        dtype: DTypeDesc,
    },
    /// `CASE WHEN c1 THEN v1 ... ELSE e END` — first true condition wins; null/false skips.
    CaseWhen {
        branches: Vec<(ExprNode, ExprNode)>,
        else_: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    Cast {
        inner: Box<ExprNode>,
        to: BaseType,
        dtype: DTypeDesc,
    },
    InList {
        inner: Box<ExprNode>,
        candidates: Vec<LiteralValue>,
        dtype: DTypeDesc,
    },
    Between {
        inner: Box<ExprNode>,
        low: Box<ExprNode>,
        high: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    StringConcat {
        parts: Vec<ExprNode>,
        dtype: DTypeDesc,
    },
    Substring {
        inner: Box<ExprNode>,
        /// 1-based start index (PySpark `substr` convention).
        start: Box<ExprNode>,
        /// Length in characters; if None, slice to end of string.
        length: Option<Box<ExprNode>>,
        dtype: DTypeDesc,
    },
    StringLength {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
}

impl ExprNode {
    pub fn dtype(&self) -> DTypeDesc {
        match self {
            ExprNode::ColumnRef { dtype, .. } => *dtype,
            ExprNode::Literal { dtype, .. } => *dtype,
            ExprNode::BinaryOp { dtype, .. } => *dtype,
            ExprNode::CompareOp { dtype, .. } => *dtype,
            ExprNode::IsNull { dtype, .. } => *dtype,
            ExprNode::IsNotNull { dtype, .. } => *dtype,
            ExprNode::Coalesce { dtype, .. } => *dtype,
            ExprNode::CaseWhen { dtype, .. } => *dtype,
            ExprNode::Cast { dtype, .. } => *dtype,
            ExprNode::InList { dtype, .. } => *dtype,
            ExprNode::Between { dtype, .. } => *dtype,
            ExprNode::StringConcat { dtype, .. } => *dtype,
            ExprNode::Substring { dtype, .. } => *dtype,
            ExprNode::StringLength { dtype, .. } => *dtype,
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
            ExprNode::IsNull { inner, .. } | ExprNode::IsNotNull { inner, .. } => {
                inner.referenced_columns()
            }
            ExprNode::Coalesce { exprs, .. } => {
                let mut out = HashSet::new();
                for e in exprs {
                    out.extend(e.referenced_columns());
                }
                out
            }
            ExprNode::CaseWhen {
                branches,
                else_,
                ..
            } => {
                let mut out = else_.referenced_columns();
                for (c, t) in branches {
                    out.extend(c.referenced_columns());
                    out.extend(t.referenced_columns());
                }
                out
            }
            ExprNode::Cast { inner, .. } => inner.referenced_columns(),
            ExprNode::InList { inner, .. } => inner.referenced_columns(),
            ExprNode::Between { inner, low, high, .. } => {
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
                if let Some(len) = length {
                    out.extend(len.referenced_columns());
                }
                out
            }
            ExprNode::StringLength { inner, .. } => inner.referenced_columns(),
        }
    }

    fn infer_arith_dtype(op: ArithOp, left: DTypeDesc, right: DTypeDesc) -> PyResult<DTypeDesc> {
        let nullable = left.nullable || right.nullable;

        let inferred_base = match (left.base, right.base) {
            (Some(a), Some(b)) => {
                // Only numeric base types are valid here.
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

        // Division always returns float in this skeleton.
        if op == ArithOp::Div {
            return Ok(DTypeDesc {
                base: Some(BaseType::Float),
                nullable,
            });
        }

        Ok(DTypeDesc {
            base: Some(inferred_base),
            nullable,
        })
    }

    fn infer_compare_dtype(op: CmpOp, left: DTypeDesc, right: DTypeDesc) -> PyResult<DTypeDesc> {
        let nullable = left.nullable || right.nullable;

        match op {
            CmpOp::Eq | CmpOp::Ne => {
                // Allowed equality combinations:
                // - numeric vs numeric (int/float)
                // - bool vs bool
                // - str vs str
                let inferred_left_base = left.base.or(right.base);
                let inferred_right_base = right.base.or(left.base);
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
                    || (lb == BaseType::Str && rb == BaseType::Str));

                if !allowed {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Equality requires numeric-numeric, bool-bool, or str-str operands.",
                    ));
                }

                Ok(DTypeDesc {
                    base: Some(BaseType::Bool),
                    nullable,
                })
            }
            CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge => {
                // Allowed ordering:
                // - numeric vs numeric
                // - str vs str
                let inferred_left_base = left.base.or(right.base);
                let inferred_right_base = right.base.or(left.base);
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

                if !(allowed_numeric || allowed_str) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Ordering comparisons require numeric-numeric or str-str operands.",
                    ));
                }

                Ok(DTypeDesc {
                    base: Some(BaseType::Bool),
                    nullable,
                })
            }
        }
    }

    pub fn make_column_ref(name: String, dtype: DTypeDesc) -> PyResult<Self> {
        if dtype.base.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "ColumnRef dtype cannot have unknown base.",
            ));
        }
        Ok(ExprNode::ColumnRef { name, dtype })
    }

    pub fn make_literal(value: Option<LiteralValue>, dtype: DTypeDesc) -> PyResult<Self> {
        if value.is_none() && dtype.base.is_some() {
            // Literal(None) should correspond to unknown base nullable.
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Literal(None) must have unknown-base nullable dtype.",
            ));
        }
        if value.is_some() && dtype.base.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Non-None Literal must have a known base dtype.",
            ));
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

    fn infer_coalesce_dtype(parts: &[ExprNode]) -> PyResult<DTypeDesc> {
        if parts.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "coalesce() requires at least one expression.",
            ));
        }
        let mut cur: Option<BaseType> = None;
        for p in parts {
            let b = p.dtype().base.ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "coalesce() operands must have known scalar types.",
                )
            })?;
            cur = Some(match (cur, b) {
                (None, b) => b,
                (Some(BaseType::Int), BaseType::Int) => BaseType::Int,
                (Some(BaseType::Int), BaseType::Float) | (Some(BaseType::Float), BaseType::Int) => {
                    BaseType::Float
                }
                (Some(BaseType::Float), BaseType::Float) => BaseType::Float,
                (Some(a), b) if a == b => a,
                (Some(a), b) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "coalesce() requires compatible types; cannot mix {a:?} with {b:?}."
                    )));
                }
            });
        }
        Ok(DTypeDesc {
            base: cur,
            nullable: true,
        })
    }

    pub fn make_is_null(inner: ExprNode) -> PyResult<Self> {
        Ok(ExprNode::IsNull {
            inner: Box::new(inner),
            dtype: DTypeDesc {
                base: Some(BaseType::Bool),
                nullable: false,
            },
        })
    }

    pub fn make_is_not_null(inner: ExprNode) -> PyResult<Self> {
        Ok(ExprNode::IsNotNull {
            inner: Box::new(inner),
            dtype: DTypeDesc {
                base: Some(BaseType::Bool),
                nullable: false,
            },
        })
    }

    pub fn make_coalesce(exprs: Vec<ExprNode>) -> PyResult<Self> {
        let dtype = Self::infer_coalesce_dtype(&exprs)?;
        Ok(ExprNode::Coalesce { exprs, dtype })
    }

    fn infer_case_when_dtype(branches: &[(ExprNode, ExprNode)], else_: &ExprNode) -> PyResult<DTypeDesc> {
        let mut parts: Vec<ExprNode> = Vec::new();
        for (_, t) in branches {
            parts.push(t.clone());
        }
        parts.push(else_.clone());
        Self::infer_coalesce_dtype(&parts)
    }

    pub fn make_case_when(
        branches: Vec<(ExprNode, ExprNode)>,
        else_: ExprNode,
    ) -> PyResult<Self> {
        if branches.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "case_when requires at least one (condition, value) branch.",
            ));
        }
        for (cond, _) in branches.iter() {
            let d = cond.dtype();
            if d.base != Some(BaseType::Bool) {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "CASE WHEN condition must be bool or Optional[bool].",
                ));
            }
        }
        let dtype = Self::infer_case_when_dtype(&branches, &else_)?;
        Ok(ExprNode::CaseWhen {
            branches,
            else_: Box::new(else_),
            dtype,
        })
    }

    pub fn make_cast(inner: ExprNode, to: BaseType) -> PyResult<Self> {
        let nullable = inner.dtype().nullable;
        Ok(ExprNode::Cast {
            inner: Box::new(inner),
            to,
            dtype: DTypeDesc {
                base: Some(to),
                nullable,
            },
        })
    }

    pub fn make_in_list(inner: ExprNode, candidates: Vec<LiteralValue>) -> PyResult<Self> {
        if candidates.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "isin() requires at least one literal value.",
            ));
        }
        let ib = inner.dtype().base.ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "isin() inner expression must have a known scalar type.",
            )
        })?;
        let nullable = inner.dtype().nullable;
        for c in candidates.iter() {
            Self::check_literal_matches_base(ib, c)?;
        }
        Ok(ExprNode::InList {
            inner: Box::new(inner),
            candidates,
            dtype: DTypeDesc {
                base: Some(BaseType::Bool),
                nullable,
            },
        })
    }

    fn check_literal_matches_base(base: BaseType, lit: &LiteralValue) -> PyResult<()> {
        let ok = match (base, lit) {
            (BaseType::Int, LiteralValue::Int(_)) => true,
            (BaseType::Float, LiteralValue::Int(_)) | (BaseType::Float, LiteralValue::Float(_)) => {
                true
            }
            (BaseType::Int, LiteralValue::Float(_)) => false,
            (BaseType::Bool, LiteralValue::Bool(_)) => true,
            (BaseType::Str, LiteralValue::Str(_)) => true,
            _ => false,
        };
        if ok {
            Ok(())
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "isin() literal type does not match column type.",
            ))
        }
    }

    pub fn make_between(inner: ExprNode, low: ExprNode, high: ExprNode) -> PyResult<Self> {
        let ib = inner.dtype().base.ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "between() requires a typed inner expression.",
            )
        })?;
        let lb = low.dtype().base.ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "between() low bound must have a known type.",
            )
        })?;
        let hb = high.dtype().base.ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "between() high bound must have a known type.",
            )
        })?;
        let allowed = matches!(
            (ib, lb, hb),
            (BaseType::Int | BaseType::Float, BaseType::Int | BaseType::Float, BaseType::Int | BaseType::Float)
                | (BaseType::Str, BaseType::Str, BaseType::Str)
        );
        if !allowed {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "between() supports numeric or string bounds compatible with the inner column.",
            ));
        }
        let nullable =
            inner.dtype().nullable || low.dtype().nullable || high.dtype().nullable;
        Ok(ExprNode::Between {
            inner: Box::new(inner),
            low: Box::new(low),
            high: Box::new(high),
            dtype: DTypeDesc {
                base: Some(BaseType::Bool),
                nullable,
            },
        })
    }

    pub fn make_string_concat(parts: Vec<ExprNode>) -> PyResult<Self> {
        if parts.len() < 2 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "concat() requires at least two expressions.",
            ));
        }
        let mut nullable = false;
        for p in parts.iter() {
            if p.dtype().base != Some(BaseType::Str) {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "concat() arguments must be string-typed expressions.",
                ));
            }
            nullable |= p.dtype().nullable;
        }
        Ok(ExprNode::StringConcat {
            parts,
            dtype: DTypeDesc {
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
        if inner.dtype().base != Some(BaseType::Str) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "substring() inner expression must be string-typed.",
            ));
        }
        if start.dtype().base != Some(BaseType::Int) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "substring() start must be int-typed (1-based position).",
            ));
        }
        if let Some(ref len) = length {
            if len.dtype().base != Some(BaseType::Int) {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "substring() length must be int-typed when provided.",
                ));
            }
        }
        let nullable = inner.dtype().nullable
            || start.dtype().nullable
            || length
                .as_ref()
                .map(|l| l.dtype().nullable)
                .unwrap_or(false);
        Ok(ExprNode::Substring {
            inner: Box::new(inner),
            start: Box::new(start),
            length: length.map(Box::new),
            dtype: DTypeDesc {
                base: Some(BaseType::Str),
                nullable,
            },
        })
    }

    pub fn make_string_length(inner: ExprNode) -> PyResult<Self> {
        if inner.dtype().base != Some(BaseType::Str) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "length() expects a string-typed expression.",
            ));
        }
        let nullable = inner.dtype().nullable;
        Ok(ExprNode::StringLength {
            inner: Box::new(inner),
            dtype: DTypeDesc {
                base: Some(BaseType::Int),
                nullable,
            },
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

                let result_base = dtype.base.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "BinaryOp result dtype base cannot be unknown.",
                    )
                })?;

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
                let left_base = left.dtype().base;
                let right_base = right.dtype().base;
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
            ExprNode::IsNull { inner, .. } => {
                let vals = inner.eval(ctx, n)?;
                Ok(vals
                    .into_iter()
                    .map(|v| Some(LiteralValue::Bool(v.is_none())))
                    .collect())
            }
            ExprNode::IsNotNull { inner, .. } => {
                let vals = inner.eval(ctx, n)?;
                Ok(vals
                    .into_iter()
                    .map(|v| Some(LiteralValue::Bool(v.is_some())))
                    .collect())
            }
            ExprNode::Coalesce { exprs, dtype } => {
                let rows: Vec<Vec<Option<LiteralValue>>> = exprs
                    .iter()
                    .map(|e| e.eval(ctx, n))
                    .collect::<PyResult<_>>()?;
                let base = dtype.base.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Coalesce result dtype base cannot be unknown.",
                    )
                })?;
                let m = rows.first().map(|r| r.len()).unwrap_or(0);
                let mut out = Vec::with_capacity(m);
                for i in 0..m {
                    let mut picked: Option<LiteralValue> = None;
                    for row in &rows {
                        if let Some(ref v) = row[i] {
                            picked = Some(v.clone());
                            break;
                        }
                    }
                    out.push(coalesce_cell_to_dtype(picked, base)?);
                }
                Ok(out)
            }
            ExprNode::CaseWhen {
                branches,
                else_,
                dtype,
            } => {
                let base = dtype.base.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "CaseWhen result dtype base cannot be unknown.",
                    )
                })?;
                let cond_cols: Vec<Vec<Option<LiteralValue>>> = branches
                    .iter()
                    .map(|(c, _)| c.eval(ctx, n))
                    .collect::<PyResult<_>>()?;
                let then_cols: Vec<Vec<Option<LiteralValue>>> = branches
                    .iter()
                    .map(|(_, t)| t.eval(ctx, n))
                    .collect::<PyResult<_>>()?;
                let else_vals = else_.eval(ctx, n)?;
                let mut out = Vec::with_capacity(n);
                for i in 0..n {
                    let mut chosen: Option<Option<LiteralValue>> = None;
                    for (ci, ti) in cond_cols.iter().zip(then_cols.iter()) {
                        match &ci[i] {
                            Some(LiteralValue::Bool(true)) => {
                                chosen = Some(ti[i].clone());
                                break;
                            }
                            Some(LiteralValue::Bool(false)) | None => {}
                            _ => {
                                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                    "CASE WHEN condition must evaluate to bool or null.",
                                ));
                            }
                        }
                    }
                    let cell = match chosen {
                        None => else_vals[i].clone(),
                        Some(v) => v,
                    };
                    out.push(coalesce_cell_to_dtype(cell, base)?);
                }
                Ok(out)
            }
            ExprNode::Cast { inner, to, .. } => {
                let vals = inner.eval(ctx, n)?;
                Ok(vals
                    .into_iter()
                    .map(|v| cast_literal_to_base(v.as_ref(), *to))
                    .collect())
            }
            ExprNode::InList {
                inner,
                candidates,
                ..
            } => {
                let vals = inner.eval(ctx, n)?;
                let ib = inner.dtype().base.unwrap();
                Ok(vals
                    .into_iter()
                    .map(|v| match v {
                        None => None,
                        Some(ref x) => Some(LiteralValue::Bool(
                            candidates.iter().any(|c| literals_equal(ib, x, c)),
                        )),
                    })
                    .collect())
            }
            ExprNode::Between {
                inner,
                low,
                high,
                ..
            } => {
                let iv = inner.eval(ctx, n)?;
                let lv = low.eval(ctx, n)?;
                let hv = high.eval(ctx, n)?;
                let mut out = Vec::with_capacity(n);
                for i in 0..n {
                    out.push(between_at(&iv[i], &lv[i], &hv[i])?);
                }
                Ok(out)
            }
            ExprNode::StringConcat { parts, .. } => {
                let mut mat: Vec<Vec<Option<LiteralValue>>> =
                    parts.iter().map(|p| p.eval(ctx, n)).collect::<PyResult<_>>()?;
                let mut out = Vec::with_capacity(n);
                for i in 0..n {
                    let mut acc = String::new();
                    let mut miss = false;
                    for col in mat.iter_mut() {
                        match &col[i] {
                            None => {
                                miss = true;
                                break;
                            }
                            Some(LiteralValue::Str(s)) => acc.push_str(s),
                            _ => {
                                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                    "concat() internal type error.",
                                ));
                            }
                        }
                    }
                    out.push(if miss {
                        None
                    } else {
                        Some(LiteralValue::Str(acc))
                    });
                }
                Ok(out)
            }
            ExprNode::Substring {
                inner,
                start,
                length,
                ..
            } => {
                let sv = inner.eval(ctx, n)?;
                let st = start.eval(ctx, n)?;
                let ln = match length {
                    None => None,
                    Some(l) => Some(l.eval(ctx, n)?),
                };
                let mut out = Vec::with_capacity(n);
                for i in 0..n {
                    let cell = match (&sv[i], &st[i]) {
                        (None, _) | (_, None) => None,
                        (Some(LiteralValue::Str(s)), Some(LiteralValue::Int(pos))) => {
                            let len_opt = match &ln {
                                None => None,
                                Some(lv) => match &lv[i] {
                                    None => None,
                                    Some(LiteralValue::Int(l)) => Some(*l),
                                    _ => {
                                        return Err(PyErr::new::<
                                            pyo3::exceptions::PyTypeError,
                                            _,
                                        >(
                                            "substring length must be int.",
                                        ));
                                    }
                                },
                            };
                            Some(LiteralValue::Str(substr_spark(s, *pos, len_opt)))
                        }
                        _ => {
                            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                "substring expects string and int start.",
                            ));
                        }
                    };
                    out.push(cell);
                }
                Ok(out)
            }
            ExprNode::StringLength { inner, .. } => {
                let sv = inner.eval(ctx, n)?;
                Ok(sv
                    .into_iter()
                    .map(|v| {
                        v.map(|lit| match lit {
                            LiteralValue::Str(s) => {
                                LiteralValue::Int(s.chars().count() as i64)
                            }
                            _ => unreachable!(),
                        })
                    })
                    .collect())
            }
        }
    }

    #[cfg(feature = "polars_engine")]
    pub fn to_polars_expr(&self) -> PyResult<PolarsExpr> {
        match self {
            ExprNode::ColumnRef { name, .. } => Ok(col(name)),
            ExprNode::Literal { value, dtype } => match value {
                Some(LiteralValue::Int(i)) => Ok(lit(*i)),
                Some(LiteralValue::Float(f)) => Ok(lit(*f)),
                Some(LiteralValue::Bool(b)) => Ok(lit(*b)),
                Some(LiteralValue::Str(s)) => Ok(lit(s.clone())),
                None => {
                    let null_expr = Null {}.lit();
                    match dtype.base {
                        Some(BaseType::Int) => Ok(null_expr.cast(DataType::Int64)),
                        Some(BaseType::Float) => Ok(null_expr.cast(DataType::Float64)),
                        Some(BaseType::Bool) => Ok(null_expr.cast(DataType::Boolean)),
                        Some(BaseType::Str) => Ok(null_expr.cast(DataType::String)),
                        None => Ok(null_expr),
                    }
                }
            },
            ExprNode::BinaryOp {
                op, left, right, ..
            } => {
                let l = left.to_polars_expr()?;
                let r = right.to_polars_expr()?;
                match op {
                    ArithOp::Add => Ok(l + r),
                    ArithOp::Sub => Ok(l - r),
                    ArithOp::Mul => Ok(l * r),
                    ArithOp::Div => Ok(l / r),
                }
            }
            ExprNode::CompareOp {
                op, left, right, ..
            } => {
                let l = left.to_polars_expr()?;
                let r = right.to_polars_expr()?;
                match op {
                    CmpOp::Eq => Ok(l.eq(r)),
                    CmpOp::Ne => Ok(l.neq(r)),
                    CmpOp::Lt => Ok(l.lt(r)),
                    CmpOp::Le => Ok(l.lt_eq(r)),
                    CmpOp::Gt => Ok(l.gt(r)),
                    CmpOp::Ge => Ok(l.gt_eq(r)),
                }
            }
            ExprNode::IsNull { inner, .. } => Ok(inner.to_polars_expr()?.is_null()),
            ExprNode::IsNotNull { inner, .. } => Ok(inner.to_polars_expr()?.is_not_null()),
            ExprNode::Coalesce { exprs, dtype } => {
                let mut polars_exprs: Vec<PolarsExpr> = Vec::with_capacity(exprs.len());
                for e in exprs {
                    let mut pe = e.to_polars_expr()?;
                    if dtype.base == Some(BaseType::Float) {
                        pe = pe.cast(DataType::Float64);
                    }
                    polars_exprs.push(pe);
                }
                Ok(pl_coalesce(polars_exprs.as_slice()))
            }
            ExprNode::CaseWhen {
                branches,
                else_,
                dtype,
            } => polars_case_when(branches, else_.as_ref(), *dtype),
            ExprNode::Cast { inner, to, .. } => {
                let ie = inner.to_polars_expr()?;
                let dt = polars_data_type(*to);
                Ok(ie.cast(dt))
            }
            ExprNode::InList {
                inner,
                candidates,
                ..
            } => {
                let ib = inner.dtype().base.unwrap();
                let s = literal_series_for_isin(candidates, ib)?;
                Ok(inner.to_polars_expr()?.is_in(lit(s), false))
            }
            ExprNode::Between {
                inner,
                low,
                high,
                ..
            } => {
                let x = inner.to_polars_expr()?;
                let lo = low.to_polars_expr()?;
                let hi = high.to_polars_expr()?;
                Ok(x.clone().gt_eq(lo).and(x.lt_eq(hi)))
            }
            ExprNode::StringConcat { parts, .. } => {
                let mut pes: Vec<PolarsExpr> = Vec::with_capacity(parts.len());
                for p in parts {
                    pes.push(p.to_polars_expr()?);
                }
                Ok(concat_str(pes.as_slice(), "", false))
            }
            ExprNode::Substring {
                inner,
                start,
                length,
                ..
            } => {
                let s = inner.to_polars_expr()?;
                let off = start.to_polars_expr()? - lit(1i64);
                let len_e = match length {
                    None => lit(1_000_000i64),
                    Some(l) => l.to_polars_expr()?,
                };
                Ok(s.str().slice(off, len_e))
            }
            ExprNode::StringLength { inner, .. } => Ok(inner.to_polars_expr()?.str().len_chars()),
        }
    }
}

#[cfg(feature = "polars_engine")]
fn polars_data_type(b: BaseType) -> DataType {
    match b {
        BaseType::Int => DataType::Int64,
        BaseType::Float => DataType::Float64,
        BaseType::Bool => DataType::Boolean,
        BaseType::Str => DataType::String,
    }
}

#[cfg(feature = "polars_engine")]
fn polars_promote_expr(e: &ExprNode, target: DTypeDesc) -> PyResult<PolarsExpr> {
    let mut pe = e.to_polars_expr()?;
    if target.base == Some(BaseType::Float) {
        pe = pe.cast(DataType::Float64);
    }
    Ok(pe)
}

#[cfg(feature = "polars_engine")]
fn polars_case_when(
    branches: &[(ExprNode, ExprNode)],
    else_: &ExprNode,
    dtype: DTypeDesc,
) -> PyResult<PolarsExpr> {
    if branches.len() == 1 {
        let (c0, t0) = &branches[0];
        return Ok(pl_when(c0.to_polars_expr()?)
            .then(polars_promote_expr(t0, dtype)?)
            .otherwise(polars_promote_expr(else_, dtype)?));
    }
    let mut ct = pl_when(branches[0].0.to_polars_expr()?)
        .then(polars_promote_expr(&branches[0].1, dtype)?)
        .when(branches[1].0.to_polars_expr()?)
        .then(polars_promote_expr(&branches[1].1, dtype)?);
    for (c, t) in branches.iter().skip(2) {
        ct = ct
            .when(c.to_polars_expr()?)
            .then(polars_promote_expr(t, dtype)?);
    }
    Ok(ct.otherwise(polars_promote_expr(else_, dtype)?))
}

#[cfg(feature = "polars_engine")]
fn literal_series_for_isin(candidates: &[LiteralValue], base: BaseType) -> PyResult<Series> {
    match base {
        BaseType::Int => {
            let v: Vec<i64> = candidates
                .iter()
                .map(|l| match l {
                    LiteralValue::Int(i) => Ok(*i),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "isin int list",
                    )),
                })
                .collect::<PyResult<_>>()?;
            Ok(Series::new(PlSmallStr::EMPTY, v))
        }
        BaseType::Float => {
            let v: Vec<f64> = candidates
                .iter()
                .map(|l| match l {
                    LiteralValue::Int(i) => Ok(*i as f64),
                    LiteralValue::Float(f) => Ok(*f),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "isin float list",
                    )),
                })
                .collect::<PyResult<_>>()?;
            Ok(Series::new(PlSmallStr::EMPTY, v))
        }
        BaseType::Bool => {
            let v: Vec<bool> = candidates
                .iter()
                .map(|l| match l {
                    LiteralValue::Bool(b) => Ok(*b),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "isin bool list",
                    )),
                })
                .collect::<PyResult<_>>()?;
            Ok(Series::new(PlSmallStr::EMPTY, v))
        }
        BaseType::Str => {
            let v: Vec<String> = candidates
                .iter()
                .map(|l| match l {
                    LiteralValue::Str(s) => Ok(s.clone()),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "isin str list",
                    )),
                })
                .collect::<PyResult<_>>()?;
            Ok(Series::new(PlSmallStr::EMPTY, v))
        }
    }
}

#[cfg(not(feature = "polars_engine"))]
fn substr_spark(s: &str, start: i64, len_opt: Option<i64>) -> String {
    if start < 1 {
        return String::new();
    }
    let idx = (start - 1) as usize;
    let chars: Vec<char> = s.chars().collect();
    if idx >= chars.len() {
        return String::new();
    }
    let rest = &chars[idx..];
    match len_opt {
        None => rest.iter().collect(),
        Some(l) if l <= 0 => String::new(),
        Some(l) => rest.iter().take(l as usize).collect(),
    }
}

#[cfg(not(feature = "polars_engine"))]
fn literal_to_f64(v: &LiteralValue) -> f64 {
    match v {
        LiteralValue::Int(i) => *i as f64,
        LiteralValue::Float(f) => *f,
        _ => f64::NAN,
    }
}

#[cfg(not(feature = "polars_engine"))]
fn literals_equal(inner_base: BaseType, cell: &LiteralValue, cand: &LiteralValue) -> bool {
    match inner_base {
        BaseType::Int => matches!(
            (cell, cand),
            (LiteralValue::Int(a), LiteralValue::Int(b)) if a == b
        ),
        BaseType::Float => literal_to_f64(cell) == literal_to_f64(cand),
        BaseType::Bool => matches!(
            (cell, cand),
            (LiteralValue::Bool(a), LiteralValue::Bool(b)) if a == b
        ),
        BaseType::Str => matches!(
            (cell, cand),
            (LiteralValue::Str(a), LiteralValue::Str(b)) if a == b
        ),
    }
}

#[cfg(not(feature = "polars_engine"))]
fn cmp_for_between(a: &LiteralValue, b: &LiteralValue) -> PyResult<Ordering> {
    Ok(match (a, b) {
        (LiteralValue::Int(x), LiteralValue::Int(y)) => x.cmp(y),
        (LiteralValue::Int(x), LiteralValue::Float(y)) => (*x as f64).total_cmp(y),
        (LiteralValue::Float(x), LiteralValue::Int(y)) => x.total_cmp(&(*y as f64)),
        (LiteralValue::Float(x), LiteralValue::Float(y)) => x.total_cmp(y),
        (LiteralValue::Str(x), LiteralValue::Str(y)) => x.cmp(y),
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "between() bound types are incompatible.",
            ));
        }
    })
}

#[cfg(not(feature = "polars_engine"))]
fn between_at(
    x: &Option<LiteralValue>,
    lo: &Option<LiteralValue>,
    hi: &Option<LiteralValue>,
) -> PyResult<Option<LiteralValue>> {
    match (x, lo, hi) {
        (None, _, _) | (_, None, _) | (_, _, None) => Ok(None),
        (Some(xv), Some(lv), Some(hv)) => {
            let ge_lo = cmp_for_between(xv, lv)? != Ordering::Less;
            let le_hi = cmp_for_between(xv, hi)? != Ordering::Greater;
            Ok(Some(LiteralValue::Bool(ge_lo && le_hi)))
        }
    }
}

#[cfg(not(feature = "polars_engine"))]
fn cast_literal_to_base(v: Option<&LiteralValue>, to: BaseType) -> Option<LiteralValue> {
    let x = v?;
    match (to, x) {
        (BaseType::Int, LiteralValue::Int(i)) => Some(LiteralValue::Int(*i)),
        (BaseType::Int, LiteralValue::Float(f)) => Some(LiteralValue::Int(*f as i64)),
        (BaseType::Float, LiteralValue::Int(i)) => Some(LiteralValue::Float(*i as f64)),
        (BaseType::Float, LiteralValue::Float(f)) => Some(LiteralValue::Float(*f)),
        (BaseType::Bool, LiteralValue::Bool(b)) => Some(LiteralValue::Bool(*b)),
        (BaseType::Str, LiteralValue::Str(s)) => Some(LiteralValue::Str(s.clone())),
        (BaseType::Str, LiteralValue::Int(i)) => Some(LiteralValue::Str(i.to_string())),
        (BaseType::Str, LiteralValue::Float(f)) => Some(LiteralValue::Str(f.to_string())),
        (BaseType::Str, LiteralValue::Bool(b)) => Some(LiteralValue::Str(b.to_string())),
        (BaseType::Int, LiteralValue::Str(s)) => s.parse().ok().map(LiteralValue::Int),
        (BaseType::Float, LiteralValue::Str(s)) => s.parse().ok().map(LiteralValue::Float),
        (BaseType::Bool, LiteralValue::Str(s)) => match s.as_str() {
            "true" | "True" | "1" => Some(LiteralValue::Bool(true)),
            "false" | "False" | "0" => Some(LiteralValue::Bool(false)),
            _ => None,
        },
        (BaseType::Int, LiteralValue::Bool(b)) => Some(LiteralValue::Int(if *b { 1 } else { 0 })),
        (BaseType::Bool, LiteralValue::Int(i)) => Some(LiteralValue::Bool(*i != 0)),
        _ => None,
    }
}

#[cfg(not(feature = "polars_engine"))]
fn coalesce_cell_to_dtype(
    v: Option<LiteralValue>,
    base: BaseType,
) -> PyResult<Option<LiteralValue>> {
    let Some(v) = v else {
        return Ok(None);
    };
    match (base, v) {
        (BaseType::Int, LiteralValue::Int(i)) => Ok(Some(LiteralValue::Int(i))),
        (BaseType::Float, LiteralValue::Int(i)) => Ok(Some(LiteralValue::Float(i as f64))),
        (BaseType::Float, LiteralValue::Float(f)) => Ok(Some(LiteralValue::Float(f))),
        (BaseType::Bool, LiteralValue::Bool(b)) => Ok(Some(LiteralValue::Bool(b))),
        (BaseType::Str, LiteralValue::Str(s)) => Ok(Some(LiteralValue::Str(s))),
        _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "coalesce() branch value does not match inferred common type.",
        )),
    }
}

fn arith_op_to_str(op: &ArithOp) -> &'static str {
    match op {
        ArithOp::Add => "add",
        ArithOp::Sub => "sub",
        ArithOp::Mul => "mul",
        ArithOp::Div => "div",
    }
}

fn cmp_op_to_str(op: &CmpOp) -> &'static str {
    match op {
        CmpOp::Eq => "eq",
        CmpOp::Ne => "ne",
        CmpOp::Lt => "lt",
        CmpOp::Le => "le",
        CmpOp::Gt => "gt",
        CmpOp::Ge => "ge",
    }
}

pub fn exprnode_to_serializable(py: Python<'_>, node: &ExprNode) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);

    dict.set_item("dtype", dtype_to_descriptor_py(py, node.dtype())?)?;

    match node {
        ExprNode::ColumnRef { name, .. } => {
            dict.set_item("kind", "column_ref")?;
            dict.set_item("name", name)?;
        }
        ExprNode::Literal { value, .. } => {
            dict.set_item("kind", "literal")?;
            let value_obj = match value {
                None => py.None(),
                Some(LiteralValue::Int(v)) => v.into_py(py),
                Some(LiteralValue::Float(v)) => v.into_py(py),
                Some(LiteralValue::Bool(v)) => v.into_py(py),
                Some(LiteralValue::Str(v)) => v.clone().into_py(py),
            };
            dict.set_item("value", value_obj)?;
        }
        ExprNode::BinaryOp {
            op, left, right, ..
        } => {
            dict.set_item("kind", "binary_op")?;
            dict.set_item("op", arith_op_to_str(op))?;
            dict.set_item("left", exprnode_to_serializable(py, left)?)?;
            dict.set_item("right", exprnode_to_serializable(py, right)?)?;
        }
        ExprNode::CompareOp {
            op, left, right, ..
        } => {
            dict.set_item("kind", "compare_op")?;
            dict.set_item("op", cmp_op_to_str(op))?;
            dict.set_item("left", exprnode_to_serializable(py, left)?)?;
            dict.set_item("right", exprnode_to_serializable(py, right)?)?;
        }
        ExprNode::IsNull { inner, .. } => {
            dict.set_item("kind", "is_null")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::IsNotNull { inner, .. } => {
            dict.set_item("kind", "is_not_null")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::Coalesce { exprs, .. } => {
            dict.set_item("kind", "coalesce")?;
            let lst = pyo3::types::PyList::empty_bound(py);
            for e in exprs {
                lst.append(exprnode_to_serializable(py, e)?)?;
            }
            dict.set_item("exprs", lst)?;
        }
        ExprNode::CaseWhen {
            branches,
            else_,
            ..
        } => {
            dict.set_item("kind", "case_when")?;
            let lst = pyo3::types::PyList::empty_bound(py);
            for (c, t) in branches {
                let pair = PyDict::new_bound(py);
                pair.set_item("condition", exprnode_to_serializable(py, c)?)?;
                pair.set_item("then", exprnode_to_serializable(py, t)?)?;
                lst.append(pair)?;
            }
            dict.set_item("branches", lst)?;
            dict.set_item("else", exprnode_to_serializable(py, else_)?)?;
        }
        ExprNode::Cast { inner, to, .. } => {
            dict.set_item("kind", "cast")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("to", base_type_to_str(*to))?;
        }
        ExprNode::InList {
            inner,
            candidates,
            ..
        } => {
            dict.set_item("kind", "in_list")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            let lst = pyo3::types::PyList::empty_bound(py);
            for c in candidates {
                lst.append(literal_value_to_py(py, c)?)?;
            }
            dict.set_item("values", lst)?;
        }
        ExprNode::Between {
            inner,
            low,
            high,
            ..
        } => {
            dict.set_item("kind", "between")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("low", exprnode_to_serializable(py, low)?)?;
            dict.set_item("high", exprnode_to_serializable(py, high)?)?;
        }
        ExprNode::StringConcat { parts, .. } => {
            dict.set_item("kind", "string_concat")?;
            let lst = pyo3::types::PyList::empty_bound(py);
            for p in parts {
                lst.append(exprnode_to_serializable(py, p)?)?;
            }
            dict.set_item("parts", lst)?;
        }
        ExprNode::Substring {
            inner,
            start,
            length,
            ..
        } => {
            dict.set_item("kind", "substring")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("start", exprnode_to_serializable(py, start)?)?;
            match length {
                None => dict.set_item("length", py.None())?,
                Some(l) => dict.set_item("length", exprnode_to_serializable(py, l)?)?,
            }
        }
        ExprNode::StringLength { inner, .. } => {
            dict.set_item("kind", "string_length")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
    }

    Ok(dict.into_py(py))
}

fn base_type_to_str(b: BaseType) -> &'static str {
    match b {
        BaseType::Int => "int",
        BaseType::Float => "float",
        BaseType::Bool => "bool",
        BaseType::Str => "str",
    }
}

fn literal_value_to_py(py: Python<'_>, v: &LiteralValue) -> PyResult<PyObject> {
    Ok(match v {
        LiteralValue::Int(i) => i.into_py(py),
        LiteralValue::Float(f) => f.into_py(py),
        LiteralValue::Bool(b) => b.into_py(py),
        LiteralValue::Str(s) => s.clone().into_py(py),
    })
}

#[derive(Clone)]
pub struct ExprHandle {
    pub node: ExprNode,
}

impl ExprHandle {
    pub fn from_py_literal(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let dtype = py_value_to_dtype(py, value)?;
        if value.is_none() {
            return Ok(Self {
                node: ExprNode::make_literal(None, dtype)?,
            });
        }

        let lit = match dtype.base {
            Some(BaseType::Int) => LiteralValue::Int(value.extract::<i64>()?),
            Some(BaseType::Float) => LiteralValue::Float(value.extract::<f64>()?),
            Some(BaseType::Bool) => LiteralValue::Bool(value.extract::<bool>()?),
            Some(BaseType::Str) => LiteralValue::Str(value.extract::<String>()?),
            None => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Non-None literal must have known base dtype.",
                ))
            }
        };

        Ok(Self {
            node: ExprNode::make_literal(Some(lit), dtype)?,
        })
    }
}

pub fn op_symbol_to_arith(op: &str) -> PyResult<ArithOp> {
    match op {
        "+" => Ok(ArithOp::Add),
        "-" => Ok(ArithOp::Sub),
        "*" => Ok(ArithOp::Mul),
        "/" => Ok(ArithOp::Div),
        other => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Unsupported arithmetic operator {:?}.",
            other
        ))),
    }
}

pub fn op_symbol_to_cmp(op: &str) -> PyResult<CmpOp> {
    match op {
        "==" => Ok(CmpOp::Eq),
        "!=" => Ok(CmpOp::Ne),
        "<" => Ok(CmpOp::Lt),
        "<=" => Ok(CmpOp::Le),
        ">" => Ok(CmpOp::Gt),
        ">=" => Ok(CmpOp::Ge),
        other => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Unsupported comparison operator {:?}.",
            other
        ))),
    }
}
