use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList, PyString};

use crate::dtype::{py_value_to_dtype, BaseType, DTypeDesc};

#[cfg(feature = "polars_engine")]
#[cfg(feature = "polars_engine")]
use polars::lazy::dsl::{col, lit, Expr as PolarsExpr};
#[cfg(feature = "polars_engine")]
use polars::prelude::Literal;
#[cfg(feature = "polars_engine")]
use polars::prelude::{DataType, Null};

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
}

impl ExprNode {
    pub fn dtype(&self) -> DTypeDesc {
        match self {
            ExprNode::ColumnRef { dtype, .. } => *dtype,
            ExprNode::Literal { dtype, .. } => *dtype,
            ExprNode::BinaryOp { dtype, .. } => *dtype,
            ExprNode::CompareOp { dtype, .. } => *dtype,
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
                                                _ => unreachable!(),
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
                                                _ => unreachable!(),
                                            }
                                        }
                                        _ => {
                                            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                            "Ordering operand types not supported by typed skeleton.",
                                        ));
                                        }
                                    }
                                }
                                _ => unreachable!(),
                            };

                            out.push(Some(LiteralValue::Bool(res_bool)));
                        }
                    }
                }

                Ok(out)
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
        }
    }
}

#[derive(Clone)]
pub struct ExprHandle {
    pub node: ExprNode,
}

impl ExprHandle {
    pub fn new_column(name: String, dtype: DTypeDesc) -> PyResult<Self> {
        Ok(Self {
            node: ExprNode::make_column_ref(name, dtype)?,
        })
    }

    pub fn new_literal(value: Option<LiteralValue>, dtype: DTypeDesc) -> PyResult<Self> {
        Ok(Self {
            node: ExprNode::make_literal(value, dtype)?,
        })
    }

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
