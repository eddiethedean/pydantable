//! Row-wise [`ExprNode`] evaluation when the Polars engine is disabled.
//!
//! Lives in a submodule of [`typing`](super) so helpers in `typing.rs` stay private
//! while remaining callable via `super::`.

use std::collections::HashMap;

use pyo3::prelude::*;

use crate::dtype::{BaseType, DTypeDesc};

use crate::expr::ir::{
    ArithOp, CmpOp, ExprNode, LiteralValue, LogicalOp, StringPredicateKind, StringUnaryOp,
    TemporalPart, UnaryNumericOp,
};

pub(super) fn eval_expr_node(
    node: &ExprNode,
    ctx: &HashMap<String, Vec<Option<LiteralValue>>>,
    n: usize,
) -> PyResult<Vec<Option<LiteralValue>>> {
    match node {
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
            if let Some(lit) = value.as_ref() {
                Ok((0..n).map(|_| Some(lit.clone())).collect())
            } else {
                Ok(vec![None; n])
            }
        }
        ExprNode::BinaryOp {
            op,
            left,
            right,
            dtype,
        } => {
            let lvals = eval_expr_node(left, ctx, n)?;
            let rvals = eval_expr_node(right, ctx, n)?;
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
                                ArithOp::Div => {
                                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                        "internal error: division in integer arithmetic path.",
                                    ));
                                }
                            };
                            out.push(Some(LiteralValue::Int(res_i)));
                        }
                        BaseType::Float => {
                            let (af, bf) = match (va, vb) {
                                (LiteralValue::Int(ai), LiteralValue::Int(bi)) => {
                                    (ai as f64, bi as f64)
                                }
                                (LiteralValue::Float(af), LiteralValue::Float(bf)) => (af, bf),
                                (LiteralValue::Int(ai), LiteralValue::Float(bf)) => (ai as f64, bf),
                                (LiteralValue::Float(af), LiteralValue::Int(bi)) => (af, bi as f64),
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
                                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                        "date arithmetic expects date ± duration.",
                                    ));
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
            let lvals = eval_expr_node(left, ctx, n)?;
            let rvals = eval_expr_node(right, ctx, n)?;
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
                                    BaseType::Str
                                    | BaseType::Enum
                                    | BaseType::Ipv4
                                    | BaseType::Ipv6 => {
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
                                            LiteralValue::Decimal(i) => i,
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
                                            LiteralValue::Decimal(i) => i,
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
                                    BaseType::DateTime => match (va, vb) {
                                        (
                                            LiteralValue::DateTimeMicros(a),
                                            LiteralValue::DateTimeMicros(b),
                                        ) => a == b,
                                        _ => {
                                            return Err(PyErr::new::<
                                                pyo3::exceptions::PyTypeError,
                                                _,
                                            >(
                                                "Typed equality expected datetime operands.",
                                            ));
                                        }
                                    },
                                    BaseType::Date => match (va, vb) {
                                        (LiteralValue::DateDays(a), LiteralValue::DateDays(b)) => {
                                            a == b
                                        }
                                        _ => {
                                            return Err(PyErr::new::<
                                                pyo3::exceptions::PyTypeError,
                                                _,
                                            >(
                                                "Typed equality expected date operands."
                                            ));
                                        }
                                    },
                                    BaseType::Duration => match (va, vb) {
                                        (
                                            LiteralValue::DurationMicros(a),
                                            LiteralValue::DurationMicros(b),
                                        ) => a == b,
                                        _ => {
                                            return Err(PyErr::new::<
                                                pyo3::exceptions::PyTypeError,
                                                _,
                                            >(
                                                "Typed equality expected duration operands.",
                                            ));
                                        }
                                    },
                                    BaseType::Time => match (va, vb) {
                                        (
                                            LiteralValue::TimeNanos(a),
                                            LiteralValue::TimeNanos(b),
                                        ) => a == b,
                                        _ => {
                                            return Err(PyErr::new::<
                                                pyo3::exceptions::PyTypeError,
                                                _,
                                            >(
                                                "Typed equality expected time operands."
                                            ));
                                        }
                                    },
                                    BaseType::Binary | BaseType::Wkb => match (va, vb) {
                                        (LiteralValue::Binary(a), LiteralValue::Binary(b)) => {
                                            a == b
                                        }
                                        _ => {
                                            return Err(PyErr::new::<
                                                pyo3::exceptions::PyTypeError,
                                                _,
                                            >(
                                                "Typed equality expected binary operands.",
                                            ));
                                        }
                                    },
                                };
                                if *op == CmpOp::Eq {
                                    eq
                                } else {
                                    !eq
                                }
                            }
                            CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge => match effective_base {
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
                                        CmpOp::Eq | CmpOp::Ne => {
                                            return Err(PyErr::new::<
                                                        pyo3::exceptions::PyRuntimeError,
                                                        _,
                                                    >(
                                                        "internal error: unexpected comparison op in numeric ordering path.",
                                                    ));
                                        }
                                    }
                                }
                                BaseType::Str
                                | BaseType::Enum
                                | BaseType::Ipv4
                                | BaseType::Ipv6 => {
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
                                        CmpOp::Eq | CmpOp::Ne => {
                                            return Err(PyErr::new::<
                                                        pyo3::exceptions::PyRuntimeError,
                                                        _,
                                                    >(
                                                        "internal error: unexpected comparison op in string ordering path.",
                                                    ));
                                        }
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
                                                "Typed ordering expected uuid operands."
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
                                                "Typed ordering expected uuid operands."
                                            ));
                                        }
                                    };
                                    match op {
                                        CmpOp::Lt => as_ < bs_,
                                        CmpOp::Le => as_ <= bs_,
                                        CmpOp::Gt => as_ > bs_,
                                        CmpOp::Ge => as_ >= bs_,
                                        CmpOp::Eq | CmpOp::Ne => {
                                            return Err(PyErr::new::<
                                                        pyo3::exceptions::PyRuntimeError,
                                                        _,
                                                    >(
                                                        "internal error: unexpected comparison op in uuid ordering path.",
                                                    ));
                                        }
                                    }
                                }
                                BaseType::Decimal => {
                                    let ai = match va {
                                        LiteralValue::Decimal(i) => i,
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
                                        LiteralValue::Decimal(i) => i,
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
                                        CmpOp::Eq | CmpOp::Ne => {
                                            return Err(PyErr::new::<
                                                        pyo3::exceptions::PyRuntimeError,
                                                        _,
                                                    >(
                                                        "internal error: unexpected comparison op in decimal ordering path.",
                                                    ));
                                        }
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
                                    let (a, b) = match (va, vb) {
                                        (LiteralValue::DateDays(a), LiteralValue::DateDays(b)) => {
                                            (a, b)
                                        }
                                        _ => {
                                            return Err(PyErr::new::<
                                                pyo3::exceptions::PyTypeError,
                                                _,
                                            >(
                                                "Typed ordering expected date operands."
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
                                BaseType::Time => {
                                    let (a, b) = match (va, vb) {
                                        (
                                            LiteralValue::TimeNanos(a),
                                            LiteralValue::TimeNanos(b),
                                        ) => (a, b),
                                        _ => {
                                            return Err(PyErr::new::<
                                                pyo3::exceptions::PyTypeError,
                                                _,
                                            >(
                                                "Typed ordering expected time operands."
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
                                BaseType::Binary | BaseType::Wkb => {
                                    let (a, b) = match (va, vb) {
                                        (LiteralValue::Binary(a), LiteralValue::Binary(b)) => {
                                            (a, b)
                                        }
                                        _ => {
                                            return Err(PyErr::new::<
                                                pyo3::exceptions::PyTypeError,
                                                _,
                                            >(
                                                "Typed ordering expected binary operands.",
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
                            },
                        };

                        out.push(Some(LiteralValue::Bool(res_bool)));
                    }
                }
            }

            Ok(out)
        }
        ExprNode::Cast { input, dtype } => {
            let vals = eval_expr_node(input, ctx, n)?;
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
                    Some(v) => out.push(Some(super::cast_literal_value(v, target)?)),
                }
            }
            Ok(out)
        }
        ExprNode::IsNull { input, .. } => {
            let vals = eval_expr_node(input, ctx, n)?;
            Ok(vals
                .into_iter()
                .map(|v| Some(LiteralValue::Bool(v.is_none())))
                .collect())
        }
        ExprNode::IsNotNull { input, .. } => {
            let vals = eval_expr_node(input, ctx, n)?;
            Ok(vals
                .into_iter()
                .map(|v| Some(LiteralValue::Bool(v.is_some())))
                .collect())
        }
        ExprNode::Coalesce { exprs, .. } => {
            let mut cols: Vec<Vec<Option<LiteralValue>>> = Vec::new();
            for e in exprs {
                cols.push(eval_expr_node(e, ctx, n)?);
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
                cond_cols.push(eval_expr_node(c, ctx, n)?);
                then_cols.push(eval_expr_node(t, ctx, n)?);
            }
            let else_v = eval_expr_node(else_, ctx, n)?;
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
            let vals = eval_expr_node(inner, ctx, n)?;
            Ok(vals
                .into_iter()
                .map(|v| v.map(|x| LiteralValue::Bool(values.iter().any(|u| u == &x))))
                .collect())
        }
        ExprNode::Between {
            inner, low, high, ..
        } => {
            let iv = eval_expr_node(inner, ctx, n)?;
            let lv = eval_expr_node(low, ctx, n)?;
            let hv = eval_expr_node(high, ctx, n)?;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let tri = match (
                    iv.get(i).and_then(|x| x.as_ref()),
                    lv.get(i).and_then(|x| x.as_ref()),
                    hv.get(i).and_then(|x| x.as_ref()),
                ) {
                    (Some(a), Some(b), Some(c)) => Some(super::literal_between_inclusive(a, b, c)),
                    _ => None,
                };
                out.push(tri.map(LiteralValue::Bool));
            }
            Ok(out)
        }
        ExprNode::StringConcat { parts, .. } => {
            let part_vals: Vec<Vec<Option<LiteralValue>>> = parts
                .iter()
                .map(|p| eval_expr_node(p, ctx, n))
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
            let svals = eval_expr_node(inner, ctx, n)?;
            let stvals = eval_expr_node(start, ctx, n)?;
            let lnvals = if let Some(l) = length {
                Some(eval_expr_node(l, ctx, n)?)
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
            let vals = eval_expr_node(inner, ctx, n)?;
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
                        crate::dtype::scaled_i128_to_decimal_string(d)
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
            literal,
            ..
        } => {
            if !literal {
                return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                    "Regex str_replace is only supported with the Polars execution engine.",
                ));
            }
            let vals = eval_expr_node(inner, ctx, n)?;
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
        ExprNode::StringPredicate {
            inner,
            kind,
            pattern,
            ..
        } => {
            #[derive(Copy, Clone)]
            enum RowWiseStrPred {
                Starts,
                Ends,
                ContainsLiteral,
            }
            let pred = match kind {
                StringPredicateKind::StartsWith => RowWiseStrPred::Starts,
                StringPredicateKind::EndsWith => RowWiseStrPred::Ends,
                StringPredicateKind::Contains { literal: true } => RowWiseStrPred::ContainsLiteral,
                StringPredicateKind::Contains { literal: false } => {
                    return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                        "Regex str_contains is only supported with the Polars execution engine.",
                    ));
                }
            };
            let vals = eval_expr_node(inner, ctx, n)?;
            let pat = pattern.as_str();
            Ok(vals
                .into_iter()
                .map(|v| {
                    let str_like = |s: &str| {
                        let b = match pred {
                            RowWiseStrPred::Starts => s.starts_with(pat),
                            RowWiseStrPred::Ends => s.ends_with(pat),
                            RowWiseStrPred::ContainsLiteral => s.contains(pat),
                        };
                        Some(LiteralValue::Bool(b))
                    };
                    match v {
                        Some(LiteralValue::Str(s)) => str_like(s.as_str()),
                        Some(LiteralValue::EnumStr(s)) => str_like(s.as_str()),
                        _ => None,
                    }
                })
                .collect())
        }
        ExprNode::UnaryNumeric { op, inner, .. } => {
            let vals = eval_expr_node(inner, ctx, n)?;
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
            let vals = eval_expr_node(inner, ctx, n)?;
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
                            Some(LiteralValue::Str(super::trim_matches_char_set(s, c)))
                        }
                        StringUnaryOp::Reverse => {
                            Some(LiteralValue::Str(s.chars().rev().collect()))
                        }
                        StringUnaryOp::PadStart { length, fill_char } => Some(LiteralValue::Str(
                            super::str_pad_start_chars(s, *length, *fill_char),
                        )),
                        StringUnaryOp::PadEnd { length, fill_char } => Some(LiteralValue::Str(
                            super::str_pad_end_chars(s, *length, *fill_char),
                        )),
                        StringUnaryOp::ZFill { length } => {
                            Some(LiteralValue::Str(super::str_zfill_chars(s, *length)))
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
            let lv = eval_expr_node(left, ctx, n)?;
            let rv = eval_expr_node(right, ctx, n)?;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let lb = lv.get(i).and_then(|x| x.as_ref());
                let rb = rv.get(i).and_then(|x| x.as_ref());
                let res = match (op, lb, rb) {
                    (LogicalOp::And, Some(LiteralValue::Bool(a)), Some(LiteralValue::Bool(b))) => {
                        Some(LiteralValue::Bool(*a && *b))
                    }
                    (LogicalOp::Or, Some(LiteralValue::Bool(a)), Some(LiteralValue::Bool(b))) => {
                        Some(LiteralValue::Bool(*a || *b))
                    }
                    _ => None,
                };
                out.push(res);
            }
            Ok(out)
        }
        ExprNode::LogicalNot { inner, .. } => {
            let vals = eval_expr_node(inner, ctx, n)?;
            Ok(vals
                .into_iter()
                .map(|v| match v {
                    Some(LiteralValue::Bool(b)) => Some(LiteralValue::Bool(!b)),
                    _ => None,
                })
                .collect())
        }
        ExprNode::DatetimeToDate { inner, .. } => {
            let vals = eval_expr_node(inner, ctx, n)?;
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
            let vals = eval_expr_node(inner, ctx, n)?;
            let is_date = inner.dtype().as_scalar_base_field().flatten() == Some(BaseType::Date);
            let is_time = inner.dtype().as_scalar_base_field().flatten() == Some(BaseType::Time);
            const NS_PER_HOUR: i64 = 3_600_000_000_000;
            const NS_PER_MIN: i64 = 60_000_000_000;
            const NS_PER_SEC: i64 = 1_000_000_000;
            Ok(vals
                .into_iter()
                .map(|v| match v {
                    None => None,
                    Some(LiteralValue::DateTimeMicros(us)) if !is_date && !is_time => {
                        let (y, mo, d, h, mi, s) = super::utc_ymdhms_from_unix_micros(us);
                        let i = match part {
                            TemporalPart::Year => i64::from(y),
                            TemporalPart::Month => i64::from(mo),
                            TemporalPart::Day => i64::from(d),
                            TemporalPart::Hour => i64::from(h),
                            TemporalPart::Minute => i64::from(mi),
                            TemporalPart::Second => i64::from(s),
                            TemporalPart::Nanosecond => {
                                let sub_us = us.rem_euclid(1_000_000);
                                sub_us * 1000
                            }
                            TemporalPart::Weekday => {
                                super::iso_weekday_monday1(y, mo as i32, d as i32)
                            }
                            TemporalPart::Quarter => ((i64::from(mo) - 1) / 3) + 1,
                            TemporalPart::Week => super::iso_week_from_ymd(y, mo, d),
                            TemporalPart::DayOfYear => super::ordinal_day_from_ymd(y, mo, d),
                        };
                        Some(LiteralValue::Int(i))
                    }
                    Some(LiteralValue::DateDays(days)) if is_date => {
                        let (y, mo, d) = super::utc_calendar_from_epoch_days(days);
                        match part {
                            TemporalPart::Hour
                            | TemporalPart::Minute
                            | TemporalPart::Second
                            | TemporalPart::Nanosecond => None,
                            TemporalPart::Year => Some(LiteralValue::Int(i64::from(y))),
                            TemporalPart::Month => Some(LiteralValue::Int(i64::from(mo))),
                            TemporalPart::Day => Some(LiteralValue::Int(i64::from(d))),
                            TemporalPart::Weekday => Some(LiteralValue::Int(
                                super::iso_weekday_monday1(y, mo as i32, d as i32),
                            )),
                            TemporalPart::Quarter => {
                                Some(LiteralValue::Int(((i64::from(mo) - 1) / 3) + 1))
                            }
                            TemporalPart::Week => {
                                Some(LiteralValue::Int(super::iso_week_from_ymd(y, mo, d)))
                            }
                            TemporalPart::DayOfYear => {
                                Some(LiteralValue::Int(super::ordinal_day_from_ymd(y, mo, d)))
                            }
                        }
                    }
                    Some(LiteralValue::TimeNanos(ns)) if is_time => match part {
                        TemporalPart::Year
                        | TemporalPart::Month
                        | TemporalPart::Day
                        | TemporalPart::Weekday
                        | TemporalPart::Quarter
                        | TemporalPart::Week
                        | TemporalPart::DayOfYear => None,
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
        | ExprNode::ListMean { .. }
        | ExprNode::ListJoin { .. }
        | ExprNode::ListSort { .. }
        | ExprNode::ListUnique { .. }
        | ExprNode::StringSplit { .. }
        | ExprNode::StringExtract { .. }
        | ExprNode::StringJsonPathMatch { .. }
        | ExprNode::StringJsonDecode { .. }
        | ExprNode::Strptime { .. }
        | ExprNode::UnixTimestamp { .. }
        | ExprNode::FromUnixTime { .. }
        | ExprNode::BinaryLength { .. }
        | ExprNode::MapLen { .. }
        | ExprNode::MapGet { .. }
        | ExprNode::MapContainsKey { .. }
        | ExprNode::MapKeys { .. }
        | ExprNode::MapValues { .. }
        | ExprNode::MapEntries { .. }
        | ExprNode::MapFromEntries { .. } => {
            Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                "This expression is only supported with the Polars execution engine.",
            ))
        }
        ExprNode::StructField { .. }
        | ExprNode::StructJsonEncode { .. }
        | ExprNode::StructJsonPathMatch { .. }
        | ExprNode::StructRenameFields { .. }
        | ExprNode::StructWithFields { .. } => {
            Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                "Struct operations are only supported with the Polars execution engine.",
            ))
        }
        ExprNode::Window { .. } => Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "Window expressions are only supported with the Polars execution engine.",
        )),
        ExprNode::GlobalAgg { .. } | ExprNode::GlobalRowCount { .. } => {
            Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                "Global aggregate expressions are only supported with the Polars execution engine.",
            ))
        }
        ExprNode::RowAccum { .. } => Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "Row accumulation expressions are only supported with the Polars execution engine.",
        )),
    }
}
