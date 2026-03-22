//! Lower [`ExprNode`] to Polars [`Expr`](polars::prelude::Expr).

use pyo3::prelude::*;

use crate::dtype::{BaseType, DECIMAL_PRECISION, DECIMAL_SCALE};

use super::ir::{
    ArithOp, CmpOp, ExprNode, LiteralValue, LogicalOp, StringUnaryOp, TemporalPart, UnaryNumericOp,
};

use polars::lazy::dsl::{coalesce, col, concat_str, lit, ternary_expr, Expr as PolarsExpr};
use polars::prelude::{
    ClosedInterval, DataType, Int128Chunked, IntoSeries, Literal, NamedFrom, NewChunkedArray, Null,
    RoundMode, Scalar, Series, TimeUnit,
};

/// Polars lowering hook (dependency inversion / extension point for new variants).
#[allow(dead_code)]
pub trait LowerToPolars {
    fn lower_to_polars(&self) -> PyResult<PolarsExpr>;
}

#[allow(dead_code)]
impl LowerToPolars for ExprNode {
    fn lower_to_polars(&self) -> PyResult<PolarsExpr> {
        self.to_polars_expr()
    }
}

#[cfg(feature = "polars_engine")]
fn literals_to_series(values: &[LiteralValue], base: BaseType) -> PyResult<Series> {
    match base {
        BaseType::Int => {
            let v: Vec<i64> = values
                .iter()
                .map(|x| match x {
                    LiteralValue::Int(i) => Ok(*i),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "isin() int list expected.",
                    )),
                })
                .collect::<PyResult<_>>()?;
            Ok(Series::new("".into(), v))
        }
        BaseType::Float => {
            let v: Vec<f64> = values
                .iter()
                .map(|x| match x {
                    LiteralValue::Float(f) => Ok(*f),
                    LiteralValue::Int(i) => Ok(*i as f64),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "isin() float list expected.",
                    )),
                })
                .collect::<PyResult<_>>()?;
            Ok(Series::new("".into(), v))
        }
        BaseType::Bool => {
            let v: Vec<bool> = values
                .iter()
                .map(|x| match x {
                    LiteralValue::Bool(b) => Ok(*b),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "isin() bool list expected.",
                    )),
                })
                .collect::<PyResult<_>>()?;
            Ok(Series::new("".into(), v))
        }
        BaseType::Str => {
            let v: Vec<String> = values
                .iter()
                .map(|x| match x {
                    LiteralValue::Str(s) => Ok(s.clone()),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "isin() str list expected.",
                    )),
                })
                .collect::<PyResult<_>>()?;
            Ok(Series::new("".into(), v))
        }
        BaseType::Enum => {
            let v: Vec<String> = values
                .iter()
                .map(|x| match x {
                    LiteralValue::EnumStr(s) | LiteralValue::Str(s) => Ok(s.clone()),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "isin() enum list expected (str or enum literal).",
                    )),
                })
                .collect::<PyResult<_>>()?;
            Ok(Series::new("".into(), v))
        }
        BaseType::Uuid => {
            let v: Vec<String> = values
                .iter()
                .map(|x| match x {
                    LiteralValue::Uuid(s) => Ok(s.clone()),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "isin() uuid list expected.",
                    )),
                })
                .collect::<PyResult<_>>()?;
            Ok(Series::new("".into(), v))
        }
        BaseType::Decimal => {
            let v: Vec<i128> = values
                .iter()
                .map(|x| match x {
                    LiteralValue::Decimal(i) => Ok(*i),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "isin() decimal list expected.",
                    )),
                })
                .collect::<PyResult<_>>()?;
            Ok(Int128Chunked::from_iter_values("".into(), v.into_iter())
                .into_decimal(DECIMAL_PRECISION, DECIMAL_SCALE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{e}")))?
                .into_series())
        }
        _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "isin() unsupported dtype for list literal.",
        )),
    }
}

impl ExprNode {
    pub fn to_polars_expr(&self) -> PyResult<PolarsExpr> {
        match self {
            ExprNode::ColumnRef { name, .. } => Ok(col(name)),
            ExprNode::Literal { value, dtype } => match value {
                Some(LiteralValue::Int(i)) => Ok(lit(*i)),
                Some(LiteralValue::Float(f)) => Ok(lit(*f)),
                Some(LiteralValue::Bool(b)) => Ok(lit(*b)),
                Some(LiteralValue::Str(s)) => Ok(lit(s.clone())),
                Some(LiteralValue::Uuid(s)) => Ok(lit(s.clone())),
                Some(LiteralValue::EnumStr(s)) => Ok(lit(s.clone())),
                Some(LiteralValue::Decimal(v)) => {
                    Ok(Scalar::new_decimal(*v, DECIMAL_PRECISION, DECIMAL_SCALE).lit())
                }
                Some(LiteralValue::DateTimeMicros(v)) => {
                    Ok(lit(*v).cast(DataType::Datetime(TimeUnit::Microseconds, None)))
                }
                Some(LiteralValue::DateDays(v)) => Ok(lit(*v).cast(DataType::Date)),
                Some(LiteralValue::DurationMicros(v)) => {
                    Ok(lit(*v).cast(DataType::Duration(TimeUnit::Microseconds)))
                }
                None => {
                    let null_expr = Null {}.lit();
                    match dtype {
                        crate::dtype::DTypeDesc::Scalar {
                            base: Some(BaseType::Int),
                            ..
                        } => Ok(null_expr.cast(DataType::Int64)),
                        crate::dtype::DTypeDesc::Scalar {
                            base: Some(BaseType::Float),
                            ..
                        } => Ok(null_expr.cast(DataType::Float64)),
                        crate::dtype::DTypeDesc::Scalar {
                            base: Some(BaseType::Bool),
                            ..
                        } => Ok(null_expr.cast(DataType::Boolean)),
                        crate::dtype::DTypeDesc::Scalar {
                            base: Some(BaseType::Str | BaseType::Enum),
                            ..
                        } => Ok(null_expr.cast(DataType::String)),
                        crate::dtype::DTypeDesc::Scalar {
                            base: Some(BaseType::Uuid),
                            ..
                        } => Ok(null_expr.cast(DataType::String)),
                        crate::dtype::DTypeDesc::Scalar {
                            base: Some(BaseType::Decimal),
                            ..
                        } => {
                            Ok(null_expr.cast(DataType::Decimal(DECIMAL_PRECISION, DECIMAL_SCALE)))
                        }
                        crate::dtype::DTypeDesc::Scalar {
                            base: Some(BaseType::DateTime),
                            ..
                        } => Ok(null_expr.cast(DataType::Datetime(TimeUnit::Microseconds, None))),
                        crate::dtype::DTypeDesc::Scalar {
                            base: Some(BaseType::Date),
                            ..
                        } => Ok(null_expr.cast(DataType::Date)),
                        crate::dtype::DTypeDesc::Scalar {
                            base: Some(BaseType::Duration),
                            ..
                        } => Ok(null_expr.cast(DataType::Duration(TimeUnit::Microseconds))),
                        crate::dtype::DTypeDesc::Scalar { base: None, .. } => Ok(null_expr),
                        crate::dtype::DTypeDesc::Struct { .. } => Err(PyErr::new::<
                            pyo3::exceptions::PyTypeError,
                            _,
                        >(
                            "Null struct literals must be lowered via schema-typed plan context.",
                        )),
                        crate::dtype::DTypeDesc::List { .. } => {
                            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                "Null list literals must be lowered via schema-typed plan context.",
                            ))
                        }
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
            ExprNode::Cast { input, dtype } => {
                let dt = match dtype {
                    crate::dtype::DTypeDesc::Scalar {
                        base: Some(BaseType::Int),
                        ..
                    } => DataType::Int64,
                    crate::dtype::DTypeDesc::Scalar {
                        base: Some(BaseType::Float),
                        ..
                    } => DataType::Float64,
                    crate::dtype::DTypeDesc::Scalar {
                        base: Some(BaseType::Bool),
                        ..
                    } => DataType::Boolean,
                    crate::dtype::DTypeDesc::Scalar {
                        base: Some(BaseType::Str | BaseType::Enum),
                        ..
                    } => DataType::String,
                    crate::dtype::DTypeDesc::Scalar {
                        base: Some(BaseType::Uuid),
                        ..
                    } => DataType::String,
                    crate::dtype::DTypeDesc::Scalar {
                        base: Some(BaseType::Decimal),
                        ..
                    } => DataType::Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
                    crate::dtype::DTypeDesc::Scalar {
                        base: Some(BaseType::DateTime),
                        ..
                    } => DataType::Datetime(TimeUnit::Microseconds, None),
                    crate::dtype::DTypeDesc::Scalar {
                        base: Some(BaseType::Date),
                        ..
                    } => DataType::Date,
                    crate::dtype::DTypeDesc::Scalar {
                        base: Some(BaseType::Duration),
                        ..
                    } => DataType::Duration(TimeUnit::Microseconds),
                    _ => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "cast() target dtype must have known scalar base.",
                        ));
                    }
                };
                Ok(input.to_polars_expr()?.cast(dt))
            }
            ExprNode::IsNull { input, .. } => Ok(input.to_polars_expr()?.is_null()),
            ExprNode::IsNotNull { input, .. } => Ok(input.to_polars_expr()?.is_not_null()),
            ExprNode::Coalesce { exprs, .. } => {
                let parts: Vec<PolarsExpr> = exprs
                    .iter()
                    .map(|e| e.to_polars_expr())
                    .collect::<PyResult<_>>()?;
                Ok(coalesce(parts.as_slice()))
            }
            ExprNode::CaseWhen {
                branches, else_, ..
            } => {
                let mut acc = else_.to_polars_expr()?;
                for (c, t) in branches.iter().rev() {
                    acc = ternary_expr(c.to_polars_expr()?, t.to_polars_expr()?, acc);
                }
                Ok(acc)
            }
            ExprNode::InList { inner, values, .. } => {
                let base = inner
                    .dtype()
                    .as_scalar_base_field()
                    .flatten()
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "isin() inner dtype unknown.",
                        )
                    })?;
                let series = literals_to_series(values, base)?;
                Ok(inner.to_polars_expr()?.is_in(lit(series), true))
            }
            ExprNode::Between {
                inner, low, high, ..
            } => Ok(inner.to_polars_expr()?.is_between(
                low.to_polars_expr()?,
                high.to_polars_expr()?,
                ClosedInterval::Both,
            )),
            ExprNode::StringConcat { parts, .. } => {
                let exprs: Vec<PolarsExpr> = parts
                    .iter()
                    .map(|p| p.to_polars_expr())
                    .collect::<PyResult<_>>()?;
                Ok(concat_str(exprs.as_slice(), "", true))
            }
            ExprNode::Substring {
                inner,
                start,
                length,
                ..
            } => {
                let inner_e = inner.to_polars_expr()?;
                let off = start.to_polars_expr()? - lit(1i64);
                let len = match length {
                    Some(l) => l.to_polars_expr()?,
                    None => lit(1_000_000i64),
                };
                Ok(inner_e.str().slice(off, len))
            }
            ExprNode::StringLength { inner, .. } => Ok(inner.to_polars_expr()?.str().len_chars()),
            ExprNode::StringReplace {
                inner,
                pattern,
                replacement,
                ..
            } => {
                let e = inner.to_polars_expr()?;
                Ok(e.str()
                    .replace_all(lit(pattern.as_str()), lit(replacement.as_str()), true))
            }
            ExprNode::StructField { base, field, .. } => Ok(base
                .to_polars_expr()?
                .struct_()
                .field_by_name(field.as_str())),
            ExprNode::UnaryNumeric { op, inner, .. } => {
                let e = inner.to_polars_expr()?;
                match op {
                    UnaryNumericOp::Abs => Ok(e.abs()),
                    UnaryNumericOp::Round { decimals } => {
                        Ok(e.round(*decimals, RoundMode::default()))
                    }
                    UnaryNumericOp::Floor => Ok(e.floor()),
                    UnaryNumericOp::Ceil => Ok(e.ceil()),
                }
            }
            ExprNode::StringUnary { op, inner, .. } => {
                let e = inner.to_polars_expr()?;
                match op {
                    StringUnaryOp::Strip => Ok(e.str().strip_chars(Null {}.lit())),
                    StringUnaryOp::Upper => Ok(e.str().to_uppercase()),
                    StringUnaryOp::Lower => Ok(e.str().to_lowercase()),
                    StringUnaryOp::StripPrefix(p) => Ok(e.str().strip_prefix(lit(p.as_str()))),
                    StringUnaryOp::StripSuffix(s) => Ok(e.str().strip_suffix(lit(s.as_str()))),
                    StringUnaryOp::StripChars(c) => Ok(e.str().strip_chars(lit(c.as_str()))),
                }
            }
            ExprNode::LogicalBinary {
                op, left, right, ..
            } => {
                let l = left.to_polars_expr()?;
                let r = right.to_polars_expr()?;
                match op {
                    LogicalOp::And => Ok(l.and(r)),
                    LogicalOp::Or => Ok(l.or(r)),
                }
            }
            ExprNode::LogicalNot { inner, .. } => Ok(inner.to_polars_expr()?.not()),
            ExprNode::TemporalPart { part, inner, .. } => {
                let e = inner.to_polars_expr()?;
                let dt = e.dt();
                match part {
                    TemporalPart::Year => Ok(dt.year()),
                    TemporalPart::Month => Ok(dt.month()),
                    TemporalPart::Day => Ok(dt.day()),
                    TemporalPart::Hour => Ok(dt.hour()),
                    TemporalPart::Minute => Ok(dt.minute()),
                    TemporalPart::Second => Ok(dt.second()),
                }
            }
            ExprNode::ListLen { inner, .. } => Ok(inner.to_polars_expr()?.list().len()),
            ExprNode::ListGet { inner, index, .. } => Ok(inner
                .to_polars_expr()?
                .list()
                .get(index.to_polars_expr()?, true)),
            ExprNode::ListContains { inner, value, .. } => Ok(inner
                .to_polars_expr()?
                .list()
                .contains(value.to_polars_expr()?, true)),
            ExprNode::ListMin { inner, .. } => Ok(inner.to_polars_expr()?.list().min()),
            ExprNode::ListMax { inner, .. } => Ok(inner.to_polars_expr()?.list().max()),
            ExprNode::ListSum { inner, .. } => Ok(inner.to_polars_expr()?.list().sum()),
            ExprNode::DatetimeToDate { inner, .. } => Ok(inner.to_polars_expr()?.dt().date()),
        }
    }
}
