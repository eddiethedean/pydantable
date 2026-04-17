//! Lower [`ExprNode`] to Polars [`Expr`](polars::prelude::Expr).

use pyo3::prelude::*;

use crate::dtype::{BaseType, DECIMAL_PRECISION, DECIMAL_SCALE};

use super::ir::{
    ArithOp, CmpOp, ExprNode, GlobalAggOp, LiteralValue, LogicalOp, RowAccumOp,
    StringPredicateKind, StringUnaryOp, TemporalPart, UnaryNumericOp, UnixTimestampUnit, WindowOp,
};

use polars::lazy::dsl::{
    coalesce, col, concat_str, element, int_range, len, lit, ternary_expr, when, Expr as PolarsExpr,
};
use polars::prelude::{
    ClosedInterval, DataType, Int128Chunked, IntoSeries, Literal, NamedFrom, NewChunkedArray, Null,
    RankMethod, RankOptions, RoundMode, Scalar, Series, SortOptions, StrptimeOptions, TimeUnit,
    WindowMapping,
};
use polars_core::series::ops::NullBehavior; // direct dep for diff(null_behavior); polars re-export is incomplete

/// Polars lowering hook for [`ExprNode`]: delegates to [`ExprNode::to_polars_expr`].
///
/// Third-party code can implement this for wrapper types; the native engine calls
/// [`ExprNode::to_polars_expr`] directly. See the repo **ADR — engines** (`docs/project/adrs/engines.md`).
#[allow(dead_code)] // Public extension surface; not yet referenced outside this blanket impl.
pub trait LowerToPolars {
    fn lower_to_polars(&self) -> PyResult<PolarsExpr>;
}

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
        BaseType::Ipv4 | BaseType::Ipv6 => {
            let v: Vec<String> = values
                .iter()
                .map(|x| match x {
                    LiteralValue::Str(s) => Ok(s.clone()),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "isin() ip address list expected str literals.",
                    )),
                })
                .collect::<PyResult<_>>()?;
            Ok(Series::new("".into(), v))
        }
        BaseType::Wkb => {
            let v: Vec<Vec<u8>> = values
                .iter()
                .map(|x| match x {
                    LiteralValue::Binary(b) => Ok(b.clone()),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "isin() WKB list expected bytes literals.",
                    )),
                })
                .collect::<PyResult<_>>()?;
            Ok(Series::new("".into(), v))
        }
        _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "isin() unsupported dtype for list literal.",
        )),
    }
}

fn apply_window_over(
    inner: PolarsExpr,
    partition_by: &[String],
    order_by: &[(String, bool, bool)],
) -> PyResult<PolarsExpr> {
    if order_by.len() > 1 {
        let (asc0, nl0) = (order_by[0].1, order_by[0].2);
        for (_name, asc, nl) in order_by.iter().skip(1) {
            if *asc != asc0 || *nl != nl0 {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "unframed multi-column window orderBy must use the same `ascending` and \
                     `nulls_last` for every sort key (Polars `.over` accepts only one SortOptions \
                     for all order columns). Use one order key, use matching options on all keys, \
                     or use a framed window (`rowsBetween` / `rangeBetween`), where per-key null \
                     placement is honored.",
                ));
            }
        }
    }
    let part_cols: Vec<PolarsExpr> = partition_by.iter().map(|n| col(n.as_str())).collect();
    let partition_arg = if part_cols.is_empty() {
        None
    } else {
        Some(part_cols.as_slice())
    };
    let order_cols: Vec<PolarsExpr> = order_by.iter().map(|(n, _, _)| col(n.as_str())).collect();
    let order_arg = if order_cols.is_empty() {
        None
    } else {
        let opts = SortOptions {
            descending: !order_by[0].1,
            nulls_last: order_by[0].2,
            ..Default::default()
        };
        Some((order_cols.as_slice(), opts))
    };
    inner
        .over_with_options(partition_arg, order_arg, WindowMapping::default())
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{e}")))
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
                Some(LiteralValue::TimeNanos(ns)) => Ok(lit(*ns).cast(DataType::Time)),
                Some(LiteralValue::Binary(b)) => Ok(lit(b.as_slice()).cast(DataType::Binary)),
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
                            base: Some(BaseType::Uuid | BaseType::Ipv4 | BaseType::Ipv6),
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
                        crate::dtype::DTypeDesc::Scalar {
                            base: Some(BaseType::Time),
                            ..
                        } => Ok(null_expr.cast(DataType::Time)),
                        crate::dtype::DTypeDesc::Scalar {
                            base: Some(BaseType::Binary | BaseType::Wkb),
                            ..
                        } => Ok(null_expr.cast(DataType::Binary)),
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
                        crate::dtype::DTypeDesc::Map { .. } => {
                            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                "Null map literals must be lowered via schema-typed plan context.",
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
                        base: Some(BaseType::Uuid | BaseType::Ipv4 | BaseType::Ipv6),
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
                    crate::dtype::DTypeDesc::Scalar {
                        base: Some(BaseType::Time),
                        ..
                    } => DataType::Time,
                    crate::dtype::DTypeDesc::Scalar {
                        base: Some(BaseType::Binary | BaseType::Wkb),
                        ..
                    } => DataType::Binary,
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
                literal,
                ..
            } => {
                let e = inner.to_polars_expr()?;
                Ok(e.str()
                    .replace_all(lit(pattern.as_str()), lit(replacement.as_str()), *literal))
            }
            ExprNode::StringPredicate {
                inner,
                kind,
                pattern,
                ..
            } => {
                let e = inner.to_polars_expr()?;
                let p = lit(pattern.as_str());
                match kind {
                    StringPredicateKind::StartsWith => Ok(e.str().starts_with(p)),
                    StringPredicateKind::EndsWith => Ok(e.str().ends_with(p)),
                    // Polars 0.53: `contains(pat, strict)` is always regex; `strict` is invalid-regex
                    // handling. Literal substring match is `contains_literal`.
                    StringPredicateKind::Contains { literal: true } => {
                        Ok(e.str().contains_literal(p))
                    }
                    StringPredicateKind::Contains { literal: false } => {
                        Ok(e.str().contains(p, false))
                    }
                }
            }
            ExprNode::StructField { base, field, .. } => Ok(base
                .to_polars_expr()?
                .struct_()
                .field_by_name(field.as_str())),
            ExprNode::StructJsonEncode { base, .. } => {
                Ok(base.to_polars_expr()?.struct_().json_encode())
            }
            ExprNode::StructJsonPathMatch { base, path, .. } => Ok(base
                .to_polars_expr()?
                .struct_()
                .json_encode()
                .str()
                .json_path_match(lit(path.as_str()))),
            ExprNode::StructRenameFields { base, names, .. } => Ok(base
                .to_polars_expr()?
                .struct_()
                .rename_fields(names.iter().map(|s| s.as_str()))),
            ExprNode::StructWithFields { base, updates, .. } => {
                let mut flds: Vec<PolarsExpr> = Vec::with_capacity(updates.len());
                for (name, en) in updates {
                    flds.push(en.to_polars_expr()?.alias(name.as_str()));
                }
                Ok(base.to_polars_expr()?.struct_().with_fields(flds))
            }
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
                    StringUnaryOp::Reverse => Ok(e.str().reverse()),
                    StringUnaryOp::PadStart { length, fill_char } => {
                        Ok(e.str().pad_start(lit(*length as i64), *fill_char))
                    }
                    StringUnaryOp::PadEnd { length, fill_char } => {
                        Ok(e.str().pad_end(lit(*length as i64), *fill_char))
                    }
                    StringUnaryOp::ZFill { length } => Ok(e.str().zfill(lit(*length as i64))),
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
                    TemporalPart::Nanosecond => Ok(dt.nanosecond()),
                    TemporalPart::Weekday => Ok(dt.weekday()),
                    TemporalPart::Quarter => Ok(dt.quarter()),
                    TemporalPart::Week => Ok(dt.week()),
                    TemporalPart::DayOfYear => Ok(dt.ordinal_day()),
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
            ExprNode::ListMean { inner, .. } => Ok(inner.to_polars_expr()?.list().mean()),
            ExprNode::ListJoin {
                inner,
                separator,
                ignore_nulls,
                ..
            } => Ok(inner
                .to_polars_expr()?
                .list()
                .join(lit(separator.as_str()), *ignore_nulls)),
            ExprNode::ListSort {
                inner,
                descending,
                nulls_last,
                maintain_order,
                ..
            } => {
                let opts = SortOptions::default()
                    .with_order_descending(*descending)
                    .with_nulls_last(*nulls_last)
                    .with_maintain_order(*maintain_order);
                Ok(inner.to_polars_expr()?.list().sort(opts))
            }
            ExprNode::ListUnique { inner, stable, .. } => {
                let e = inner.to_polars_expr()?;
                Ok(if *stable {
                    e.list().unique_stable()
                } else {
                    e.list().unique()
                })
            }
            ExprNode::StringSplit {
                inner, delimiter, ..
            } => Ok(inner.to_polars_expr()?.str().split(lit(delimiter.as_str()))),
            ExprNode::StringExtract {
                inner,
                pattern,
                group_index,
                ..
            } => Ok(inner
                .to_polars_expr()?
                .str()
                .extract(lit(pattern.as_str()), *group_index)),
            ExprNode::StringJsonPathMatch { inner, path, .. } => Ok(inner
                .to_polars_expr()?
                .str()
                .json_path_match(lit(path.as_str()))),
            ExprNode::StringJsonDecode { inner, target, .. } => {
                let dt = crate::polars_dtype::dtype_desc_to_polars_data_type(target)?;
                Ok(inner.to_polars_expr()?.str().json_decode(dt))
            }
            ExprNode::DatetimeToDate { inner, .. } => Ok(inner.to_polars_expr()?.dt().date()),
            ExprNode::Strptime {
                inner,
                format,
                to_datetime,
                ..
            } => {
                let e = inner.to_polars_expr()?;
                let target = if *to_datetime {
                    DataType::Datetime(TimeUnit::Microseconds, None)
                } else {
                    DataType::Date
                };
                let opts = StrptimeOptions {
                    format: Some(format.as_str().into()),
                    ..Default::default()
                };
                Ok(e.str().strptime(target, opts, lit("raise")))
            }
            ExprNode::UnixTimestamp { inner, unit, .. } => {
                let mut e = inner.to_polars_expr()?;
                if matches!(
                    inner.dtype().as_scalar_base_field().flatten(),
                    Some(BaseType::Date)
                ) {
                    e = e.cast(DataType::Datetime(TimeUnit::Microseconds, None));
                }
                let dt = e.dt();
                match unit {
                    UnixTimestampUnit::Seconds => Ok((dt.timestamp(TimeUnit::Milliseconds)
                        / lit(1000i64))
                    .cast(DataType::Int64)),
                    UnixTimestampUnit::Milliseconds => Ok(dt.timestamp(TimeUnit::Milliseconds)),
                }
            }
            ExprNode::FromUnixTime { inner, unit, .. } => {
                let e = inner.to_polars_expr()?.cast(DataType::Int64);
                // Polars interprets integer casts to `Datetime(Microseconds)` as µs since UNIX epoch.
                let micros = match unit {
                    UnixTimestampUnit::Seconds => e * lit(1_000_000i64),
                    UnixTimestampUnit::Milliseconds => e * lit(1_000i64),
                };
                Ok(micros.cast(DataType::Datetime(TimeUnit::Microseconds, None)))
            }
            ExprNode::BinaryLength { inner, .. } => {
                Ok(inner.to_polars_expr()?.binary().size_bytes())
            }
            ExprNode::MapLen { inner, .. } => Ok(inner.to_polars_expr()?.list().len()),
            ExprNode::MapGet { inner, key, .. } => {
                let list_e = inner.to_polars_expr()?;
                let pred = element()
                    .struct_()
                    .field_by_name("key")
                    .eq(lit(key.as_str()));
                let filtered = element().filter(pred);
                Ok(list_e
                    .list()
                    .eval(filtered)
                    .list()
                    .first()
                    .struct_()
                    .field_by_name("value"))
            }
            ExprNode::MapContainsKey { inner, key, .. } => {
                let list_e = inner.to_polars_expr()?;
                let pred = element()
                    .struct_()
                    .field_by_name("key")
                    .eq(lit(key.as_str()));
                let filtered = element().filter(pred);
                Ok(list_e.list().eval(filtered).list().len().gt(lit(0u32)))
            }
            ExprNode::MapKeys { inner, .. } => {
                let list_e = inner.to_polars_expr()?;
                let projected = element().struct_().field_by_name("key");
                Ok(list_e.list().eval(projected))
            }
            ExprNode::MapValues { inner, .. } => {
                let list_e = inner.to_polars_expr()?;
                let projected = element().struct_().field_by_name("value");
                Ok(list_e.list().eval(projected))
            }
            ExprNode::MapEntries { inner, .. } => Ok(inner.to_polars_expr()?),
            ExprNode::MapFromEntries { inner, .. } => Ok(inner.to_polars_expr()?),
            ExprNode::RowAccum { op, inner, .. } => {
                let e = inner.to_polars_expr()?;
                Ok(match *op {
                    RowAccumOp::CumSum => e.cum_sum(false),
                    RowAccumOp::CumProd => e.cum_prod(false),
                    RowAccumOp::CumMin => e.cum_min(false),
                    RowAccumOp::CumMax => e.cum_max(false),
                    RowAccumOp::Diff { periods } => e.diff(lit(periods), NullBehavior::Ignore),
                    RowAccumOp::PctChange { periods } => e.pct_change(lit(periods)),
                })
            }
            ExprNode::Window {
                op,
                operand,
                partition_by,
                order_by,
                frame,
                ..
            } => {
                if frame.is_some() {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "framed window execution (rowsBetween/rangeBetween) is not yet supported by the Polars lowering path.",
                    ));
                }
                let part = partition_by.as_slice();
                let ord = order_by.as_slice();
                match op {
                    WindowOp::RowNumber => {
                        if ord.is_empty() {
                            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "row_number() requires order_by columns.",
                            ));
                        }
                        // Polars `rank(Ordinal)` ignores `nulls_last` on the window sort (it uses a
                        // fixed null placement internally). `int_range(0, len)+1` matches SQL
                        // `ROW_NUMBER()` and respects `over_with_options` sort keys.
                        let inner = int_range(lit(0i64), len(), 1, DataType::Int64) + lit(1i64);
                        apply_window_over(inner, part, ord)
                    }
                    WindowOp::Rank => {
                        let order_name = ord.first().ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "rank() requires order_by columns.",
                            )
                        })?;
                        let inner = col(order_name.0.as_str()).rank(
                            RankOptions {
                                method: RankMethod::Min,
                                descending: !order_name.1,
                            },
                            None,
                        );
                        apply_window_over(inner, part, ord)
                    }
                    WindowOp::DenseRank => {
                        let order_name = ord.first().ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "dense_rank() requires order_by columns.",
                            )
                        })?;
                        let inner = col(order_name.0.as_str()).rank(
                            RankOptions {
                                method: RankMethod::Dense,
                                descending: !order_name.1,
                            },
                            None,
                        );
                        apply_window_over(inner, part, ord)
                    }
                    WindowOp::FirstValue => {
                        let op_inner = operand.as_ref().ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "internal: first_value missing operand",
                            )
                        })?;
                        let inner = op_inner.to_polars_expr()?.first();
                        apply_window_over(inner, part, ord)
                    }
                    WindowOp::LastValue => {
                        let op_inner = operand.as_ref().ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "internal: last_value missing operand",
                            )
                        })?;
                        let inner = op_inner.to_polars_expr()?.last();
                        apply_window_over(inner, part, ord)
                    }
                    WindowOp::NthValue { n } => {
                        let op_inner = operand.as_ref().ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "internal: nth_value missing operand",
                            )
                        })?;
                        let idx0: i64 = (*n as i64) - 1;
                        let inner = op_inner
                            .to_polars_expr()?
                            .implode()
                            .list()
                            .get(lit(idx0), true);
                        apply_window_over(inner, part, ord)
                    }
                    WindowOp::NTile { n } => {
                        if ord.is_empty() {
                            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "ntile() requires order_by columns.",
                            ));
                        }
                        let rn = apply_window_over(
                            int_range(lit(0i64), len(), 1, DataType::Int64) + lit(1i64),
                            part,
                            ord,
                        )?;
                        let cnt = apply_window_over(len().cast(DataType::Int64), part, ord)?;
                        let nn = lit(i64::from(*n));
                        // bucket = floor((rn-1) * n / cnt) + 1
                        Ok((((rn - lit(1i64)) * nn) / cnt) + lit(1i64))
                    }
                    WindowOp::PercentRank => {
                        let order_name = ord.first().ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "percent_rank() requires order_by columns.",
                            )
                        })?;
                        let rk = apply_window_over(
                            col(order_name.0.as_str()).rank(
                                RankOptions {
                                    method: RankMethod::Min,
                                    descending: !order_name.1,
                                },
                                None,
                            ),
                            part,
                            ord,
                        )?;
                        let cnt = apply_window_over(len().cast(DataType::Int64), part, ord)?;
                        // (rank-1)/(cnt-1) with cnt<=1 -> 0.0
                        Ok(when(cnt.clone().lt_eq(lit(1i64))).then(lit(0.0)).otherwise(
                            (rk.cast(DataType::Float64) - lit(1.0))
                                / (cnt.cast(DataType::Float64) - lit(1.0)),
                        ))
                    }
                    WindowOp::CumeDist => {
                        let order_name = ord.first().ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "cume_dist() requires order_by columns.",
                            )
                        })?;
                        let rk = apply_window_over(
                            col(order_name.0.as_str()).rank(
                                RankOptions {
                                    method: RankMethod::Max,
                                    descending: !order_name.1,
                                },
                                None,
                            ),
                            part,
                            ord,
                        )?;
                        let cnt = apply_window_over(len().cast(DataType::Int64), part, ord)?;
                        Ok(rk.cast(DataType::Float64) / cnt.cast(DataType::Float64))
                    }
                    WindowOp::Sum => {
                        let op_inner = operand.as_ref().ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "internal: window sum missing operand",
                            )
                        })?;
                        let inner = op_inner.to_polars_expr()?.sum();
                        apply_window_over(inner, part, ord)
                    }
                    WindowOp::Mean => {
                        let op_inner = operand.as_ref().ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "internal: window mean missing operand",
                            )
                        })?;
                        let inner = op_inner.to_polars_expr()?.mean();
                        apply_window_over(inner, part, ord)
                    }
                    WindowOp::Min => {
                        let op_inner = operand.as_ref().ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "internal: window min missing operand",
                            )
                        })?;
                        let inner = op_inner.to_polars_expr()?.min();
                        apply_window_over(inner, part, ord)
                    }
                    WindowOp::Max => {
                        let op_inner = operand.as_ref().ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "internal: window max missing operand",
                            )
                        })?;
                        let inner = op_inner.to_polars_expr()?.max();
                        apply_window_over(inner, part, ord)
                    }
                    WindowOp::Lag { n } => {
                        if ord.is_empty() {
                            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "lag() requires order_by columns.",
                            ));
                        }
                        let op_inner = operand.as_ref().ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "internal: lag missing operand",
                            )
                        })?;
                        let shifted = op_inner.to_polars_expr()?.shift(lit(i64::from(*n)));
                        apply_window_over(shifted, part, ord)
                    }
                    WindowOp::Lead { n } => {
                        if ord.is_empty() {
                            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "lead() requires order_by columns.",
                            ));
                        }
                        let op_inner = operand.as_ref().ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "internal: lead missing operand",
                            )
                        })?;
                        let shifted = op_inner.to_polars_expr()?.shift(lit(-i64::from(*n)));
                        apply_window_over(shifted, part, ord)
                    }
                }
            }
            ExprNode::GlobalAgg { op, inner, .. } => {
                let e = inner.to_polars_expr()?;
                match op {
                    GlobalAggOp::Sum => Ok(e.sum()),
                    GlobalAggOp::Mean => Ok(e.mean()),
                    GlobalAggOp::Count => Ok(e.count()),
                    GlobalAggOp::Min => Ok(e.min()),
                    GlobalAggOp::Max => Ok(e.max()),
                }
            }
            ExprNode::GlobalRowCount { .. } => Ok(len().cast(DataType::Int64)),
        }
    }
}
