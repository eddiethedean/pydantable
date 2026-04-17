//! Shared helpers for expression typing and row-wise literal casts.

use pyo3::prelude::*;

use crate::dtype::{BaseType, DTypeDesc, LiteralSet};
use crate::expr::ir::{CmpOp, ExprNode, LiteralValue};

pub(crate) fn dtype_is_string_like(dtype: &DTypeDesc) -> bool {
    matches!(
        dtype.as_scalar_base_field().flatten(),
        Some(BaseType::Str | BaseType::Enum | BaseType::Uuid | BaseType::Ipv4 | BaseType::Ipv6)
    )
}

pub(crate) fn literal_set_contains(ls: &LiteralSet, v: &LiteralValue) -> bool {
    match (ls, v) {
        (LiteralSet::Str(vals), LiteralValue::Str(s)) => vals.iter().any(|x| x == s),
        (LiteralSet::Str(vals), LiteralValue::EnumStr(s)) => vals.iter().any(|x| x == s),
        (LiteralSet::Int(vals), LiteralValue::Int(i)) => vals.contains(i),
        (LiteralSet::Bool(vals), LiteralValue::Bool(b)) => vals.contains(b),
        _ => false,
    }
}

pub(crate) fn validate_literal_membership_compare(
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

/// Cast a literal for row-wise evaluation (`eval_rowwise`); exported to sibling modules.
#[cfg(not(feature = "polars_engine"))]
pub(crate) fn cast_literal_value(v: LiteralValue, target: BaseType) -> PyResult<LiteralValue> {
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
