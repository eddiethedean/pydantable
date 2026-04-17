//! Python literal parsing and operator symbol helpers.

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDate, PyDateTime, PyDelta, PyTime};
use pyo3::IntoPyObjectExt;

use crate::dtype::{
    py_decimal_to_scaled_i128, py_enum_to_wire_string, py_value_to_dtype,
    scaled_i128_to_py_decimal, BaseType,
};
use crate::py_datetime::{
    days_to_py_date, micros_to_py_datetime, micros_to_py_timedelta, nanos_to_py_time,
};

use super::ir::{ArithOp, CmpOp, ExprNode, LiteralValue};

/// Convert a [`LiteralValue`] to a Python object (UUID, `datetime`, `Decimal`, etc. where applicable).
pub fn literal_value_to_pyobject(py: Python<'_>, v: &LiteralValue) -> PyResult<PyObject> {
    match v {
        LiteralValue::Int(i) => i.into_py_any(py),
        LiteralValue::Float(f) => f.into_py_any(py),
        LiteralValue::Bool(b) => b.into_py_any(py),
        LiteralValue::Str(s) => s.clone().into_py_any(py),
        LiteralValue::EnumStr(s) => s.clone().into_py_any(py),
        LiteralValue::Uuid(s) => match py
            .import("uuid")
            .and_then(|m| m.getattr("UUID"))
            .and_then(|c| c.call1((s.as_str(),)))
        {
            Ok(o) => Ok(o.unbind()),
            Err(_) => s.clone().into_py_any(py),
        },
        LiteralValue::Decimal(v) => {
            scaled_i128_to_py_decimal(py, *v).or_else(|_| (*v).into_py_any(py))
        }
        LiteralValue::DateTimeMicros(v) => {
            micros_to_py_datetime(py, *v).or_else(|_| (*v).into_py_any(py))
        }
        LiteralValue::DateDays(v) => days_to_py_date(py, *v).or_else(|_| (*v).into_py_any(py)),
        LiteralValue::DurationMicros(v) => {
            micros_to_py_timedelta(py, *v).or_else(|_| (*v).into_py_any(py))
        }
        LiteralValue::TimeNanos(ns) => nanos_to_py_time(py, *ns).or_else(|_| (*ns).into_py_any(py)),
        LiteralValue::Binary(b) => PyBytes::new(py, b).into_py_any(py),
    }
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

        let lit = match &dtype {
            crate::dtype::DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                ..
            } => LiteralValue::Int(value.extract::<i64>()?),
            crate::dtype::DTypeDesc::Scalar {
                base: Some(BaseType::Float),
                ..
            } => LiteralValue::Float(value.extract::<f64>()?),
            crate::dtype::DTypeDesc::Scalar {
                base: Some(BaseType::Bool),
                ..
            } => LiteralValue::Bool(value.extract::<bool>()?),
            crate::dtype::DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                ..
            } => LiteralValue::Str(value.extract::<String>()?),
            crate::dtype::DTypeDesc::Scalar {
                base: Some(BaseType::Enum),
                ..
            } => LiteralValue::EnumStr(py_enum_to_wire_string(value)?),
            crate::dtype::DTypeDesc::Scalar {
                base: Some(BaseType::Uuid),
                ..
            } => {
                let s = if let Ok(s) = value.extract::<String>() {
                    s
                } else {
                    value.str()?.extract()?
                };
                LiteralValue::Uuid(s)
            }
            crate::dtype::DTypeDesc::Scalar {
                base: Some(BaseType::Ipv4 | BaseType::Ipv6),
                ..
            } => {
                let s = if let Ok(s) = value.extract::<String>() {
                    s
                } else {
                    value.str()?.extract()?
                };
                LiteralValue::Str(s)
            }
            crate::dtype::DTypeDesc::Scalar {
                base: Some(BaseType::Decimal),
                ..
            } => LiteralValue::Decimal(py_decimal_to_scaled_i128(value)?),
            crate::dtype::DTypeDesc::Scalar {
                base: Some(BaseType::DateTime),
                ..
            } => {
                let dt = value.downcast::<PyDateTime>()?;
                let secs: f64 = dt.call_method0("timestamp")?.extract()?;
                LiteralValue::DateTimeMicros((secs * 1_000_000.0).round() as i64)
            }
            crate::dtype::DTypeDesc::Scalar {
                base: Some(BaseType::Date),
                ..
            } => {
                let d = value.downcast::<PyDate>()?;
                let ordinal: i32 = d.call_method0("toordinal")?.extract()?;
                LiteralValue::DateDays(ordinal - 719_163)
            }
            crate::dtype::DTypeDesc::Scalar {
                base: Some(BaseType::Duration),
                ..
            } => {
                let td = value.downcast::<PyDelta>()?;
                let secs: f64 = td.call_method0("total_seconds")?.extract()?;
                LiteralValue::DurationMicros((secs * 1_000_000.0).round() as i64)
            }
            crate::dtype::DTypeDesc::Scalar {
                base: Some(BaseType::Time),
                ..
            } => {
                let t = value.downcast::<PyTime>()?;
                let h: i64 = t.getattr("hour")?.extract()?;
                let m: i64 = t.getattr("minute")?.extract()?;
                let s: i64 = t.getattr("second")?.extract()?;
                let micro: i64 = t.getattr("microsecond")?.extract()?;
                let ns = ((h * 3600 + m * 60 + s) * 1_000_000_000i64) + micro * 1000;
                LiteralValue::TimeNanos(ns)
            }
            crate::dtype::DTypeDesc::Scalar {
                base: Some(BaseType::Binary | BaseType::Wkb),
                ..
            } => {
                let b = value.downcast::<PyBytes>()?;
                LiteralValue::Binary(b.as_bytes().to_vec())
            }
            crate::dtype::DTypeDesc::Scalar { base: None, .. } => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Non-None literal must have known base dtype.",
                ));
            }
            crate::dtype::DTypeDesc::Struct { .. } => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Struct literals are not supported.",
                ));
            }
            crate::dtype::DTypeDesc::List { .. } => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "List literals are not supported.",
                ));
            }
            crate::dtype::DTypeDesc::Map { .. } => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Map literals are not supported.",
                ));
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
