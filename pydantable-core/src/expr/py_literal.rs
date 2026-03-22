//! Python literal parsing and operator symbol helpers.

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDate, PyDateTime, PyDelta};

use crate::dtype::{
    py_decimal_to_scaled_i128, py_enum_to_wire_string, py_value_to_dtype, BaseType,
};

use super::ir::{ArithOp, CmpOp, ExprNode, LiteralValue};

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
