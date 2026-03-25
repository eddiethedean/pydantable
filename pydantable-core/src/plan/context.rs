//! Row-wise execution context: Python root data → columnar literal storage.

use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDate, PyDateTime, PyDelta, PyDict, PyList, PyTime};

use crate::dtype::{py_decimal_to_scaled_i128, py_enum_to_wire_string, BaseType, DTypeDesc};
use crate::expr::LiteralValue;

#[cfg(not(feature = "polars_engine"))]
pub(crate) fn ctx_len(ctx: &HashMap<String, Vec<Option<LiteralValue>>>) -> PyResult<usize> {
    ctx.values().next().map(|v| v.len()).ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Cannot execute plan with an empty input context.",
        )
    })
}

#[cfg(not(feature = "polars_engine"))]
pub fn root_data_to_ctx(
    _py: Python<'_>,
    root_schema: &HashMap<String, DTypeDesc>,
    root_data: &Bound<'_, PyAny>,
) -> PyResult<HashMap<String, Vec<Option<LiteralValue>>>> {
    let dict: &Bound<'_, PyDict> = root_data.downcast()?;
    let mut ctx: HashMap<String, Vec<Option<LiteralValue>>> = HashMap::new();

    for (k, v) in dict.iter() {
        let name: String = k.extract()?;
        let values_any = v;
        let list: &Bound<'_, PyList> = values_any.downcast()?;

        let expected = root_schema.get(&name).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "Root data contains unknown column '{}'.",
                name
            ))
        })?;

        if matches!(
            expected,
            DTypeDesc::Struct { .. } | DTypeDesc::List { .. } | DTypeDesc::Map { .. }
        ) {
            return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                "Struct/list/map columns require the Polars execution engine (polars_engine feature).",
            ));
        }

        let mut out: Vec<Option<LiteralValue>> = Vec::with_capacity(list.len());
        for item in list.iter() {
            let item = item;
            if item.is_none() {
                out.push(None);
            } else {
                let lit = match expected {
                    DTypeDesc::Scalar {
                        base: Some(BaseType::Int),
                        ..
                    } => LiteralValue::Int(item.extract::<i64>()?),
                    DTypeDesc::Scalar {
                        base: Some(BaseType::Float),
                        ..
                    } => LiteralValue::Float(item.extract::<f64>()?),
                    DTypeDesc::Scalar {
                        base: Some(BaseType::Bool),
                        ..
                    } => LiteralValue::Bool(item.extract::<bool>()?),
                    DTypeDesc::Scalar {
                        base: Some(BaseType::Str),
                        ..
                    } => LiteralValue::Str(item.extract::<String>()?),
                    DTypeDesc::Scalar {
                        base: Some(BaseType::Enum),
                        ..
                    } => LiteralValue::EnumStr(py_enum_to_wire_string(&item)?),
                    DTypeDesc::Scalar {
                        base: Some(BaseType::Uuid),
                        ..
                    } => {
                        let s = if let Ok(s) = item.extract::<String>() {
                            s
                        } else {
                            item.str()?.extract()?
                        };
                        LiteralValue::Uuid(s)
                    }
                    DTypeDesc::Scalar {
                        base: Some(BaseType::Decimal),
                        ..
                    } => LiteralValue::Decimal(py_decimal_to_scaled_i128(&item)?),
                    DTypeDesc::Scalar {
                        base: Some(BaseType::DateTime),
                        ..
                    } => {
                        let dt = item.downcast::<PyDateTime>()?;
                        let secs: f64 = dt.call_method0("timestamp")?.extract()?;
                        LiteralValue::DateTimeMicros((secs * 1_000_000.0).round() as i64)
                    }
                    DTypeDesc::Scalar {
                        base: Some(BaseType::Date),
                        ..
                    } => {
                        let d = item.downcast::<PyDate>()?;
                        let ordinal: i32 = d.call_method0("toordinal")?.extract()?;
                        LiteralValue::DateDays(ordinal - 719_163)
                    }
                    DTypeDesc::Scalar {
                        base: Some(BaseType::Duration),
                        ..
                    } => {
                        let td = item.downcast::<PyDelta>()?;
                        let secs: f64 = td.call_method0("total_seconds")?.extract()?;
                        LiteralValue::DurationMicros((secs * 1_000_000.0).round() as i64)
                    }
                    DTypeDesc::Scalar {
                        base: Some(BaseType::Time),
                        ..
                    } => {
                        let t = item.downcast::<PyTime>()?;
                        let h: i64 = t.getattr("hour")?.extract()?;
                        let m: i64 = t.getattr("minute")?.extract()?;
                        let s: i64 = t.getattr("second")?.extract()?;
                        let micro: i64 = t.getattr("microsecond")?.extract()?;
                        let nanos = ((h * 3600 + m * 60 + s) * 1_000_000_000) + micro * 1000;
                        LiteralValue::TimeNanos(nanos)
                    }
                    DTypeDesc::Scalar {
                        base: Some(BaseType::Binary),
                        ..
                    } => {
                        let b = item.downcast::<PyBytes>()?;
                        LiteralValue::Binary(b.as_bytes().to_vec())
                    }
                    DTypeDesc::Scalar { base: None, .. } => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Root schema cannot have unknown-base dtype.",
                        ));
                    }
                    DTypeDesc::Struct { .. } | DTypeDesc::List { .. } | DTypeDesc::Map { .. } => {
                        return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                            "Struct/list/map columns require the Polars execution engine (polars_engine feature).",
                        ));
                    }
                };
                out.push(Some(lit));
            }
        }
        ctx.insert(name, out);
    }

    Ok(ctx)
}
