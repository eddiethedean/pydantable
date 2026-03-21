//! Row-wise execution context: Python root data → columnar literal storage.

use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDate, PyDateTime, PyDelta, PyDict, PyList};

use crate::dtype::DTypeDesc;
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

        let mut out: Vec<Option<LiteralValue>> = Vec::with_capacity(list.len());
        for item in list.iter() {
            let item = item;
            if item.is_none() {
                out.push(None);
            } else {
                let lit = match expected.base {
                    Some(crate::dtype::BaseType::Int) => LiteralValue::Int(item.extract::<i64>()?),
                    Some(crate::dtype::BaseType::Float) => {
                        LiteralValue::Float(item.extract::<f64>()?)
                    }
                    Some(crate::dtype::BaseType::Bool) => {
                        LiteralValue::Bool(item.extract::<bool>()?)
                    }
                    Some(crate::dtype::BaseType::Str) => {
                        LiteralValue::Str(item.extract::<String>()?)
                    }
                    Some(crate::dtype::BaseType::DateTime) => {
                        let dt = item.downcast::<PyDateTime>()?;
                        let secs: f64 = dt.call_method0("timestamp")?.extract()?;
                        LiteralValue::DateTimeMicros((secs * 1_000_000.0).round() as i64)
                    }
                    Some(crate::dtype::BaseType::Date) => {
                        let d = item.downcast::<PyDate>()?;
                        let ordinal: i32 = d.call_method0("toordinal")?.extract()?;
                        LiteralValue::DateDays(ordinal - 719_163)
                    }
                    Some(crate::dtype::BaseType::Duration) => {
                        let td = item.downcast::<PyDelta>()?;
                        let secs: f64 = td.call_method0("total_seconds")?.extract()?;
                        LiteralValue::DurationMicros((secs * 1_000_000.0).round() as i64)
                    }
                    None => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Root schema cannot have unknown-base dtype.",
                        ))
                    }
                };
                out.push(Some(lit));
            }
        }
        ctx.insert(name, out);
    }

    Ok(ctx)
}
