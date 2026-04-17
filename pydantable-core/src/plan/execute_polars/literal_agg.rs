#![cfg(feature = "polars_engine")]
#![allow(unused_imports)]

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Cursor;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDate, PyDateTime, PyDelta, PyDict, PyList, PyTime};

use crate::dtype::{
    py_decimal_to_scaled_i128, py_enum_to_wire_string, scaled_i128_to_py_decimal, BaseType,
    DTypeDesc, DECIMAL_PRECISION, DECIMAL_SCALE,
};
use crate::expr::{ExprNode, LiteralValue, WindowFrame, WindowOp};

use crate::plan::ir::{PlanInner, PlanStep};
use crate::plan::schema_py::schema_descriptors_as_py;

use polars::chunked_array::builder::get_list_builder;
use polars::lazy::dsl::{col, cols, lit, when, Expr as PolarsExpr};
use polars::prelude::{
    AnyValue, BooleanChunked, CrossJoin, DataFrame, DataType, ExplodeOptions, Field,
    FillNullStrategy, Float64Chunked, Int128Chunked, Int32Chunked, Int64Chunked, IntoColumn,
    IntoLazy, IntoSeries, JoinArgs, JoinType, LazyFrame, Literal, MaintainOrderJoin, NamedFrom,
    NewChunkedArray, PlSmallStr, PolarsError, Scalar, Series, SortMultipleOptions, StringChunked,
    StructChunked, TimeUnit, UniqueKeepStrategy, UnpivotArgsDSL, NULL,
};
use polars_io::ipc::{IpcReader, IpcWriter};
use polars_io::prelude::{SerReader, SerWriter};

#[cfg(feature = "polars_engine")]
use numpy::PyReadonlyArray1;

use super::common::*;

pub(super) fn py_dict_to_literal_ctx(
    schema: &HashMap<String, DTypeDesc>,
    data_obj: &Bound<'_, PyAny>,
) -> PyResult<HashMap<String, Vec<Option<LiteralValue>>>> {
    let dict: &Bound<'_, PyDict> = data_obj.downcast()?;
    let mut out: HashMap<String, Vec<Option<LiteralValue>>> = HashMap::new();
    for (name, dtype) in schema.iter() {
        let col_any = dict.get_item(name)?.ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "Missing column '{}' in intermediate data.",
                name
            ))
        })?;
        let list: &Bound<'_, PyList> = col_any.downcast()?;
        let mut values: Vec<Option<LiteralValue>> = Vec::with_capacity(list.len());
        for item in list.iter() {
            if item.is_none() {
                values.push(None);
                continue;
            }
            let lit = match dtype.as_scalar_base_field().flatten() {
                Some(crate::dtype::BaseType::Int) => LiteralValue::Int(item.extract::<i64>()?),
                Some(crate::dtype::BaseType::Float) => LiteralValue::Float(item.extract::<f64>()?),
                Some(crate::dtype::BaseType::Bool) => LiteralValue::Bool(item.extract::<bool>()?),
                Some(crate::dtype::BaseType::Str) => LiteralValue::Str(item.extract::<String>()?),
                Some(crate::dtype::BaseType::Enum) => {
                    LiteralValue::EnumStr(py_enum_to_wire_string(&item)?)
                }
                Some(crate::dtype::BaseType::Uuid) => {
                    LiteralValue::Uuid(py_extract_uuid_canonical(&item)?)
                }
                Some(crate::dtype::BaseType::Decimal) => {
                    LiteralValue::Decimal(py_decimal_to_scaled_i128(&item)?)
                }
                Some(crate::dtype::BaseType::DateTime) => {
                    LiteralValue::DateTimeMicros(py_datetime_to_micros(&item)?)
                }
                Some(crate::dtype::BaseType::Date) => {
                    LiteralValue::DateDays(py_date_to_days(&item)?)
                }
                Some(crate::dtype::BaseType::Duration) => {
                    LiteralValue::DurationMicros(py_timedelta_to_micros(&item)?)
                }
                Some(crate::dtype::BaseType::Time) => {
                    LiteralValue::TimeNanos(py_time_to_nanos(&item)?)
                }
                Some(crate::dtype::BaseType::Binary) => {
                    LiteralValue::Binary(item.extract::<Vec<u8>>()?)
                }
                Some(crate::dtype::BaseType::Ipv4) => {
                    let py = item.py();
                    let ip_mod = py.import("ipaddress")?;
                    let cls = ip_mod.getattr("IPv4Address")?;
                    let obj = cls.call1((item,))?;
                    LiteralValue::Str(obj.str()?.extract()?)
                }
                Some(crate::dtype::BaseType::Ipv6) => {
                    let py = item.py();
                    let ip_mod = py.import("ipaddress")?;
                    let cls = ip_mod.getattr("IPv6Address")?;
                    let obj = cls.call1((item,))?;
                    LiteralValue::Str(obj.str()?.extract()?)
                }
                Some(crate::dtype::BaseType::Wkb) => {
                    LiteralValue::Binary(item.extract::<Vec<u8>>()?)
                }
                None => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Unsupported unknown-base dtype in reshape path.",
                    ))
                }
            };
            values.push(Some(lit));
        }
        out.insert(name.clone(), values);
    }
    Ok(out)
}

#[cfg(feature = "polars_engine")]
pub(super) fn literal_to_py(py: Python<'_>, v: &LiteralValue) -> PyObject {
    match v {
        LiteralValue::Int(i) => i.into_py(py),
        LiteralValue::Float(f) => f.into_py(py),
        LiteralValue::Bool(b) => b.into_py(py),
        LiteralValue::Str(s) => s.clone().into_py(py),
        LiteralValue::EnumStr(s) => s.clone().into_py(py),
        LiteralValue::Uuid(s) => py
            .import("uuid")
            .and_then(|m| m.getattr("UUID"))
            .and_then(|c| c.call1((s.as_str(),)))
            .map(|o| o.into_py(py))
            .unwrap_or_else(|_| s.clone().into_py(py)),
        LiteralValue::Decimal(v) => {
            scaled_i128_to_py_decimal(py, *v).unwrap_or_else(|_| v.into_py(py))
        }
        LiteralValue::DateTimeMicros(v) => {
            micros_to_py_datetime(py, *v).unwrap_or_else(|_| v.into_py(py))
        }
        LiteralValue::DateDays(v) => days_to_py_date(py, *v).unwrap_or_else(|_| v.into_py(py)),
        LiteralValue::DurationMicros(v) => {
            micros_to_py_timedelta(py, *v).unwrap_or_else(|_| v.into_py(py))
        }
        LiteralValue::TimeNanos(ns) => nanos_to_py_time(py, *ns).unwrap_or_else(|_| ns.into_py(py)),
        LiteralValue::Binary(b) => PyBytes::new(py, b).into_py(py),
    }
}

#[cfg(feature = "polars_engine")]
pub(super) fn agg_literal(
    op: &str,
    vals: &[Option<LiteralValue>],
    base: crate::dtype::BaseType,
) -> PyResult<Option<LiteralValue>> {
    let non_null: Vec<&LiteralValue> = vals.iter().filter_map(|v| v.as_ref()).collect();
    match op {
        "count" => Ok(Some(LiteralValue::Int(non_null.len() as i64))),
        "n_unique" => {
            let mut uniq: std::collections::HashSet<String> = std::collections::HashSet::new();
            for v in non_null {
                uniq.insert(format!("{v:?}"));
            }
            Ok(Some(LiteralValue::Int(uniq.len() as i64)))
        }
        "first" => Ok(non_null.first().map(|v| (*v).clone())),
        "last" => Ok(non_null.last().map(|v| (*v).clone())),
        "min" | "max" | "sum" | "mean" | "median" | "std" | "var" => {
            if matches!(base, crate::dtype::BaseType::Decimal) {
                let decs: Vec<i128> = non_null
                    .iter()
                    .filter_map(|v| match v {
                        LiteralValue::Decimal(i) => Some(*i),
                        _ => None,
                    })
                    .collect();
                if decs.is_empty() {
                    return Ok(None);
                }
                let scale = 10_f64.powi(crate::dtype::DECIMAL_SCALE as i32);
                let as_float = |x: i128| x as f64 / scale;
                return match op {
                    "sum" => Ok(Some(LiteralValue::Decimal(decs.iter().copied().sum()))),
                    "min" => {
                        let m = decs.iter().copied().min().ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                                "internal error: empty decimal list for min aggregate",
                            )
                        })?;
                        Ok(Some(LiteralValue::Decimal(m)))
                    }
                    "max" => {
                        let m = decs.iter().copied().max().ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                                "internal error: empty decimal list for max aggregate",
                            )
                        })?;
                        Ok(Some(LiteralValue::Decimal(m)))
                    }
                    "mean" => Ok(Some(LiteralValue::Float(
                        decs.iter().map(|&x| as_float(x)).sum::<f64>() / decs.len() as f64,
                    ))),
                    "median" => {
                        let mut v: Vec<f64> = decs.iter().map(|&x| as_float(x)).collect();
                        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                        let n = v.len();
                        let m = if n % 2 == 1 {
                            v[n / 2]
                        } else {
                            (v[n / 2 - 1] + v[n / 2]) / 2.0
                        };
                        Ok(Some(LiteralValue::Float(m)))
                    }
                    "std" | "var" => {
                        if decs.len() < 2 {
                            return Ok(None);
                        }
                        let vf: Vec<f64> = decs.iter().map(|&x| as_float(x)).collect();
                        let mean = vf.iter().sum::<f64>() / vf.len() as f64;
                        let sq = vf.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>();
                        let var = sq / (vf.len() as f64 - 1.0);
                        if op == "var" {
                            Ok(Some(LiteralValue::Float(var)))
                        } else {
                            Ok(Some(LiteralValue::Float(var.sqrt())))
                        }
                    }
                    _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Unsupported decimal aggregation op.",
                    )),
                };
            }
            let nums = non_null
                .iter()
                .filter_map(|v| match v {
                    LiteralValue::Int(i) => Some(*i as f64),
                    LiteralValue::Float(f) => Some(*f),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if nums.is_empty() {
                return Ok(None);
            }
            match op {
                "sum" => {
                    if matches!(base, crate::dtype::BaseType::Int) {
                        Ok(Some(LiteralValue::Int(nums.iter().sum::<f64>() as i64)))
                    } else {
                        Ok(Some(LiteralValue::Float(nums.iter().sum::<f64>())))
                    }
                }
                "mean" => Ok(Some(LiteralValue::Float(
                    nums.iter().sum::<f64>() / nums.len() as f64,
                ))),
                "median" => {
                    let mut v = nums;
                    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    let n = v.len();
                    let m = if n % 2 == 1 {
                        v[n / 2]
                    } else {
                        (v[n / 2 - 1] + v[n / 2]) / 2.0
                    };
                    Ok(Some(LiteralValue::Float(m)))
                }
                "std" | "var" => {
                    if nums.len() < 2 {
                        return Ok(None);
                    }
                    let mean = nums.iter().sum::<f64>() / nums.len() as f64;
                    let sq = nums.iter().map(|x| (x - mean) * (x - mean)).sum::<f64>();
                    let var = sq / (nums.len() as f64 - 1.0);
                    if op == "var" {
                        Ok(Some(LiteralValue::Float(var)))
                    } else {
                        Ok(Some(LiteralValue::Float(var.sqrt())))
                    }
                }
                "min" => match base {
                    crate::dtype::BaseType::Int => Ok(Some(LiteralValue::Int(
                        nums.iter().fold(f64::INFINITY, |a, b| a.min(*b)) as i64,
                    ))),
                    _ => Ok(Some(LiteralValue::Float(
                        nums.iter().fold(f64::INFINITY, |a, b| a.min(*b)),
                    ))),
                },
                "max" => match base {
                    crate::dtype::BaseType::Int => Ok(Some(LiteralValue::Int(
                        nums.iter().fold(f64::NEG_INFINITY, |a, b| a.max(*b)) as i64,
                    ))),
                    _ => Ok(Some(LiteralValue::Float(
                        nums.iter().fold(f64::NEG_INFINITY, |a, b| a.max(*b)),
                    ))),
                },
                _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Unsupported numeric aggregation op.",
                )),
            }
        }
        _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Unsupported aggregation '{}'.",
            op
        ))),
    }
}
