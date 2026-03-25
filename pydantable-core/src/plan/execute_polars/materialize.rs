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
    AnyValue, BooleanChunked, CrossJoin, DataFrame, DataType, Engine, ExplodeOptions, Field,
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
use super::root_lazy::plan_to_lazyframe;
use super::runner::PolarsPlanRunner;

fn map_list_series_to_py_dict(
    py: Python<'_>,
    s: &Series,
    val_dt: &DTypeDesc,
) -> PyResult<PyObject> {
    let d = PyDict::new_bound(py);
    let ca = s.struct_().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Map cells must be encoded as a struct list (key/value pairs).",
        )
    })?;
    let key_s = ca.field_by_name("key").map_err(polars_err)?;
    let val_s = ca.field_by_name("value").map_err(polars_err)?;
    for i in 0..ca.len() {
        let k_av = key_s.get(i).map_err(polars_err)?;
        let v_av = val_s.get(i).map_err(polars_err)?;
        if matches!(k_av, AnyValue::Null) {
            continue;
        }
        let k = match k_av {
            AnyValue::String(x) => x.to_string(),
            AnyValue::StringOwned(x) => x.to_string(),
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Map key must be a string.",
                ));
            }
        };
        let py_v = polars_anyvalue_to_py(py, v_av, val_dt)?;
        d.set_item(k, py_v)?;
    }
    Ok(d.into_py(py))
}

#[cfg(feature = "polars_engine")]
fn map_av_to_py_dict(py: Python<'_>, av: AnyValue<'_>, val_dt: &DTypeDesc) -> PyResult<PyObject> {
    let avs = av.into_static();
    let AnyValue::List(s) = avs else {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Map cell must be a Polars list of key/value structs.",
        ));
    };
    map_list_series_to_py_dict(py, &s, val_dt)
}

fn polars_anyvalue_to_py(py: Python<'_>, av: AnyValue<'_>, fd: &DTypeDesc) -> PyResult<PyObject> {
    if matches!(av, AnyValue::Null) {
        return Ok(py.None());
    }
    match fd {
        DTypeDesc::Map { value, .. } => map_av_to_py_dict(py, av, value.as_ref()),
        DTypeDesc::Scalar {
            base: Some(BaseType::Int),
            ..
        } => {
            let i = match av {
                AnyValue::Int64(v) => v,
                AnyValue::Int32(v) => v as i64,
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Expected int AnyValue.",
                    ));
                }
            };
            Ok(i.into_py(py))
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Float),
            ..
        } => {
            let f = match av {
                AnyValue::Float64(v) => v,
                AnyValue::Float32(v) => v as f64,
                AnyValue::Int64(v) => v as f64,
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Expected float AnyValue.",
                    ));
                }
            };
            Ok(f.into_py(py))
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Bool),
            ..
        } => match av {
            AnyValue::Boolean(b) => Ok(b.into_py(py)),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Expected bool AnyValue.",
            )),
        },
        DTypeDesc::Scalar {
            base: Some(BaseType::Str | BaseType::Enum),
            ..
        } => match av {
            AnyValue::String(s) => Ok(s.into_py(py)),
            AnyValue::StringOwned(s) => Ok(s.to_string().into_py(py)),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Expected str AnyValue.",
            )),
        },
        DTypeDesc::Scalar {
            base: Some(BaseType::Uuid),
            ..
        } => match av {
            AnyValue::String(s) => {
                let uuid_mod = py.import_bound("uuid")?;
                let ctor = uuid_mod.getattr("UUID")?;
                Ok(ctor.call1((s.to_string(),))?.into_py(py))
            }
            AnyValue::StringOwned(s) => {
                let uuid_mod = py.import_bound("uuid")?;
                let ctor = uuid_mod.getattr("UUID")?;
                Ok(ctor.call1((s.to_string(),))?.into_py(py))
            }
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Expected str AnyValue for UUID column.",
            )),
        },
        DTypeDesc::Scalar {
            base: Some(BaseType::Decimal),
            ..
        } => match av {
            AnyValue::Decimal(v, _, _) => scaled_i128_to_py_decimal(py, v),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Expected decimal AnyValue.",
            )),
        },
        DTypeDesc::Scalar {
            base: Some(BaseType::DateTime),
            ..
        } => {
            let micros = match av {
                AnyValue::Datetime(ts, tu, _) => match tu {
                    TimeUnit::Microseconds => ts,
                    TimeUnit::Milliseconds => ts * 1000,
                    TimeUnit::Nanoseconds => ts / 1000,
                },
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Expected datetime AnyValue.",
                    ));
                }
            };
            micros_to_py_datetime(py, micros)
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Date),
            ..
        } => match av {
            AnyValue::Date(d) => days_to_py_date(py, d),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Expected date AnyValue.",
            )),
        },
        DTypeDesc::Scalar {
            base: Some(BaseType::Duration),
            ..
        } => {
            let micros = match av {
                AnyValue::Duration(ts, tu) => match tu {
                    TimeUnit::Microseconds => ts,
                    TimeUnit::Milliseconds => ts * 1000,
                    TimeUnit::Nanoseconds => ts / 1000,
                },
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Expected duration AnyValue.",
                    ));
                }
            };
            micros_to_py_timedelta(py, micros)
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Time),
            ..
        } => {
            let ns = match av {
                AnyValue::Time(v) => v,
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Expected time AnyValue.",
                    ));
                }
            };
            nanos_to_py_time(py, ns)
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Binary),
            ..
        } => match av {
            AnyValue::Binary(b) => Ok(PyBytes::new(py, b).into_py(py)),
            AnyValue::BinaryOwned(b) => Ok(PyBytes::new(py, b.as_slice()).into_py(py)),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Expected binary AnyValue.",
            )),
        },
        DTypeDesc::List { inner, .. } => {
            let av_static = av.into_static();
            let AnyValue::List(series) = av_static else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Expected list AnyValue.",
                ));
            };
            let py_inner = PyList::empty_bound(py);
            for sub_av in series.iter() {
                py_inner.append(polars_anyvalue_to_py(py, sub_av, inner)?)?;
            }
            Ok(py_inner.into_py(py))
        }
        DTypeDesc::Struct { fields, .. } => {
            let av_static = av.into_static();
            let AnyValue::StructOwned(payload) = av_static else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Expected struct AnyValue.",
                ));
            };
            let (vals, _) = payload.as_ref();
            let d = PyDict::new_bound(py);
            for ((name, fdt), inner) in fields.iter().zip(vals.iter()) {
                let py_v = polars_anyvalue_to_py(py, inner.clone(), fdt)?;
                d.set_item(name.as_str(), py_v)?;
            }
            Ok(d.into_py(py))
        }
        _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Unsupported dtype in polars_anyvalue_to_py.",
        )),
    }
}

#[cfg(feature = "polars_engine")]
pub(crate) fn series_to_py_list(
    py: Python<'_>,
    series: &Series,
    dtype: &DTypeDesc,
) -> PyResult<PyObject> {
    let mut values: Vec<PyObject> = Vec::with_capacity(series.len());
    match dtype {
        DTypeDesc::Struct { .. } => {
            for av in series.iter() {
                let py_v = match av {
                    AnyValue::Null => py.None(),
                    _ => polars_anyvalue_to_py(py, av, dtype)?,
                };
                values.push(py_v);
            }
        }
        DTypeDesc::List { inner, .. } => {
            for av in series.iter() {
                let py_v = match av {
                    AnyValue::Null => py.None(),
                    AnyValue::List(s) => {
                        let py_inner = PyList::empty_bound(py);
                        for sub_av in s.iter() {
                            py_inner.append(polars_anyvalue_to_py(py, sub_av, inner)?)?;
                        }
                        py_inner.into_py(py)
                    }
                    _ => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Expected list cell in list column.",
                        ));
                    }
                };
                values.push(py_v);
            }
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Int),
            ..
        } => {
            let casted = series.cast(&DataType::Int64).map_err(polars_err)?;
            for item in casted.i64().map_err(polars_err)?.into_iter() {
                match item {
                    Some(v) => values.push(v.into_py(py)),
                    None => values.push(py.None()),
                }
            }
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Float),
            ..
        } => {
            let casted = series.cast(&DataType::Float64).map_err(polars_err)?;
            for item in casted.f64().map_err(polars_err)?.into_iter() {
                match item {
                    Some(v) => values.push(v.into_py(py)),
                    None => values.push(py.None()),
                }
            }
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Bool),
            ..
        } => {
            let casted = series.cast(&DataType::Boolean).map_err(polars_err)?;
            for item in casted.bool().map_err(polars_err)?.into_iter() {
                match item {
                    Some(v) => values.push(v.into_py(py)),
                    None => values.push(py.None()),
                }
            }
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Str | BaseType::Enum),
            ..
        } => {
            let casted = series.cast(&DataType::String).map_err(polars_err)?;
            for item in casted.str().map_err(polars_err)?.into_iter() {
                match item {
                    Some(v) => values.push(v.into_py(py)),
                    None => values.push(py.None()),
                }
            }
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Uuid),
            ..
        } => {
            let casted = series.cast(&DataType::String).map_err(polars_err)?;
            let uuid_mod = py.import_bound("uuid")?;
            let uuid_ctor = uuid_mod.getattr("UUID")?;
            for item in casted.str().map_err(polars_err)?.into_iter() {
                match item {
                    Some(v) => {
                        let u = uuid_ctor.call1((v,))?;
                        values.push(u.into_py(py));
                    }
                    None => values.push(py.None()),
                }
            }
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Decimal),
            ..
        } => {
            let casted = series
                .cast(&DataType::Decimal(DECIMAL_PRECISION, DECIMAL_SCALE))
                .map_err(polars_err)?;
            for av in casted.iter() {
                let py_v = match av {
                    AnyValue::Null => py.None(),
                    _ => polars_anyvalue_to_py(py, av, dtype)?,
                };
                values.push(py_v);
            }
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::DateTime),
            ..
        } => {
            let casted = series.cast(&DataType::Int64).map_err(polars_err)?;
            for item in casted.i64().map_err(polars_err)?.into_iter() {
                match item {
                    Some(v) => values.push(micros_to_py_datetime(py, v)?),
                    None => values.push(py.None()),
                }
            }
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Date),
            ..
        } => {
            let casted = series.cast(&DataType::Int32).map_err(polars_err)?;
            for item in casted.i32().map_err(polars_err)?.into_iter() {
                match item {
                    Some(v) => values.push(days_to_py_date(py, v)?),
                    None => values.push(py.None()),
                }
            }
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Duration),
            ..
        } => {
            let casted = series.cast(&DataType::Int64).map_err(polars_err)?;
            for item in casted.i64().map_err(polars_err)?.into_iter() {
                match item {
                    Some(v) => values.push(micros_to_py_timedelta(py, v)?),
                    None => values.push(py.None()),
                }
            }
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Time),
            ..
        } => {
            let casted = series.cast(&DataType::Time).map_err(polars_err)?;
            for av in casted.iter() {
                let py_v = match av {
                    AnyValue::Null => py.None(),
                    AnyValue::Time(ns) => nanos_to_py_time(py, ns)?,
                    _ => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Expected time AnyValue in series.",
                        ));
                    }
                };
                values.push(py_v);
            }
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Binary),
            ..
        } => {
            for av in series.iter() {
                let py_v = match av {
                    AnyValue::Null => py.None(),
                    AnyValue::Binary(b) => PyBytes::new(py, b).into_py(py),
                    AnyValue::BinaryOwned(b) => PyBytes::new(py, b.as_slice()).into_py(py),
                    _ => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Expected binary AnyValue.",
                        ));
                    }
                };
                values.push(py_v);
            }
        }
        DTypeDesc::Map { value, .. } => {
            for av in series.iter() {
                let py_v = match av {
                    AnyValue::Null => py.None(),
                    _ => map_av_to_py_dict(py, av, value.as_ref())?,
                };
                values.push(py_v);
            }
        }
        DTypeDesc::Scalar { base: None, .. } => {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Output schema cannot have unknown-base dtype.",
            ));
        }
    }
    Ok(PyList::new_bound(py, values).into_py(py))
}

#[cfg(feature = "polars_engine")]
pub(crate) fn execute_plan_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    as_python_lists: bool,
    streaming: bool,
) -> PyResult<PyObject> {
    let lf = plan_to_lazyframe(py, plan, root_data)?;
    let engine = if streaming {
        Engine::Streaming
    } else {
        Engine::InMemory
    };
    let mut out_df = py
        .allow_threads(move || lf.collect_with_engine(engine))
        .map_err(polars_err)?;

    if !as_python_lists {
        return polars_dataframe_to_python_via_ipc(py, &mut out_df);
    }

    let out_dict = PyDict::new_bound(py);
    for (name, dtype) in plan.schema.iter() {
        let col = out_df
            .column(name)
            .map_err(polars_err)?
            .as_materialized_series()
            .clone();
        let py_list = series_to_py_list(py, &col, dtype)?;
        out_dict.set_item(name, py_list)?;
    }
    Ok(out_dict.into_py(py))
}

#[cfg(feature = "polars_engine")]
pub(crate) fn dtype_from_polars(dt: &DataType) -> PyResult<DTypeDesc> {
    match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Ok(DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable: true,
            })
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Ok(DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable: true,
            })
        }
        DataType::Float32 | DataType::Float64 => Ok(DTypeDesc::Scalar {
            base: Some(BaseType::Float),
            nullable: true,
        }),
        DataType::Boolean => Ok(DTypeDesc::Scalar {
            base: Some(BaseType::Bool),
            nullable: true,
        }),
        DataType::String => Ok(DTypeDesc::Scalar {
            base: Some(BaseType::Str),
            nullable: true,
        }),
        DataType::Decimal(_, _) => Ok(DTypeDesc::Scalar {
            base: Some(BaseType::Decimal),
            nullable: true,
        }),
        DataType::Datetime(_, _) => Ok(DTypeDesc::Scalar {
            base: Some(BaseType::DateTime),
            nullable: true,
        }),
        DataType::Date => Ok(DTypeDesc::Scalar {
            base: Some(BaseType::Date),
            nullable: true,
        }),
        DataType::Duration(_) => Ok(DTypeDesc::Scalar {
            base: Some(BaseType::Duration),
            nullable: true,
        }),
        DataType::Time => Ok(DTypeDesc::Scalar {
            base: Some(BaseType::Time),
            nullable: true,
        }),
        DataType::Binary => Ok(DTypeDesc::Scalar {
            base: Some(BaseType::Binary),
            nullable: true,
        }),
        DataType::Struct(flds) => {
            let mut fields: Vec<(String, DTypeDesc)> = Vec::with_capacity(flds.len());
            for f in flds {
                fields.push((f.name().to_string(), dtype_from_polars(f.dtype())?));
            }
            Ok(DTypeDesc::Struct {
                fields,
                nullable: true,
            })
        }
        DataType::List(inner) => {
            if let DataType::Struct(flds) = inner.as_ref() {
                if flds.len() == 2 {
                    let f0 = &flds[0];
                    let f1 = &flds[1];
                    if f0.name.as_str() == "key"
                        && f1.name.as_str() == "value"
                        && f0.dtype == DataType::String
                    {
                        return Ok(DTypeDesc::Map {
                            value: Box::new(dtype_from_polars(&f1.dtype)?),
                            nullable: true,
                        });
                    }
                }
            }
            Ok(DTypeDesc::List {
                inner: Box::new(dtype_from_polars(inner)?),
                nullable: true,
            })
        }
        other => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
            "Unsupported Polars dtype in result schema: {other:?}"
        ))),
    }
}
