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

#[cfg(feature = "polars_engine")]
fn try_series_from_numpy(
    name: &str,
    obj: &Bound<'_, PyAny>,
    dtype: &DTypeDesc,
) -> PyResult<Option<Series>> {
    let type_name = obj
        .get_type()
        .qualname()
        .ok()
        .and_then(|q| q.extract::<String>().ok());
    if type_name.as_deref() != Some("ndarray") {
        return Ok(None);
    }
    let module: String = obj.get_type().getattr("__module__")?.extract()?;
    if module != "numpy" {
        return Ok(None);
    }
    let kind: String = obj.getattr("dtype")?.getattr("kind")?.extract()?;
    let base = match dtype {
        DTypeDesc::Scalar { base: Some(b), .. } => b,
        _ => return Ok(None),
    };
    let name_pl: PlSmallStr = name.into();

    let series = match (base, kind.as_str()) {
        (BaseType::Float, "f") => {
            let arr: PyReadonlyArray1<f64> = obj.extract()?;
            Series::new(name_pl, arr.as_slice()?)
        }
        (BaseType::Int, "i") => {
            let arr: PyReadonlyArray1<i64> = obj.extract()?;
            Series::new(name_pl, arr.as_slice()?)
        }
        (BaseType::Int, "u") => {
            let arr: PyReadonlyArray1<u64> = obj.extract()?;
            let v: Vec<i64> = arr.as_slice()?.iter().map(|&x| x as i64).collect();
            Series::new(name_pl, v.as_slice())
        }
        (BaseType::Bool, "b") => {
            let arr: PyReadonlyArray1<bool> = obj.extract()?;
            Series::new(name_pl, arr.as_slice()?)
        }
        _ => return Ok(None),
    };

    Ok(Some(series))
}

#[cfg(feature = "polars_engine")]
fn try_series_from_column_buffer(
    name: &str,
    obj: &Bound<'_, PyAny>,
    dtype: &DTypeDesc,
) -> PyResult<Option<Series>> {
    let module = obj
        .get_type()
        .getattr("__module__")
        .ok()
        .and_then(|m| m.extract::<String>().ok());
    let pa_type = obj
        .get_type()
        .qualname()
        .ok()
        .and_then(|q| q.extract::<String>().ok());

    if let (Some(m), Some(tn)) = (module.as_deref(), pa_type.as_deref()) {
        if m.starts_with("pyarrow") && (tn == "Array" || tn == "ChunkedArray") {
            let np = obj.call_method0("to_numpy")?;
            return try_series_from_numpy(name, &np, dtype);
        }
    }
    try_series_from_numpy(name, obj, dtype)
}

#[cfg(feature = "polars_engine")]
pub(super) fn polars_err(e: PolarsError) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Polars execution error: {e}"))
}

/// Hand off an in-memory Polars `DataFrame` to Python Polars via Arrow IPC (avoids
/// per-cell `series_to_py_list` materialization).
#[cfg(feature = "polars_engine")]
pub(super) fn polars_dataframe_to_python_via_ipc(py: Python<'_>, df: &mut DataFrame) -> PyResult<PyObject> {
    let mut buf = Cursor::new(Vec::<u8>::new());
    IpcWriter::new(&mut buf).finish(df).map_err(polars_err)?;
    let bytes = buf.into_inner();
    let io_mod = py.import_bound("io")?;
    let bytes_io = io_mod.call_method1("BytesIO", (bytes,))?;
    let pl_mod = py.import_bound("polars")?;
    let read_ipc = pl_mod.getattr("read_ipc")?;
    Ok(read_ipc.call1((bytes_io,))?.into_py(py))
}

#[cfg(feature = "polars_engine")]
fn try_python_polars_dataframe_to_native(
    py: Python<'_>,
    root_schema: &HashMap<String, DTypeDesc>,
    root_data: &Bound<'_, PyAny>,
) -> PyResult<Option<DataFrame>> {
    let type_name = root_data
        .get_type()
        .qualname()
        .ok()
        .and_then(|q| q.extract::<String>().ok());
    if type_name.as_deref() != Some("DataFrame") {
        return Ok(None);
    }
    let module: String = root_data.get_type().getattr("__module__")?.extract()?;
    if !module.starts_with("polars") {
        return Ok(None);
    }
    let bytes_io = py.import_bound("io")?.call_method0("BytesIO")?;
    root_data.call_method1("write_ipc", (bytes_io.as_ref(),))?;
    let bytes: Vec<u8> = bytes_io.call_method0("getvalue")?.extract()?;
    let cursor = Cursor::new(bytes);
    let mut df = IpcReader::new(cursor).finish().map_err(polars_err)?;
    let names: Vec<&str> = root_schema.keys().map(|s| s.as_str()).collect();
    df = df.select(&names).map_err(polars_err)?;
    Ok(Some(df))
}

#[cfg(feature = "polars_engine")]
pub(super) fn py_datetime_to_micros(item: &Bound<'_, PyAny>) -> PyResult<i64> {
    let dt = item.downcast::<PyDateTime>()?;
    let secs: f64 = dt.call_method0("timestamp")?.extract()?;
    Ok((secs * 1_000_000.0).round() as i64)
}

#[cfg(feature = "polars_engine")]
pub(super) fn py_date_to_days(item: &Bound<'_, PyAny>) -> PyResult<i32> {
    let d = item.downcast::<PyDate>()?;
    let ordinal: i32 = d.call_method0("toordinal")?.extract()?;
    Ok(ordinal - 719_163)
}

#[cfg(feature = "polars_engine")]
pub(super) fn py_timedelta_to_micros(item: &Bound<'_, PyAny>) -> PyResult<i64> {
    let td = item.downcast::<PyDelta>()?;
    let secs: f64 = td.call_method0("total_seconds")?.extract()?;
    Ok((secs * 1_000_000.0).round() as i64)
}

#[cfg(feature = "polars_engine")]
pub(super) fn micros_to_py_datetime(py: Python<'_>, micros: i64) -> PyResult<PyObject> {
    let dt_mod = py.import_bound("datetime")?;
    let dt = dt_mod.getattr("datetime")?;
    Ok(dt
        .call_method1("fromtimestamp", (micros as f64 / 1_000_000.0,))?
        .into_py(py))
}

#[cfg(feature = "polars_engine")]
pub(super) fn days_to_py_date(py: Python<'_>, days: i32) -> PyResult<PyObject> {
    let dt_mod = py.import_bound("datetime")?;
    let date = dt_mod.getattr("date")?;
    Ok(date
        .call_method1("fromordinal", (days + 719_163,))?
        .into_py(py))
}

#[cfg(feature = "polars_engine")]
pub(super) fn micros_to_py_timedelta(py: Python<'_>, micros: i64) -> PyResult<PyObject> {
    let dt_mod = py.import_bound("datetime")?;
    let td = dt_mod.getattr("timedelta")?;
    Ok(td.call1((0, 0, micros))?.into_py(py))
}

#[cfg(feature = "polars_engine")]
pub(super) fn py_time_to_nanos(item: &Bound<'_, PyAny>) -> PyResult<i64> {
    let t = item.downcast::<PyTime>()?;
    let h: i64 = t.getattr("hour")?.extract()?;
    let m: i64 = t.getattr("minute")?.extract()?;
    let s: i64 = t.getattr("second")?.extract()?;
    let micro: i64 = t.getattr("microsecond")?.extract()?;
    Ok(((h * 3600 + m * 60 + s) * 1_000_000_000) + micro * 1000)
}

#[cfg(feature = "polars_engine")]
pub(super) fn nanos_to_py_time(py: Python<'_>, ns: i64) -> PyResult<PyObject> {
    let dt_mod = py.import_bound("datetime")?;
    let time_cls = dt_mod.getattr("time")?;
    let nanos = ns.rem_euclid(86_400 * 1_000_000_000);
    let secs = nanos / 1_000_000_000;
    let nsub = nanos % 1_000_000_000;
    let micro = (nsub / 1000) as i32;
    let h = (secs / 3600) as i32;
    let m = ((secs % 3600) / 60) as i32;
    let s = (secs % 60) as i32;
    Ok(time_cls.call1((h, m, s, micro))?.into_py(py))
}

fn py_row_get_field<'py>(item: &Bound<'py, PyAny>, fname: &str) -> PyResult<Bound<'py, PyAny>> {
    if let Ok(d) = item.downcast::<PyDict>() {
        return d.call_method1("get", (fname, item.py().None()));
    }
    item.getattr(fname)
}

/// Canonical UUID string for `uuid.UUID` or `str` cells (logical `BaseType::Uuid`).
#[cfg(feature = "polars_engine")]
pub(super) fn py_extract_uuid_canonical(item: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(s) = item.extract::<String>() {
        return Ok(s);
    }
    let py = item.py();
    let builtins = py.import_bound("builtins")?;
    let isinstance = builtins.getattr("isinstance")?;
    let uuid_cls = py.import_bound("uuid")?.getattr("UUID")?;
    if isinstance
        .call1((item, &uuid_cls))?
        .extract::<bool>()
        .unwrap_or(false)
    {
        return item.str()?.extract();
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "UUID column cells must be uuid.UUID or str.",
    ))
}

/// Map a pydantable dtype to a Polars [`DataType`] for list builders and validation.
#[cfg(feature = "polars_engine")]
fn dtype_desc_to_polars_data_type(d: &DTypeDesc) -> PyResult<DataType> {
    match d {
        DTypeDesc::Scalar {
            base: Some(BaseType::Int),
            ..
        } => Ok(DataType::Int64),
        DTypeDesc::Scalar {
            base: Some(BaseType::Float),
            ..
        } => Ok(DataType::Float64),
        DTypeDesc::Scalar {
            base: Some(BaseType::Bool),
            ..
        } => Ok(DataType::Boolean),
        DTypeDesc::Scalar {
            base: Some(BaseType::Str | BaseType::Enum),
            ..
        } => Ok(DataType::String),
        DTypeDesc::Scalar {
            base: Some(BaseType::Uuid),
            ..
        } => Ok(DataType::String),
        DTypeDesc::Scalar {
            base: Some(BaseType::Decimal),
            ..
        } => Ok(DataType::Decimal(DECIMAL_PRECISION, DECIMAL_SCALE)),
        DTypeDesc::Scalar {
            base: Some(BaseType::DateTime),
            ..
        } => Ok(DataType::Datetime(TimeUnit::Microseconds, None)),
        DTypeDesc::Scalar {
            base: Some(BaseType::Date),
            ..
        } => Ok(DataType::Date),
        DTypeDesc::Scalar {
            base: Some(BaseType::Duration),
            ..
        } => Ok(DataType::Duration(TimeUnit::Microseconds)),
        DTypeDesc::Scalar {
            base: Some(BaseType::Time),
            ..
        } => Ok(DataType::Time),
        DTypeDesc::Scalar {
            base: Some(BaseType::Binary),
            ..
        } => Ok(DataType::Binary),
        DTypeDesc::Scalar { base: None, .. } => {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Root schema cannot have unknown-base dtype.",
            ))
        }
        DTypeDesc::Struct { fields, .. } => {
            let sub: Vec<Field> = fields
                .iter()
                .map(|(n, fd)| {
                    Ok(Field::new(
                        PlSmallStr::from_str(n),
                        dtype_desc_to_polars_data_type(fd)?,
                    ))
                })
                .collect::<PyResult<_>>()?;
            Ok(DataType::Struct(sub))
        }
        DTypeDesc::List { inner, .. } => Ok(DataType::List(Box::new(
            dtype_desc_to_polars_data_type(inner)?,
        ))),
        DTypeDesc::Map { value, .. } => {
            let vdt = dtype_desc_to_polars_data_type(value)?;
            Ok(DataType::List(Box::new(DataType::Struct(vec![
                Field::new(PlSmallStr::from("key"), DataType::String),
                Field::new(PlSmallStr::from("value"), vdt),
            ]))))
        }
    }
}

/// Build a Polars `Series` from a Python list column and a pydantable [`DTypeDesc`].
#[cfg(feature = "polars_engine")]
fn py_list_to_series(
    py: Python<'_>,
    name: &str,
    list: &Bound<'_, PyList>,
    dtype: &DTypeDesc,
) -> PyResult<Series> {
    match dtype {
        DTypeDesc::Scalar {
            base: Some(BaseType::Int),
            ..
        } => {
            let mut v: Vec<Option<i64>> = Vec::with_capacity(list.len());
            for item in list.iter() {
                if item.is_none() {
                    v.push(None);
                } else {
                    v.push(Some(item.extract::<i64>()?));
                }
            }
            let ca: Int64Chunked = Int64Chunked::from_iter_options(name.into(), v.into_iter());
            Ok(ca.into_series())
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Float),
            ..
        } => {
            let mut v: Vec<Option<f64>> = Vec::with_capacity(list.len());
            for item in list.iter() {
                if item.is_none() {
                    v.push(None);
                } else {
                    v.push(Some(item.extract::<f64>()?));
                }
            }
            let ca: Float64Chunked = Float64Chunked::from_iter_options(name.into(), v.into_iter());
            Ok(ca.into_series())
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Bool),
            ..
        } => {
            let mut v: Vec<Option<bool>> = Vec::with_capacity(list.len());
            for item in list.iter() {
                if item.is_none() {
                    v.push(None);
                } else {
                    v.push(Some(item.extract::<bool>()?));
                }
            }
            let ca: BooleanChunked = BooleanChunked::from_iter_options(name.into(), v.into_iter());
            Ok(ca.into_series())
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Str),
            ..
        } => {
            let mut v: Vec<Option<String>> = Vec::with_capacity(list.len());
            for item in list.iter() {
                if item.is_none() {
                    v.push(None);
                } else {
                    v.push(Some(item.extract::<String>()?));
                }
            }
            let ca: StringChunked = StringChunked::from_iter_options(name.into(), v.into_iter());
            Ok(ca.into_series())
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Enum),
            ..
        } => {
            let mut v: Vec<Option<String>> = Vec::with_capacity(list.len());
            for item in list.iter() {
                if item.is_none() {
                    v.push(None);
                } else {
                    v.push(Some(py_enum_to_wire_string(&item)?));
                }
            }
            let ca: StringChunked = StringChunked::from_iter_options(name.into(), v.into_iter());
            Ok(ca.into_series())
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Uuid),
            ..
        } => {
            let mut v: Vec<Option<String>> = Vec::with_capacity(list.len());
            for item in list.iter() {
                if item.is_none() {
                    v.push(None);
                } else {
                    v.push(Some(py_extract_uuid_canonical(&item)?));
                }
            }
            let ca: StringChunked = StringChunked::from_iter_options(name.into(), v.into_iter());
            Ok(ca.into_series())
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Decimal),
            ..
        } => {
            let mut v: Vec<Option<i128>> = Vec::with_capacity(list.len());
            for item in list.iter() {
                if item.is_none() {
                    v.push(None);
                } else {
                    v.push(Some(py_decimal_to_scaled_i128(&item)?));
                }
            }
            Ok(
                Int128Chunked::from_iter_options(PlSmallStr::from(name), v.into_iter())
                    .into_decimal(DECIMAL_PRECISION, DECIMAL_SCALE)
                    .map_err(polars_err)?
                    .into_series(),
            )
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::DateTime),
            ..
        } => {
            let mut v: Vec<Option<i64>> = Vec::with_capacity(list.len());
            for item in list.iter() {
                if item.is_none() {
                    v.push(None);
                } else {
                    v.push(Some(py_datetime_to_micros(&item)?));
                }
            }
            let base: Int64Chunked = Int64Chunked::from_iter_options(name.into(), v.into_iter());
            base.into_series()
                .cast(&DataType::Datetime(
                    polars::prelude::TimeUnit::Microseconds,
                    None,
                ))
                .map_err(polars_err)
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Date),
            ..
        } => {
            let mut v: Vec<Option<i32>> = Vec::with_capacity(list.len());
            for item in list.iter() {
                if item.is_none() {
                    v.push(None);
                } else {
                    v.push(Some(py_date_to_days(&item)?));
                }
            }
            let base: Int32Chunked = Int32Chunked::from_iter_options(name.into(), v.into_iter());
            base.into_series().cast(&DataType::Date).map_err(polars_err)
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Duration),
            ..
        } => {
            let mut v: Vec<Option<i64>> = Vec::with_capacity(list.len());
            for item in list.iter() {
                if item.is_none() {
                    v.push(None);
                } else {
                    v.push(Some(py_timedelta_to_micros(&item)?));
                }
            }
            let base: Int64Chunked = Int64Chunked::from_iter_options(name.into(), v.into_iter());
            base.into_series()
                .cast(&DataType::Duration(polars::prelude::TimeUnit::Microseconds))
                .map_err(polars_err)
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Time),
            ..
        } => {
            let mut v: Vec<Option<i64>> = Vec::with_capacity(list.len());
            for item in list.iter() {
                if item.is_none() {
                    v.push(None);
                } else {
                    v.push(Some(py_time_to_nanos(&item)?));
                }
            }
            Int64Chunked::from_iter_options(name.into(), v.into_iter())
                .into_series()
                .cast(&DataType::Time)
                .map_err(polars_err)
        }
        DTypeDesc::Scalar {
            base: Some(BaseType::Binary),
            ..
        } => {
            let mut v: Vec<Option<Vec<u8>>> = Vec::with_capacity(list.len());
            for item in list.iter() {
                if item.is_none() {
                    v.push(None);
                } else {
                    v.push(Some(item.extract::<Vec<u8>>()?));
                }
            }
            Ok(
                polars::chunked_array::ChunkedArray::<polars::datatypes::BinaryType>::from_iter_options(
                    name.into(),
                    v.into_iter(),
                )
                .into_series(),
            )
        }
        DTypeDesc::Scalar { base: None, .. } => {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Root schema cannot have unknown-base dtype.",
            ))
        }
        DTypeDesc::Map { value, .. } => {
            let vdt = dtype_desc_to_polars_data_type(value)?;
            let inner_struct = DataType::Struct(vec![
                Field::new(PlSmallStr::from("key"), DataType::String),
                Field::new(PlSmallStr::from("value"), vdt),
            ]);
            let est_vals = list.len().saturating_mul(8).max(8);
            let mut builder =
                get_list_builder(&inner_struct, est_vals, list.len(), PlSmallStr::from(name));
            for item in list.iter() {
                if item.is_none() {
                    builder.append_null();
                } else {
                    let d = item.downcast::<PyDict>()?;
                    let mut pairs: Vec<(String, Bound<'_, PyAny>)> = Vec::new();
                    for (k, v) in d.iter() {
                        pairs.push((k.extract::<String>()?, v));
                    }
                    pairs.sort_by(|a, b| a.0.cmp(&b.0));
                    let n = pairs.len();
                    let key_list = PyList::empty_bound(py);
                    let val_list = PyList::empty_bound(py);
                    for (k, v) in pairs {
                        key_list.append(k)?;
                        val_list.append(v)?;
                    }
                    let ks = py_list_to_series(
                        py,
                        "key",
                        &key_list,
                        &DTypeDesc::non_nullable(BaseType::Str),
                    )?;
                    let vs = py_list_to_series(py, "value", &val_list, value.as_ref())?;
                    let ca = StructChunked::from_series(
                        PlSmallStr::from("item"),
                        n,
                        vec![&ks, &vs].into_iter(),
                    )
                    .map_err(polars_err)?;
                    builder
                        .append_series(&ca.into_series())
                        .map_err(polars_err)?;
                }
            }
            Ok(builder.finish().into_series())
        }
        DTypeDesc::List { inner, .. } => {
            let inner_polars = dtype_desc_to_polars_data_type(inner)?;
            let est_vals = list.len().saturating_mul(8).max(8);
            let mut builder =
                get_list_builder(&inner_polars, est_vals, list.len(), PlSmallStr::from(name));
            for item in list.iter() {
                if item.is_none() {
                    builder.append_null();
                } else {
                    let sub = item.downcast::<PyList>()?;
                    let inner_series = py_list_to_series(py, "item", sub, inner)?;
                    builder.append_series(&inner_series).map_err(polars_err)?;
                }
            }
            Ok(builder.finish().into_series())
        }
        DTypeDesc::Struct { fields, .. } => {
            let mut field_series: Vec<Series> = Vec::with_capacity(fields.len());
            for (fname, fd) in fields {
                let field_list = PyList::empty_bound(py);
                for item in list.iter() {
                    if item.is_none() {
                        field_list.append(py.None())?;
                    } else {
                        let sub = py_row_get_field(&item, fname.as_str())?;
                        field_list.append(sub)?;
                    }
                }
                let fs = py_list_to_series(py, fname.as_str(), &field_list, fd)?;
                field_series.push(fs);
            }
            let len = list.len();
            let ca = StructChunked::from_series(name.into(), len, field_series.iter())
                .map_err(polars_err)?;
            Ok(ca.into_series())
        }
    }
}

#[cfg(feature = "polars_engine")]
pub(super) fn root_data_to_polars_df(
    py: Python<'_>,
    root_schema: &HashMap<String, DTypeDesc>,
    root_data: &Bound<'_, PyAny>,
) -> PyResult<DataFrame> {
    if let Some(df) = try_python_polars_dataframe_to_native(py, root_schema, root_data)? {
        return Ok(df);
    }
    let dict: &Bound<'_, PyDict> = root_data.downcast()?;

    let mut series_list: Vec<Series> = Vec::new();
    for (name, dtype) in root_schema.iter() {
        let values_any = dict.get_item(name)?.ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "Root data missing required column '{}'.",
                name
            ))
        })?;
        if let Some(s) = try_series_from_column_buffer(name.as_str(), &values_any, dtype)? {
            series_list.push(s);
            continue;
        }

        let list: Bound<'_, PyList> = if let Ok(l) = values_any.downcast::<PyList>() {
            l.clone()
        } else {
            values_any
                .call_method0("tolist")?
                .downcast::<PyList>()
                .map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "Column '{name}' must be a list or a numpy/pyarrow column convertible with tolist().",
                    ))
                })?
                .clone()
        };

        let s = py_list_to_series(py, name.as_str(), &list, dtype)?;
        series_list.push(s);
    }

    let columns = series_list
        .into_iter()
        .map(|s| s.into_column())
        .collect::<Vec<polars::prelude::Column>>();
    DataFrame::new_infer_height(columns).map_err(polars_err)
}
