//! Polars-backed physical execution for logical plans.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Cursor;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDate, PyDateTime, PyDelta, PyDict, PyList};

use crate::dtype::{
    py_decimal_to_scaled_i128, py_enum_to_wire_string, scaled_i128_to_py_decimal, BaseType,
    DTypeDesc, DECIMAL_PRECISION, DECIMAL_SCALE,
};
use crate::expr::LiteralValue;

use super::ir::{PlanInner, PlanStep};
use super::schema_py::schema_descriptors_as_py;

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
fn polars_err(e: PolarsError) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Polars execution error: {e}"))
}

/// Hand off an in-memory Polars `DataFrame` to Python Polars via Arrow IPC (avoids
/// per-cell `series_to_py_list` materialization).
#[cfg(feature = "polars_engine")]
fn polars_dataframe_to_python_via_ipc(py: Python<'_>, df: &mut DataFrame) -> PyResult<PyObject> {
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
fn py_datetime_to_micros(item: &Bound<'_, PyAny>) -> PyResult<i64> {
    let dt = item.downcast::<PyDateTime>()?;
    let secs: f64 = dt.call_method0("timestamp")?.extract()?;
    Ok((secs * 1_000_000.0).round() as i64)
}

#[cfg(feature = "polars_engine")]
fn py_date_to_days(item: &Bound<'_, PyAny>) -> PyResult<i32> {
    let d = item.downcast::<PyDate>()?;
    let ordinal: i32 = d.call_method0("toordinal")?.extract()?;
    Ok(ordinal - 719_163)
}

#[cfg(feature = "polars_engine")]
fn py_timedelta_to_micros(item: &Bound<'_, PyAny>) -> PyResult<i64> {
    let td = item.downcast::<PyDelta>()?;
    let secs: f64 = td.call_method0("total_seconds")?.extract()?;
    Ok((secs * 1_000_000.0).round() as i64)
}

#[cfg(feature = "polars_engine")]
fn micros_to_py_datetime(py: Python<'_>, micros: i64) -> PyResult<PyObject> {
    let dt_mod = py.import_bound("datetime")?;
    let dt = dt_mod.getattr("datetime")?;
    Ok(dt
        .call_method1("fromtimestamp", (micros as f64 / 1_000_000.0,))?
        .into_py(py))
}

#[cfg(feature = "polars_engine")]
fn days_to_py_date(py: Python<'_>, days: i32) -> PyResult<PyObject> {
    let dt_mod = py.import_bound("datetime")?;
    let date = dt_mod.getattr("date")?;
    Ok(date
        .call_method1("fromordinal", (days + 719_163,))?
        .into_py(py))
}

#[cfg(feature = "polars_engine")]
fn micros_to_py_timedelta(py: Python<'_>, micros: i64) -> PyResult<PyObject> {
    let dt_mod = py.import_bound("datetime")?;
    let td = dt_mod.getattr("timedelta")?;
    Ok(td.call1((0, 0, micros))?.into_py(py))
}

#[cfg(feature = "polars_engine")]
fn py_row_get_field<'py>(item: &Bound<'py, PyAny>, fname: &str) -> PyResult<Bound<'py, PyAny>> {
    if let Ok(d) = item.downcast::<PyDict>() {
        return d.call_method1("get", (fname, item.py().None()));
    }
    item.getattr(fname)
}

/// Canonical UUID string for `uuid.UUID` or `str` cells (logical `BaseType::Uuid`).
#[cfg(feature = "polars_engine")]
fn py_extract_uuid_canonical(item: &Bound<'_, PyAny>) -> PyResult<String> {
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
        DTypeDesc::Scalar { base: None, .. } => {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Root schema cannot have unknown-base dtype.",
            ))
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
fn root_data_to_polars_df(
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

/// Polars physical plan runner: one step at a time for easier extension.
#[cfg(feature = "polars_engine")]
pub struct PolarsPlanRunner;

#[cfg(feature = "polars_engine")]
impl PolarsPlanRunner {
    pub fn apply_steps(mut lf: LazyFrame, steps: &[PlanStep]) -> PyResult<LazyFrame> {
        for step in steps.iter() {
            lf = Self::apply_step(lf, step)?;
        }
        Ok(lf)
    }

    fn apply_step(mut lf: LazyFrame, step: &PlanStep) -> PyResult<LazyFrame> {
        match step {
            PlanStep::Select { columns } => {
                let exprs = columns.iter().map(col).collect::<Vec<_>>();
                lf = lf.select(exprs);
            }
            PlanStep::WithColumns { columns } => {
                let mut exprs = Vec::with_capacity(columns.len());
                for (name, expr) in columns.iter() {
                    let pe = expr.to_polars_expr()?.alias(name);
                    exprs.push(pe);
                }
                lf = lf.with_columns(exprs);
            }
            PlanStep::Filter { condition } => {
                // SQL-like null semantics for filter: keep exactly True; drop False/NULL.
                let cond = condition.to_polars_expr()?.fill_null(lit(false));
                lf = lf.filter(cond);
            }
            PlanStep::Sort { by, descending } => {
                let exprs = by.iter().map(col).collect::<Vec<PolarsExpr>>();
                let mut desc = descending.clone();
                if desc.is_empty() {
                    desc = vec![false; by.len()];
                }
                lf = lf.sort_by_exprs(
                    exprs,
                    SortMultipleOptions::new().with_order_descending_multi(desc),
                );
            }
            PlanStep::Unique { subset, keep } => {
                let keep_strategy = match keep.as_str() {
                    "first" => UniqueKeepStrategy::First,
                    "last" => UniqueKeepStrategy::Last,
                    _ => UniqueKeepStrategy::Any,
                };
                let subset_exprs = subset
                    .clone()
                    .map(|v| v.into_iter().map(col).collect::<Vec<PolarsExpr>>());
                lf = lf.unique_stable_generic(subset_exprs, keep_strategy);
            }
            PlanStep::Rename { columns } => {
                let old = columns.keys().cloned().collect::<Vec<_>>();
                let new = columns.values().cloned().collect::<Vec<_>>();
                lf = lf.rename(old, new, true);
            }
            PlanStep::Slice { offset, length } => {
                lf = lf.slice(*offset, *length as u32);
            }
            PlanStep::FillNull {
                subset,
                value,
                strategy,
            } => {
                let all_cols = lf
                    .collect_schema()
                    .map_err(polars_err)?
                    .iter_names_cloned()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                let targets = subset.clone().unwrap_or(all_cols);
                let exprs = targets
                    .into_iter()
                    .map(|name| {
                        let base = col(&name);
                        let filled = if let Some(v) = value.as_ref() {
                            match v {
                                LiteralValue::Int(i) => base.fill_null(lit(*i)),
                                LiteralValue::Float(f) => base.fill_null(lit(*f)),
                                LiteralValue::Bool(b) => base.fill_null(lit(*b)),
                                LiteralValue::Str(s) => base.fill_null(lit(s.clone())),
                                LiteralValue::EnumStr(s) => base.fill_null(lit(s.clone())),
                                LiteralValue::Uuid(s) => base.fill_null(lit(s.clone())),
                                LiteralValue::Decimal(v) => base.fill_null(
                                    Scalar::new_decimal(*v, DECIMAL_PRECISION, DECIMAL_SCALE).lit(),
                                ),
                                LiteralValue::DateTimeMicros(v) => {
                                    base.fill_null(lit(*v).cast(DataType::Datetime(
                                        polars::prelude::TimeUnit::Microseconds,
                                        None,
                                    )))
                                }
                                LiteralValue::DateDays(v) => {
                                    base.fill_null(lit(*v).cast(DataType::Date))
                                }
                                LiteralValue::DurationMicros(v) => base.fill_null(lit(*v).cast(
                                    DataType::Duration(polars::prelude::TimeUnit::Microseconds),
                                )),
                            }
                        } else {
                            match strategy.as_ref().expect("validated strategy").as_str() {
                                "forward" => {
                                    base.fill_null_with_strategy(FillNullStrategy::Forward(None))
                                }
                                "backward" => {
                                    base.fill_null_with_strategy(FillNullStrategy::Backward(None))
                                }
                                "min" => base.fill_null_with_strategy(FillNullStrategy::Min),
                                "max" => base.fill_null_with_strategy(FillNullStrategy::Max),
                                "mean" => base.fill_null_with_strategy(FillNullStrategy::Mean),
                                "zero" => base.fill_null_with_strategy(FillNullStrategy::Zero),
                                "one" => base.fill_null_with_strategy(FillNullStrategy::One),
                                _ => base,
                            }
                        };
                        filled.alias(&name)
                    })
                    .collect::<Vec<_>>();
                lf = lf.with_columns(exprs);
            }
            PlanStep::DropNulls { subset } => {
                let all_cols = lf
                    .collect_schema()
                    .map_err(polars_err)?
                    .iter_names_cloned()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                let targets = subset.clone().unwrap_or(all_cols);
                if let Some(first) = targets.first() {
                    let mut cond = col(first).is_not_null();
                    for c in targets.iter().skip(1) {
                        cond = cond.and(col(c).is_not_null());
                    }
                    lf = lf.filter(cond);
                }
            }
        }
        Ok(lf)
    }
}

#[cfg(feature = "polars_engine")]
fn polars_anyvalue_to_py(py: Python<'_>, av: AnyValue<'_>, fd: &DTypeDesc) -> PyResult<PyObject> {
    if matches!(av, AnyValue::Null) {
        return Ok(py.None());
    }
    match fd {
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
fn series_to_py_list(py: Python<'_>, series: &Series, dtype: &DTypeDesc) -> PyResult<PyObject> {
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
) -> PyResult<PyObject> {
    let df = root_data_to_polars_df(py, &plan.root_schema, root_data)?;
    let lf = PolarsPlanRunner::apply_steps(df.lazy(), &plan.steps)?;
    let mut out_df = lf.collect().map_err(polars_err)?;

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
fn dtype_from_polars(dt: &DataType) -> PyResult<DTypeDesc> {
    match dt {
        DataType::Int64 => Ok(DTypeDesc::Scalar {
            base: Some(BaseType::Int),
            nullable: true,
        }),
        DataType::Float64 => Ok(DTypeDesc::Scalar {
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
        DataType::List(inner) => Ok(DTypeDesc::List {
            inner: Box::new(dtype_from_polars(inner)?),
            nullable: true,
        }),
        other => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
            "Unsupported Polars dtype in result schema: {other:?}"
        ))),
    }
}

#[cfg(feature = "polars_engine")]
#[allow(clippy::too_many_arguments)]
pub fn execute_join_polars(
    py: Python<'_>,
    left_plan: &PlanInner,
    left_root_data: &Bound<'_, PyAny>,
    right_plan: &PlanInner,
    right_root_data: &Bound<'_, PyAny>,
    left_on: Vec<String>,
    right_on: Vec<String>,
    how: String,
    suffix: String,
    as_python_lists: bool,
) -> PyResult<(PyObject, PyObject)> {
    let is_cross = how == "cross";
    let is_semi = how == "semi";
    let is_anti = how == "anti";
    if !is_cross && (left_on.is_empty() || right_on.is_empty()) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "join(on=...) requires at least one join key.",
        ));
    }
    if !is_cross && left_on.len() != right_on.len() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "join() left and right join key lists must have the same length.",
        ));
    }
    if is_cross && (!left_on.is_empty() || !right_on.is_empty()) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "cross join does not accept join keys.",
        ));
    }
    for key in left_on.iter() {
        if !left_plan.schema.contains_key(key) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "join() unknown left join key '{}'.",
                key
            )));
        }
    }
    for key in right_on.iter() {
        if !right_plan.schema.contains_key(key) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "join() unknown right join key '{}'.",
                key
            )));
        }
    }

    let join_type = match how.as_str() {
        "inner" => JoinType::Inner,
        "left" => JoinType::Left,
        "right" => JoinType::Right,
        "full" | "outer" => JoinType::Full,
        "semi" | "anti" => JoinType::Left,
        "cross" => JoinType::Cross,
        other => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Unsupported join how '{}'. Use one of: inner, left, full, right, semi, anti, cross.",
            other
        )))
        }
    };

    let left_df = root_data_to_polars_df(py, &left_plan.root_schema, left_root_data)?;
    let right_df = root_data_to_polars_df(py, &right_plan.root_schema, right_root_data)?;
    let left_lf = PolarsPlanRunner::apply_steps(left_df.lazy(), &left_plan.steps)?;
    let mut right_lf = PolarsPlanRunner::apply_steps(right_df.lazy(), &right_plan.steps)?;

    // Deterministic collision handling:
    // - keep left names unchanged
    // - for right non-key collisions, apply suffix
    // - right join keys are dropped (joined on same-name keys)
    let mut right_select = Vec::new();
    for name in right_plan.schema.keys() {
        if is_semi || is_anti {
            if right_on.contains(name) {
                right_select.push(col(name));
            }
            continue;
        }
        // The join operation needs the right-side join keys to still exist in the
        // LazyFrame at join time.
        if right_on.contains(name) {
            right_select.push(col(name));
            continue;
        }

        // Deterministic collision handling for non-key columns.
        if left_plan.schema.contains_key(name) {
            right_select.push(col(name).alias(format!("{}{}", name, suffix)));
        } else {
            right_select.push(col(name));
        }
    }
    if is_semi || is_anti {
        right_select.push(lit(1i64).alias("__pydantable_join_marker"));
    }
    if !right_select.is_empty() {
        right_lf = right_lf.select(right_select);
    } else if !is_cross && !right_on.is_empty() {
        right_lf = right_lf.select([col(right_on[0].as_str())]);
    }

    let out_df = if is_cross {
        let left_df = left_lf.collect().map_err(polars_err)?;
        let right_df = right_lf.collect().map_err(polars_err)?;
        left_df
            .cross_join(
                &right_df,
                Some(suffix.clone().into()),
                None,
                MaintainOrderJoin::Left,
            )
            .map_err(polars_err)?
    } else {
        let left_key_exprs = left_on.iter().map(col).collect::<Vec<_>>();
        let right_key_exprs = right_on.iter().map(col).collect::<Vec<_>>();
        let mut joined = left_lf.join(
            right_lf,
            left_key_exprs,
            right_key_exprs,
            JoinArgs::new(join_type.clone()),
        );
        if is_semi {
            joined = joined
                .filter(col("__pydantable_join_marker").is_not_null())
                .select(left_plan.schema.keys().map(col).collect::<Vec<_>>());
        } else if is_anti {
            joined = joined
                .filter(col("__pydantable_join_marker").is_null())
                .select(left_plan.schema.keys().map(col).collect::<Vec<_>>());
        }
        joined.collect().map_err(polars_err)?
    };

    // Build schema descriptors from actual output dtypes.
    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
    if is_semi || is_anti {
        out_schema = left_plan.schema.clone();
    }
    for col_name in out_df.get_column_names() {
        if is_semi || is_anti {
            continue;
        }
        let col_name_str = col_name.as_str();
        // Preserve nullable semantics from the input schemas instead of
        // inferring them from observed output nulls. This keeps
        // `Optional[T]` stable across joins even when the matched rows
        // happen to contain no nulls.
        let out_desc = if let Some(left_d) = left_plan.schema.get(col_name_str) {
            let mut d = left_d.clone();
            if matches!(join_type, JoinType::Right | JoinType::Full) {
                d = d.with_assigned_none_nullability();
            }
            d
        } else if let Some(stripped) = col_name_str.strip_suffix(suffix.as_str()) {
            // Collision columns from the right are renamed with the suffix.
            if let Some(right_d) = right_plan.schema.get(stripped) {
                let mut d = right_d.clone();
                if matches!(join_type, JoinType::Left | JoinType::Full) {
                    d = d.with_assigned_none_nullability();
                }
                d
            } else {
                let s = out_df
                    .column(col_name)
                    .map_err(polars_err)?
                    .as_materialized_series();
                dtype_from_polars(s.dtype())?
            }
        } else if let Some(right_d) = right_plan.schema.get(col_name_str) {
            let mut d = right_d.clone();
            if matches!(join_type, JoinType::Left | JoinType::Full) {
                d = d.with_assigned_none_nullability();
            }
            d
        } else {
            let s = out_df
                .column(col_name)
                .map_err(polars_err)?
                .as_materialized_series();
            dtype_from_polars(s.dtype())?
        };

        out_schema.insert(col_name.to_string(), out_desc);
    }

    if !as_python_lists {
        let mut out_only = out_df;
        let names: Vec<&str> = out_schema.keys().map(|s| s.as_str()).collect();
        out_only = out_only.select(&names).map_err(polars_err)?;
        let py_df = polars_dataframe_to_python_via_ipc(py, &mut out_only)?;
        let desc = schema_descriptors_as_py(py, &out_schema)?;
        return Ok((py_df, desc));
    }

    let out_dict = PyDict::new_bound(py);
    for (name, dtype) in out_schema.iter() {
        let col = out_df
            .column(name)
            .map_err(polars_err)?
            .as_materialized_series()
            .clone();
        let py_list = series_to_py_list(py, &col, dtype)?;
        out_dict.set_item(name, py_list)?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    Ok((out_dict.into_py(py), desc))
}

#[cfg(feature = "polars_engine")]
fn mask_groupby_sum_mean_columns(
    mut df: DataFrame,
    tmp_count_cols: &HashMap<String, String>,
    out_schema: &HashMap<String, DTypeDesc>,
) -> PyResult<DataFrame> {
    if tmp_count_cols.is_empty() {
        let names: Vec<&str> = out_schema.keys().map(|s| s.as_str()).collect();
        return df.select(&names).map_err(polars_err);
    }
    let mut lf = df.lazy();
    for (out_name, tmp_name) in tmp_count_cols.iter() {
        let dtype = out_schema.get(out_name).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "Internal error: missing schema for masked column '{out_name}'."
            ))
        })?;
        let expr = match dtype.as_scalar_base_field().flatten() {
            Some(crate::dtype::BaseType::Int) => when(col(tmp_name).eq(lit(0i64)))
                .then(lit(NULL))
                .otherwise(col(out_name))
                .alias(out_name),
            Some(crate::dtype::BaseType::Float) => when(col(tmp_name).eq(lit(0i64)))
                .then(lit(NULL))
                .otherwise(col(out_name))
                .alias(out_name),
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "sum/mean masking expects int or float output dtypes.",
                ))
            }
        };
        lf = lf.with_columns([expr]);
    }
    df = lf.collect().map_err(polars_err)?;
    let names: Vec<&str> = out_schema.keys().map(|s| s.as_str()).collect();
    df.select(&names).map_err(polars_err)
}

#[cfg(feature = "polars_engine")]
pub fn execute_groupby_agg_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    by: Vec<String>,
    aggregations: Vec<(String, String, String)>,
    as_python_lists: bool,
) -> PyResult<(PyObject, PyObject)> {
    if by.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "group_by(...) requires at least one key.",
        ));
    }
    for key in by.iter() {
        if !plan.schema.contains_key(key) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "group_by() unknown key '{}'.",
                key
            )));
        }
    }
    if aggregations.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "agg(...) requires at least one aggregation.",
        ));
    }

    let df = root_data_to_polars_df(py, &plan.root_schema, root_data)?;
    let lf = PolarsPlanRunner::apply_steps(df.lazy(), &plan.steps)?;
    let by_exprs = by.iter().map(col).collect::<Vec<_>>();
    let mut agg_exprs = Vec::new();
    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
    let mut tmp_count_cols: HashMap<String, String> = HashMap::new();
    for key in by.iter() {
        out_schema.insert(key.clone(), plan.schema.get(key).unwrap().clone());
    }

    for (out_name, op, in_col) in aggregations.into_iter() {
        let in_dtype = plan.schema.get(&in_col).cloned().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "agg() unknown input column '{}'.",
                in_col
            ))
        })?;
        match op.as_str() {
            "count" => {
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc::Scalar {
                        base: Some(crate::dtype::BaseType::Int),
                        nullable: false,
                    },
                );
                agg_exprs.push(col(&in_col).count().alias(&out_name));
            }
            "sum" => {
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "sum() requires known-base numeric dtype.",
                    )
                })?;
                if !matches!(
                    base,
                    crate::dtype::BaseType::Int | crate::dtype::BaseType::Float
                ) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "sum() requires int or float input columns.",
                    ));
                }
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc::Scalar {
                        base: Some(base),
                        nullable: true,
                    },
                );
                // Polars returns `0` for `sum` over all-null values.
                // For SQL-like semantics (and our contract), mask that case
                // to null by tracking non-null counts.
                let tmp_count_name = format!("__pydantable_tmp_count_sum_{out_name}");
                tmp_count_cols.insert(out_name.clone(), tmp_count_name.clone());
                agg_exprs.push(col(&in_col).count().alias(&tmp_count_name));
                agg_exprs.push(col(&in_col).sum().alias(&out_name));
            }
            "mean" => {
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "mean() requires known-base numeric dtype.",
                    )
                })?;
                if !matches!(
                    base,
                    crate::dtype::BaseType::Int | crate::dtype::BaseType::Float
                ) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "mean() requires int or float input columns.",
                    ));
                }
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc::Scalar {
                        base: Some(crate::dtype::BaseType::Float),
                        nullable: true,
                    },
                );
                // Same masking approach as for `sum`: all-null -> None.
                let tmp_count_name = format!("__pydantable_tmp_count_mean_{out_name}");
                tmp_count_cols.insert(out_name.clone(), tmp_count_name.clone());
                agg_exprs.push(col(&in_col).count().alias(&tmp_count_name));
                agg_exprs.push(col(&in_col).mean().alias(&out_name));
            }
            "min" => {
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "min() requires known-base dtype.",
                    )
                })?;
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc::Scalar {
                        base: Some(base),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).min().alias(&out_name));
            }
            "max" => {
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "max() requires known-base dtype.",
                    )
                })?;
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc::Scalar {
                        base: Some(base),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).max().alias(&out_name));
            }
            "median" => {
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "median() requires known-base numeric dtype.",
                    )
                })?;
                if !matches!(
                    base,
                    crate::dtype::BaseType::Int | crate::dtype::BaseType::Float
                ) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "median() requires int or float input columns.",
                    ));
                }
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc::Scalar {
                        base: Some(crate::dtype::BaseType::Float),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).median().alias(&out_name));
            }
            "std" => {
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "std() requires known-base numeric dtype.",
                    )
                })?;
                if !matches!(
                    base,
                    crate::dtype::BaseType::Int | crate::dtype::BaseType::Float
                ) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "std() requires int or float input columns.",
                    ));
                }
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc::Scalar {
                        base: Some(crate::dtype::BaseType::Float),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).std(1).alias(&out_name));
            }
            "var" => {
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "var() requires known-base numeric dtype.",
                    )
                })?;
                if !matches!(
                    base,
                    crate::dtype::BaseType::Int | crate::dtype::BaseType::Float
                ) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "var() requires int or float input columns.",
                    ));
                }
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc::Scalar {
                        base: Some(crate::dtype::BaseType::Float),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).var(1).alias(&out_name));
            }
            "first" => {
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "first() requires known-base dtype.",
                    )
                })?;
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc::Scalar {
                        base: Some(base),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).first().alias(&out_name));
            }
            "last" => {
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "last() requires known-base dtype.",
                    )
                })?;
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc::Scalar {
                        base: Some(base),
                        nullable: true,
                    },
                );
                agg_exprs.push(col(&in_col).last().alias(&out_name));
            }
            "n_unique" => {
                out_schema.insert(
                    out_name.clone(),
                    DTypeDesc::Scalar {
                        base: Some(crate::dtype::BaseType::Int),
                        nullable: false,
                    },
                );
                // SQL-like behavior: distinct count ignores NULL values.
                agg_exprs.push(col(&in_col).drop_nulls().n_unique().alias(&out_name));
            }
            other => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Unsupported aggregation '{}'. Use one of: count, sum, mean, min, max, median, std, var, first, last, n_unique.",
                    other
                )))
            }
        }
    }

    let mut out_df = lf
        .group_by(by_exprs)
        .agg(agg_exprs)
        .collect()
        .map_err(polars_err)?;

    out_df = mask_groupby_sum_mean_columns(out_df, &tmp_count_cols, &out_schema)?;

    if !as_python_lists {
        let mut out_only = out_df;
        let py_df = polars_dataframe_to_python_via_ipc(py, &mut out_only)?;
        let desc = schema_descriptors_as_py(py, &out_schema)?;
        return Ok((py_df, desc));
    }

    let out_dict = PyDict::new_bound(py);
    for (name, dtype) in out_schema.iter() {
        let col = out_df
            .column(name)
            .map_err(polars_err)?
            .as_materialized_series()
            .clone();
        let py_list = series_to_py_list(py, &col, dtype)?;
        out_dict.set_item(name, py_list)?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    Ok((out_dict.into_py(py), desc))
}

#[cfg(feature = "polars_engine")]
pub fn execute_concat_polars(
    py: Python<'_>,
    left_plan: &PlanInner,
    left_root_data: &Bound<'_, PyAny>,
    right_plan: &PlanInner,
    right_root_data: &Bound<'_, PyAny>,
    how: String,
    as_python_lists: bool,
) -> PyResult<(PyObject, PyObject)> {
    let left_df = root_data_to_polars_df(py, &left_plan.root_schema, left_root_data)?;
    let right_df = root_data_to_polars_df(py, &right_plan.root_schema, right_root_data)?;
    let left_out = PolarsPlanRunner::apply_steps(left_df.lazy(), &left_plan.steps)?
        .collect()
        .map_err(polars_err)?;
    let right_out = PolarsPlanRunner::apply_steps(right_df.lazy(), &right_plan.steps)?
        .collect()
        .map_err(polars_err)?;

    let out_df = match how.as_str() {
        "vertical" => {
            let left_names = left_out.get_column_names();
            let right_names = right_out.get_column_names();
            let right_aligned = if left_names == right_names {
                right_out
            } else {
                let ls: HashSet<_> = left_names.iter().collect();
                let rs: HashSet<_> = right_names.iter().collect();
                if ls != rs {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "concat(vertical) requires both inputs to have identical columns.",
                    ));
                }
                let names: Vec<&str> = left_names.iter().map(|n| n.as_str()).collect();
                right_out.select(&names).map_err(polars_err)?
            };
            let mut df = left_out.clone();
            df.vstack_mut(&right_aligned).map_err(polars_err)?;
            df
        }
        "horizontal" => {
            for c in right_out.get_column_names_owned().iter() {
                if left_out.get_column_names_owned().contains(c) {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "concat(horizontal) duplicate column '{}' not supported.",
                        c
                    )));
                }
            }
            let mut df = left_out.clone();
            df.hstack_mut(right_out.columns()).map_err(polars_err)?;
            df
        }
        other => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unsupported concat how '{}'. Use one of: vertical, horizontal.",
                other
            )))
        }
    };

    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
    for name in out_df.get_column_names().iter() {
        if let Some(d) = left_plan.schema.get(name.as_str()) {
            out_schema.insert(name.to_string(), d.clone());
        } else if let Some(d) = right_plan.schema.get(name.as_str()) {
            out_schema.insert(name.to_string(), d.clone());
        } else {
            let s = out_df
                .column(name)
                .map_err(polars_err)?
                .as_materialized_series();
            out_schema.insert(name.to_string(), dtype_from_polars(s.dtype())?);
        }
    }

    if !as_python_lists {
        let mut out_only = out_df;
        let names: Vec<&str> = out_schema.keys().map(|s| s.as_str()).collect();
        out_only = out_only.select(&names).map_err(polars_err)?;
        let py_df = polars_dataframe_to_python_via_ipc(py, &mut out_only)?;
        let desc = schema_descriptors_as_py(py, &out_schema)?;
        return Ok((py_df, desc));
    }

    let out_dict = PyDict::new_bound(py);
    for (name, dtype) in out_schema.iter() {
        let col = out_df
            .column(name)
            .map_err(polars_err)?
            .as_materialized_series()
            .clone();
        let py_list = series_to_py_list(py, &col, dtype)?;
        out_dict.set_item(name, py_list)?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    Ok((out_dict.into_py(py), desc))
}

#[cfg(feature = "polars_engine")]
fn py_dict_to_literal_ctx(
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
fn literal_to_py(py: Python<'_>, v: &LiteralValue) -> PyObject {
    match v {
        LiteralValue::Int(i) => i.into_py(py),
        LiteralValue::Float(f) => f.into_py(py),
        LiteralValue::Bool(b) => b.into_py(py),
        LiteralValue::Str(s) => s.clone().into_py(py),
        LiteralValue::EnumStr(s) => s.clone().into_py(py),
        LiteralValue::Uuid(s) => py
            .import_bound("uuid")
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
    }
}

#[cfg(feature = "polars_engine")]
fn agg_literal(
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
                    "min" => Ok(Some(LiteralValue::Decimal(*decs.iter().min().unwrap()))),
                    "max" => Ok(Some(LiteralValue::Decimal(*decs.iter().max().unwrap()))),
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

#[cfg(feature = "polars_engine")]
#[allow(clippy::too_many_arguments)]
pub fn execute_melt_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    id_vars: Vec<String>,
    value_vars: Option<Vec<String>>,
    variable_name: String,
    value_name: String,
    as_python_lists: bool,
) -> PyResult<(PyObject, PyObject)> {
    if variable_name == value_name {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "melt() variable_name and value_name must be different.",
        ));
    }
    if plan.schema.contains_key(&variable_name) || plan.schema.contains_key(&value_name) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "melt() output column names collide with existing schema columns.",
        ));
    }
    for c in id_vars.iter() {
        if !plan.schema.contains_key(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "melt() unknown id column '{}'.",
                c
            )));
        }
    }
    let mut values = value_vars.unwrap_or_else(|| {
        plan.schema
            .keys()
            .filter(|k| !id_vars.contains(k))
            .cloned()
            .collect::<Vec<_>>()
    });
    if values.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "melt() requires at least one value column.",
        ));
    }
    for c in values.iter() {
        if !plan.schema.contains_key(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "melt() unknown value column '{}'.",
                c
            )));
        }
        if id_vars.contains(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "melt() value_vars cannot overlap id_vars.",
            ));
        }
    }
    values.sort();
    let first_base = plan
        .schema
        .get(&values[0])
        .and_then(|d| d.as_scalar_base_field().flatten());
    for c in values.iter().skip(1) {
        if plan
            .schema
            .get(c)
            .and_then(|d| d.as_scalar_base_field().flatten())
            != first_base
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "melt() requires all value columns to share the same base dtype.",
            ));
        }
    }
    let base = first_base.ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "melt() value columns must have known-base dtypes.",
        )
    })?;

    let df = root_data_to_polars_df(py, &plan.root_schema, root_data)?;
    let lf = PolarsPlanRunner::apply_steps(df.lazy(), &plan.steps)?;
    let args = UnpivotArgsDSL {
        on: Some(cols(values.iter().map(|s| s.as_str()))),
        index: cols(id_vars.iter().map(|s| s.as_str())),
        variable_name: Some(variable_name.clone().into()),
        value_name: Some(value_name.clone().into()),
    };
    let mut out_df = lf.unpivot(args).collect().map_err(polars_err)?;

    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
    for k in id_vars.iter() {
        out_schema.insert(k.clone(), plan.schema.get(k).unwrap().clone());
    }
    out_schema.insert(
        variable_name.clone(),
        DTypeDesc::non_nullable(crate::dtype::BaseType::Str),
    );
    let nullable = values.iter().any(|c| {
        plan.schema
            .get(c)
            .map(|d| d.nullable_flag())
            .unwrap_or(true)
    });
    out_schema.insert(
        value_name.clone(),
        DTypeDesc::Scalar {
            base: Some(base),
            nullable,
        },
    );

    if !as_python_lists {
        let py_df = polars_dataframe_to_python_via_ipc(py, &mut out_df)?;
        let desc = schema_descriptors_as_py(py, &out_schema)?;
        return Ok((py_df, desc));
    }

    let out_dict = PyDict::new_bound(py);
    for (name, dtype) in out_schema.iter() {
        let col = out_df
            .column(name)
            .map_err(polars_err)?
            .as_materialized_series()
            .clone();
        let py_list = series_to_py_list(py, &col, dtype)?;
        out_dict.set_item(name, py_list)?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    Ok((out_dict.into_py(py), desc))
}

#[cfg(feature = "polars_engine")]
#[allow(clippy::needless_range_loop)]
#[allow(clippy::too_many_arguments)]
pub fn execute_pivot_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    index: Vec<String>,
    columns: String,
    values: Vec<String>,
    aggregate_function: String,
    as_python_lists: bool,
) -> PyResult<(PyObject, PyObject)> {
    if index.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "pivot() requires at least one index column.",
        ));
    }
    if values.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "pivot() requires at least one value column.",
        ));
    }
    for c in index.iter() {
        if !plan.schema.contains_key(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "pivot() unknown index column '{}'.",
                c
            )));
        }
    }
    if !plan.schema.contains_key(&columns) {
        return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
            "pivot() unknown columns argument '{}'.",
            columns
        )));
    }
    for v in values.iter() {
        if !plan.schema.contains_key(v) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "pivot() unknown value column '{}'.",
                v
            )));
        }
    }
    let supported = [
        "count", "sum", "mean", "min", "max", "median", "std", "var", "first", "last", "n_unique",
    ];
    if !supported.contains(&aggregate_function.as_str()) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "pivot() unsupported aggregate_function '{}'.",
            aggregate_function
        )));
    }

    let data_obj = super::execute_plan(py, plan, root_data, true)?;
    let data_bound = data_obj.bind(py);
    let ctx = py_dict_to_literal_ctx(&plan.schema, data_bound)?;

    let mut pivot_values: Vec<String> = Vec::new();
    let mut seen_pivot: std::collections::HashSet<String> = std::collections::HashSet::new();
    let pivot_col = ctx.get(&columns).ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
            "pivot() unknown columns argument '{}'.",
            columns
        ))
    })?;
    for item in pivot_col.iter() {
        let key = match item {
            Some(LiteralValue::Str(s)) => s.clone(),
            Some(LiteralValue::EnumStr(s)) => s.clone(),
            Some(LiteralValue::Uuid(s)) => s.clone(),
            Some(LiteralValue::Decimal(v)) => v.to_string(),
            Some(LiteralValue::Int(v)) => v.to_string(),
            Some(LiteralValue::Float(v)) => v.to_string(),
            Some(LiteralValue::Bool(v)) => v.to_string(),
            Some(LiteralValue::DateTimeMicros(v)) => v.to_string(),
            Some(LiteralValue::DateDays(v)) => v.to_string(),
            Some(LiteralValue::DurationMicros(v)) => v.to_string(),
            None => "null".to_string(),
        };
        if seen_pivot.insert(key.clone()) {
            pivot_values.push(key);
        }
    }

    let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let row_count = ctx.values().next().map_or(0, std::vec::Vec::len);
    for i in 0..row_count {
        let mut sig = String::new();
        for c in index.iter() {
            sig.push_str(&format!("{:?}|", ctx[c][i]));
        }
        groups.entry(sig).or_default().push(i);
    }

    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
    for c in index.iter() {
        out_schema.insert(c.clone(), plan.schema.get(c).unwrap().clone());
    }
    let mut out_cols: HashMap<String, Vec<PyObject>> = HashMap::new();
    for c in index.iter() {
        out_cols.insert(c.clone(), Vec::new());
    }

    let mut generated_cols: Vec<(String, String, DTypeDesc)> = Vec::new();
    for pv in pivot_values.iter() {
        for v in values.iter() {
            let name = if values.len() == 1 {
                format!("{}_{}", pv, aggregate_function)
            } else {
                format!("{}_{}_{}", pv, v, aggregate_function)
            };
            if out_schema.contains_key(&name) {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "pivot() generated duplicate output column '{}'.",
                    name
                )));
            }
            let in_d = plan.schema.get(v).unwrap().clone();
            let base = in_d.as_scalar_base_field().flatten().ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "pivot() value columns must have known-base dtypes.",
                )
            })?;
            if matches!(
                aggregate_function.as_str(),
                "sum" | "mean" | "median" | "std" | "var"
            ) && !matches!(
                base,
                crate::dtype::BaseType::Int
                    | crate::dtype::BaseType::Float
                    | crate::dtype::BaseType::Decimal
            ) {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "pivot() numeric aggregations require int, float, or decimal value columns.",
                ));
            }
            let out_d = match aggregate_function.as_str() {
                "count" | "n_unique" => DTypeDesc::non_nullable(crate::dtype::BaseType::Int),
                "mean" | "median" | "std" | "var" => {
                    DTypeDesc::scalar_nullable(crate::dtype::BaseType::Float)
                }
                _ => DTypeDesc::Scalar {
                    base: Some(base),
                    nullable: true,
                },
            };
            generated_cols.push((name.clone(), v.clone(), out_d.clone()));
            out_schema.insert(name.clone(), out_d);
            out_cols.insert(name, Vec::new());
        }
    }

    for row_idx in groups.values() {
        let first = row_idx[0];
        for c in index.iter() {
            let val = ctx[c][first]
                .as_ref()
                .map_or(py.None(), |x| literal_to_py(py, x));
            out_cols.get_mut(c).unwrap().push(val);
        }
        for pv in pivot_values.iter() {
            let matching = row_idx
                .iter()
                .copied()
                .filter(|i| {
                    let key = match &ctx[&columns][*i] {
                        Some(LiteralValue::Str(s)) => s.clone(),
                        Some(LiteralValue::EnumStr(s)) => s.clone(),
                        Some(LiteralValue::Uuid(s)) => s.clone(),
                        Some(LiteralValue::Decimal(v)) => v.to_string(),
                        Some(LiteralValue::Int(v)) => v.to_string(),
                        Some(LiteralValue::Float(v)) => v.to_string(),
                        Some(LiteralValue::Bool(v)) => v.to_string(),
                        Some(LiteralValue::DateTimeMicros(v)) => v.to_string(),
                        Some(LiteralValue::DateDays(v)) => v.to_string(),
                        Some(LiteralValue::DurationMicros(v)) => v.to_string(),
                        None => "null".to_string(),
                    };
                    &key == pv
                })
                .collect::<Vec<_>>();
            for (name, source_col, out_d) in generated_cols.iter() {
                let expected_name = if values.len() == 1 {
                    format!("{}_{}", pv, aggregate_function)
                } else {
                    format!("{}_{}_{}", pv, source_col, aggregate_function)
                };
                if &expected_name != name {
                    continue;
                }
                let vals = matching
                    .iter()
                    .map(|i| ctx[source_col][*i].clone())
                    .collect::<Vec<_>>();
                let lit = agg_literal(
                    &aggregate_function,
                    &vals,
                    out_d
                        .as_scalar_base_field()
                        .flatten()
                        .unwrap_or(crate::dtype::BaseType::Float),
                )?;
                out_cols
                    .get_mut(name)
                    .unwrap()
                    .push(lit.as_ref().map_or(py.None(), |x| literal_to_py(py, x)));
            }
        }
    }

    let out_dict = PyDict::new_bound(py);
    for (k, v) in out_cols {
        out_dict.set_item(k, PyList::new_bound(py, v))?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    if !as_python_lists {
        let pl = py.import_bound("polars")?;
        let df_obj = pl.getattr("DataFrame")?.call1((out_dict.as_ref(),))?;
        return Ok((df_obj.into_py(py), desc));
    }
    Ok((out_dict.into_py(py), desc))
}

#[cfg(feature = "polars_engine")]
fn py_index_value_to_seconds(item: &Bound<'_, PyAny>) -> PyResult<f64> {
    if item.is_none() {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "group_by_dynamic index column must be time-like or numeric.",
        ));
    }
    if let Ok(dt) = item.downcast::<PyDateTime>() {
        let secs: f64 = dt.call_method0("timestamp")?.extract()?;
        return Ok(secs);
    }
    if let Ok(d) = item.downcast::<PyDate>() {
        let py = item.py();
        let dt_mod = py.import_bound("datetime")?;
        let datetime = dt_mod.getattr("datetime")?;
        let combine = datetime.getattr("combine")?;
        let min_time = datetime.getattr("min")?.getattr("time")?;
        let dt_obj = combine.call1((d, min_time))?;
        let secs: f64 = dt_obj.call_method0("timestamp")?.extract()?;
        return Ok(secs);
    }
    if let Ok(td) = item.downcast::<PyDelta>() {
        let secs: f64 = td.call_method0("total_seconds")?.extract()?;
        return Ok(secs);
    }
    if let Ok(i) = item.extract::<i64>() {
        return Ok(i as f64);
    }
    if let Ok(f) = item.extract::<f64>() {
        return Ok(f);
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "group_by_dynamic index column must be time-like or numeric.",
    ))
}

#[cfg(feature = "polars_engine")]
fn parse_duration_seconds_strict(text: &str) -> PyResult<f64> {
    let text = text.trim();
    if text.len() < 2 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "invalid duration string",
        ));
    }
    let unit = text.chars().last().unwrap();
    let num: f64 = text[..text.len() - 1].parse().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("invalid duration {text:?}"))
    })?;
    let factor = match unit {
        's' => 1.0,
        'm' => 60.0,
        'h' => 3600.0,
        'd' => 86400.0,
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Duration supports s/m/h/d suffixes.",
            ))
        }
    };
    Ok(num * factor)
}

#[cfg(feature = "polars_engine")]
fn dynamic_group_key_fragment(value: &Option<LiteralValue>) -> String {
    match value {
        None => "N".to_string(),
        Some(LiteralValue::Int(i)) => format!("I:{i}"),
        Some(LiteralValue::Float(f)) => format!("F:{f:?}"),
        Some(LiteralValue::Bool(b)) => format!("B:{b}"),
        Some(LiteralValue::Str(s)) => format!("S:{s}"),
        Some(LiteralValue::EnumStr(s)) => format!("E:{s}"),
        Some(LiteralValue::Uuid(s)) => format!("U:{s}"),
        Some(LiteralValue::Decimal(v)) => format!("DEC:{v}"),
        Some(LiteralValue::DateTimeMicros(v)) => format!("DT:{v}"),
        Some(LiteralValue::DateDays(v)) => format!("D:{v}"),
        Some(LiteralValue::DurationMicros(v)) => format!("TD:{v}"),
    }
}

#[cfg(feature = "polars_engine")]
fn dynamic_row_group_key(
    ctx: &HashMap<String, Vec<Option<LiteralValue>>>,
    by: &[String],
    row: usize,
) -> String {
    let mut s = String::new();
    for c in by {
        s.push('|');
        s.push_str(&dynamic_group_key_fragment(&ctx[c][row]));
    }
    s
}

#[cfg(feature = "polars_engine")]
#[allow(clippy::too_many_arguments)]
pub fn execute_groupby_dynamic_agg_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    index_column: String,
    every: String,
    period: Option<String>,
    by: Option<Vec<String>>,
    aggregations: Vec<(String, String, String)>,
    as_python_lists: bool,
) -> PyResult<(PyObject, PyObject)> {
    let by = by.unwrap_or_default();
    if !plan.schema.contains_key(&index_column) {
        return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
            "group_by_dynamic() unknown index column '{index_column}'.",
        )));
    }
    for c in by.iter() {
        if !plan.schema.contains_key(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "group_by_dynamic() unknown by column '{c}'.",
            )));
        }
    }
    if aggregations.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "agg(...) requires at least one aggregation.",
        ));
    }
    for (_, op, _) in aggregations.iter() {
        if !matches!(op.as_str(), "count" | "sum" | "mean" | "min" | "max") {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unsupported dynamic aggregation '{op}'.",
            )));
        }
    }

    let data_obj = super::execute_plan(py, plan, root_data, true)?;
    let data_bound = data_obj.bind(py);
    let ctx = py_dict_to_literal_ctx(&plan.schema, data_bound)?;

    let dict: &Bound<'_, PyDict> = data_bound.downcast()?;
    let index_list_any = dict.get_item(&index_column)?.ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
            "group_by_dynamic() unknown index column '{index_column}'.",
        ))
    })?;
    let index_list: &Bound<'_, PyList> = index_list_any.downcast()?;
    let n = index_list.len();
    let mut times: Vec<f64> = Vec::with_capacity(n);
    for item in index_list.iter() {
        times.push(py_index_value_to_seconds(&item)?);
    }

    let every_s = parse_duration_seconds_strict(&every)?;
    let period_str = period.unwrap_or_else(|| every.clone());
    let period_s = parse_duration_seconds_strict(&period_str)?;
    if every_s <= 0.0 || period_s <= 0.0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "group_by_dynamic() requires positive every= and period= durations \
             (got every={every:?} -> {every_s}, period={period_str:?} -> {period_s}).",
        )));
    }

    let (t_min, t_max) = if times.is_empty() {
        (0.0, 0.0)
    } else {
        (
            times.iter().copied().fold(f64::INFINITY, f64::min),
            times.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        )
    };

    let mut start = t_min;
    let mut out_cols: HashMap<String, Vec<PyObject>> = HashMap::new();
    out_cols.insert(index_column.clone(), Vec::new());
    for c in by.iter() {
        out_cols.insert(c.clone(), Vec::new());
    }
    for (name, _, _) in aggregations.iter() {
        out_cols.insert(name.clone(), Vec::new());
    }

    while start <= t_max {
        let end = start + period_s;
        let win_rows: Vec<usize> = times
            .iter()
            .enumerate()
            .filter_map(|(i, t)| {
                if *t >= start && *t < end {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        let mut group_order: Vec<String> = Vec::new();
        let mut group_rows: HashMap<String, Vec<usize>> = HashMap::new();

        if by.is_empty() {
            group_order.push(String::new());
            group_rows.insert(String::new(), win_rows);
        } else {
            for &i in &win_rows {
                let key = dynamic_row_group_key(&ctx, &by, i);
                if !group_rows.contains_key(&key) {
                    group_order.push(key.clone());
                }
                group_rows.entry(key).or_default().push(i);
            }
        }

        for key in group_order {
            let rows = group_rows.get(&key).unwrap();
            if rows.is_empty() {
                continue;
            }
            let first = rows[0];
            let idx_val = index_list.get_item(first)?;
            out_cols
                .get_mut(&index_column)
                .unwrap()
                .push(idx_val.into_py(py));

            for c in by.iter() {
                let v = ctx[c][first]
                    .as_ref()
                    .map_or(py.None(), |x| literal_to_py(py, x));
                out_cols.get_mut(c).unwrap().push(v);
            }

            for (out_name, op, in_col) in aggregations.iter() {
                let in_dtype = plan.schema.get(in_col).cloned().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                        "agg() unknown input column '{in_col}'.",
                    ))
                })?;
                let base = in_dtype.as_scalar_base_field().flatten().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "aggregation requires known-base dtype.",
                    )
                })?;
                let vals: Vec<Option<LiteralValue>> =
                    rows.iter().map(|&i| ctx[in_col][i].clone()).collect();
                let lit = agg_literal(op, &vals, base)?;
                out_cols
                    .get_mut(out_name)
                    .unwrap()
                    .push(lit.as_ref().map_or(py.None(), |x| literal_to_py(py, x)));
            }
        }

        start += every_s;
    }

    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
    out_schema.insert(
        index_column.clone(),
        plan.schema.get(&index_column).unwrap().clone(),
    );
    for c in by.iter() {
        out_schema.insert(c.clone(), plan.schema.get(c).unwrap().clone());
    }
    for (out_name, op, in_col) in aggregations.iter() {
        let in_dtype = plan.schema.get(in_col).unwrap().clone();
        let out_d = match op.as_str() {
            "count" => DTypeDesc::Scalar {
                base: Some(crate::dtype::BaseType::Int),
                nullable: false,
            },
            "mean" => DTypeDesc::Scalar {
                base: Some(crate::dtype::BaseType::Float),
                nullable: true,
            },
            "sum" | "min" | "max" => in_dtype.clone(),
            _ => unreachable!(),
        };
        out_schema.insert(out_name.clone(), out_d);
    }

    let out_dict = PyDict::new_bound(py);
    for (k, v) in out_cols {
        out_dict.set_item(k, PyList::new_bound(py, v))?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    if !as_python_lists {
        let pl = py.import_bound("polars")?;
        let df_obj = pl.getattr("DataFrame")?.call1((out_dict.as_ref(),))?;
        return Ok((df_obj.into_py(py), desc));
    }
    Ok((out_dict.into_py(py), desc))
}

#[cfg(feature = "polars_engine")]
fn dtype_after_explode(inner: &DTypeDesc) -> DTypeDesc {
    match inner {
        DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
            base: *base,
            nullable: true,
        },
        DTypeDesc::Struct { fields, .. } => DTypeDesc::Struct {
            fields: fields.clone(),
            nullable: true,
        },
        DTypeDesc::List {
            inner: i,
            nullable: _,
        } => DTypeDesc::List {
            inner: i.clone(),
            nullable: true,
        },
    }
}

#[cfg(feature = "polars_engine")]
pub fn execute_explode_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    columns: Vec<String>,
) -> PyResult<(PyObject, PyObject)> {
    for c in columns.iter() {
        let dt = plan.schema.get(c).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "explode() unknown column '{}'.",
                c
            ))
        })?;
        if !matches!(dt, DTypeDesc::List { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "explode() column '{}' must have list dtype.",
                c
            )));
        }
    }

    let df = root_data_to_polars_df(py, &plan.root_schema, root_data)?;
    let mut lf = PolarsPlanRunner::apply_steps(df.lazy(), &plan.steps)?;
    lf = lf.explode(
        cols(columns.iter().map(|c| c.as_str())),
        ExplodeOptions {
            empty_as_null: false,
            keep_nulls: true,
        },
    );
    let out_df = lf.collect().map_err(polars_err)?;

    let mut out_schema: HashMap<String, DTypeDesc> = plan.schema.clone();
    for c in &columns {
        if let Some(DTypeDesc::List { inner, .. }) = out_schema.get(c) {
            let new_d = dtype_after_explode(inner);
            out_schema.insert(c.clone(), new_d);
        }
    }

    let out_dict = PyDict::new_bound(py);
    for (name, dtype) in out_schema.iter() {
        let s = out_df
            .column(name)
            .map_err(polars_err)?
            .as_materialized_series()
            .clone();
        let py_list = series_to_py_list(py, &s, dtype)?;
        out_dict.set_item(name, py_list)?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    Ok((out_dict.into_py(py), desc))
}

#[cfg(feature = "polars_engine")]
fn dtype_for_unnest_output_column(
    name: &str,
    plan_schema: &HashMap<String, DTypeDesc>,
    unnest_parents: &[String],
) -> Option<DTypeDesc> {
    for parent in unnest_parents {
        if let Some(DTypeDesc::Struct {
            fields,
            nullable: struct_nullable,
        }) = plan_schema.get(parent)
        {
            for (fname, fdt) in fields {
                let composed = format!("{parent}_{fname}");
                if composed == name {
                    let mut out = fdt.clone();
                    if *struct_nullable {
                        out = out.with_assigned_none_nullability();
                    }
                    return Some(out);
                }
            }
        }
    }
    plan_schema.get(name).cloned()
}

#[cfg(feature = "polars_engine")]
pub fn execute_unnest_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    columns: Vec<String>,
) -> PyResult<(PyObject, PyObject)> {
    for c in columns.iter() {
        let dt = plan.schema.get(c).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "unnest() unknown column '{}'.",
                c
            ))
        })?;
        if !matches!(dt, DTypeDesc::Struct { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "unnest() column '{}' must have struct dtype (nested model column).",
                c
            )));
        }
    }

    let df = root_data_to_polars_df(py, &plan.root_schema, root_data)?;
    let mut lf = PolarsPlanRunner::apply_steps(df.lazy(), &plan.steps)?;
    let sep: PlSmallStr = "_".into();
    lf = lf.unnest(cols(columns.iter().map(|s| s.as_str())), Some(sep));
    let out_df = lf.collect().map_err(polars_err)?;

    let mut out_schema: HashMap<String, DTypeDesc> = HashMap::new();
    for col_name in out_df.get_column_names() {
        let col_name_str = col_name.as_str();
        let out_desc =
            if let Some(d) = dtype_for_unnest_output_column(col_name_str, &plan.schema, &columns) {
                d
            } else {
                let s = out_df
                    .column(col_name)
                    .map_err(polars_err)?
                    .as_materialized_series();
                dtype_from_polars(s.dtype())?
            };
        out_schema.insert(col_name_str.to_string(), out_desc);
    }

    let out_dict = PyDict::new_bound(py);
    for (name, dtype) in out_schema.iter() {
        let s = out_df
            .column(name)
            .map_err(polars_err)?
            .as_materialized_series()
            .clone();
        let py_list = series_to_py_list(py, &s, dtype)?;
        out_dict.set_item(name, py_list)?;
    }
    let desc = schema_descriptors_as_py(py, &out_schema)?;
    Ok((out_dict.into_py(py), desc))
}
