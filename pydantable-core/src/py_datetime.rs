//! Shared helpers to build Python `datetime`, `date`, `time`, and `timedelta` values from scalar
//! encodings used across expression literals and Polars materialization.
//!
//! Type checks use ``isinstance`` against ``datetime`` module classes so the extension can be built
//! with PyO3's **abi3** (limited API), where ``PyDateTime`` / ``PyDate`` / … are unavailable.

use pyo3::prelude::*;
use pyo3::types::PyAny;

/// ``isinstance(obj, datetime.datetime)`` (abi3-safe).
pub(crate) fn is_py_datetime(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<bool> {
    let dt_mod = py.import("datetime")?;
    let cls = dt_mod.getattr("datetime")?;
    obj.is_instance(&cls)
}

/// Plain ``datetime.date`` instance, excluding ``datetime.datetime`` subclasses.
pub(crate) fn is_py_date_only(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<bool> {
    let dt_mod = py.import("datetime")?;
    let date_cls = dt_mod.getattr("date")?;
    let datetime_cls = dt_mod.getattr("datetime")?;
    Ok(obj.is_instance(&date_cls)? && !obj.is_instance(&datetime_cls)?)
}

/// ``isinstance(obj, datetime.timedelta)``.
pub(crate) fn is_py_timedelta(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<bool> {
    let dt_mod = py.import("datetime")?;
    let cls = dt_mod.getattr("timedelta")?;
    obj.is_instance(&cls)
}

/// ``isinstance(obj, datetime.time)``.
pub(crate) fn is_py_time(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<bool> {
    let dt_mod = py.import("datetime")?;
    let cls = dt_mod.getattr("time")?;
    obj.is_instance(&cls)
}

pub(crate) fn micros_to_py_datetime(py: Python<'_>, micros: i64) -> PyResult<PyObject> {
    let dt_mod = py.import("datetime")?;
    let dt = dt_mod.getattr("datetime")?;
    Ok(dt
        .call_method1("fromtimestamp", (micros as f64 / 1_000_000.0,))?
        .unbind())
}

pub(crate) fn days_to_py_date(py: Python<'_>, days: i32) -> PyResult<PyObject> {
    let dt_mod = py.import("datetime")?;
    let date = dt_mod.getattr("date")?;
    Ok(date
        .call_method1("fromordinal", (days + 719_163,))?
        .unbind())
}

pub(crate) fn micros_to_py_timedelta(py: Python<'_>, micros: i64) -> PyResult<PyObject> {
    let dt_mod = py.import("datetime")?;
    let td = dt_mod.getattr("timedelta")?;
    Ok(td.call1((0, 0, micros))?.unbind())
}

pub(crate) fn nanos_to_py_time(py: Python<'_>, ns: i64) -> PyResult<PyObject> {
    let dt_mod = py.import("datetime")?;
    let time_cls = dt_mod.getattr("time")?;
    let nanos = ns.rem_euclid(86_400 * 1_000_000_000);
    let secs = nanos / 1_000_000_000;
    let nsub = nanos % 1_000_000_000;
    let micro = (nsub / 1000) as i32;
    let h = (secs / 3600) as i32;
    let m = ((secs % 3600) / 60) as i32;
    let s = (secs % 60) as i32;
    Ok(time_cls.call1((h, m, s, micro))?.unbind())
}
