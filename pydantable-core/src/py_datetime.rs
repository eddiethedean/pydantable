//! Shared helpers to build Python `datetime`, `date`, `time`, and `timedelta` values from scalar
//! encodings used across expression literals and Polars materialization.

use pyo3::prelude::*;

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
