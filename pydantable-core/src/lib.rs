#![allow(deprecated)]

mod dtype;
mod expr;
#[cfg(feature = "polars_engine")]
mod io_polars;
mod plan;
mod python_api;

use pyo3::prelude::*;
use pyo3::types::PyModule;

/// PyO3 extension module `pydantable._core` (built via maturin).
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    python_api::register(m)
}
