//! PyO3 surface: `PyExpr`, `PyPlan`, and exported `#[pyfunction]`s.

#![cfg_attr(not(feature = "polars_engine"), allow(unused_variables))]

mod exec_fns;
mod expr_fns;
mod io_fns;
mod plan_fns;
mod types;

use pyo3::prelude::*;
use pyo3::types::PyModule;

/// Register all classes and functions on the `pydantable._core` extension module.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    types::register_classes(m)?;
    exec_fns::register_functions(m)?;
    io_fns::register_functions(m)?;
    expr_fns::register_functions(m)?;
    plan_fns::register_functions(m)?;
    Ok(())
}
