//! Error constructors for row-wise `ExprNode` evaluation.

use pyo3::prelude::*;

pub(super) fn unknown_column(name: &str) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
        "Unknown column '{}' during expression evaluation.",
        name
    ))
}

pub(super) fn type_error(msg: &str) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyTypeError, _>(msg.to_string())
}
