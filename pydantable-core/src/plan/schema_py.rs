//! Python-facing schema helpers for plans.

use std::collections::HashMap;

use pyo3::prelude::*;

use crate::dtype::{dtype_to_descriptor_py, dtype_to_python_type, DTypeDesc};

pub fn schema_fields_as_py(
    py: Python<'_>,
    schema: &HashMap<String, DTypeDesc>,
) -> PyResult<PyObject> {
    let dict = pyo3::types::PyDict::new_bound(py);
    for (name, dtype) in schema.iter() {
        let t = dtype_to_python_type(py, *dtype)?;
        dict.set_item(name, t)?;
    }
    Ok(dict.into_py(py))
}

pub fn schema_descriptors_as_py(
    py: Python<'_>,
    schema: &HashMap<String, DTypeDesc>,
) -> PyResult<PyObject> {
    let dict = pyo3::types::PyDict::new_bound(py);
    for (name, dtype) in schema.iter() {
        let d = dtype_to_descriptor_py(py, *dtype)?;
        dict.set_item(name, d)?;
    }
    Ok(dict.into_py(py))
}
