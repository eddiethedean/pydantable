//! `PyExpr` and `PyPlan` PyO3 classes.

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::dtype::dtype_to_python_type;
use crate::expr::{exprnode_to_serializable, ExprNode};
use crate::plan::{
    execute_plan as execute_plan_inner, planinner_to_serializable, schema_descriptors_as_py,
    schema_fields_as_py, PlanInner,
};

#[pyclass]
#[derive(Clone)]
pub struct PyExpr {
    pub(crate) node: ExprNode,
}

#[pymethods]
impl PyExpr {
    #[getter]
    fn dtype(&self, py: Python<'_>) -> PyResult<PyObject> {
        dtype_to_python_type(py, self.node.dtype())
    }

    fn referenced_columns(&self) -> Vec<String> {
        self.node.referenced_columns().into_iter().collect()
    }

    fn to_serializable(&self, py: Python<'_>) -> PyResult<PyObject> {
        exprnode_to_serializable(py, &self.node)
    }
}

/// Lazy on-disk table root (Parquet, CSV, NDJSON, IPC); avoids materializing to Python lists.
#[pyclass(module = "pydantable._core", name = "ScanFileRoot")]
#[derive(Clone)]
pub struct ScanFileRoot {
    #[pyo3(get)]
    pub path: String,
    /// ``parquet``, ``csv``, ``ndjson``, or ``ipc``.
    #[pyo3(get)]
    pub format: String,
    #[pyo3(get)]
    pub columns: Option<Vec<String>>,
}

#[pymethods]
impl ScanFileRoot {
    #[new]
    #[pyo3(signature = (path, format, columns=None))]
    fn new(path: String, format: String, columns: Option<Vec<String>>) -> Self {
        Self {
            path,
            format,
            columns,
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyPlan {
    pub(crate) inner: PlanInner,
}

#[pymethods]
impl PyPlan {
    fn schema_fields(&self, py: Python<'_>) -> PyResult<PyObject> {
        schema_fields_as_py(py, &self.inner.schema)
    }

    fn schema_descriptors(&self, py: Python<'_>) -> PyResult<PyObject> {
        schema_descriptors_as_py(py, &self.inner.schema)
    }

    fn to_serializable(&self, py: Python<'_>) -> PyResult<PyObject> {
        planinner_to_serializable(py, &self.inner)
    }

    #[pyo3(signature = (root_data, as_python_lists=false, streaming=false))]
    fn execute(
        &self,
        py: Python<'_>,
        root_data: &Bound<'_, PyAny>,
        as_python_lists: bool,
        streaming: bool,
    ) -> PyResult<PyObject> {
        execute_plan_inner(py, &self.inner, root_data, as_python_lists, streaming)
    }
}

pub(super) fn register_classes(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyExpr>()?;
    m.add_class::<ScanFileRoot>()?;
    m.add_class::<PyPlan>()?;
    Ok(())
}
