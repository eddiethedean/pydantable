use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyFloat, PyInt, PyString, PyType};

/// Supported base scalar types for the skeleton expression system.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BaseType {
    Int,
    Float,
    Bool,
    Str,
}

/// DType descriptor for expression typing and nullability.
///
/// - `base == None` is used for an "unknown base" nullable literal (`Literal(None)`).
///   The base must be inferred from the other operand during operator typing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DTypeDesc {
    pub base: Option<BaseType>,
    pub nullable: bool,
}

impl DTypeDesc {
    pub fn unknown_nullable() -> Self {
        Self {
            base: None,
            nullable: true,
        }
    }

    pub fn non_nullable(base: BaseType) -> Self {
        Self {
            base: Some(base),
            nullable: false,
        }
    }

    pub fn nullable(base: BaseType) -> Self {
        Self {
            base: Some(base),
            nullable: true,
        }
    }
}

fn is_py_type(obj: &Bound<'_, PyAny>, expected: &str) -> bool {
    // Compare the Python type's `__name__` to avoid identity issues.
    // This works fine for skeleton-level supported types.
    obj.getattr("__name__")
        .and_then(|v| v.extract::<String>())
        .map(|name| name == expected)
        .unwrap_or(false)
}

pub fn py_annotation_to_dtype(py: Python<'_>, dtype_obj: &Bound<'_, PyAny>) -> PyResult<DTypeDesc> {
    // Handle direct supported scalar annotations.
    //
    // Pydantic schema annotations typically come through as actual Python classes
    // like `int`, `float`, `bool`, `str`, or as `typing.Optional[T]` which is a
    // Union[T, NoneType].
    if let Ok(py_type) = dtype_obj.downcast::<PyType>() {
        // Special-case: `bool` must be detected before `int` because `bool` is a
        // subclass of `int` in Python.
        if is_py_type(&py_type, "bool") {
            return Ok(DTypeDesc::non_nullable(BaseType::Bool));
        }
        if is_py_type(&py_type, "int") {
            return Ok(DTypeDesc::non_nullable(BaseType::Int));
        }
        if is_py_type(&py_type, "float") {
            return Ok(DTypeDesc::non_nullable(BaseType::Float));
        }
        if is_py_type(&py_type, "str") {
            return Ok(DTypeDesc::non_nullable(BaseType::Str));
        }

        // `type(None)` comes through as a Python type object named "NoneType".
        if is_py_type(&py_type, "NoneType") {
            return Ok(DTypeDesc::unknown_nullable());
        }
    }

    // Optional / Union[T, None] handling via Python's typing module.
    let typing = py.import("typing")?;
    let origin = typing.call_method1("get_origin", (dtype_obj,))?;
    if origin.is_none() {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Unsupported dtype annotation for skeleton.".to_string(),
        ));
    }

    {
        // Only support the Optional[T] / Union[T, None] form in the skeleton.
        let union_args = typing.call_method1("get_args", (dtype_obj,))?;
        let mut seen_none = false;
        let mut seen_base: Option<BaseType> = None;

        for arg in union_args.iter()? {
            let arg = arg?;
            if let Ok(arg_type) = arg.downcast::<PyType>() {
                if is_py_type(&arg_type, "bool") {
                    seen_base = Some(BaseType::Bool);
                } else if is_py_type(&arg_type, "int") {
                    seen_base = Some(BaseType::Int);
                } else if is_py_type(&arg_type, "float") {
                    seen_base = Some(BaseType::Float);
                } else if is_py_type(&arg_type, "str") {
                    seen_base = Some(BaseType::Str);
                } else if is_py_type(&arg_type, "NoneType") {
                    seen_none = true;
                } else {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Unsupported Optional/Union arg base type.".to_string(),
                    ));
                }
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Unsupported Optional/Union arg (expected a type object).".to_string(),
                ));
            }
        }

        if seen_none {
            if let Some(base) = seen_base {
                return Ok(DTypeDesc::nullable(base));
            }
            return Ok(DTypeDesc::unknown_nullable());
        }
    }

    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "Unsupported dtype annotation for skeleton.".to_string(),
    ))
}

pub fn py_value_to_dtype(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<DTypeDesc> {
    let _ = py; // reserved for future coercions
    if value.is_none() {
        return Ok(DTypeDesc::unknown_nullable());
    }
    if value.downcast::<PyBool>().is_ok() {
        return Ok(DTypeDesc::non_nullable(BaseType::Bool));
    }
    if value.downcast::<PyInt>().is_ok() {
        return Ok(DTypeDesc::non_nullable(BaseType::Int));
    }
    if value.downcast::<PyFloat>().is_ok() {
        return Ok(DTypeDesc::non_nullable(BaseType::Float));
    }
    if value.downcast::<PyString>().is_ok() {
        return Ok(DTypeDesc::non_nullable(BaseType::Str));
    }

    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
        "Unsupported literal value type for skeleton."
    )))
}

pub fn dtype_to_python_type(py: Python<'_>, dtype: DTypeDesc) -> PyResult<PyObject> {
    let typing = py.import_bound("typing")?;
    let builtins = py.import_bound("builtins")?;

    let base_type_obj = match dtype.base {
        Some(BaseType::Int) => builtins.getattr("int")?,
        Some(BaseType::Float) => builtins.getattr("float")?,
        Some(BaseType::Bool) => builtins.getattr("bool")?,
        Some(BaseType::Str) => builtins.getattr("str")?,
        None => {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Cannot convert unknown-base nullable dtype to a Python schema type.",
            ))
        }
    };

    if dtype.nullable {
        let optional = typing.getattr("Optional")?;
        // `typing.Optional[T]` uses `__getitem__`.
        let t = optional.get_item(base_type_obj)?;
        Ok(t.into_py(py))
    } else {
        Ok(base_type_obj.into_py(py))
    }
}

pub fn dtype_to_descriptor_py(py: Python<'_>, dtype: DTypeDesc) -> PyResult<PyObject> {
    let dict = pyo3::types::PyDict::new_bound(py);
    let base = match dtype.base {
        Some(BaseType::Int) => "int",
        Some(BaseType::Float) => "float",
        Some(BaseType::Bool) => "bool",
        Some(BaseType::Str) => "str",
        None => "unknown",
    };
    dict.set_item("base", base)?;
    dict.set_item("nullable", dtype.nullable)?;
    Ok(dict.into_py(py))
}
