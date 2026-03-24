use pyo3::prelude::*;
use pyo3::types::{
    PyAny, PyBool, PyBytes, PyDate, PyDateTime, PyDelta, PyFloat, PyInt, PyString, PyTime, PyTuple,
    PyType,
};

/// Polars precision for [`BaseType::Decimal`] columns and literals.
#[cfg_attr(not(feature = "polars_engine"), allow(dead_code))]
pub const DECIMAL_PRECISION: usize = 38;
/// Polars scale for [`BaseType::Decimal`] columns and literals (`10^{-scale}` units).
pub const DECIMAL_SCALE: usize = 9;

/// Supported base scalar types for the skeleton expression system.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BaseType {
    Int,
    Float,
    Bool,
    Str,
    Uuid,
    /// Fixed-point decimal: Polars `Decimal(38, 9)` (values scaled by `10^9`).
    Decimal,
    /// Python `enum.Enum` subclass (Polars Utf8; values use the enum member’s `.value` when string-coercible).
    Enum,
    DateTime,
    Date,
    Duration,
    /// Wall clock time (`datetime.time`); Polars `Time` (nanoseconds since midnight).
    Time,
    /// Raw bytes (`bytes`); Polars `Binary`.
    Binary,
}

/// DType descriptor for expression typing and nullability.
///
/// - Scalar with `base == None` is used for an "unknown base" nullable literal (`Literal(None)`).
///   The base must be inferred from the other operand during operator typing.
/// - `Struct` represents nested Pydantic models (recursive).
/// - `List` is a homogeneous list column (`list[T]` / `List[T]`).
/// - `Map` is `dict[str, V]` with a homogeneous value dtype (string keys only).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DTypeDesc {
    Scalar {
        base: Option<BaseType>,
        nullable: bool,
    },
    Struct {
        fields: Vec<(String, DTypeDesc)>,
        nullable: bool,
    },
    List {
        inner: Box<DTypeDesc>,
        nullable: bool,
    },
    Map {
        value: Box<DTypeDesc>,
        nullable: bool,
    },
}

impl DTypeDesc {
    pub fn unknown_nullable() -> Self {
        Self::Scalar {
            base: None,
            nullable: true,
        }
    }

    pub fn non_nullable(base: BaseType) -> Self {
        Self::Scalar {
            base: Some(base),
            nullable: false,
        }
    }

    #[cfg_attr(not(feature = "polars_engine"), allow(dead_code))]
    pub fn scalar_nullable(base: BaseType) -> Self {
        Self::Scalar {
            base: Some(base),
            nullable: true,
        }
    }

    /// True for scalar unknown-base nullable literal dtype.
    pub fn is_scalar_unknown_nullable(&self) -> bool {
        matches!(
            self,
            DTypeDesc::Scalar {
                base: None,
                nullable: true,
            }
        )
    }

    pub fn nullable_flag(&self) -> bool {
        match self {
            DTypeDesc::Scalar { nullable, .. }
            | DTypeDesc::Struct { nullable, .. }
            | DTypeDesc::List { nullable, .. }
            | DTypeDesc::Map { nullable, .. } => *nullable,
        }
    }

    /// After assigning `Literal(None)` to a typed column, the column is nullable.
    pub fn with_assigned_none_nullability(self) -> Self {
        match self {
            DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                base,
                nullable: true,
            },
            DTypeDesc::Struct { fields, .. } => DTypeDesc::Struct {
                fields,
                nullable: true,
            },
            DTypeDesc::List { inner, .. } => DTypeDesc::List {
                inner,
                nullable: true,
            },
            DTypeDesc::Map { value, .. } => DTypeDesc::Map {
                value,
                nullable: true,
            },
        }
    }

    #[inline]
    pub fn is_struct(&self) -> bool {
        matches!(self, DTypeDesc::Struct { .. })
    }

    #[inline]
    pub fn is_list(&self) -> bool {
        matches!(self, DTypeDesc::List { .. })
    }

    #[inline]
    pub fn is_map(&self) -> bool {
        matches!(self, DTypeDesc::Map { .. })
    }

    /// `Some(base_field)` for [`DTypeDesc::Scalar`]; [`None`] for composite dtypes.
    #[inline]
    pub fn as_scalar_base_field(&self) -> Option<Option<BaseType>> {
        match self {
            DTypeDesc::Scalar { base, .. } => Some(*base),
            DTypeDesc::Struct { .. } | DTypeDesc::List { .. } | DTypeDesc::Map { .. } => None,
        }
    }
}

pub fn dtype_structural_eq(a: &DTypeDesc, b: &DTypeDesc) -> bool {
    match (a, b) {
        (
            DTypeDesc::Scalar {
                base: ba,
                nullable: na,
            },
            DTypeDesc::Scalar {
                base: bb,
                nullable: nb,
            },
        ) => ba == bb && na == nb,
        (
            DTypeDesc::Struct {
                fields: fa,
                nullable: na,
            },
            DTypeDesc::Struct {
                fields: fb,
                nullable: nb,
            },
        ) => {
            na == nb
                && fa.len() == fb.len()
                && fa
                    .iter()
                    .zip(fb.iter())
                    .all(|((na, da), (nb, db))| na == nb && dtype_structural_eq(da, db))
        }
        (
            DTypeDesc::List {
                inner: ia,
                nullable: na,
            },
            DTypeDesc::List {
                inner: ib,
                nullable: nb,
            },
        ) => na == nb && dtype_structural_eq(ia, ib),
        (
            DTypeDesc::Map {
                value: va,
                nullable: na,
            },
            DTypeDesc::Map {
                value: vb,
                nullable: nb,
            },
        ) => na == nb && dtype_structural_eq(va, vb),
        _ => false,
    }
}

fn is_py_type(obj: &Bound<'_, PyAny>, expected: &str) -> bool {
    obj.getattr("__name__")
        .and_then(|v| v.extract::<String>())
        .map(|name| name == expected)
        .unwrap_or(false)
}

fn unwrap_annotated<'py>(
    py: Python<'py>,
    dtype_obj: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let typing = py.import_bound("typing")?;
    let annotated = typing.getattr("Annotated")?;
    let mut current = dtype_obj.clone();
    loop {
        let origin = typing.call_method1("get_origin", (&current,))?;
        if origin.is_none() {
            break;
        }
        if origin.eq(&annotated)? {
            let args = typing.call_method1("get_args", (&current,))?;
            let tuple = args.downcast::<PyTuple>()?;
            current = tuple.get_item(0)?;
        } else {
            break;
        }
    }
    Ok(current)
}

fn is_py_enum_type(py: Python<'_>, py_type: &Bound<'_, PyType>) -> PyResult<bool> {
    let enums = py.import_bound("enum")?;
    let enum_base = enums.getattr("Enum")?;
    let builtins = py.import_bound("builtins")?;
    let issubclass = builtins.getattr("issubclass")?;
    issubclass.call1((py_type, enum_base))?.extract::<bool>()
}

/// String used for Polars Utf8 cells and expression literals (`enum.Enum.value` or `str(...)`).
pub fn py_enum_to_wire_string(item: &Bound<'_, PyAny>) -> PyResult<String> {
    let val = item.getattr("value")?;
    if let Ok(s) = val.extract::<String>() {
        return Ok(s);
    }
    val.str()?.extract()
}

fn is_pydantic_model_class(py: Python<'_>, py_type: &Bound<'_, PyType>) -> PyResult<bool> {
    let pydantic = py.import_bound("pydantic")?;
    let base_model = pydantic.getattr("BaseModel")?;
    let builtins = py.import_bound("builtins")?;
    let issubclass = builtins.getattr("issubclass")?;
    issubclass.call1((py_type, base_model))?.extract::<bool>()
}

fn pydantic_model_to_struct_dtype(
    py: Python<'_>,
    model_cls: &Bound<'_, PyType>,
) -> PyResult<DTypeDesc> {
    let model_fields = model_cls.getattr("model_fields")?;
    let items = model_fields.call_method0("items")?;
    let mut fields: Vec<(String, DTypeDesc)> = Vec::new();
    for item in items.try_iter()? {
        let item = item?;
        let tuple = item.downcast::<PyTuple>()?;
        let name: String = tuple.get_item(0)?.extract()?;
        let field_info = tuple.get_item(1)?;
        let annotation = field_info.getattr("annotation")?;
        let inner = py_annotation_to_dtype(py, &annotation)?;
        fields.push((name, inner));
    }
    Ok(DTypeDesc::Struct {
        fields,
        nullable: false,
    })
}

/// If `dtype_obj` is `T | None` / `Optional[T]`, return inner `T` and `true`.
fn unwrap_optional_union<'py>(
    py: Python<'py>,
    dtype_obj: &Bound<'py, PyAny>,
) -> PyResult<(Bound<'py, PyAny>, bool)> {
    let typing = py.import_bound("typing")?;
    let get_origin = typing.getattr("get_origin")?;
    let get_args = typing.getattr("get_args")?;
    let origin = get_origin.call1((dtype_obj,))?;
    if origin.is_none() {
        return Ok((dtype_obj.clone(), false));
    }
    let args = get_args.call1((dtype_obj,))?;
    let tuple = args.downcast::<PyTuple>()?;
    let mut non_none: Vec<Bound<'_, PyAny>> = Vec::new();
    let mut saw_none = false;
    for i in 0..tuple.len() {
        let arg = tuple.get_item(i)?;
        if let Ok(t) = arg.downcast::<PyType>() {
            if is_py_type(t, "NoneType") {
                saw_none = true;
                continue;
            }
        }
        non_none.push(arg);
    }
    if saw_none && non_none.len() == 1 {
        return Ok((non_none[0].clone(), true));
    }
    Ok((dtype_obj.clone(), false))
}

pub fn py_annotation_to_dtype(py: Python<'_>, dtype_obj: &Bound<'_, PyAny>) -> PyResult<DTypeDesc> {
    let unannot = unwrap_annotated(py, dtype_obj)?;
    let (inner, opt_union) = unwrap_optional_union(py, &unannot)?;
    let mut dt = py_annotation_to_dtype_impl(py, &inner)?;
    if opt_union {
        dt = match dt {
            DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                base,
                nullable: true,
            },
            DTypeDesc::Struct { fields, .. } => DTypeDesc::Struct {
                fields,
                nullable: true,
            },
            DTypeDesc::List { inner, .. } => DTypeDesc::List {
                inner,
                nullable: true,
            },
            DTypeDesc::Map { value, .. } => DTypeDesc::Map {
                value,
                nullable: true,
            },
        };
    }
    Ok(dt)
}

fn py_annotation_to_dtype_impl(
    py: Python<'_>,
    dtype_obj: &Bound<'_, PyAny>,
) -> PyResult<DTypeDesc> {
    if let Ok(py_type) = dtype_obj.downcast::<PyType>() {
        if is_py_type(py_type, "bool") {
            return Ok(DTypeDesc::non_nullable(BaseType::Bool));
        }
        if is_py_type(py_type, "int") {
            return Ok(DTypeDesc::non_nullable(BaseType::Int));
        }
        if is_py_type(py_type, "float") {
            return Ok(DTypeDesc::non_nullable(BaseType::Float));
        }
        if is_py_type(py_type, "str") {
            return Ok(DTypeDesc::non_nullable(BaseType::Str));
        }
        if is_py_type(py_type, "UUID") {
            let module: String = py_type.getattr("__module__")?.extract()?;
            if module == "uuid" {
                return Ok(DTypeDesc::non_nullable(BaseType::Uuid));
            }
        }
        if is_py_type(py_type, "Decimal") {
            let module: String = py_type.getattr("__module__")?.extract()?;
            if module == "decimal" {
                return Ok(DTypeDesc::non_nullable(BaseType::Decimal));
            }
        }
        if is_py_type(py_type, "datetime") {
            return Ok(DTypeDesc::non_nullable(BaseType::DateTime));
        }
        if is_py_type(py_type, "date") {
            return Ok(DTypeDesc::non_nullable(BaseType::Date));
        }
        if is_py_type(py_type, "timedelta") {
            return Ok(DTypeDesc::non_nullable(BaseType::Duration));
        }
        if is_py_type(py_type, "time") {
            let module: String = py_type.getattr("__module__")?.extract()?;
            if module == "datetime" {
                return Ok(DTypeDesc::non_nullable(BaseType::Time));
            }
        }
        if is_py_type(py_type, "bytes") {
            return Ok(DTypeDesc::non_nullable(BaseType::Binary));
        }
        if is_py_type(py_type, "NoneType") {
            return Ok(DTypeDesc::unknown_nullable());
        }

        if is_py_enum_type(py, py_type)? {
            return Ok(DTypeDesc::non_nullable(BaseType::Enum));
        }

        if is_pydantic_model_class(py, py_type)? {
            return pydantic_model_to_struct_dtype(py, py_type);
        }
    }

    let typing = py.import_bound("typing")?;
    let builtins = py.import_bound("builtins")?;
    let origin = typing.call_method1("get_origin", (dtype_obj,))?;
    let tuple_binding = typing.call_method1("get_args", (dtype_obj,))?;
    let tuple = tuple_binding.downcast::<PyTuple>()?;

    if !origin.is_none() {
        let list_origin = builtins.getattr("list")?;
        let list_legacy = typing.getattr("List")?;
        if (origin.eq(&list_origin)? || origin.eq(&list_legacy)?) && tuple.len() == 1 {
            let inner = py_annotation_to_dtype_impl(py, &tuple.get_item(0)?)?;
            return Ok(DTypeDesc::List {
                inner: Box::new(inner),
                nullable: false,
            });
        }

        let dict_cls = builtins.getattr("dict")?;
        if origin.eq(&dict_cls)? && tuple.len() == 2 {
            let key_dt = py_annotation_to_dtype_impl(py, &tuple.get_item(0)?)?;
            let val_dt = py_annotation_to_dtype(py, &tuple.get_item(1)?)?;
            match key_dt {
                DTypeDesc::Scalar {
                    base: Some(BaseType::Str),
                    nullable: false,
                } => {
                    return Ok(DTypeDesc::Map {
                        value: Box::new(val_dt),
                        nullable: false,
                    });
                }
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Map columns must be typed as dict[str, V] (string keys only).",
                    ));
                }
            }
        }
    }

    if origin.is_none() {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Unsupported dtype annotation for pydantable.",
        ));
    }
    let mut seen_none = false;
    let mut seen_inner: Option<DTypeDesc> = None;

    for i in 0..tuple.len() {
        let arg = tuple.get_item(i)?;
        if let Ok(arg_type) = arg.downcast::<PyType>() {
            if is_py_type(arg_type, "bool") {
                seen_inner = Some(DTypeDesc::non_nullable(BaseType::Bool));
            } else if is_py_type(arg_type, "int") {
                seen_inner = Some(DTypeDesc::non_nullable(BaseType::Int));
            } else if is_py_type(arg_type, "float") {
                seen_inner = Some(DTypeDesc::non_nullable(BaseType::Float));
            } else if is_py_type(arg_type, "str") {
                seen_inner = Some(DTypeDesc::non_nullable(BaseType::Str));
            } else if is_py_type(arg_type, "UUID") {
                let module: String = arg_type.getattr("__module__")?.extract()?;
                if module == "uuid" {
                    seen_inner = Some(DTypeDesc::non_nullable(BaseType::Uuid));
                }
            } else if is_py_type(arg_type, "Decimal") {
                let module: String = arg_type.getattr("__module__")?.extract()?;
                if module == "decimal" {
                    seen_inner = Some(DTypeDesc::non_nullable(BaseType::Decimal));
                }
            } else if is_py_enum_type(py, arg_type)? {
                seen_inner = Some(DTypeDesc::non_nullable(BaseType::Enum));
            } else if is_py_type(arg_type, "datetime") {
                seen_inner = Some(DTypeDesc::non_nullable(BaseType::DateTime));
            } else if is_py_type(arg_type, "date") {
                seen_inner = Some(DTypeDesc::non_nullable(BaseType::Date));
            } else if is_py_type(arg_type, "timedelta") {
                seen_inner = Some(DTypeDesc::non_nullable(BaseType::Duration));
            } else if is_py_type(arg_type, "time") {
                let module: String = arg_type.getattr("__module__")?.extract()?;
                if module == "datetime" {
                    seen_inner = Some(DTypeDesc::non_nullable(BaseType::Time));
                }
            } else if is_py_type(arg_type, "bytes") {
                seen_inner = Some(DTypeDesc::non_nullable(BaseType::Binary));
            } else if is_py_type(arg_type, "NoneType") {
                seen_none = true;
            } else if is_pydantic_model_class(py, arg_type)? {
                seen_inner = Some(pydantic_model_to_struct_dtype(py, arg_type)?);
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Unsupported Optional/Union arg base type.",
                ));
            }
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Unsupported Optional/Union arg (expected a type object or known annotation).",
            ));
        }
    }

    if seen_none {
        if let Some(inner) = seen_inner {
            return Ok(match inner {
                DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                    base,
                    nullable: true,
                },
                DTypeDesc::Struct { fields, .. } => DTypeDesc::Struct {
                    fields,
                    nullable: true,
                },
                DTypeDesc::List { inner, .. } => DTypeDesc::List {
                    inner,
                    nullable: true,
                },
                DTypeDesc::Map { value, .. } => DTypeDesc::Map {
                    value,
                    nullable: true,
                },
            });
        }
        return Ok(DTypeDesc::unknown_nullable());
    }

    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "Unsupported dtype annotation for pydantable.",
    ))
}

pub fn py_value_to_dtype(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<DTypeDesc> {
    let _ = py;
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
    let builtins = py.import_bound("builtins")?;
    let isinstance = builtins.getattr("isinstance")?;
    let uuid_mod = py.import_bound("uuid")?;
    let uuid_cls = uuid_mod.getattr("UUID")?;
    if isinstance
        .call1((value, &uuid_cls))?
        .extract::<bool>()
        .unwrap_or(false)
    {
        return Ok(DTypeDesc::non_nullable(BaseType::Uuid));
    }
    let dec_mod = py.import_bound("decimal")?;
    let dec_cls = dec_mod.getattr("Decimal")?;
    if isinstance
        .call1((value, &dec_cls))?
        .extract::<bool>()
        .unwrap_or(false)
    {
        return Ok(DTypeDesc::non_nullable(BaseType::Decimal));
    }
    let enums = py.import_bound("enum")?;
    let enum_cls = enums.getattr("Enum")?;
    if isinstance
        .call1((value, &enum_cls))?
        .extract::<bool>()
        .unwrap_or(false)
    {
        return Ok(DTypeDesc::non_nullable(BaseType::Enum));
    }
    if value.downcast::<PyDateTime>().is_ok() {
        return Ok(DTypeDesc::non_nullable(BaseType::DateTime));
    }
    if value.downcast::<PyDate>().is_ok() {
        return Ok(DTypeDesc::non_nullable(BaseType::Date));
    }
    if value.downcast::<PyDelta>().is_ok() {
        return Ok(DTypeDesc::non_nullable(BaseType::Duration));
    }
    if value.downcast::<PyTime>().is_ok() {
        return Ok(DTypeDesc::non_nullable(BaseType::Time));
    }
    if value.downcast::<PyBytes>().is_ok() {
        return Ok(DTypeDesc::non_nullable(BaseType::Binary));
    }

    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "Unsupported literal value type (struct literals are not supported; use column references).",
    ))
}

fn create_model_for_struct_dtype(
    py: Python<'_>,
    dtype: &DTypeDesc,
    counter: &mut usize,
) -> PyResult<PyObject> {
    match dtype {
        DTypeDesc::Scalar {
            base: Some(b),
            nullable,
        } => {
            let t = scalar_base_to_py_type(py, *b)?;
            if *nullable {
                let typing = py.import_bound("typing")?;
                let opt = typing.getattr("Optional")?;
                Ok(opt.get_item(t)?.into_py(py))
            } else {
                Ok(t.into_py(py))
            }
        }
        DTypeDesc::Scalar { base: None, .. } => {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Cannot convert unknown-base dtype to a Python schema type.",
            ))
        }
        DTypeDesc::List { inner, nullable } => {
            let builtins = py.import_bound("builtins")?;
            let list_cls = builtins.getattr("list")?;
            let inner_ann = create_model_for_struct_dtype(py, inner, counter)?;
            let list_ann = list_cls.call_method1("__getitem__", (inner_ann,))?;
            if *nullable {
                let typing = py.import_bound("typing")?;
                let opt = typing.getattr("Optional")?;
                Ok(opt.get_item(list_ann)?.into_py(py))
            } else {
                Ok(list_ann.into_py(py))
            }
        }
        DTypeDesc::Struct { fields, nullable } => {
            let pydantic = py.import_bound("pydantic")?;
            let create_model = pydantic.getattr("create_model")?;
            let config_dict = pydantic.getattr("ConfigDict")?;
            let extra = pyo3::types::PyDict::new_bound(py);
            extra.set_item("extra", "forbid")?;
            let mc = config_dict.call1((extra,))?;

            let kwargs = pyo3::types::PyDict::new_bound(py);
            kwargs.set_item("model_config", &mc)?;
            for (fname, fd) in fields {
                let ann = create_model_for_struct_dtype(py, fd, counter)?;
                let ellipsis = pyo3::types::PyEllipsis::get(py);
                let tup = pyo3::types::PyTuple::new_bound(py, [ann, ellipsis.into_py(py)]);
                kwargs.set_item(fname, tup)?;
            }
            *counter += 1;
            let model_name = format!("PydantableStruct{}", counter);
            let model = create_model.call((model_name,), Some(&kwargs))?;
            if *nullable {
                let typing = py.import_bound("typing")?;
                let opt = typing.getattr("Optional")?;
                Ok(opt.get_item(model)?.into_py(py))
            } else {
                Ok(model.into_py(py))
            }
        }
        DTypeDesc::Map { value, nullable } => {
            let builtins = py.import_bound("builtins")?;
            let types_mod = py.import_bound("types")?;
            let generic_alias = types_mod.getattr("GenericAlias")?;
            let dict_cls = builtins.getattr("dict")?;
            let str_t = builtins.getattr("str")?;
            let val_ann = create_model_for_struct_dtype(py, value, counter)?;
            let val_b = val_ann.into_bound(py);
            let tup = pyo3::types::PyTuple::new_bound(py, [str_t, val_b]);
            let map_ann = generic_alias.call1((dict_cls, tup))?;
            if *nullable {
                let typing = py.import_bound("typing")?;
                let opt = typing.getattr("Optional")?;
                Ok(opt.get_item(map_ann)?.into_py(py))
            } else {
                Ok(map_ann.into_py(py))
            }
        }
    }
}

fn scalar_base_to_py_type(py: Python<'_>, base: BaseType) -> PyResult<PyObject> {
    let builtins = py.import_bound("builtins")?;
    Ok(match base {
        BaseType::Int => builtins.getattr("int")?.into_py(py),
        BaseType::Float => builtins.getattr("float")?.into_py(py),
        BaseType::Bool => builtins.getattr("bool")?.into_py(py),
        BaseType::Str => builtins.getattr("str")?.into_py(py),
        BaseType::Uuid => py.import_bound("uuid")?.getattr("UUID")?.into_py(py),
        BaseType::Decimal => py.import_bound("decimal")?.getattr("Decimal")?.into_py(py),
        BaseType::Enum => {
            let typing = py.import_bound("typing")?;
            typing.getattr("Any")?.into_py(py)
        }
        BaseType::DateTime => py
            .import_bound("datetime")?
            .getattr("datetime")?
            .into_py(py),
        BaseType::Date => py.import_bound("datetime")?.getattr("date")?.into_py(py),
        BaseType::Duration => py
            .import_bound("datetime")?
            .getattr("timedelta")?
            .into_py(py),
        BaseType::Time => py.import_bound("datetime")?.getattr("time")?.into_py(py),
        BaseType::Binary => builtins.getattr("bytes")?.into_py(py),
    })
}

pub fn dtype_to_python_type(py: Python<'_>, dtype: DTypeDesc) -> PyResult<PyObject> {
    let mut c = 0usize;
    create_model_for_struct_dtype(py, &dtype, &mut c)
}

pub fn dtype_to_descriptor_py(py: Python<'_>, dtype: &DTypeDesc) -> PyResult<PyObject> {
    let dict = pyo3::types::PyDict::new_bound(py);
    match dtype {
        DTypeDesc::Scalar { base, nullable } => {
            let base_s = match base {
                Some(BaseType::Int) => "int",
                Some(BaseType::Float) => "float",
                Some(BaseType::Bool) => "bool",
                Some(BaseType::Str) => "str",
                Some(BaseType::Uuid) => "uuid",
                Some(BaseType::Decimal) => "decimal",
                Some(BaseType::Enum) => "enum",
                Some(BaseType::DateTime) => "datetime",
                Some(BaseType::Date) => "date",
                Some(BaseType::Duration) => "duration",
                Some(BaseType::Time) => "time",
                Some(BaseType::Binary) => "binary",
                None => "unknown",
            };
            dict.set_item("base", base_s)?;
            dict.set_item("nullable", *nullable)?;
        }
        DTypeDesc::Struct { fields, nullable } => {
            dict.set_item("kind", "struct")?;
            dict.set_item("nullable", *nullable)?;
            let field_list = pyo3::types::PyList::empty_bound(py);
            for (name, fd) in fields {
                let fe = pyo3::types::PyDict::new_bound(py);
                fe.set_item("name", name)?;
                fe.set_item("dtype", dtype_to_descriptor_py(py, fd)?)?;
                field_list.append(fe)?;
            }
            dict.set_item("fields", field_list)?;
        }
        DTypeDesc::List { inner, nullable } => {
            dict.set_item("kind", "list")?;
            dict.set_item("nullable", *nullable)?;
            dict.set_item("inner", dtype_to_descriptor_py(py, inner)?)?;
        }
        DTypeDesc::Map { value, nullable } => {
            dict.set_item("kind", "map")?;
            dict.set_item("nullable", *nullable)?;
            dict.set_item("value", dtype_to_descriptor_py(py, value)?)?;
        }
    }
    Ok(dict.into_py(py))
}

/// Convert a Python `decimal.Decimal` to Polars `Decimal(`[`DECIMAL_PRECISION`], [`DECIMAL_SCALE`]`)` unscaled `i128`.
pub fn py_decimal_to_scaled_i128(item: &Bound<'_, PyAny>) -> PyResult<i128> {
    let py = item.py();
    let dec_mod = py.import_bound("decimal")?;
    let dec_cls = dec_mod.getattr("Decimal")?;
    let builtins = py.import_bound("builtins")?;
    let isinstance = builtins.getattr("isinstance")?;
    if !isinstance.call1((item, &dec_cls))?.extract::<bool>()? {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Expected decimal.Decimal.",
        ));
    }
    let pow10 = 10_i128.pow(DECIMAL_SCALE as u32);
    let int_cls = builtins.getattr("int")?;
    let factor = int_cls.call1((pow10.to_string(),))?;
    let prod = item.call_method1("__mul__", (factor,))?;
    let integral = prod.call_method0("to_integral_value")?;
    let s: String = integral.call_method0("__str__")?.extract()?;
    s.parse::<i128>().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "decimal value out of range for fixed-precision engine storage",
        )
    })
}

/// Inverse of [`py_decimal_to_scaled_i128`].
pub fn scaled_i128_to_py_decimal(py: Python<'_>, v: i128) -> PyResult<PyObject> {
    let dec_mod = py.import_bound("decimal")?;
    let dec_cls = dec_mod.getattr("Decimal")?;
    let builtins = py.import_bound("builtins")?;
    let int_cls = builtins.getattr("int")?;
    let numer = dec_cls.call1((int_cls.call1((v.to_string(),))?,))?;
    let denom_str = format!("1{}", "0".repeat(DECIMAL_SCALE));
    let denom = dec_cls.call1((denom_str.as_str(),))?;
    let out = numer.call_method1("__truediv__", (denom,))?;
    Ok(out.into_py(py))
}

/// Fixed-point string for a scaled `i128` (`Decimal(`[`DECIMAL_PRECISION`], [`DECIMAL_SCALE`]`)` cell).
#[cfg(not(feature = "polars_engine"))]
pub fn scaled_i128_to_decimal_string(v: i128) -> String {
    let sign = if v < 0 { "-" } else { "" };
    let av = v.unsigned_abs();
    let p = 10_u128.pow(DECIMAL_SCALE as u32);
    let whole = av / p;
    let frac = av % p;
    format!("{}{}.{:0>width$}", sign, whole, frac, width = DECIMAL_SCALE)
}
