use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyBytes, PyFloat, PyInt, PyString, PyTuple, PyType};

use crate::py_datetime::{is_py_date_only, is_py_datetime, is_py_time, is_py_timedelta};
use pyo3::IntoPyObjectExt;

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
    /// `ipaddress.IPv4Address`; Polars Utf8 (canonical string form).
    Ipv4,
    /// `ipaddress.IPv6Address`; Polars Utf8 (canonical compressed form).
    Ipv6,
    /// Well-Known Binary geometry (`pydantable.types.WKB`); Polars `Binary`.
    Wkb,
}

/// Allowed values for `typing.Literal[...]` columns (homogeneous).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LiteralSet {
    Str(Vec<String>),
    Int(Vec<i64>),
    Bool(Vec<bool>),
}

/// Drop literal constraints when widening dtypes (e.g. after arithmetic).
pub fn widen_scalar_drop_literals(d: DTypeDesc) -> DTypeDesc {
    match d {
        DTypeDesc::Scalar {
            base,
            nullable,
            literals: _,
        } => DTypeDesc::Scalar {
            base,
            nullable,
            literals: None,
        },
        other => other,
    }
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
        /// When set, column is a `typing.Literal[...]` (homogeneous); `base` is `Str` / `Int` / `Bool`.
        literals: Option<LiteralSet>,
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
            literals: None,
        }
    }

    pub fn non_nullable(base: BaseType) -> Self {
        Self::Scalar {
            base: Some(base),
            nullable: false,
            literals: None,
        }
    }

    #[cfg_attr(not(feature = "polars_engine"), allow(dead_code))]
    pub fn scalar_nullable(base: BaseType) -> Self {
        Self::Scalar {
            base: Some(base),
            nullable: true,
            literals: None,
        }
    }

    pub fn non_nullable_literal(base: BaseType, literals: LiteralSet) -> Self {
        Self::Scalar {
            base: Some(base),
            nullable: false,
            literals: Some(literals),
        }
    }

    #[inline]
    pub fn literals(&self) -> Option<&LiteralSet> {
        match self {
            DTypeDesc::Scalar { literals, .. } => literals.as_ref(),
            _ => None,
        }
    }

    /// True for scalar unknown-base nullable literal dtype.
    pub fn is_scalar_unknown_nullable(&self) -> bool {
        matches!(
            self,
            DTypeDesc::Scalar {
                base: None,
                nullable: true,
                literals: None,
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
            DTypeDesc::Scalar { base, literals, .. } => DTypeDesc::Scalar {
                base,
                nullable: true,
                literals,
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
                literals: la,
            },
            DTypeDesc::Scalar {
                base: bb,
                nullable: nb,
                literals: lb,
            },
        ) => ba == bb && na == nb && la == lb,
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
    let typing = py.import("typing")?;
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

fn is_wkb_type(py_type: &Bound<'_, PyType>) -> PyResult<bool> {
    let module: String = py_type.getattr("__module__")?.extract()?;
    Ok(module == "pydantable.types" && is_py_type(py_type, "WKB"))
}

fn py_literal_values_to_dtype(py: Python<'_>, tuple: &Bound<'_, PyTuple>) -> PyResult<DTypeDesc> {
    use std::collections::BTreeSet;
    let mut strings: BTreeSet<String> = BTreeSet::new();
    let mut ints: BTreeSet<i64> = BTreeSet::new();
    let mut bools: BTreeSet<bool> = BTreeSet::new();
    let builtins = py.import("builtins")?;

    for i in 0..tuple.len() {
        let arg = tuple.get_item(i)?;
        if arg.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Use Optional[Literal[...]] / Literal[...] | None instead of Literal[..., None] on pydantable columns.",
            ));
        }
        if arg.downcast::<PyBool>().is_ok() {
            bools.insert(arg.extract::<bool>()?);
            continue;
        }
        if let Ok(s) = arg.extract::<String>() {
            strings.insert(s);
            continue;
        }
        if let Ok(py_int) = arg.downcast::<pyo3::types::PyInt>() {
            ints.insert(py_int.extract::<i64>()?);
            continue;
        }
        let isinstance = builtins.getattr("isinstance")?;
        let float_cls = builtins.getattr("float")?;
        if isinstance.call1((&arg, float_cls))?.extract::<bool>()? {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Literal[float] column dtypes are not supported (use float columns without Literal).",
            ));
        }
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "typing.Literal[...] columns must use homogeneous str, int, or bool values only.",
        ));
    }

    let n_kinds =
        (!strings.is_empty() as u8) + (!ints.is_empty() as u8) + (!bools.is_empty() as u8);
    if n_kinds != 1 {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "typing.Literal[...] column dtypes must be all-str, all-int, or all-bool (not mixed).",
        ));
    }
    if !strings.is_empty() {
        return Ok(DTypeDesc::non_nullable_literal(
            BaseType::Str,
            LiteralSet::Str(strings.into_iter().collect()),
        ));
    }
    if !ints.is_empty() {
        return Ok(DTypeDesc::non_nullable_literal(
            BaseType::Int,
            LiteralSet::Int(ints.into_iter().collect()),
        ));
    }
    Ok(DTypeDesc::non_nullable_literal(
        BaseType::Bool,
        LiteralSet::Bool(bools.into_iter().collect()),
    ))
}

fn is_py_enum_type(py: Python<'_>, py_type: &Bound<'_, PyType>) -> PyResult<bool> {
    let enums = py.import("enum")?;
    let enum_base = enums.getattr("Enum")?;
    let builtins = py.import("builtins")?;
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
    let pydantic = py.import("pydantic")?;
    let base_model = pydantic.getattr("BaseModel")?;
    let builtins = py.import("builtins")?;
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
    let typing = py.import("typing")?;
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
            DTypeDesc::Scalar { base, literals, .. } => DTypeDesc::Scalar {
                base,
                nullable: true,
                literals,
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
        if is_wkb_type(py_type)? {
            return Ok(DTypeDesc::non_nullable(BaseType::Wkb));
        }
        if is_py_type(py_type, "bytes") {
            return Ok(DTypeDesc::non_nullable(BaseType::Binary));
        }
        if is_py_type(py_type, "IPv4Address") {
            let module: String = py_type.getattr("__module__")?.extract()?;
            if module == "ipaddress" {
                return Ok(DTypeDesc::non_nullable(BaseType::Ipv4));
            }
        }
        if is_py_type(py_type, "IPv6Address") {
            let module: String = py_type.getattr("__module__")?.extract()?;
            if module == "ipaddress" {
                return Ok(DTypeDesc::non_nullable(BaseType::Ipv6));
            }
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

    let typing = py.import("typing")?;
    let builtins = py.import("builtins")?;
    let origin = typing.call_method1("get_origin", (dtype_obj,))?;
    let tuple_binding = typing.call_method1("get_args", (dtype_obj,))?;
    let tuple = tuple_binding.downcast::<PyTuple>()?;

    if !origin.is_none() {
        let literal_cls = typing.getattr("Literal").ok();
        let literal_cls = match literal_cls {
            Some(c) => c,
            None => py.import("typing_extensions")?.getattr("Literal")?,
        };
        if origin.eq(&literal_cls)? {
            if tuple.is_empty() {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "typing.Literal[...] requires at least one value.",
                ));
            }
            return py_literal_values_to_dtype(py, tuple);
        }

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
                    ..
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
            } else if is_wkb_type(arg_type)? {
                seen_inner = Some(DTypeDesc::non_nullable(BaseType::Wkb));
            } else if is_py_type(arg_type, "bytes") {
                seen_inner = Some(DTypeDesc::non_nullable(BaseType::Binary));
            } else if is_py_type(arg_type, "IPv4Address") {
                let module: String = arg_type.getattr("__module__")?.extract()?;
                if module == "ipaddress" {
                    seen_inner = Some(DTypeDesc::non_nullable(BaseType::Ipv4));
                }
            } else if is_py_type(arg_type, "IPv6Address") {
                let module: String = arg_type.getattr("__module__")?.extract()?;
                if module == "ipaddress" {
                    seen_inner = Some(DTypeDesc::non_nullable(BaseType::Ipv6));
                }
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
                DTypeDesc::Scalar { base, literals, .. } => DTypeDesc::Scalar {
                    base,
                    nullable: true,
                    literals,
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
    let builtins = py.import("builtins")?;
    let isinstance = builtins.getattr("isinstance")?;
    let uuid_mod = py.import("uuid")?;
    let uuid_cls = uuid_mod.getattr("UUID")?;
    if isinstance
        .call1((value, &uuid_cls))?
        .extract::<bool>()
        .unwrap_or(false)
    {
        return Ok(DTypeDesc::non_nullable(BaseType::Uuid));
    }
    let dec_mod = py.import("decimal")?;
    let dec_cls = dec_mod.getattr("Decimal")?;
    if isinstance
        .call1((value, &dec_cls))?
        .extract::<bool>()
        .unwrap_or(false)
    {
        return Ok(DTypeDesc::non_nullable(BaseType::Decimal));
    }
    let enums = py.import("enum")?;
    let enum_cls = enums.getattr("Enum")?;
    if isinstance
        .call1((value, &enum_cls))?
        .extract::<bool>()
        .unwrap_or(false)
    {
        return Ok(DTypeDesc::non_nullable(BaseType::Enum));
    }
    if is_py_datetime(py, value)? {
        return Ok(DTypeDesc::non_nullable(BaseType::DateTime));
    }
    if is_py_date_only(py, value)? {
        return Ok(DTypeDesc::non_nullable(BaseType::Date));
    }
    if is_py_timedelta(py, value)? {
        return Ok(DTypeDesc::non_nullable(BaseType::Duration));
    }
    if is_py_time(py, value)? {
        return Ok(DTypeDesc::non_nullable(BaseType::Time));
    }
    let ip_mod = py.import("ipaddress")?;
    let v4_cls = ip_mod.getattr("IPv4Address")?;
    if isinstance
        .call1((value, &v4_cls))?
        .extract::<bool>()
        .unwrap_or(false)
    {
        return Ok(DTypeDesc::non_nullable(BaseType::Ipv4));
    }
    let v6_cls = ip_mod.getattr("IPv6Address")?;
    if isinstance
        .call1((value, &v6_cls))?
        .extract::<bool>()
        .unwrap_or(false)
    {
        return Ok(DTypeDesc::non_nullable(BaseType::Ipv6));
    }
    let wkb_cls = py.import("pydantable.types").and_then(|m| m.getattr("WKB"));
    if let Ok(cls) = wkb_cls {
        if isinstance
            .call1((value, &cls))?
            .extract::<bool>()
            .unwrap_or(false)
        {
            return Ok(DTypeDesc::non_nullable(BaseType::Wkb));
        }
    }
    if value.downcast::<PyBytes>().is_ok() {
        return Ok(DTypeDesc::non_nullable(BaseType::Binary));
    }

    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "Unsupported literal value type (struct literals are not supported; use column references).",
    ))
}

fn literal_set_to_py_typing_literal(
    py: Python<'_>,
    ls: &LiteralSet,
    base: BaseType,
) -> PyResult<PyObject> {
    let typing = py.import("typing")?;
    let literal = typing.getattr("Literal")?;
    let tup = match (ls, base) {
        (LiteralSet::Str(v), BaseType::Str) => {
            let mut items = Vec::with_capacity(v.len());
            for s in v {
                items.push(s.clone().into_py_any(py)?);
            }
            PyTuple::new(py, items)?.unbind()
        }
        (LiteralSet::Int(v), BaseType::Int) => {
            let mut items = Vec::with_capacity(v.len());
            for i in v {
                items.push((*i).into_py_any(py)?);
            }
            PyTuple::new(py, items)?.unbind()
        }
        (LiteralSet::Bool(v), BaseType::Bool) => {
            let mut items = Vec::with_capacity(v.len());
            for b in v {
                items.push((*b).into_py_any(py)?);
            }
            PyTuple::new(py, items)?.unbind()
        }
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Internal error: literal set does not match scalar base.",
            ));
        }
    };
    let tup_b = tup.bind(py);
    Ok(literal.call_method1("__getitem__", (tup_b,))?.unbind())
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
            literals,
        } => {
            let t = if let Some(ls) = literals {
                literal_set_to_py_typing_literal(py, ls, *b)?
            } else {
                scalar_base_to_py_type(py, *b)?
            };
            if *nullable {
                let typing = py.import("typing")?;
                let opt = typing.getattr("Optional")?;
                Ok(opt.get_item(t.bind(py))?.unbind())
            } else {
                Ok(t)
            }
        }
        DTypeDesc::Scalar { base: None, .. } => {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Cannot convert unknown-base dtype to a Python schema type.",
            ))
        }
        DTypeDesc::List { inner, nullable } => {
            let builtins = py.import("builtins")?;
            let list_cls = builtins.getattr("list")?;
            let inner_ann = create_model_for_struct_dtype(py, inner, counter)?;
            let list_ann = list_cls.call_method1("__getitem__", (inner_ann,))?;
            if *nullable {
                let typing = py.import("typing")?;
                let opt = typing.getattr("Optional")?;
                Ok(opt.get_item(list_ann)?.unbind())
            } else {
                Ok(list_ann.unbind())
            }
        }
        DTypeDesc::Struct { fields, nullable } => {
            let pydantic = py.import("pydantic")?;
            let create_model = pydantic.getattr("create_model")?;
            let config_dict = pydantic.getattr("ConfigDict")?;
            let extra = pyo3::types::PyDict::new(py);
            extra.set_item("extra", "forbid")?;
            let mc = config_dict.call1((extra,))?;

            let kwargs = pyo3::types::PyDict::new(py);
            kwargs.set_item("model_config", &mc)?;
            for (fname, fd) in fields {
                let ann = create_model_for_struct_dtype(py, fd, counter)?;
                let ellipsis = pyo3::types::PyEllipsis::get(py);
                let tup = pyo3::types::PyTuple::new(py, [ann.bind(py), &ellipsis])?;
                kwargs.set_item(fname, tup)?;
            }
            *counter += 1;
            let model_name = format!("PydantableStruct{}", counter);
            let model = create_model.call((model_name,), Some(&kwargs))?;
            if *nullable {
                let typing = py.import("typing")?;
                let opt = typing.getattr("Optional")?;
                Ok(opt.get_item(model)?.unbind())
            } else {
                Ok(model.unbind())
            }
        }
        DTypeDesc::Map { value, nullable } => {
            let builtins = py.import("builtins")?;
            let types_mod = py.import("types")?;
            let generic_alias = types_mod.getattr("GenericAlias")?;
            let dict_cls = builtins.getattr("dict")?;
            let str_t = builtins.getattr("str")?;
            let val_ann = create_model_for_struct_dtype(py, value, counter)?;
            let val_b = val_ann.into_bound(py);
            let tup = pyo3::types::PyTuple::new(py, [str_t, val_b])?;
            let map_ann = generic_alias.call1((dict_cls, tup))?;
            if *nullable {
                let typing = py.import("typing")?;
                let opt = typing.getattr("Optional")?;
                Ok(opt.get_item(map_ann)?.unbind())
            } else {
                Ok(map_ann.unbind())
            }
        }
    }
}

fn scalar_base_to_py_type(py: Python<'_>, base: BaseType) -> PyResult<PyObject> {
    let builtins = py.import("builtins")?;
    Ok(match base {
        BaseType::Int => builtins.getattr("int")?.into_py_any(py)?,
        BaseType::Float => builtins.getattr("float")?.into_py_any(py)?,
        BaseType::Bool => builtins.getattr("bool")?.into_py_any(py)?,
        BaseType::Str => builtins.getattr("str")?.into_py_any(py)?,
        BaseType::Uuid => py.import("uuid")?.getattr("UUID")?.into_py_any(py)?,
        BaseType::Decimal => py.import("decimal")?.getattr("Decimal")?.into_py_any(py)?,
        BaseType::Enum => {
            let typing = py.import("typing")?;
            typing.getattr("Any")?.into_py_any(py)?
        }
        BaseType::DateTime => py
            .import("datetime")?
            .getattr("datetime")?
            .into_py_any(py)?,
        BaseType::Date => py.import("datetime")?.getattr("date")?.into_py_any(py)?,
        BaseType::Duration => py
            .import("datetime")?
            .getattr("timedelta")?
            .into_py_any(py)?,
        BaseType::Time => py.import("datetime")?.getattr("time")?.into_py_any(py)?,
        BaseType::Binary => builtins.getattr("bytes")?.into_py_any(py)?,
        BaseType::Ipv4 => py
            .import("ipaddress")?
            .getattr("IPv4Address")?
            .into_py_any(py)?,
        BaseType::Ipv6 => py
            .import("ipaddress")?
            .getattr("IPv6Address")?
            .into_py_any(py)?,
        BaseType::Wkb => py
            .import("pydantable.types")?
            .getattr("WKB")?
            .into_py_any(py)?,
    })
}

pub fn dtype_to_python_type(py: Python<'_>, dtype: DTypeDesc) -> PyResult<PyObject> {
    let mut c = 0usize;
    create_model_for_struct_dtype(py, &dtype, &mut c)
}

pub fn dtype_to_descriptor_py(py: Python<'_>, dtype: &DTypeDesc) -> PyResult<PyObject> {
    let dict = pyo3::types::PyDict::new(py);
    match dtype {
        DTypeDesc::Scalar {
            base,
            nullable,
            literals,
        } => {
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
                Some(BaseType::Ipv4) => "ipv4",
                Some(BaseType::Ipv6) => "ipv6",
                Some(BaseType::Wkb) => "wkb",
                None => "unknown",
            };
            dict.set_item("base", base_s)?;
            dict.set_item("nullable", *nullable)?;
            if let Some(ls) = literals {
                let list = pyo3::types::PyList::empty(py);
                match ls {
                    LiteralSet::Str(vals) => {
                        for s in vals {
                            list.append(s)?;
                        }
                    }
                    LiteralSet::Int(vals) => {
                        for i in vals {
                            list.append(*i)?;
                        }
                    }
                    LiteralSet::Bool(vals) => {
                        for b in vals {
                            list.append(*b)?;
                        }
                    }
                }
                dict.set_item("literals", list)?;
            }
        }
        DTypeDesc::Struct { fields, nullable } => {
            dict.set_item("kind", "struct")?;
            dict.set_item("nullable", *nullable)?;
            let field_list = pyo3::types::PyList::empty(py);
            for (name, fd) in fields {
                let fe = pyo3::types::PyDict::new(py);
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
    Ok(dict.unbind().into())
}

/// Convert a Python `decimal.Decimal` to Polars `Decimal(`[`DECIMAL_PRECISION`], [`DECIMAL_SCALE`]`)` unscaled `i128`.
pub fn py_decimal_to_scaled_i128(item: &Bound<'_, PyAny>) -> PyResult<i128> {
    let py = item.py();
    let dec_mod = py.import("decimal")?;
    let dec_cls = dec_mod.getattr("Decimal")?;
    let builtins = py.import("builtins")?;
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
    let dec_mod = py.import("decimal")?;
    let dec_cls = dec_mod.getattr("Decimal")?;
    let builtins = py.import("builtins")?;
    let int_cls = builtins.getattr("int")?;
    let numer = dec_cls.call1((int_cls.call1((v.to_string(),))?,))?;
    let denom_str = format!("1{}", "0".repeat(DECIMAL_SCALE));
    let denom = dec_cls.call1((denom_str.as_str(),))?;
    let out = numer.call_method1("__truediv__", (denom,))?;
    Ok(out.unbind())
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
