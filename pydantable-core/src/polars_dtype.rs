//! Map pydantable [`DTypeDesc`] to Polars [`DataType`] for execution and lowering.

use pyo3::prelude::*;

use crate::dtype::{BaseType, DTypeDesc, DECIMAL_PRECISION, DECIMAL_SCALE};

use polars::prelude::{DataType, Field, PlSmallStr, TimeUnit};

pub(crate) fn dtype_desc_to_polars_data_type(d: &DTypeDesc) -> PyResult<DataType> {
    match d {
        DTypeDesc::Scalar {
            base: Some(BaseType::Int),
            ..
        } => Ok(DataType::Int64),
        DTypeDesc::Scalar {
            base: Some(BaseType::Float),
            ..
        } => Ok(DataType::Float64),
        DTypeDesc::Scalar {
            base: Some(BaseType::Bool),
            ..
        } => Ok(DataType::Boolean),
        DTypeDesc::Scalar {
            base: Some(BaseType::Str | BaseType::Enum),
            ..
        } => Ok(DataType::String),
        DTypeDesc::Scalar {
            base: Some(BaseType::Uuid),
            ..
        } => Ok(DataType::String),
        DTypeDesc::Scalar {
            base: Some(BaseType::Ipv4 | BaseType::Ipv6),
            ..
        } => Ok(DataType::String),
        DTypeDesc::Scalar {
            base: Some(BaseType::Decimal),
            ..
        } => Ok(DataType::Decimal(DECIMAL_PRECISION, DECIMAL_SCALE)),
        DTypeDesc::Scalar {
            base: Some(BaseType::DateTime),
            ..
        } => Ok(DataType::Datetime(TimeUnit::Microseconds, None)),
        DTypeDesc::Scalar {
            base: Some(BaseType::Date),
            ..
        } => Ok(DataType::Date),
        DTypeDesc::Scalar {
            base: Some(BaseType::Duration),
            ..
        } => Ok(DataType::Duration(TimeUnit::Microseconds)),
        DTypeDesc::Scalar {
            base: Some(BaseType::Time),
            ..
        } => Ok(DataType::Time),
        DTypeDesc::Scalar {
            base: Some(BaseType::Binary | BaseType::Wkb),
            ..
        } => Ok(DataType::Binary),
        DTypeDesc::Scalar { base: None, .. } => {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Root schema cannot have unknown-base dtype.",
            ))
        }
        DTypeDesc::Struct { fields, .. } => {
            let sub: Vec<Field> = fields
                .iter()
                .map(|(n, fd)| {
                    Ok(Field::new(
                        PlSmallStr::from_str(n),
                        dtype_desc_to_polars_data_type(fd)?,
                    ))
                })
                .collect::<PyResult<_>>()?;
            Ok(DataType::Struct(sub))
        }
        DTypeDesc::List { inner, .. } => Ok(DataType::List(Box::new(
            dtype_desc_to_polars_data_type(inner)?,
        ))),
        DTypeDesc::Map { value, .. } => {
            let vdt = dtype_desc_to_polars_data_type(value)?;
            Ok(DataType::List(Box::new(DataType::Struct(vec![
                Field::new(PlSmallStr::from("key"), DataType::String),
                Field::new(PlSmallStr::from("value"), vdt),
            ]))))
        }
    }
}
