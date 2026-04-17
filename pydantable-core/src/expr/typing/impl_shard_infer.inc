impl ExprNode {
    fn infer_arith_dtype(op: ArithOp, left: DTypeDesc, right: DTypeDesc) -> PyResult<DTypeDesc> {
        if left.is_struct()
            || right.is_struct()
            || left.is_list()
            || right.is_list()
            || left.is_map()
            || right.is_map()
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Arithmetic operators do not support struct-, list-, or map-typed columns.",
            ));
        }
        let nullable = left.nullable_flag() || right.nullable_flag();
        let left_b = left.as_scalar_base_field().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Arithmetic operators do not support struct-, list-, or map-typed columns.",
            )
        })?;
        let right_b = right.as_scalar_base_field().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Arithmetic operators do not support struct-, list-, or map-typed columns.",
            )
        })?;

        if let (Some(a), Some(b)) = (left_b, right_b) {
            use ArithOp::*;
            match (a, b, op) {
                (BaseType::DateTime, BaseType::Duration, Add)
                | (BaseType::Duration, BaseType::DateTime, Add) => {
                    return Ok(DTypeDesc::Scalar {
                        base: Some(BaseType::DateTime),
                        nullable,
                        literals: None,
                    });
                }
                (BaseType::DateTime, BaseType::Duration, Sub) => {
                    return Ok(DTypeDesc::Scalar {
                        base: Some(BaseType::DateTime),
                        nullable,
                        literals: None,
                    });
                }
                (BaseType::Date, BaseType::Duration, Add)
                | (BaseType::Duration, BaseType::Date, Add) => {
                    return Ok(DTypeDesc::Scalar {
                        base: Some(BaseType::Date),
                        nullable,
                        literals: None,
                    });
                }
                (BaseType::Date, BaseType::Duration, Sub) => {
                    return Ok(DTypeDesc::Scalar {
                        base: Some(BaseType::Date),
                        nullable,
                        literals: None,
                    });
                }
                _ => {}
            }
        }

        let inferred_base = match (left_b, right_b) {
            (Some(a), Some(b)) => {
                let valid = matches!(
                    (a, b),
                    (BaseType::Int, BaseType::Int | BaseType::Float)
                        | (BaseType::Float, BaseType::Int | BaseType::Float)
                        | (BaseType::Decimal, BaseType::Decimal)
                );
                if !valid {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "Arithmetic operator requires numeric operands; got {:?} and {:?}.",
                        a, b
                    )));
                }
                match (a, b) {
                    (BaseType::Int, BaseType::Int) => Some(BaseType::Int),
                    (BaseType::Decimal, BaseType::Decimal) => Some(BaseType::Decimal),
                    _ => Some(BaseType::Float),
                }
            }
            (None, Some(b)) => match b {
                BaseType::Int => Some(BaseType::Int),
                BaseType::Float => Some(BaseType::Float),
                BaseType::Decimal => Some(BaseType::Decimal),
                _ => None,
            },
            (Some(a), None) => match a {
                BaseType::Int => Some(BaseType::Int),
                BaseType::Float => Some(BaseType::Float),
                BaseType::Decimal => Some(BaseType::Decimal),
                _ => None,
            },
            (None, None) => None,
        };

        let inferred_base = inferred_base.ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Cannot infer arithmetic result type from unknown/None base alone.",
            )
        })?;

        if op == ArithOp::Div {
            return Ok(DTypeDesc::Scalar {
                base: Some(BaseType::Float),
                nullable,
                literals: None,
            });
        }

        Ok(DTypeDesc::Scalar {
            base: Some(inferred_base),
            nullable,
            literals: None,
        })
    }

    fn infer_compare_dtype(op: CmpOp, left: DTypeDesc, right: DTypeDesc) -> PyResult<DTypeDesc> {
        let nullable = left.nullable_flag() || right.nullable_flag();

        match op {
            CmpOp::Eq | CmpOp::Ne => {
                if left.is_struct() && right.is_struct() {
                    if !dtype_structural_eq(&left, &right) {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Equality requires structurally equal struct dtypes.",
                        ));
                    }
                    return Ok(DTypeDesc::Scalar {
                        base: Some(BaseType::Bool),
                        nullable,
                        literals: None,
                    });
                }
                if left.is_struct() || right.is_struct() {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Cannot compare struct columns to scalar columns.",
                    ));
                }
                if left.is_list() || right.is_list() {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Equality comparisons for list columns are not supported yet.",
                    ));
                }
                if left.is_map() || right.is_map() {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Equality comparisons for map columns are not supported yet.",
                    ));
                }

                let lb = left.as_scalar_base_field().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Equality comparisons do not support struct-, list-, or map-typed columns.",
                    )
                })?;
                let rb = right.as_scalar_base_field().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Equality comparisons do not support struct-, list-, or map-typed columns.",
                    )
                })?;
                let inferred_left_base = lb.or(rb);
                let inferred_right_base = rb.or(lb);
                let (lb, rb) = match (inferred_left_base, inferred_right_base) {
                    (Some(a), Some(b)) => (a, b),
                    (None, None) => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Cannot infer equality from Literal(None) alone.",
                        ));
                    }
                    _ => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Cannot infer equality from a typed operand and an unknown-base literal.",
                        ));
                    }
                };

                let allowed = (matches!(
                    (lb, rb),
                    (BaseType::Int, BaseType::Int | BaseType::Float)
                        | (BaseType::Float, BaseType::Int | BaseType::Float)
                ) || (lb == BaseType::Bool && rb == BaseType::Bool)
                    || (lb == BaseType::Str && rb == BaseType::Str)
                    || (lb == BaseType::Enum && rb == BaseType::Enum)
                    || (lb == BaseType::Str && rb == BaseType::Enum)
                    || (lb == BaseType::Enum && rb == BaseType::Str)
                    || (lb == BaseType::Uuid && rb == BaseType::Uuid)
                    || (lb == BaseType::Ipv4 && rb == BaseType::Ipv4)
                    || (lb == BaseType::Ipv6 && rb == BaseType::Ipv6)
                    || (lb == BaseType::Wkb && rb == BaseType::Wkb)
                    || (lb == BaseType::Decimal && rb == BaseType::Decimal)
                    || (lb == BaseType::DateTime && rb == BaseType::DateTime)
                    || (lb == BaseType::Date && rb == BaseType::Date)
                    || (lb == BaseType::Duration && rb == BaseType::Duration)
                    || (lb == BaseType::Time && rb == BaseType::Time)
                    || (lb == BaseType::Binary && rb == BaseType::Binary));

                if !allowed {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Equality requires compatible scalar operands or matching struct dtypes.",
                    ));
                }

                Ok(DTypeDesc::Scalar {
                    base: Some(BaseType::Bool),
                    nullable,
                    literals: None,
                })
            }
            CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge => {
                if left.is_struct()
                    || right.is_struct()
                    || left.is_list()
                    || right.is_list()
                    || left.is_map()
                    || right.is_map()
                {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Ordering comparisons do not support struct-, list-, or map-typed columns.",
                    ));
                }

                let lb = left.as_scalar_base_field().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Ordering comparisons do not support struct-, list-, or map-typed columns.",
                    )
                })?;
                let rb = right.as_scalar_base_field().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Ordering comparisons do not support struct-, list-, or map-typed columns.",
                    )
                })?;
                let inferred_left_base = lb.or(rb);
                let inferred_right_base = rb.or(lb);
                let (lb, rb) = match (inferred_left_base, inferred_right_base) {
                    (Some(a), Some(b)) => (a, b),
                    (None, None) => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Cannot infer ordering from Literal(None) alone.",
                        ));
                    }
                    _ => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Cannot infer ordering from a typed operand and an unknown-base literal.",
                        ));
                    }
                };

                let allowed_numeric = matches!(
                    (lb, rb),
                    (BaseType::Int, BaseType::Int | BaseType::Float)
                        | (BaseType::Float, BaseType::Int | BaseType::Float)
                );
                let allowed_str = lb == BaseType::Str && rb == BaseType::Str;
                let allowed_enum = ((lb == BaseType::Str || lb == BaseType::Enum)
                    && rb == BaseType::Enum)
                    || (lb == BaseType::Enum && rb == BaseType::Str);
                let allowed_uuid = lb == BaseType::Uuid && rb == BaseType::Uuid;
                let allowed_decimal = lb == BaseType::Decimal && rb == BaseType::Decimal;
                let allowed_temporal = (lb == BaseType::DateTime && rb == BaseType::DateTime)
                    || (lb == BaseType::Date && rb == BaseType::Date)
                    || (lb == BaseType::Duration && rb == BaseType::Duration)
                    || (lb == BaseType::Time && rb == BaseType::Time);
                let allowed_ip = (lb == BaseType::Ipv4 && rb == BaseType::Ipv4)
                    || (lb == BaseType::Ipv6 && rb == BaseType::Ipv6);
                let allowed_wkb = lb == BaseType::Wkb && rb == BaseType::Wkb;

                if !(allowed_numeric
                    || allowed_str
                    || allowed_enum
                    || allowed_uuid
                    || allowed_decimal
                    || allowed_temporal
                    || allowed_ip
                    || allowed_wkb)
                {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Ordering comparisons require numeric-numeric, str/str-like, uuid-uuid, decimal-decimal, or same temporal operands.",
                    ));
                }

                Ok(DTypeDesc::Scalar {
                    base: Some(BaseType::Bool),
                    nullable,
                    literals: None,
                })
            }
        }
    }
}
