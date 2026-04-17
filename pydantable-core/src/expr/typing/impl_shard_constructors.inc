impl ExprNode {
    pub fn make_column_ref(name: String, dtype: DTypeDesc) -> PyResult<Self> {
        if dtype.is_scalar_unknown_nullable() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "ColumnRef dtype cannot have unknown scalar base.",
            ));
        }
        Ok(ExprNode::ColumnRef { name, dtype })
    }

    pub fn make_literal(value: Option<LiteralValue>, dtype: DTypeDesc) -> PyResult<Self> {
        match (&value, &dtype) {
            (None, DTypeDesc::Scalar { base: None, .. }) => {}
            (None, DTypeDesc::Scalar { base: Some(_), .. }) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Literal(None) must use unknown-base nullable dtype or a nullable struct column type.",
                ));
            }
            (None, DTypeDesc::Struct { nullable: true, .. }) => {}
            (
                None,
                DTypeDesc::Struct {
                    nullable: false, ..
                },
            ) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Literal(None) cannot target a non-nullable struct column.",
                ));
            }
            (None, DTypeDesc::List { nullable: true, .. }) => {}
            (
                None,
                DTypeDesc::List {
                    nullable: false, ..
                },
            ) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Literal(None) cannot target a non-nullable list column.",
                ));
            }
            (None, DTypeDesc::Map { nullable: true, .. }) => {}
            (
                None,
                DTypeDesc::Map {
                    nullable: false, ..
                },
            ) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Literal(None) cannot target a non-nullable map column.",
                ));
            }
            (Some(_), DTypeDesc::Scalar { base: None, .. }) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Non-None Literal must have a known scalar base dtype.",
                ));
            }
            (Some(_), DTypeDesc::Struct { .. }) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Struct literals are not supported; use column references.",
                ));
            }
            (Some(_), DTypeDesc::List { .. }) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "List literals are not supported; use column references.",
                ));
            }
            (Some(_), DTypeDesc::Map { .. }) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Map literals are not supported; use column references.",
                ));
            }
            (Some(_), _) => {}
        }
        Ok(ExprNode::Literal { value, dtype })
    }

    pub fn make_binary_op(op: ArithOp, left: ExprNode, right: ExprNode) -> PyResult<Self> {
        let dtype =
            widen_scalar_drop_literals(Self::infer_arith_dtype(op, left.dtype(), right.dtype())?);
        Ok(ExprNode::BinaryOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
            dtype,
        })
    }

    pub fn make_compare_op(op: CmpOp, left: ExprNode, right: ExprNode) -> PyResult<Self> {
        validate_literal_membership_compare(&left, &right, op)?;
        let dtype = Self::infer_compare_dtype(op, left.dtype(), right.dtype())?;
        Ok(ExprNode::CompareOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
            dtype,
        })
    }

    pub fn make_cast(input: ExprNode, target: DTypeDesc) -> PyResult<Self> {
        if target.is_struct()
            || input.dtype().is_struct()
            || target.is_list()
            || input.dtype().is_list()
            || target.is_map()
            || input.dtype().is_map()
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() does not support struct, list, or map dtypes.",
            ));
        }
        let base = match &target {
            DTypeDesc::Scalar { base: Some(b), .. } => *b,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "cast() target dtype must have known base.",
                ));
            }
        };
        // SQL NULL (`Literal(None)` with unknown-base dtype): cast to a concrete nullable scalar.
        if input.dtype().is_scalar_unknown_nullable() {
            return Ok(ExprNode::Cast {
                input: Box::new(input),
                dtype: DTypeDesc::Scalar {
                    base: Some(base),
                    nullable: true,
                    literals: None,
                },
            });
        }
        let in_base = match input.dtype() {
            DTypeDesc::Scalar { base: Some(b), .. } => b,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "cast() input dtype must have known base.",
                ));
            }
        };
        let allowed = matches!(
            (in_base, base),
            (
                BaseType::Int,
                BaseType::Int | BaseType::Float | BaseType::Bool | BaseType::Str
            ) | (
                BaseType::Float,
                BaseType::Int | BaseType::Float | BaseType::Bool | BaseType::Str
            ) | (
                BaseType::Bool,
                BaseType::Int | BaseType::Float | BaseType::Bool | BaseType::Str
            ) | (
                BaseType::Str,
                BaseType::Int | BaseType::Float | BaseType::Bool | BaseType::Str
            ) | (BaseType::Uuid, BaseType::Str | BaseType::Uuid)
                | (BaseType::Str, BaseType::Uuid)
                | (BaseType::Decimal, BaseType::Str | BaseType::Decimal)
                | (BaseType::Str, BaseType::Decimal)
                | (BaseType::Enum, BaseType::Str | BaseType::Enum)
                | (BaseType::Str, BaseType::Enum)
                | (BaseType::DateTime, BaseType::Date)
                | (BaseType::DateTime, BaseType::Str)
                | (BaseType::Date, BaseType::Str)
                | (BaseType::Str, BaseType::Date)
                | (BaseType::Str, BaseType::DateTime)
                | (BaseType::Ipv4, BaseType::Str | BaseType::Ipv4)
                | (BaseType::Str, BaseType::Ipv4)
                | (BaseType::Ipv6, BaseType::Str | BaseType::Ipv6)
                | (BaseType::Str, BaseType::Ipv6)
                | (BaseType::Wkb, BaseType::Binary | BaseType::Wkb)
                | (BaseType::Binary, BaseType::Wkb)
        );
        if !allowed {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast() unsupported primitive conversion.",
            ));
        }
        let input_nullable = input.dtype().nullable_flag();
        Ok(ExprNode::Cast {
            input: Box::new(input),
            dtype: DTypeDesc::Scalar {
                base: Some(base),
                nullable: input_nullable || target.nullable_flag(),
                literals: None,
            },
        })
    }

    pub fn make_is_null(input: ExprNode) -> PyResult<Self> {
        Ok(ExprNode::IsNull {
            input: Box::new(input),
            dtype: DTypeDesc::non_nullable(BaseType::Bool),
        })
    }

    pub fn make_is_not_null(input: ExprNode) -> PyResult<Self> {
        Ok(ExprNode::IsNotNull {
            input: Box::new(input),
            dtype: DTypeDesc::non_nullable(BaseType::Bool),
        })
    }

    pub fn make_coalesce(exprs: Vec<ExprNode>) -> PyResult<Self> {
        if exprs.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "coalesce() requires at least one expression.",
            ));
        }
        let mut nullable = false;
        if exprs
            .iter()
            .any(|e| e.dtype().is_struct() || e.dtype().is_list())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "coalesce() does not support struct or list dtypes.",
            ));
        }
        let first_base = exprs[0]
            .dtype()
            .as_scalar_base_field()
            .flatten()
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "coalesce() requires expressions with known dtypes.",
                )
            })?;
        for e in &exprs {
            if e.dtype().as_scalar_base_field().flatten() != Some(first_base) {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "coalesce() requires compatible scalar dtypes.",
                ));
            }
            nullable |= e.dtype().nullable_flag();
        }
        Ok(ExprNode::Coalesce {
            exprs,
            dtype: DTypeDesc::Scalar {
                base: Some(first_base),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_case_when(branches: Vec<(ExprNode, ExprNode)>, else_: ExprNode) -> PyResult<Self> {
        if branches.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "case_when() requires at least one branch.",
            ));
        }
        if else_.dtype().is_struct()
            || else_.dtype().is_list()
            || branches
                .iter()
                .any(|(_, t)| t.dtype().is_struct() || t.dtype().is_list())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "case_when() does not support struct or list dtypes.",
            ));
        }
        let mut nullable = else_.dtype().nullable_flag();
        let first_base = branches[0]
            .1
            .dtype()
            .as_scalar_base_field()
            .flatten()
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "case_when() requires then-branches with known dtypes.",
                )
            })?;
        for (_, t) in &branches {
            if t.dtype().as_scalar_base_field().flatten() != Some(first_base) {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "case_when() requires compatible then dtypes.",
                ));
            }
            nullable |= t.dtype().nullable_flag();
        }
        if else_.dtype().as_scalar_base_field().flatten() != Some(first_base) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "case_when() else branch dtype must match then branches.",
            ));
        }
        Ok(ExprNode::CaseWhen {
            branches,
            else_: Box::new(else_),
            dtype: DTypeDesc::Scalar {
                base: Some(first_base),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_in_list(inner: ExprNode, values: Vec<LiteralValue>) -> PyResult<Self> {
        if inner.dtype().is_struct() || inner.dtype().is_list() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "isin() does not support struct or list dtypes.",
            ));
        }
        let ib = inner
            .dtype()
            .as_scalar_base_field()
            .flatten()
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "isin() requires a column expression with known dtype.",
                )
            })?;
        if values.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "isin() requires at least one value.",
            ));
        }
        for v in &values {
            let vb = match v {
                LiteralValue::Int(_) => Some(BaseType::Int),
                LiteralValue::Float(_) => Some(BaseType::Float),
                LiteralValue::Bool(_) => Some(BaseType::Bool),
                LiteralValue::Str(_) => Some(BaseType::Str),
                LiteralValue::Uuid(_) => Some(BaseType::Uuid),
                LiteralValue::Decimal(_) => Some(BaseType::Decimal),
                LiteralValue::EnumStr(_) => Some(BaseType::Enum),
                LiteralValue::DateTimeMicros(_)
                | LiteralValue::DateDays(_)
                | LiteralValue::DurationMicros(_) => None,
                LiteralValue::TimeNanos(_) => Some(BaseType::Time),
                LiteralValue::Binary(_) => Some(BaseType::Binary),
            };
            let ok = match (ib, vb) {
                (a, Some(b)) if a == b => true,
                (BaseType::Enum, Some(BaseType::Str)) => true,
                (BaseType::Str, Some(BaseType::Enum)) => true,
                _ => false,
            };
            if !ok {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "isin() value types must match column dtype.",
                ));
            }
        }
        Ok(ExprNode::InList {
            inner: Box::new(inner),
            values,
            dtype: DTypeDesc::non_nullable(BaseType::Bool),
        })
    }

    pub fn make_between(inner: ExprNode, low: ExprNode, high: ExprNode) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || low.dtype().is_struct()
            || low.dtype().is_list()
            || high.dtype().is_struct()
            || high.dtype().is_list()
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "between() does not support struct or list dtypes.",
            ));
        }
        let ib = inner
            .dtype()
            .as_scalar_base_field()
            .flatten()
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "between() requires known inner dtype.",
                )
            })?;
        if low.dtype().as_scalar_base_field().flatten() != Some(ib)
            || high.dtype().as_scalar_base_field().flatten() != Some(ib)
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "between() bounds must match inner dtype.",
            ));
        }
        Ok(ExprNode::Between {
            inner: Box::new(inner),
            low: Box::new(low),
            high: Box::new(high),
            dtype: DTypeDesc::non_nullable(BaseType::Bool),
        })
    }

    pub fn make_string_concat(parts: Vec<ExprNode>) -> PyResult<Self> {
        if parts.len() < 2 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "concat() requires at least two expressions.",
            ));
        }
        for p in &parts {
            if p.dtype().is_struct() || p.dtype().is_list() || !dtype_is_string_like(&p.dtype()) {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "concat() requires string-typed expressions.",
                ));
            }
        }
        let nullable = parts.iter().any(|p| p.dtype().nullable_flag());
        Ok(ExprNode::StringConcat {
            parts,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_substring(
        inner: ExprNode,
        start: ExprNode,
        length: Option<ExprNode>,
    ) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "substring() requires a string column.",
            ));
        }
        if start.dtype().is_struct()
            || start.dtype().is_list()
            || start.dtype().as_scalar_base_field().flatten() != Some(BaseType::Int)
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "substring() start must be int.",
            ));
        }
        if let Some(l) = &length {
            if l.dtype().is_struct()
                || l.dtype().is_list()
                || l.dtype().as_scalar_base_field().flatten() != Some(BaseType::Int)
            {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "substring() length must be int.",
                ));
            }
        }
        let nullable = inner.dtype().nullable_flag()
            || start.dtype().nullable_flag()
            || length
                .as_ref()
                .map(|l| l.dtype().nullable_flag())
                .unwrap_or(false);
        Ok(ExprNode::Substring {
            inner: Box::new(inner),
            start: Box::new(start),
            length: length.map(Box::new),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_string_length(inner: ExprNode) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "length() requires a string column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StringLength {
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_string_replace(
        inner: ExprNode,
        pattern: String,
        replacement: String,
        literal: bool,
    ) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "str_replace() requires a string-like column.",
            ));
        }
        if !literal && pattern.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Regex pattern must not be empty.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StringReplace {
            inner: Box::new(inner),
            pattern,
            replacement,
            literal,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_string_predicate(
        inner: ExprNode,
        kind: StringPredicateKind,
        pattern: String,
    ) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "String predicate requires a string-like column.",
            ));
        }
        if let StringPredicateKind::Contains { literal: false } = &kind {
            if pattern.is_empty() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Regex pattern must not be empty.",
                ));
            }
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StringPredicate {
            inner: Box::new(inner),
            kind,
            pattern,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Bool),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_struct_field(inner: ExprNode, field: String) -> PyResult<Self> {
        let dtype = match inner.dtype() {
            DTypeDesc::Struct {
                fields,
                nullable: struct_nullable,
            } => {
                let (_, fd) = fields.iter().find(|(n, _)| n == &field).ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "Unknown struct field {:?}.",
                        field
                    ))
                })?;
                let mut out = fd.clone();
                if struct_nullable {
                    out = match out {
                        DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                            base,
                            nullable: true,
                            literals: None,
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
                out
            }
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "struct_field() requires a struct-typed expression.",
                ));
            }
        };
        Ok(ExprNode::StructField {
            base: Box::new(inner),
            field,
            dtype,
        })
    }

    fn dtype_under_nullable_struct(struct_nullable: bool, d: DTypeDesc) -> DTypeDesc {
        if !struct_nullable {
            return d;
        }
        match d {
            DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                base,
                nullable: true,
                literals: None,
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

    pub fn make_struct_json_encode(inner: ExprNode) -> PyResult<Self> {
        if !inner.dtype().is_struct() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "struct_json_encode() requires a struct-typed expression.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StructJsonEncode {
            base: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_struct_json_path_match(inner: ExprNode, path: String) -> PyResult<Self> {
        if !inner.dtype().is_struct() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "struct_json_path_match() requires a struct-typed expression.",
            ));
        }
        if path.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "JSONPath pattern must not be empty.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StructJsonPathMatch {
            base: Box::new(inner),
            path,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_struct_rename_fields(inner: ExprNode, names: Vec<String>) -> PyResult<Self> {
        let DTypeDesc::Struct {
            fields,
            nullable: struct_nullable,
        } = inner.dtype()
        else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "struct_rename_fields() requires a struct-typed expression.",
            ));
        };
        if names.len() != fields.len() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "struct_rename_fields() expected {} names (one per struct field), got {}.",
                fields.len(),
                names.len()
            )));
        }
        let mut seen = std::collections::HashSet::new();
        for n in &names {
            if !seen.insert(n.as_str()) {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "struct_rename_fields() duplicate new field name {:?}.",
                    n
                )));
            }
        }
        let new_fields: Vec<(String, DTypeDesc)> = names
            .iter()
            .zip(fields.iter())
            .map(|(n, (_, dt))| (n.clone(), dt.clone()))
            .collect();
        let dtype = DTypeDesc::Struct {
            fields: new_fields,
            nullable: struct_nullable,
        };
        Ok(ExprNode::StructRenameFields {
            base: Box::new(inner),
            names,
            dtype,
        })
    }

    pub fn make_struct_with_fields(
        inner: ExprNode,
        updates: Vec<(String, ExprNode)>,
    ) -> PyResult<Self> {
        let DTypeDesc::Struct {
            fields: base_fields,
            nullable: struct_nullable,
        } = inner.dtype()
        else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "struct_with_fields() requires a struct-typed expression.",
            ));
        };
        if updates.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "struct_with_fields() requires at least one field expression.",
            ));
        }
        let mut seen_update_keys = std::collections::HashSet::new();
        for (name, _) in &updates {
            if !seen_update_keys.insert(name.as_str()) {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "struct_with_fields() duplicate field name {:?} in updates.",
                    name
                )));
            }
        }
        let mut out_fields: Vec<(String, DTypeDesc)> = base_fields.clone();
        let mut boxed_updates: Vec<(String, Box<ExprNode>)> = Vec::with_capacity(updates.len());
        for (name, expr) in updates {
            let fd = Self::dtype_under_nullable_struct(struct_nullable, expr.dtype());
            if let Some(pos) = out_fields.iter().position(|(n, _)| n == &name) {
                out_fields[pos] = (name.clone(), fd);
            } else {
                out_fields.push((name.clone(), fd));
            }
            boxed_updates.push((name, Box::new(expr)));
        }
        let dtype = DTypeDesc::Struct {
            fields: out_fields,
            nullable: struct_nullable,
        };
        Ok(ExprNode::StructWithFields {
            base: Box::new(inner),
            updates: boxed_updates,
            dtype,
        })
    }

    fn numeric_inner_dtype(inner: &DTypeDesc) -> PyResult<(BaseType, bool)> {
        if inner.is_struct() || inner.is_list() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Numeric unary operations do not support struct- or list-typed columns.",
            ));
        }
        let nullable = inner.nullable_flag();
        match inner.as_scalar_base_field().flatten() {
            Some(b @ BaseType::Int) | Some(b @ BaseType::Float) => Ok((b, nullable)),
            Some(other) => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Numeric unary operation requires int or float column; got {:?}.",
                other
            ))),
            None => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Numeric unary operation requires a column with known numeric dtype.",
            )),
        }
    }

    pub fn make_unary_numeric(inner: ExprNode, op: UnaryNumericOp) -> PyResult<Self> {
        let (in_base, nullable) = Self::numeric_inner_dtype(&inner.dtype())?;
        let dtype = match op {
            UnaryNumericOp::Abs | UnaryNumericOp::Round { .. } => DTypeDesc::Scalar {
                base: Some(in_base),
                nullable,
                literals: None,
            },
            UnaryNumericOp::Floor | UnaryNumericOp::Ceil => DTypeDesc::Scalar {
                base: Some(BaseType::Float),
                nullable,
                literals: None,
            },
        };
        Ok(ExprNode::UnaryNumeric {
            op,
            inner: Box::new(inner),
            dtype,
        })
    }

    pub fn make_string_unary(inner: ExprNode, op: StringUnaryOp) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "String operation requires a string column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StringUnary {
            op,
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_str_reverse(inner: ExprNode) -> PyResult<Self> {
        Self::make_string_unary(inner, StringUnaryOp::Reverse)
    }

    pub fn make_str_pad_start(inner: ExprNode, length: u32, fill_char: char) -> PyResult<Self> {
        Self::make_string_unary(inner, StringUnaryOp::PadStart { length, fill_char })
    }

    pub fn make_str_pad_end(inner: ExprNode, length: u32, fill_char: char) -> PyResult<Self> {
        Self::make_string_unary(inner, StringUnaryOp::PadEnd { length, fill_char })
    }

    pub fn make_str_zfill(inner: ExprNode, length: u32) -> PyResult<Self> {
        Self::make_string_unary(inner, StringUnaryOp::ZFill { length })
    }

    pub fn make_logical_binary(op: LogicalOp, left: ExprNode, right: ExprNode) -> PyResult<Self> {
        let is_bool = |d: &DTypeDesc| {
            matches!(
                d,
                DTypeDesc::Scalar {
                    base: Some(BaseType::Bool),
                    ..
                }
            )
        };
        if !is_bool(&left.dtype()) || !is_bool(&right.dtype()) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Logical operations require boolean expressions.",
            ));
        }
        let nullable = left.dtype().nullable_flag() || right.dtype().nullable_flag();
        Ok(ExprNode::LogicalBinary {
            op,
            left: Box::new(left),
            right: Box::new(right),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Bool),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_logical_not(inner: ExprNode) -> PyResult<Self> {
        let is_bool = matches!(
            inner.dtype(),
            DTypeDesc::Scalar {
                base: Some(BaseType::Bool),
                ..
            }
        );
        if !is_bool {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Logical not requires a boolean expression.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::LogicalNot {
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Bool),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_temporal_part(inner: ExprNode, part: TemporalPart) -> PyResult<Self> {
        let d = inner.dtype();
        if d.is_struct() || d.is_list() || d.is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Temporal extraction requires a datetime, date, or time column.",
            ));
        }
        let base = d.as_scalar_base_field().flatten();
        let is_dt = base == Some(BaseType::DateTime);
        let is_date = base == Some(BaseType::Date);
        let is_time = base == Some(BaseType::Time);
        match part {
            TemporalPart::Year
            | TemporalPart::Month
            | TemporalPart::Day
            | TemporalPart::Weekday
            | TemporalPart::Quarter
            | TemporalPart::Week
            | TemporalPart::DayOfYear => {
                if !(is_dt || is_date) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "That temporal part requires a datetime or date column.",
                    ));
                }
            }
            TemporalPart::Hour
            | TemporalPart::Minute
            | TemporalPart::Second
            | TemporalPart::Nanosecond => {
                if !(is_dt || is_time) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "hour(), minute(), second(), nanosecond() require a datetime or time column.",
                    ));
                }
            }
        }
        let nullable = d.nullable_flag();
        Ok(ExprNode::TemporalPart {
            part,
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_list_len(inner: ExprNode) -> PyResult<Self> {
        if !inner.dtype().is_list() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "list_len() requires a list-typed column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::ListLen {
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_list_get(inner: ExprNode, index: ExprNode) -> PyResult<Self> {
        let element = match inner.dtype() {
            DTypeDesc::List { inner: e, .. } => e.as_ref().clone(),
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "list_get() requires a list-typed column.",
                ));
            }
        };
        if matches!(&element, DTypeDesc::List { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "list_get() does not support nested list columns.",
            ));
        }
        if matches!(&element, DTypeDesc::Map { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "list_get() does not support list columns whose element type is a map.",
            ));
        }
        if index.dtype().is_struct()
            || index.dtype().is_list()
            || index.dtype().as_scalar_base_field().flatten() != Some(BaseType::Int)
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "list_get() index must be an int expression.",
            ));
        }
        let dtype = match element {
            DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                base,
                nullable: true,
                literals: None,
            },
            DTypeDesc::Struct { fields, .. } => DTypeDesc::Struct {
                fields,
                nullable: true,
            },
            DTypeDesc::Map { .. } => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "internal: list_get() does not support map element dtypes.",
                ));
            }
            DTypeDesc::List { .. } => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "internal: list_get() does not support nested list element dtypes.",
                ));
            }
        };
        Ok(ExprNode::ListGet {
            inner: Box::new(inner),
            index: Box::new(index),
            dtype,
        })
    }

    pub fn make_list_contains(inner: ExprNode, value: ExprNode) -> PyResult<Self> {
        let elt = match inner.dtype() {
            DTypeDesc::List { inner: e, .. } => e.as_ref().clone(),
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "list_contains() requires a list-typed column.",
                ));
            }
        };
        if matches!(&elt, DTypeDesc::Map { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "list_contains() does not support list columns whose element type is a map.",
            ));
        }
        if !dtype_structural_eq(&elt, &value.dtype()) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "list_contains() value dtype must match the list element dtype.",
            ));
        }
        let nullable = inner.dtype().nullable_flag() || value.dtype().nullable_flag();
        Ok(ExprNode::ListContains {
            inner: Box::new(inner),
            value: Box::new(value),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Bool),
                nullable,
                literals: None,
            },
        })
    }

    fn make_list_numeric_agg(inner: ExprNode, kind: ListAggKind) -> PyResult<Self> {
        let (base, list_nullable) = match inner.dtype() {
            DTypeDesc::List {
                inner: e, nullable, ..
            } => match e.as_ref() {
                DTypeDesc::Scalar {
                    base: Some(BaseType::Int),
                    ..
                } => (BaseType::Int, nullable),
                DTypeDesc::Scalar {
                    base: Some(BaseType::Float),
                    ..
                } => (BaseType::Float, nullable),
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "list_min/list_max/list_sum require list[int] or list[float].",
                    ));
                }
            },
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "list aggregation requires a list-typed column.",
                ));
            }
        };
        let dtype = DTypeDesc::Scalar {
            base: Some(base),
            nullable: list_nullable,
            literals: None,
        };
        let inner = Box::new(inner);
        Ok(match kind {
            ListAggKind::Min => ExprNode::ListMin { inner, dtype },
            ListAggKind::Max => ExprNode::ListMax { inner, dtype },
            ListAggKind::Sum => ExprNode::ListSum { inner, dtype },
        })
    }

    pub fn make_list_min(inner: ExprNode) -> PyResult<Self> {
        Self::make_list_numeric_agg(inner, ListAggKind::Min)
    }

    pub fn make_list_max(inner: ExprNode) -> PyResult<Self> {
        Self::make_list_numeric_agg(inner, ListAggKind::Max)
    }

    pub fn make_list_sum(inner: ExprNode) -> PyResult<Self> {
        Self::make_list_numeric_agg(inner, ListAggKind::Sum)
    }

    pub fn make_list_mean(inner: ExprNode) -> PyResult<Self> {
        let list_nullable = match inner.dtype() {
            DTypeDesc::List {
                inner: e, nullable, ..
            } => match e.as_ref() {
                DTypeDesc::Scalar {
                    base: Some(BaseType::Int | BaseType::Float),
                    ..
                } => nullable,
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "list_mean() requires list[int] or list[float].",
                    ));
                }
            },
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "list_mean() requires a list-typed column.",
                ));
            }
        };
        Ok(ExprNode::ListMean {
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Float),
                nullable: list_nullable,
                literals: None,
            },
        })
    }

    pub fn make_string_split(inner: ExprNode, delimiter: String) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "str_split() requires a string-like column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StringSplit {
            inner: Box::new(inner),
            delimiter,
            dtype: DTypeDesc::List {
                inner: Box::new(DTypeDesc::Scalar {
                    base: Some(BaseType::Str),
                    nullable: true,
                    literals: None,
                }),
                nullable,
            },
        })
    }

    fn dtype_is_sortable_list_element(d: &DTypeDesc) -> bool {
        matches!(
            d,
            DTypeDesc::Scalar {
                base: Some(
                    BaseType::Int
                        | BaseType::Float
                        | BaseType::Bool
                        | BaseType::Str
                        | BaseType::Date
                        | BaseType::DateTime
                        | BaseType::Time
                        | BaseType::Enum
                        | BaseType::Uuid
                ),
                ..
            }
        )
    }

    pub fn make_list_join(
        inner: ExprNode,
        separator: String,
        ignore_nulls: bool,
    ) -> PyResult<Self> {
        let nullable = match inner.dtype() {
            DTypeDesc::List {
                inner: e, nullable, ..
            } => match e.as_ref() {
                DTypeDesc::Scalar {
                    base: Some(BaseType::Str),
                    ..
                } => nullable,
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "list_join() requires list[str].",
                    ));
                }
            },
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "list_join() requires a list-typed column.",
                ));
            }
        };
        Ok(ExprNode::ListJoin {
            inner: Box::new(inner),
            separator,
            ignore_nulls,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_list_sort(
        inner: ExprNode,
        descending: bool,
        nulls_last: bool,
        maintain_order: bool,
    ) -> PyResult<Self> {
        let dtype = match &inner.dtype() {
            DTypeDesc::List { inner: elt, .. } => {
                if !Self::dtype_is_sortable_list_element(elt.as_ref()) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "list_sort() requires list elements that are int, float, bool, str, date, datetime, time, enum, or uuid.",
                    ));
                }
                inner.dtype().clone()
            }
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "list_sort() requires a list-typed column.",
                ));
            }
        };
        Ok(ExprNode::ListSort {
            inner: Box::new(inner),
            descending,
            nulls_last,
            maintain_order,
            dtype,
        })
    }

    pub fn make_list_unique(inner: ExprNode, stable: bool) -> PyResult<Self> {
        let dtype = match &inner.dtype() {
            DTypeDesc::List { inner: elt, .. } => {
                if !Self::dtype_is_sortable_list_element(elt.as_ref()) {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "list_unique() requires list elements that are int, float, bool, str, date, datetime, time, enum, or uuid.",
                    ));
                }
                inner.dtype().clone()
            }
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "list_unique() requires a list-typed column.",
                ));
            }
        };
        Ok(ExprNode::ListUnique {
            inner: Box::new(inner),
            stable,
            dtype,
        })
    }

    pub fn make_string_extract(
        inner: ExprNode,
        pattern: String,
        group_index: usize,
    ) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "str_extract_regex() requires a string-like column.",
            ));
        }
        if pattern.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Regex pattern must not be empty.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StringExtract {
            inner: Box::new(inner),
            pattern,
            group_index,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_string_json_path_match(inner: ExprNode, path: String) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "str_json_path_match() requires a string-like column.",
            ));
        }
        if path.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "JSONPath pattern must not be empty.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::StringJsonPathMatch {
            inner: Box::new(inner),
            path,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Str),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_str_json_decode(inner: ExprNode, target: DTypeDesc) -> PyResult<Self> {
        if inner.dtype().is_struct()
            || inner.dtype().is_list()
            || !dtype_is_string_like(&inner.dtype())
        {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "str_json_decode() requires a string-like column.",
            ));
        }
        let dtype = match &target {
            DTypeDesc::Struct { fields, .. } => DTypeDesc::Struct {
                fields: fields.clone(),
                nullable: true,
            },
            DTypeDesc::Map { value, .. } => DTypeDesc::Map {
                value: value.clone(),
                nullable: true,
            },
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "str_json_decode() target dtype must be a struct (nested model) or dict[str, T] map.",
                ));
            }
        };
        Ok(ExprNode::StringJsonDecode {
            inner: Box::new(inner),
            target,
            dtype,
        })
    }

    pub fn make_datetime_to_date(inner: ExprNode) -> PyResult<Self> {
        let d = inner.dtype();
        if d.is_struct() || d.is_list() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "dt_date() requires a datetime column.",
            ));
        }
        if d.as_scalar_base_field().flatten() != Some(BaseType::DateTime) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "dt_date() requires a datetime column.",
            ));
        }
        let nullable = d.nullable_flag();
        Ok(ExprNode::DatetimeToDate {
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Date),
                nullable,
                literals: None,
            },
        })
    }
}
