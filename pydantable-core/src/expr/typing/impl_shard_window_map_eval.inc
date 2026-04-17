impl ExprNode {
    fn parse_window_frame(
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Option<WindowFrame>> {
        match (frame_kind, frame_start, frame_end) {
            (None, None, None) => Ok(None),
            (Some(kind), Some(start), Some(end)) => {
                if start > end {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Window frame requires start <= end.",
                    ));
                }
                match kind.as_str() {
                    "rows" => Ok(Some(WindowFrame::Rows { start, end })),
                    "range" => Ok(Some(WindowFrame::Range { start, end })),
                    _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Window frame kind must be 'rows' or 'range'.",
                    )),
                }
            }
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Window frame requires kind/start/end together.",
            )),
        }
    }

    fn reject_range_frame(frame: &Option<WindowFrame>, op_name: &str) -> PyResult<()> {
        if matches!(frame, Some(WindowFrame::Range { .. })) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "{op_name}() does not support rangeBetween frames."
            )));
        }
        Ok(())
    }

    fn validate_range_frame_order_keys(
        frame: &Option<WindowFrame>,
        order_by: &[WindowOrderKey],
        op_name: &str,
    ) -> PyResult<()> {
        if matches!(frame, Some(WindowFrame::Range { .. })) && order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "{op_name}() with rangeBetween requires at least one order_by column; \
                 range bounds apply to the first order column (PostgreSQL-style)."
            )));
        }
        Ok(())
    }

    pub fn make_window_row_number(
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if partition_by.is_empty() && order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "window expression requires at least one partition_by or order_by column.",
            ));
        }
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "row_number() requires at least one order_by column.",
            ));
        }
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "row_number")?;
        Ok(ExprNode::Window {
            op: WindowOp::RowNumber,
            operand: None,
            partition_by,
            order_by,
            frame,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable: true,
                literals: None,
            },
        })
    }

    pub fn make_window_rank(
        dense: bool,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "rank() and dense_rank() require at least one order_by column.",
            ));
        }
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "rank")?;
        Ok(ExprNode::Window {
            op: if dense {
                WindowOp::DenseRank
            } else {
                WindowOp::Rank
            },
            operand: None,
            partition_by,
            order_by,
            frame,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable: true,
                literals: None,
            },
        })
    }

    pub fn make_window_first_value(
        inner: ExprNode,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "first_value() requires at least one order_by column.",
            ));
        }
        let dtype = match inner.dtype() {
            DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                base,
                nullable: true,
                literals: None,
            },
            other => other,
        };
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "first_value")?;
        Ok(ExprNode::Window {
            op: WindowOp::FirstValue,
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn make_window_last_value(
        inner: ExprNode,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "last_value() requires at least one order_by column.",
            ));
        }
        let dtype = match inner.dtype() {
            DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                base,
                nullable: true,
                literals: None,
            },
            other => other,
        };
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "last_value")?;
        Ok(ExprNode::Window {
            op: WindowOp::LastValue,
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn make_window_nth_value(
        inner: ExprNode,
        n: u32,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if n == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "nth_value() n must be >= 1.",
            ));
        }
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "nth_value() requires at least one order_by column.",
            ));
        }
        let dtype = match inner.dtype() {
            DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                base,
                nullable: true,
                literals: None,
            },
            other => other,
        };
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "nth_value")?;
        Ok(ExprNode::Window {
            op: WindowOp::NthValue { n },
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn make_window_ntile(
        ntiles: u32,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if ntiles == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "ntile() n must be >= 1.",
            ));
        }
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "ntile() requires at least one order_by column.",
            ));
        }
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "ntile")?;
        Ok(ExprNode::Window {
            op: WindowOp::NTile { n: ntiles },
            operand: None,
            partition_by,
            order_by,
            frame,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable: true,
                literals: None,
            },
        })
    }

    pub fn make_window_percent_rank(
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "percent_rank() requires at least one order_by column.",
            ));
        }
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "percent_rank")?;
        Ok(ExprNode::Window {
            op: WindowOp::PercentRank,
            operand: None,
            partition_by,
            order_by,
            frame,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Float),
                nullable: true,
                literals: None,
            },
        })
    }

    pub fn make_window_cume_dist(
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "cume_dist() requires at least one order_by column.",
            ));
        }
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "cume_dist")?;
        Ok(ExprNode::Window {
            op: WindowOp::CumeDist,
            operand: None,
            partition_by,
            order_by,
            frame,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Float),
                nullable: true,
                literals: None,
            },
        })
    }

    fn infer_window_sum_mean_dtype(inner: &ExprNode, mean: bool) -> PyResult<DTypeDesc> {
        let d = inner.dtype();
        if d.is_struct() || d.is_list() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "window sum/mean expect a numeric scalar column.",
            ));
        }
        let b = d.as_scalar_base_field().flatten().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "window sum/mean require a column with known scalar dtype.",
            )
        })?;
        match b {
            BaseType::Int => Ok(DTypeDesc::Scalar {
                base: Some(if mean { BaseType::Float } else { BaseType::Int }),
                nullable: true,
                literals: None,
            }),
            BaseType::Float => Ok(DTypeDesc::Scalar {
                base: Some(BaseType::Float),
                nullable: true,
                literals: None,
            }),
            BaseType::Decimal => Ok(DTypeDesc::Scalar {
                base: Some(if mean {
                    BaseType::Float
                } else {
                    BaseType::Decimal
                }),
                nullable: true,
                literals: None,
            }),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "window sum/mean require int, float, or decimal column.",
            )),
        }
    }

    pub fn make_window_sum(
        inner: ExprNode,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if partition_by.is_empty() && order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "window expression requires at least one partition_by or order_by column.",
            ));
        }
        let dtype = Self::infer_window_sum_mean_dtype(&inner, false)?;
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::validate_range_frame_order_keys(&frame, &order_by, "window_sum")?;
        Ok(ExprNode::Window {
            op: WindowOp::Sum,
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn make_window_mean(
        inner: ExprNode,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if partition_by.is_empty() && order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "window expression requires at least one partition_by or order_by column.",
            ));
        }
        let dtype = Self::infer_window_sum_mean_dtype(&inner, true)?;
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::validate_range_frame_order_keys(&frame, &order_by, "window_mean")?;
        Ok(ExprNode::Window {
            op: WindowOp::Mean,
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn make_window_min(
        inner: ExprNode,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if partition_by.is_empty() && order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "window expression requires at least one partition_by or order_by column.",
            ));
        }
        let dtype = Self::infer_global_min_max_dtype(&inner)?;
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::validate_range_frame_order_keys(&frame, &order_by, "window_min")?;
        Ok(ExprNode::Window {
            op: WindowOp::Min,
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn make_window_max(
        inner: ExprNode,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if partition_by.is_empty() && order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "window expression requires at least one partition_by or order_by column.",
            ));
        }
        let dtype = Self::infer_global_min_max_dtype(&inner)?;
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::validate_range_frame_order_keys(&frame, &order_by, "window_max")?;
        Ok(ExprNode::Window {
            op: WindowOp::Max,
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn make_global_sum(inner: ExprNode) -> PyResult<Self> {
        if !matches!(&inner, ExprNode::ColumnRef { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "global sum() expects a column reference expression.",
            ));
        }
        let dtype = Self::infer_window_sum_mean_dtype(&inner, false)?;
        Ok(ExprNode::GlobalAgg {
            op: GlobalAggOp::Sum,
            inner: Box::new(inner),
            dtype,
        })
    }

    pub fn make_global_mean(inner: ExprNode) -> PyResult<Self> {
        if !matches!(&inner, ExprNode::ColumnRef { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "global mean() expects a column reference expression.",
            ));
        }
        let dtype = Self::infer_window_sum_mean_dtype(&inner, true)?;
        Ok(ExprNode::GlobalAgg {
            op: GlobalAggOp::Mean,
            inner: Box::new(inner),
            dtype,
        })
    }

    pub fn make_global_count(inner: ExprNode) -> PyResult<Self> {
        if !matches!(&inner, ExprNode::ColumnRef { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "global count() expects a column reference expression.",
            ));
        }
        Ok(ExprNode::GlobalAgg {
            op: GlobalAggOp::Count,
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable: true,
                literals: None,
            },
        })
    }

    fn infer_global_min_max_dtype(inner: &ExprNode) -> PyResult<DTypeDesc> {
        let d = inner.dtype();
        if d.is_struct() || d.is_list() || d.is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "global min/max expect a numeric scalar column.",
            ));
        }
        let b = d.as_scalar_base_field().flatten().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "global min/max require a column with known scalar dtype.",
            )
        })?;
        match b {
            BaseType::Int | BaseType::Float | BaseType::Decimal => Ok(DTypeDesc::Scalar {
                base: Some(b),
                nullable: true,
                literals: None,
            }),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "global min/max require int, float, or decimal column.",
            )),
        }
    }

    pub fn make_global_min(inner: ExprNode) -> PyResult<Self> {
        if !matches!(&inner, ExprNode::ColumnRef { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "global min() expects a column reference expression.",
            ));
        }
        let dtype = Self::infer_global_min_max_dtype(&inner)?;
        Ok(ExprNode::GlobalAgg {
            op: GlobalAggOp::Min,
            inner: Box::new(inner),
            dtype,
        })
    }

    pub fn make_global_max(inner: ExprNode) -> PyResult<Self> {
        if !matches!(&inner, ExprNode::ColumnRef { .. }) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "global max() expects a column reference expression.",
            ));
        }
        let dtype = Self::infer_global_min_max_dtype(&inner)?;
        Ok(ExprNode::GlobalAgg {
            op: GlobalAggOp::Max,
            inner: Box::new(inner),
            dtype,
        })
    }

    /// Whole-frame row count (Spark `count(*)`-style), for [`DataFrame.select`] only.
    pub fn make_global_row_count() -> Self {
        ExprNode::GlobalRowCount {
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable: false,
                literals: None,
            },
        }
    }

    fn assert_row_accum_numeric_inner(inner: &ExprNode) -> PyResult<()> {
        let d = inner.dtype();
        if d.is_struct() || d.is_list() || d.is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "row accumulations expect a numeric scalar expression.",
            ));
        }
        let b = d.as_scalar_base_field().flatten().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "row accumulations require a column or expression with known scalar dtype.",
            )
        })?;
        match b {
            BaseType::Int | BaseType::Float | BaseType::Decimal => Ok(()),
            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "row accumulations require int, float, or decimal values.",
            )),
        }
    }

    /// Cumulative / `diff` / `pct_change` along scan order (no partition).
    pub fn make_row_accum(inner: ExprNode, op: RowAccumOp) -> PyResult<Self> {
        Self::assert_row_accum_numeric_inner(&inner)?;
        let dtype = match op {
            RowAccumOp::CumSum | RowAccumOp::CumProd => {
                Self::infer_window_sum_mean_dtype(&inner, false)?
            }
            RowAccumOp::CumMin | RowAccumOp::CumMax => Self::infer_global_min_max_dtype(&inner)?,
            RowAccumOp::Diff { periods } | RowAccumOp::PctChange { periods } => {
                if periods <= 0 {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "diff and pct_change require a positive periods value.",
                    ));
                }
                DTypeDesc::Scalar {
                    base: Some(BaseType::Float),
                    nullable: true,
                    literals: None,
                }
            }
        };
        Ok(ExprNode::RowAccum {
            op,
            inner: Box::new(inner),
            dtype,
        })
    }

    pub fn make_strptime(inner: ExprNode, format: String, to_datetime: bool) -> PyResult<Self> {
        if format.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "strptime format must be non-empty.",
            ));
        }
        if inner.dtype().as_scalar_base_field().flatten() != Some(BaseType::Str) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "strptime() requires a string column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        let dtype = if to_datetime {
            DTypeDesc::Scalar {
                base: Some(BaseType::DateTime),
                nullable,
                literals: None,
            }
        } else {
            DTypeDesc::Scalar {
                base: Some(BaseType::Date),
                nullable,
                literals: None,
            }
        };
        Ok(ExprNode::Strptime {
            inner: Box::new(inner),
            format,
            to_datetime,
            dtype,
        })
    }

    pub fn make_unix_timestamp(inner: ExprNode, unit: UnixTimestampUnit) -> PyResult<Self> {
        let b = inner.dtype().as_scalar_base_field().flatten();
        if b != Some(BaseType::Date) && b != Some(BaseType::DateTime) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "unix_timestamp() requires a date or datetime column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::UnixTimestamp {
            inner: Box::new(inner),
            unit,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_from_unix_time(inner: ExprNode, unit: UnixTimestampUnit) -> PyResult<Self> {
        let b = inner.dtype().as_scalar_base_field().flatten();
        if b != Some(BaseType::Int) && b != Some(BaseType::Float) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "from_unix_time() requires an int or float column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::FromUnixTime {
            inner: Box::new(inner),
            unit,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::DateTime),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_binary_length(inner: ExprNode) -> PyResult<Self> {
        if inner.dtype().as_scalar_base_field().flatten() != Some(BaseType::Binary) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "binary_len() requires a bytes column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::BinaryLength {
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_map_len(inner: ExprNode) -> PyResult<Self> {
        if !inner.dtype().is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_len() requires a map column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::MapLen {
            inner: Box::new(inner),
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Int),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_map_get(inner: ExprNode, key: String) -> PyResult<Self> {
        if !inner.dtype().is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_get() requires a map column.",
            ));
        }
        let dtype_src = inner.dtype();
        let DTypeDesc::Map { value, .. } = dtype_src else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "internal error: expected map dtype after is_map() check",
            ));
        };
        let dtype = match value.as_ref().clone() {
            DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                base,
                nullable: true,
                literals: None,
            },
            DTypeDesc::Struct { fields, .. } => DTypeDesc::Struct {
                fields,
                nullable: true,
            },
            DTypeDesc::List { inner: li, .. } => DTypeDesc::List {
                inner: li,
                nullable: true,
            },
            DTypeDesc::Map { value: v, .. } => DTypeDesc::Map {
                value: v,
                nullable: true,
            },
        };
        Ok(ExprNode::MapGet {
            inner: Box::new(inner),
            key,
            dtype,
        })
    }

    pub fn make_map_contains_key(inner: ExprNode, key: String) -> PyResult<Self> {
        if !inner.dtype().is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_contains_key() requires a map column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::MapContainsKey {
            inner: Box::new(inner),
            key,
            dtype: DTypeDesc::Scalar {
                base: Some(BaseType::Bool),
                nullable,
                literals: None,
            },
        })
    }

    pub fn make_map_keys(inner: ExprNode) -> PyResult<Self> {
        if !inner.dtype().is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_keys() requires a map column.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::MapKeys {
            inner: Box::new(inner),
            dtype: DTypeDesc::List {
                inner: Box::new(DTypeDesc::Scalar {
                    base: Some(BaseType::Str),
                    nullable: false,
                    literals: None,
                }),
                nullable,
            },
        })
    }

    pub fn make_map_values(inner: ExprNode) -> PyResult<Self> {
        if !inner.dtype().is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_values() requires a map column.",
            ));
        }
        let dtype_src = inner.dtype();
        let DTypeDesc::Map { value, .. } = dtype_src else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "internal error: expected map dtype after is_map() check",
            ));
        };
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::MapValues {
            inner: Box::new(inner),
            dtype: DTypeDesc::List {
                inner: Box::new(value.as_ref().clone()),
                nullable,
            },
        })
    }

    pub fn make_map_entries(inner: ExprNode) -> PyResult<Self> {
        if !inner.dtype().is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_entries() requires a map column.",
            ));
        }
        let dtype_src = inner.dtype();
        let DTypeDesc::Map { value, .. } = dtype_src else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "internal error: expected map dtype after is_map() check",
            ));
        };
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::MapEntries {
            inner: Box::new(inner),
            dtype: DTypeDesc::List {
                inner: Box::new(DTypeDesc::Struct {
                    fields: vec![
                        (
                            "key".to_string(),
                            DTypeDesc::Scalar {
                                base: Some(BaseType::Str),
                                nullable: false,
                                literals: None,
                            },
                        ),
                        ("value".to_string(), value.as_ref().clone()),
                    ],
                    nullable: false,
                }),
                nullable,
            },
        })
    }

    pub fn make_map_from_entries(inner: ExprNode) -> PyResult<Self> {
        let DTypeDesc::List {
            inner: entry_dtype, ..
        } = inner.dtype()
        else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_from_entries() requires a list of entry structs.",
            ));
        };
        let DTypeDesc::Struct { fields, .. } = entry_dtype.as_ref() else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_from_entries() requires a list of entry structs.",
            ));
        };
        if fields.len() != 2 || fields[0].0 != "key" || fields[1].0 != "value" {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_from_entries() requires struct fields ['key', 'value'] in order.",
            ));
        }
        let key_dtype = &fields[0].1;
        if key_dtype.as_scalar_base_field().flatten() != Some(BaseType::Str) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "map_from_entries() requires a string key field.",
            ));
        }
        let nullable = inner.dtype().nullable_flag();
        Ok(ExprNode::MapFromEntries {
            inner: Box::new(inner),
            dtype: DTypeDesc::Map {
                value: Box::new(fields[1].1.clone()),
                nullable,
            },
        })
    }

    fn window_shift_operand_dtype(inner: &ExprNode) -> PyResult<DTypeDesc> {
        let d = inner.dtype();
        if d.is_struct() || d.is_list() || d.is_map() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "lag/lead expect a scalar column expression.",
            ));
        }
        let base = d.as_scalar_base_field().flatten().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "lag/lead require a column with known scalar dtype.",
            )
        })?;
        Ok(DTypeDesc::Scalar {
            base: Some(base),
            nullable: true,
            literals: None,
        })
    }

    pub fn make_window_lag(
        inner: ExprNode,
        n: u32,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if partition_by.is_empty() && order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "window expression requires at least one partition_by or order_by column.",
            ));
        }
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "lag() requires order_by columns.",
            ));
        }
        let dtype = Self::window_shift_operand_dtype(&inner)?;
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "lag")?;
        Ok(ExprNode::Window {
            op: WindowOp::Lag { n },
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn make_window_lead(
        inner: ExprNode,
        n: u32,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        frame_kind: Option<String>,
        frame_start: Option<i64>,
        frame_end: Option<i64>,
    ) -> PyResult<Self> {
        if partition_by.is_empty() && order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "window expression requires at least one partition_by or order_by column.",
            ));
        }
        if order_by.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "lead() requires order_by columns.",
            ));
        }
        let dtype = Self::window_shift_operand_dtype(&inner)?;
        let frame = Self::parse_window_frame(frame_kind, frame_start, frame_end)?;
        Self::reject_range_frame(&frame, "lead")?;
        Ok(ExprNode::Window {
            op: WindowOp::Lead { n },
            operand: Some(Box::new(inner)),
            partition_by,
            order_by,
            frame,
            dtype,
        })
    }

    pub fn global_agg_default_alias(&self) -> Option<String> {
        match self {
            ExprNode::GlobalAgg { op, inner, .. } => {
                let ExprNode::ColumnRef { name, .. } = inner.as_ref() else {
                    return None;
                };
                Some(match op {
                    GlobalAggOp::Sum => format!("sum_{name}"),
                    GlobalAggOp::Mean => format!("mean_{name}"),
                    GlobalAggOp::Count => format!("count_{name}"),
                    GlobalAggOp::Min => format!("min_{name}"),
                    GlobalAggOp::Max => format!("max_{name}"),
                })
            }
            ExprNode::GlobalRowCount { .. } => Some("row_count".to_string()),
            _ => None,
        }
    }

    #[cfg(not(feature = "polars_engine"))]
    pub fn eval(
        &self,
        ctx: &HashMap<String, Vec<Option<LiteralValue>>>,
        n: usize,
    ) -> PyResult<Vec<Option<LiteralValue>>> {
        super::eval_rowwise::eval_expr_node(self, ctx, n)
    }
}
