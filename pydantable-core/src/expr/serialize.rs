//! JSON-ish serialization of [`ExprNode`] for planners / Python.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

use crate::dtype::{dtype_to_descriptor_py, BaseType};

use super::ir::{
    ArithOp, CmpOp, ExprNode, GlobalAggOp, LiteralValue, LogicalOp, StringUnaryOp, TemporalPart,
    UnaryNumericOp, UnixTimestampUnit, WindowFrame, WindowOp,
};

fn base_type_json(b: BaseType) -> &'static str {
    match b {
        BaseType::Int => "int",
        BaseType::Float => "float",
        BaseType::Bool => "bool",
        BaseType::Str => "str",
        BaseType::Uuid => "uuid",
        BaseType::Decimal => "decimal",
        BaseType::Enum => "enum",
        BaseType::DateTime => "datetime",
        BaseType::Date => "date",
        BaseType::Duration => "duration",
        BaseType::Time => "time",
        BaseType::Binary => "binary",
    }
}

fn arith_op_to_str(op: &ArithOp) -> &'static str {
    match op {
        ArithOp::Add => "add",
        ArithOp::Sub => "sub",
        ArithOp::Mul => "mul",
        ArithOp::Div => "div",
    }
}

fn cmp_op_to_str(op: &CmpOp) -> &'static str {
    match op {
        CmpOp::Eq => "eq",
        CmpOp::Ne => "ne",
        CmpOp::Lt => "lt",
        CmpOp::Le => "le",
        CmpOp::Gt => "gt",
        CmpOp::Ge => "ge",
    }
}

pub fn exprnode_to_serializable(py: Python<'_>, node: &ExprNode) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);

    dict.set_item("dtype", dtype_to_descriptor_py(py, &node.dtype())?)?;

    match node {
        ExprNode::ColumnRef { name, .. } => {
            dict.set_item("kind", "column_ref")?;
            dict.set_item("name", name)?;
        }
        ExprNode::Literal { value, .. } => {
            dict.set_item("kind", "literal")?;
            let value_obj = match value {
                None => py.None(),
                Some(LiteralValue::Int(v)) => v.into_py(py),
                Some(LiteralValue::Float(v)) => v.into_py(py),
                Some(LiteralValue::Bool(v)) => v.into_py(py),
                Some(LiteralValue::Str(v)) => v.clone().into_py(py),
                Some(LiteralValue::Uuid(v)) => v.clone().into_py(py),
                Some(LiteralValue::Decimal(v)) => v.into_py(py),
                Some(LiteralValue::EnumStr(v)) => v.clone().into_py(py),
                Some(LiteralValue::DateTimeMicros(v)) => v.into_py(py),
                Some(LiteralValue::DateDays(v)) => v.into_py(py),
                Some(LiteralValue::DurationMicros(v)) => v.into_py(py),
                Some(LiteralValue::TimeNanos(v)) => v.into_py(py),
                Some(LiteralValue::Binary(b)) => PyBytes::new(py, b).into_py(py),
            };
            dict.set_item("value", value_obj)?;
        }
        ExprNode::BinaryOp {
            op, left, right, ..
        } => {
            dict.set_item("kind", "binary_op")?;
            dict.set_item("op", arith_op_to_str(op))?;
            dict.set_item("left", exprnode_to_serializable(py, left)?)?;
            dict.set_item("right", exprnode_to_serializable(py, right)?)?;
        }
        ExprNode::CompareOp {
            op, left, right, ..
        } => {
            dict.set_item("kind", "compare_op")?;
            dict.set_item("op", cmp_op_to_str(op))?;
            dict.set_item("left", exprnode_to_serializable(py, left)?)?;
            dict.set_item("right", exprnode_to_serializable(py, right)?)?;
        }
        ExprNode::Cast { input, dtype } => {
            dict.set_item("kind", "cast")?;
            dict.set_item("input", exprnode_to_serializable(py, input)?)?;
            dict.set_item("inner", exprnode_to_serializable(py, input)?)?;
            if let crate::dtype::DTypeDesc::Scalar { base: Some(b), .. } = dtype {
                dict.set_item("to", base_type_json(*b))?;
            }
        }
        ExprNode::IsNull { input, .. } => {
            dict.set_item("kind", "is_null")?;
            dict.set_item("input", exprnode_to_serializable(py, input)?)?;
            dict.set_item("inner", exprnode_to_serializable(py, input)?)?;
        }
        ExprNode::IsNotNull { input, .. } => {
            dict.set_item("kind", "is_not_null")?;
            dict.set_item("input", exprnode_to_serializable(py, input)?)?;
            dict.set_item("inner", exprnode_to_serializable(py, input)?)?;
        }
        ExprNode::Coalesce { exprs, .. } => {
            dict.set_item("kind", "coalesce")?;
            let list = PyList::empty_bound(py);
            for e in exprs {
                list.append(exprnode_to_serializable(py, e)?)?;
            }
            dict.set_item("exprs", list)?;
        }
        ExprNode::CaseWhen {
            branches, else_, ..
        } => {
            dict.set_item("kind", "case_when")?;
            let list = PyList::empty_bound(py);
            for (c, t) in branches {
                let br = PyDict::new_bound(py);
                br.set_item("condition", exprnode_to_serializable(py, c)?)?;
                br.set_item("then", exprnode_to_serializable(py, t)?)?;
                list.append(br)?;
            }
            dict.set_item("branches", list)?;
            dict.set_item("else", exprnode_to_serializable(py, else_)?)?;
        }
        ExprNode::InList { inner, values, .. } => {
            dict.set_item("kind", "in_list")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            let list = PyList::empty_bound(py);
            for v in values {
                let pyv = match v {
                    LiteralValue::Int(i) => i.into_py(py),
                    LiteralValue::Float(f) => f.into_py(py),
                    LiteralValue::Bool(b) => b.into_py(py),
                    LiteralValue::Str(s) => s.clone().into_py(py),
                    LiteralValue::Uuid(s) => s.clone().into_py(py),
                    LiteralValue::Decimal(i) => i.into_py(py),
                    LiteralValue::EnumStr(s) => s.clone().into_py(py),
                    LiteralValue::DateTimeMicros(v) => v.into_py(py),
                    LiteralValue::DateDays(v) => v.into_py(py),
                    LiteralValue::DurationMicros(v) => v.into_py(py),
                    LiteralValue::TimeNanos(v) => v.into_py(py),
                    LiteralValue::Binary(b) => PyBytes::new(py, b).into_py(py),
                };
                list.append(pyv)?;
            }
            dict.set_item("values", list)?;
        }
        ExprNode::Between {
            inner, low, high, ..
        } => {
            dict.set_item("kind", "between")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("low", exprnode_to_serializable(py, low)?)?;
            dict.set_item("high", exprnode_to_serializable(py, high)?)?;
        }
        ExprNode::StringConcat { parts, .. } => {
            dict.set_item("kind", "string_concat")?;
            let list = PyList::empty_bound(py);
            for p in parts {
                list.append(exprnode_to_serializable(py, p)?)?;
            }
            dict.set_item("parts", list)?;
        }
        ExprNode::Substring {
            inner,
            start,
            length,
            ..
        } => {
            dict.set_item("kind", "substring")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("start", exprnode_to_serializable(py, start)?)?;
            match length {
                Some(l) => {
                    dict.set_item("length", exprnode_to_serializable(py, l)?)?;
                }
                None => {
                    dict.set_item("length", py.None())?;
                }
            }
        }
        ExprNode::StringLength { inner, .. } => {
            dict.set_item("kind", "string_length")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::StringReplace {
            inner,
            pattern,
            replacement,
            ..
        } => {
            dict.set_item("kind", "string_replace")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("pattern", pattern.as_str())?;
            dict.set_item("replacement", replacement.as_str())?;
        }
        ExprNode::StructField { base, field, .. } => {
            dict.set_item("kind", "struct_field")?;
            dict.set_item("base", exprnode_to_serializable(py, base)?)?;
            dict.set_item("field", field)?;
        }
        ExprNode::UnaryNumeric { op, inner, .. } => {
            dict.set_item("kind", "unary_numeric")?;
            let (op_s, dec) = match op {
                UnaryNumericOp::Abs => ("abs", None),
                UnaryNumericOp::Round { decimals } => ("round", Some(*decimals)),
                UnaryNumericOp::Floor => ("floor", None),
                UnaryNumericOp::Ceil => ("ceil", None),
            };
            dict.set_item("op", op_s)?;
            if let Some(d) = dec {
                dict.set_item("decimals", d)?;
            }
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::StringUnary { op, inner, .. } => {
            dict.set_item("kind", "string_unary")?;
            match op {
                StringUnaryOp::Strip => {
                    dict.set_item("op", "strip")?;
                }
                StringUnaryOp::Upper => {
                    dict.set_item("op", "upper")?;
                }
                StringUnaryOp::Lower => {
                    dict.set_item("op", "lower")?;
                }
                StringUnaryOp::StripPrefix(p) => {
                    dict.set_item("op", "strip_prefix")?;
                    dict.set_item("pattern", p.as_str())?;
                }
                StringUnaryOp::StripSuffix(s) => {
                    dict.set_item("op", "strip_suffix")?;
                    dict.set_item("pattern", s.as_str())?;
                }
                StringUnaryOp::StripChars(c) => {
                    dict.set_item("op", "strip_chars")?;
                    dict.set_item("chars", c.as_str())?;
                }
            }
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::LogicalBinary {
            op, left, right, ..
        } => {
            dict.set_item("kind", "logical_binary")?;
            dict.set_item(
                "op",
                match op {
                    LogicalOp::And => "and",
                    LogicalOp::Or => "or",
                },
            )?;
            dict.set_item("left", exprnode_to_serializable(py, left)?)?;
            dict.set_item("right", exprnode_to_serializable(py, right)?)?;
        }
        ExprNode::LogicalNot { inner, .. } => {
            dict.set_item("kind", "logical_not")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::TemporalPart { part, inner, .. } => {
            dict.set_item("kind", "temporal_part")?;
            dict.set_item(
                "part",
                match part {
                    TemporalPart::Year => "year",
                    TemporalPart::Month => "month",
                    TemporalPart::Day => "day",
                    TemporalPart::Hour => "hour",
                    TemporalPart::Minute => "minute",
                    TemporalPart::Second => "second",
                    TemporalPart::Nanosecond => "nanosecond",
                },
            )?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::ListLen { inner, .. } => {
            dict.set_item("kind", "list_len")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::ListGet { inner, index, .. } => {
            dict.set_item("kind", "list_get")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("index", exprnode_to_serializable(py, index)?)?;
        }
        ExprNode::ListContains { inner, value, .. } => {
            dict.set_item("kind", "list_contains")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("value", exprnode_to_serializable(py, value)?)?;
        }
        ExprNode::ListMin { inner, .. } => {
            dict.set_item("kind", "list_min")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::ListMax { inner, .. } => {
            dict.set_item("kind", "list_max")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::ListSum { inner, .. } => {
            dict.set_item("kind", "list_sum")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::DatetimeToDate { inner, .. } => {
            dict.set_item("kind", "datetime_to_date")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::Strptime {
            inner,
            format,
            to_datetime,
            ..
        } => {
            dict.set_item("kind", "strptime")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("format", format.as_str())?;
            dict.set_item("to_datetime", *to_datetime)?;
        }
        ExprNode::UnixTimestamp { inner, unit, .. } => {
            dict.set_item("kind", "unix_timestamp")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item(
                "unit",
                match unit {
                    UnixTimestampUnit::Seconds => "seconds",
                    UnixTimestampUnit::Milliseconds => "milliseconds",
                },
            )?;
        }
        ExprNode::BinaryLength { inner, .. } => {
            dict.set_item("kind", "binary_length")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::MapLen { inner, .. } => {
            dict.set_item("kind", "map_len")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::MapGet { inner, key, .. } => {
            dict.set_item("kind", "map_get")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("key", key)?;
        }
        ExprNode::MapContainsKey { inner, key, .. } => {
            dict.set_item("kind", "map_contains_key")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("key", key)?;
        }
        ExprNode::Window {
            op,
            operand,
            partition_by,
            order_by,
            frame,
            ..
        } => {
            dict.set_item("kind", "window")?;
            match op {
                WindowOp::RowNumber => dict.set_item("op", "row_number")?,
                WindowOp::Rank => dict.set_item("op", "rank")?,
                WindowOp::DenseRank => dict.set_item("op", "dense_rank")?,
                WindowOp::Sum => dict.set_item("op", "sum")?,
                WindowOp::Mean => dict.set_item("op", "mean")?,
                WindowOp::Min => dict.set_item("op", "min")?,
                WindowOp::Max => dict.set_item("op", "max")?,
                WindowOp::Lag { n } => {
                    dict.set_item("op", "lag")?;
                    dict.set_item("n", *n)?;
                }
                WindowOp::Lead { n } => {
                    dict.set_item("op", "lead")?;
                    dict.set_item("n", *n)?;
                }
            }
            match frame {
                None => dict.set_item("frame", py.None())?,
                Some(WindowFrame::Rows { start, end }) => {
                    let f = PyDict::new_bound(py);
                    f.set_item("kind", "rows")?;
                    f.set_item("start", *start)?;
                    f.set_item("end", *end)?;
                    dict.set_item("frame", f)?;
                }
                Some(WindowFrame::Range { start, end }) => {
                    let f = PyDict::new_bound(py);
                    f.set_item("kind", "range")?;
                    f.set_item("start", *start)?;
                    f.set_item("end", *end)?;
                    dict.set_item("frame", f)?;
                }
            }
            if let Some(op) = operand {
                dict.set_item("operand", exprnode_to_serializable(py, op)?)?;
            }
            dict.set_item("partition_by", partition_by.clone().into_py(py))?;
            let ord_list = PyList::empty_bound(py);
            for (name, asc) in order_by {
                let t = PyList::new_bound(py, [name.into_py(py), (*asc).into_py(py)]);
                ord_list.append(t)?;
            }
            dict.set_item("order_by", ord_list)?;
        }
        ExprNode::GlobalAgg { op, inner, .. } => {
            dict.set_item("kind", "global_agg")?;
            dict.set_item(
                "op",
                match op {
                    GlobalAggOp::Sum => "sum",
                    GlobalAggOp::Mean => "mean",
                    GlobalAggOp::Count => "count",
                    GlobalAggOp::Min => "min",
                    GlobalAggOp::Max => "max",
                },
            )?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::GlobalRowCount { .. } => {
            dict.set_item("kind", "global_row_count")?;
        }
    }

    Ok(dict.into_py(py))
}
