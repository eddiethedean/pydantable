//! JSON-ish serialization of [`ExprNode`] for planners / Python.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use pyo3::IntoPyObjectExt;

use crate::dtype::{dtype_to_descriptor_py, BaseType};

use super::ir::{
    ArithOp, CmpOp, ExprNode, GlobalAggOp, LiteralValue, LogicalOp, RowAccumOp,
    StringPredicateKind, StringUnaryOp, TemporalPart, UnaryNumericOp, UnixTimestampUnit,
    WindowFrame, WindowOp,
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
        BaseType::Ipv4 => "ipv4",
        BaseType::Ipv6 => "ipv6",
        BaseType::Wkb => "wkb",
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
    let dict = PyDict::new(py);

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
                Some(LiteralValue::Int(v)) => v.into_py_any(py)?,
                Some(LiteralValue::Float(v)) => v.into_py_any(py)?,
                Some(LiteralValue::Bool(v)) => v.into_py_any(py)?,
                Some(LiteralValue::Str(v)) => v.clone().into_py_any(py)?,
                Some(LiteralValue::Uuid(v)) => v.clone().into_py_any(py)?,
                Some(LiteralValue::Decimal(v)) => v.into_py_any(py)?,
                Some(LiteralValue::EnumStr(v)) => v.clone().into_py_any(py)?,
                Some(LiteralValue::DateTimeMicros(v)) => v.into_py_any(py)?,
                Some(LiteralValue::DateDays(v)) => v.into_py_any(py)?,
                Some(LiteralValue::DurationMicros(v)) => v.into_py_any(py)?,
                Some(LiteralValue::TimeNanos(v)) => v.into_py_any(py)?,
                Some(LiteralValue::Binary(b)) => PyBytes::new(py, b).into_py_any(py)?,
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
            let list = PyList::empty(py);
            for e in exprs {
                list.append(exprnode_to_serializable(py, e)?)?;
            }
            dict.set_item("exprs", list)?;
        }
        ExprNode::CaseWhen {
            branches, else_, ..
        } => {
            dict.set_item("kind", "case_when")?;
            let list = PyList::empty(py);
            for (c, t) in branches {
                let br = PyDict::new(py);
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
            let list = PyList::empty(py);
            for v in values {
                let pyv = match v {
                    LiteralValue::Int(i) => i.into_py_any(py)?,
                    LiteralValue::Float(f) => f.into_py_any(py)?,
                    LiteralValue::Bool(b) => b.into_py_any(py)?,
                    LiteralValue::Str(s) => s.clone().into_py_any(py)?,
                    LiteralValue::Uuid(s) => s.clone().into_py_any(py)?,
                    LiteralValue::Decimal(i) => i.into_py_any(py)?,
                    LiteralValue::EnumStr(s) => s.clone().into_py_any(py)?,
                    LiteralValue::DateTimeMicros(v) => v.into_py_any(py)?,
                    LiteralValue::DateDays(v) => v.into_py_any(py)?,
                    LiteralValue::DurationMicros(v) => v.into_py_any(py)?,
                    LiteralValue::TimeNanos(v) => v.into_py_any(py)?,
                    LiteralValue::Binary(b) => PyBytes::new(py, b).into_py_any(py)?,
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
            let list = PyList::empty(py);
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
            literal,
            ..
        } => {
            dict.set_item("kind", "string_replace")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("pattern", pattern.as_str())?;
            dict.set_item("replacement", replacement.as_str())?;
            dict.set_item("literal", *literal)?;
        }
        ExprNode::StringPredicate {
            inner,
            kind,
            pattern,
            ..
        } => {
            dict.set_item("kind", "string_predicate")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("pattern", pattern.as_str())?;
            match kind {
                StringPredicateKind::StartsWith => dict.set_item("op", "starts_with")?,
                StringPredicateKind::EndsWith => dict.set_item("op", "ends_with")?,
                StringPredicateKind::Contains { literal } => {
                    dict.set_item("op", "contains")?;
                    dict.set_item("literal", *literal)?;
                }
            }
        }
        ExprNode::StructField { base, field, .. } => {
            dict.set_item("kind", "struct_field")?;
            dict.set_item("base", exprnode_to_serializable(py, base)?)?;
            dict.set_item("field", field)?;
        }
        ExprNode::StructJsonEncode { base, .. } => {
            dict.set_item("kind", "struct_json_encode")?;
            dict.set_item("base", exprnode_to_serializable(py, base)?)?;
        }
        ExprNode::StructJsonPathMatch { base, path, .. } => {
            dict.set_item("kind", "struct_json_path_match")?;
            dict.set_item("base", exprnode_to_serializable(py, base)?)?;
            dict.set_item("path", path.as_str())?;
        }
        ExprNode::StructRenameFields { base, names, .. } => {
            dict.set_item("kind", "struct_rename_fields")?;
            dict.set_item("base", exprnode_to_serializable(py, base)?)?;
            dict.set_item("names", names.clone())?;
        }
        ExprNode::StructWithFields { base, updates, .. } => {
            dict.set_item("kind", "struct_with_fields")?;
            dict.set_item("base", exprnode_to_serializable(py, base)?)?;
            let upd = pyo3::types::PyList::empty(py);
            for (n, e) in updates {
                let pair = pyo3::types::PyList::empty(py);
                pair.append(n)?;
                pair.append(exprnode_to_serializable(py, e.as_ref())?)?;
                upd.append(pair)?;
            }
            dict.set_item("updates", upd)?;
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
                StringUnaryOp::Reverse => {
                    dict.set_item("op", "reverse")?;
                }
                StringUnaryOp::PadStart { length, fill_char } => {
                    dict.set_item("op", "pad_start")?;
                    dict.set_item("length", *length)?;
                    dict.set_item("fill_char", fill_char.to_string())?;
                }
                StringUnaryOp::PadEnd { length, fill_char } => {
                    dict.set_item("op", "pad_end")?;
                    dict.set_item("length", *length)?;
                    dict.set_item("fill_char", fill_char.to_string())?;
                }
                StringUnaryOp::ZFill { length } => {
                    dict.set_item("op", "zfill")?;
                    dict.set_item("length", *length)?;
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
                    TemporalPart::Weekday => "weekday",
                    TemporalPart::Quarter => "quarter",
                    TemporalPart::Week => "week",
                    TemporalPart::DayOfYear => "dayofyear",
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
        ExprNode::ListMean { inner, .. } => {
            dict.set_item("kind", "list_mean")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::ListJoin {
            inner,
            separator,
            ignore_nulls,
            ..
        } => {
            dict.set_item("kind", "list_join")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("separator", separator.as_str())?;
            dict.set_item("ignore_nulls", *ignore_nulls)?;
        }
        ExprNode::ListSort {
            inner,
            descending,
            nulls_last,
            maintain_order,
            ..
        } => {
            dict.set_item("kind", "list_sort")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("descending", *descending)?;
            dict.set_item("nulls_last", *nulls_last)?;
            dict.set_item("maintain_order", *maintain_order)?;
        }
        ExprNode::ListUnique { inner, stable, .. } => {
            dict.set_item("kind", "list_unique")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("stable", *stable)?;
        }
        ExprNode::StringSplit {
            inner, delimiter, ..
        } => {
            dict.set_item("kind", "string_split")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("delimiter", delimiter.as_str())?;
        }
        ExprNode::StringExtract {
            inner,
            pattern,
            group_index,
            ..
        } => {
            dict.set_item("kind", "string_extract")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("pattern", pattern.as_str())?;
            dict.set_item("group_index", *group_index)?;
        }
        ExprNode::StringJsonPathMatch { inner, path, .. } => {
            dict.set_item("kind", "string_json_path_match")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("path", path.as_str())?;
        }
        ExprNode::StringJsonDecode { inner, target, .. } => {
            dict.set_item("kind", "string_json_decode")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
            dict.set_item("target", crate::dtype::dtype_to_descriptor_py(py, target)?)?;
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
        ExprNode::FromUnixTime { inner, unit, .. } => {
            dict.set_item("kind", "from_unix_time")?;
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
        ExprNode::MapKeys { inner, .. } => {
            dict.set_item("kind", "map_keys")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::MapValues { inner, .. } => {
            dict.set_item("kind", "map_values")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::MapEntries { inner, .. } => {
            dict.set_item("kind", "map_entries")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::MapFromEntries { inner, .. } => {
            dict.set_item("kind", "map_from_entries")?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
        }
        ExprNode::RowAccum { op, inner, .. } => {
            dict.set_item("kind", "row_accum")?;
            dict.set_item(
                "op",
                match op {
                    RowAccumOp::CumSum => "cum_sum",
                    RowAccumOp::CumProd => "cum_prod",
                    RowAccumOp::CumMin => "cum_min",
                    RowAccumOp::CumMax => "cum_max",
                    RowAccumOp::Diff { periods } => {
                        dict.set_item("periods", periods)?;
                        "diff"
                    }
                    RowAccumOp::PctChange { periods } => {
                        dict.set_item("periods", periods)?;
                        "pct_change"
                    }
                },
            )?;
            dict.set_item("inner", exprnode_to_serializable(py, inner)?)?;
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
                WindowOp::FirstValue => dict.set_item("op", "first_value")?,
                WindowOp::LastValue => dict.set_item("op", "last_value")?,
                WindowOp::NthValue { n } => {
                    dict.set_item("op", "nth_value")?;
                    dict.set_item("n", *n)?;
                }
                WindowOp::NTile { n } => {
                    dict.set_item("op", "ntile")?;
                    dict.set_item("n", *n)?;
                }
                WindowOp::PercentRank => dict.set_item("op", "percent_rank")?,
                WindowOp::CumeDist => dict.set_item("op", "cume_dist")?,
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
                    let f = PyDict::new(py);
                    f.set_item("kind", "rows")?;
                    f.set_item("start", *start)?;
                    f.set_item("end", *end)?;
                    dict.set_item("frame", f)?;
                }
                Some(WindowFrame::Range { start, end }) => {
                    let f = PyDict::new(py);
                    f.set_item("kind", "range")?;
                    f.set_item("start", *start)?;
                    f.set_item("end", *end)?;
                    dict.set_item("frame", f)?;
                }
            }
            if let Some(op) = operand {
                dict.set_item("operand", exprnode_to_serializable(py, op)?)?;
            }
            dict.set_item("partition_by", partition_by.clone().into_py_any(py)?)?;
            let ord_list = PyList::empty(py);
            for (name, asc, nulls_last) in order_by {
                let t = PyList::new(
                    py,
                    [
                        name.clone().into_py_any(py)?,
                        (*asc).into_py_any(py)?,
                        (*nulls_last).into_py_any(py)?,
                    ],
                )?;
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

    Ok(dict.unbind().into())
}
