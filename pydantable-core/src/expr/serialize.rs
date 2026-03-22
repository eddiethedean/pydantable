//! JSON-ish serialization of [`ExprNode`] for planners / Python.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::dtype::{dtype_to_descriptor_py, BaseType};

use super::ir::{ArithOp, CmpOp, ExprNode, LiteralValue};

fn base_type_json(b: BaseType) -> &'static str {
    match b {
        BaseType::Int => "int",
        BaseType::Float => "float",
        BaseType::Bool => "bool",
        BaseType::Str => "str",
        BaseType::DateTime => "datetime",
        BaseType::Date => "date",
        BaseType::Duration => "duration",
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
                Some(LiteralValue::DateTimeMicros(v)) => v.into_py(py),
                Some(LiteralValue::DateDays(v)) => v.into_py(py),
                Some(LiteralValue::DurationMicros(v)) => v.into_py(py),
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
            if let crate::dtype::DTypeDesc::Scalar {
                base: Some(b), ..
            } = dtype
            {
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
                    LiteralValue::DateTimeMicros(v) => v.into_py(py),
                    LiteralValue::DateDays(v) => v.into_py(py),
                    LiteralValue::DurationMicros(v) => v.into_py(py),
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
        ExprNode::StructField { base, field, .. } => {
            dict.set_item("kind", "struct_field")?;
            dict.set_item("base", exprnode_to_serializable(py, base)?)?;
            dict.set_item("field", field)?;
        }
    }

    Ok(dict.into_py(py))
}
