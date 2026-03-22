//! Expression IR: operators, literals, and the [`ExprNode`] AST.

use crate::dtype::DTypeDesc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LiteralValue {
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
    DateTimeMicros(i64),
    DateDays(i32),
    DurationMicros(i64),
}

#[derive(Clone, Debug)]
pub enum ExprNode {
    ColumnRef {
        name: String,
        dtype: DTypeDesc,
    },
    Literal {
        value: Option<LiteralValue>,
        dtype: DTypeDesc,
    },
    BinaryOp {
        op: ArithOp,
        left: Box<ExprNode>,
        right: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    CompareOp {
        op: CmpOp,
        left: Box<ExprNode>,
        right: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    Cast {
        input: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    IsNull {
        input: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    IsNotNull {
        input: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    Coalesce {
        exprs: Vec<ExprNode>,
        dtype: DTypeDesc,
    },
    CaseWhen {
        branches: Vec<(ExprNode, ExprNode)>,
        else_: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    InList {
        inner: Box<ExprNode>,
        values: Vec<LiteralValue>,
        dtype: DTypeDesc,
    },
    Between {
        inner: Box<ExprNode>,
        low: Box<ExprNode>,
        high: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    StringConcat {
        parts: Vec<ExprNode>,
        dtype: DTypeDesc,
    },
    Substring {
        inner: Box<ExprNode>,
        start: Box<ExprNode>,
        length: Option<Box<ExprNode>>,
        dtype: DTypeDesc,
    },
    StringLength {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    /// Extract one field from a struct-typed expression (Polars `struct.field`).
    StructField {
        base: Box<ExprNode>,
        field: String,
        dtype: DTypeDesc,
    },
}
