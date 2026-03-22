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
    /// Canonical string form (e.g. `8f4b2e1a-...`) for `uuid.UUID` logical type.
    Uuid(String),
    /// `decimal.Decimal` scaled to Polars `Decimal(`[`DECIMAL_PRECISION`](crate::dtype::DECIMAL_PRECISION)`, `[`DECIMAL_SCALE`](crate::dtype::DECIMAL_SCALE)`)`.
    Decimal(i128),
    /// String form of an `enum.Enum` member (typically `str(member.value)`).
    EnumStr(String),
    DateTimeMicros(i64),
    DateDays(i32),
    DurationMicros(i64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryNumericOp {
    Abs,
    Round { decimals: u32 },
    Floor,
    Ceil,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StringUnaryOp {
    Strip,
    Upper,
    Lower,
    /// Remove a single literal prefix when present (Polars `strip_prefix`).
    StripPrefix(String),
    /// Remove a single literal suffix when present (Polars `strip_suffix`).
    StripSuffix(String),
    /// Trim any of the given characters from both ends (Polars `strip_chars`).
    StripChars(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalOp {
    And,
    Or,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemporalPart {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
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
    /// Replace all non-overlapping occurrences of `pattern` (literal substring).
    StringReplace {
        inner: Box<ExprNode>,
        pattern: String,
        replacement: String,
        dtype: DTypeDesc,
    },
    /// Extract one field from a struct-typed expression (Polars `struct.field`).
    StructField {
        base: Box<ExprNode>,
        field: String,
        dtype: DTypeDesc,
    },
    UnaryNumeric {
        op: UnaryNumericOp,
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    StringUnary {
        op: StringUnaryOp,
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    LogicalBinary {
        op: LogicalOp,
        left: Box<ExprNode>,
        right: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    LogicalNot {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    TemporalPart {
        part: TemporalPart,
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    ListLen {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    /// Index into a list column (`list.get`); result is nullable (null list, null index, OOB).
    ListGet {
        inner: Box<ExprNode>,
        index: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    /// Whether `value` appears in each list (`list.contains`).
    ListContains {
        inner: Box<ExprNode>,
        value: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    /// Per-row minimum of a numeric list (`int` / `float` elements only).
    ListMin {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    ListMax {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    ListSum {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    /// `datetime` column → calendar `date` (Polars `dt.date()`).
    DatetimeToDate {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
}
