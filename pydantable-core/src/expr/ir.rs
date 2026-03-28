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
    /// Nanoseconds since midnight (Polars `Time`).
    TimeNanos(i64),
    Binary(Vec<u8>),
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
    /// Reverse UTF-8 string (Polars `str.reverse`).
    Reverse,
    /// Pad start to at least `length` characters (`str.pad_start`).
    PadStart {
        length: u32,
        fill_char: char,
    },
    /// Pad end to at least `length` characters (`str.pad_end`).
    PadEnd {
        length: u32,
        fill_char: char,
    },
    /// Zero-pad after sign (`str.zfill`).
    ZFill {
        length: u32,
    },
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
    /// Sub-second component (`datetime` and `time` columns).
    Nanosecond,
    /// Calendar weekday (Polars `dt.weekday()`; Monday = 1, Sunday = 7).
    Weekday,
    /// Calendar quarter 1–4 (`datetime` / `date`).
    Quarter,
    /// ISO 8601 week number 1–53 (`datetime` / `date`; Polars `dt.week()`).
    Week,
}

/// String column predicate producing `bool` (Polars `str` namespace).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StringPredicateKind {
    StartsWith,
    EndsWith,
    /// When `literal` is true, match a substring; when false, `pattern` is a **Rust regex**.
    Contains {
        literal: bool,
    },
}

/// Unit for [`ExprNode::UnixTimestamp`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnixTimestampUnit {
    Seconds,
    Milliseconds,
}

/// Spark-style window frame bounds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowFrame {
    /// Inclusive bounds relative to the current row (`0` = current row), matching Spark `rowsBetween`.
    Rows { start: i64, end: i64 },
    /// Inclusive bounds on ordered values, matching Spark `rangeBetween`.
    Range { start: i64, end: i64 },
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
    /// Replace occurrences of `pattern`; `literal` uses substring replace, else Rust regex.
    StringReplace {
        inner: Box<ExprNode>,
        pattern: String,
        replacement: String,
        literal: bool,
        dtype: DTypeDesc,
    },
    /// `str` predicates: prefix / suffix / contains (literal or regex).
    StringPredicate {
        inner: Box<ExprNode>,
        kind: StringPredicateKind,
        pattern: String,
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
    /// Per-row mean of a numeric list (`int` / `float` elements) as `float`.
    ListMean {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    /// Join `list[str]` with separator → `str`.
    ListJoin {
        inner: Box<ExprNode>,
        separator: String,
        ignore_nulls: bool,
        dtype: DTypeDesc,
    },
    /// Sort each list cell (orderable scalar elements).
    ListSort {
        inner: Box<ExprNode>,
        descending: bool,
        nulls_last: bool,
        maintain_order: bool,
        dtype: DTypeDesc,
    },
    /// Deduplicate each list cell (first-seen order unless `stable`).
    ListUnique {
        inner: Box<ExprNode>,
        stable: bool,
        dtype: DTypeDesc,
    },
    /// Split string column by delimiter → `list[str]`.
    StringSplit {
        inner: Box<ExprNode>,
        delimiter: String,
        dtype: DTypeDesc,
    },
    /// Regex capture (`str.extract`); group 0 = whole match, 1+ = capture groups.
    StringExtract {
        inner: Box<ExprNode>,
        pattern: String,
        group_index: usize,
        dtype: DTypeDesc,
    },
    /// JSONPath match against a JSON string cell (`str.json_path_match`); result `str` (null if no match / invalid JSON per Polars).
    StringJsonPathMatch {
        inner: Box<ExprNode>,
        path: String,
        dtype: DTypeDesc,
    },
    /// `datetime` column → calendar `date` (Polars `dt.date()`).
    DatetimeToDate {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    /// `str` column → `date` / `datetime` using Polars `str.strptime` (format must match data).
    Strptime {
        inner: Box<ExprNode>,
        format: String,
        to_datetime: bool,
        dtype: DTypeDesc,
    },
    /// `datetime` / `date` → Unix timestamp (integer).
    UnixTimestamp {
        inner: Box<ExprNode>,
        unit: UnixTimestampUnit,
        dtype: DTypeDesc,
    },
    /// Byte length of a `bytes` column (`Binary`).
    BinaryLength {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    /// Number of entries in a `dict[str, T]` map column (logical list length).
    MapLen {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    /// Value for a string key in a `dict[str, T]` map (physical list-of-struct).
    MapGet {
        inner: Box<ExprNode>,
        key: String,
        dtype: DTypeDesc,
    },
    /// Whether `key` is present in a map column.
    MapContainsKey {
        inner: Box<ExprNode>,
        key: String,
        dtype: DTypeDesc,
    },
    /// List of map keys for each row (`list[str]`).
    MapKeys {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    /// List of map values for each row (`list[V]`).
    MapValues {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    /// List of map entries for each row (`list[struct{key, value}]`).
    MapEntries {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    /// Map built from a list of entry structs (`list[struct{key, value}]`).
    MapFromEntries {
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    /// Windowed aggregate or ranking function (Polars `.over(...)`).
    Window {
        op: WindowOp,
        operand: Option<Box<ExprNode>>,
        partition_by: Vec<String>,
        order_by: Vec<WindowOrderKey>,
        /// Optional row-based frame; `None` uses engine default (partition + order only).
        frame: Option<WindowFrame>,
        dtype: DTypeDesc,
    },
    /// Reduction over the full table (no `group_by`): `sum`, `mean`, …
    GlobalAgg {
        op: GlobalAggOp,
        inner: Box<ExprNode>,
        dtype: DTypeDesc,
    },
    /// Row count of the current frame (`Polars len()`), for global `select` only.
    GlobalRowCount {
        dtype: DTypeDesc,
    },
}

/// Window `orderBy` key: column name, ascending, **`nulls_last`** (Polars `SortOptions`).
/// `nulls_last == false` places nulls before non-nulls for that key (**NULLS FIRST**).
pub type WindowOrderKey = (String, bool, bool);

/// Built-in window operations lowered to Polars window expressions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowOp {
    RowNumber,
    Rank,
    DenseRank,
    Sum,
    Mean,
    Min,
    Max,
    /// Previous row within partition/order (`shift(n)`).
    Lag {
        n: u32,
    },
    /// Next row within partition/order (`shift(-n)`).
    Lead {
        n: u32,
    },
}

/// Whole-frame aggregation (Polars `select(sum(col))` — single output row).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GlobalAggOp {
    Sum,
    Mean,
    /// Count of non-null cells (Polars `count`).
    Count,
    Min,
    Max,
}
