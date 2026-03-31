//! Logical plan intermediate representation (`PlanStep`, `PlanInner`).

use std::collections::HashMap;

use crate::dtype::DTypeDesc;
use crate::expr::{ExprNode, LiteralValue};

#[derive(Clone, Debug)]
pub enum PlanStep {
    Select {
        columns: Vec<String>,
    },
    /// Replace the frame with a single row of global aggregates (Polars `select(agg1, agg2, ...)`).
    GlobalSelect {
        items: Vec<(String, ExprNode)>,
    },
    WithColumns {
        columns: HashMap<String, ExprNode>,
    },
    Filter {
        condition: ExprNode,
    },
    Sort {
        by: Vec<String>,
        descending: Vec<bool>,
        nulls_last: Vec<bool>,
        maintain_order: bool,
    },
    Unique {
        subset: Option<Vec<String>>,
        keep: String,
        maintain_order: bool,
    },
    Rename {
        columns: HashMap<String, String>,
    },
    Slice {
        offset: i64,
        length: usize,
    },
    FillNull {
        subset: Option<Vec<String>>,
        value: Option<LiteralValue>,
        strategy: Option<String>,
    },
    DropNulls {
        subset: Option<Vec<String>>,
        how: String,
        threshold: Option<usize>,
    },
    Melt {
        id_vars: Vec<String>,
        value_vars: Vec<String>,
        variable_name: String,
        value_name: String,
    },
    RollingAgg {
        column: String,
        window_size: usize,
        min_periods: usize,
        op: String,
        out_name: String,
        /// Empty = frame-global rolling (current row order). Non-empty = `.over(...)` keys.
        partition_by: Vec<String>,
    },
    /// Row-wise duplicate boolean mask (pandas `duplicated`); output schema is a single `bool` column.
    DuplicateMask {
        subset: Vec<String>,
        /// `first` | `last` | `none` (same as pandas `keep=False` for the mask).
        keep: String,
    },
    /// Drop every row that belongs to a duplicate key group (pandas `drop_duplicates(keep=False)`).
    DropDuplicateGroups {
        subset: Vec<String>,
    },
    WithRowCount {
        name: String,
        offset: i64,
    },
}

#[derive(Clone, Debug)]
pub struct PlanInner {
    pub steps: Vec<PlanStep>,
    pub schema: HashMap<String, DTypeDesc>,
    pub root_schema: HashMap<String, DTypeDesc>,
}

pub fn make_plan(schema: HashMap<String, DTypeDesc>) -> PlanInner {
    let root_schema = schema.clone();
    PlanInner {
        steps: Vec::new(),
        schema,
        root_schema,
    }
}
