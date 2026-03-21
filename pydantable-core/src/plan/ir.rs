//! Logical plan intermediate representation (`PlanStep`, `PlanInner`).

use std::collections::HashMap;

use crate::dtype::DTypeDesc;
use crate::expr::{ExprNode, LiteralValue};

#[derive(Clone, Debug)]
pub enum PlanStep {
    Select {
        columns: Vec<String>,
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
    },
    Unique {
        subset: Option<Vec<String>>,
        keep: String,
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
