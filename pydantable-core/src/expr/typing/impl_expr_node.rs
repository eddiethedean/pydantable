//! `ExprNode` dtype inference, [`ExprNode::make_*`] constructors, and row-wise [`ExprNode::eval`].

#[cfg(not(feature = "polars_engine"))]
use std::collections::HashMap;
use std::collections::HashSet;

use pyo3::prelude::*;

use crate::dtype::{dtype_structural_eq, widen_scalar_drop_literals, BaseType, DTypeDesc};

use super::helpers::{dtype_is_string_like, validate_literal_membership_compare};

use crate::expr::ir::{
    ArithOp, CmpOp, ExprNode, GlobalAggOp, LiteralValue, LogicalOp, RowAccumOp,
    StringPredicateKind, StringUnaryOp, TemporalPart, UnaryNumericOp, UnixTimestampUnit,
    WindowFrame, WindowOp, WindowOrderKey,
};

enum ListAggKind {
    Min,
    Max,
    Sum,
}

include!("impl_shard_analysis.inc");
include!("impl_shard_infer.inc");
include!("impl_shard_constructors.inc");
include!("impl_shard_window_map_eval.inc");
