//! Typing rules, `ExprNode::make_*`, [`ExprNode::eval`], and [`ExprNode::referenced_columns`].
//!
//! Logic lives in [`impl_expr_node`] (constructors and inference) and [`helpers`] (literal
//! parsing / cast helpers for row-wise evaluation).

mod helpers;
mod impl_expr_node;

#[cfg(not(feature = "polars_engine"))]
#[path = "../rowwise_support.rs"]
mod rowwise_support;

#[cfg(not(feature = "polars_engine"))]
use rowwise_support::*;

#[cfg(not(feature = "polars_engine"))]
#[path = "../eval_rowwise.rs"]
mod eval_rowwise;

/// Used by [`eval_rowwise`](eval_rowwise) for `cast()` in row-wise mode.
#[cfg(not(feature = "polars_engine"))]
pub(crate) use helpers::cast_literal_value;
