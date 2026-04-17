//! Expression AST, typing/evaluation, Polars lowering, and Python interop.
//!
//! The `typing` package splits the former monolithic `typing.rs` into `typing/mod.rs`,
//! `typing/impl_expr_node.rs`, and `typing/helpers.rs` for navigation only.

mod ir;
#[cfg(feature = "polars_engine")]
mod lower_polars;
mod py_literal;
mod serialize;
mod typing;

#[cfg(test)]
mod tests;

pub use ir::{
    ArithOp, CmpOp, ExprNode, LiteralValue, LogicalOp, RowAccumOp, StringPredicateKind,
    StringUnaryOp, TemporalPart, UnaryNumericOp, UnixTimestampUnit,
};
#[cfg(feature = "polars_engine")]
pub use ir::{WindowFrame, WindowOp};
pub use py_literal::{op_symbol_to_arith, op_symbol_to_cmp, ExprHandle};
pub use serialize::exprnode_to_serializable;
