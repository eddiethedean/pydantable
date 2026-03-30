//! Unit tests for expression IR, operator parsing, and typing helpers.

use std::collections::HashSet;

use pyo3::prelude::*;

use crate::dtype::{BaseType, DTypeDesc};

use super::{
    exprnode_to_serializable, op_symbol_to_arith, op_symbol_to_cmp, ArithOp, CmpOp, ExprNode,
    LiteralValue,
};
use pyo3::prepare_freethreaded_python;
use pyo3::types::PyDict;
use std::sync::Once;

static INIT_PYO3: Once = Once::new();

fn ensure_python_initialized() {
    INIT_PYO3.call_once(|| {
        prepare_freethreaded_python();
    });
}

#[test]
fn op_symbol_to_arith_maps_core_operators() {
    assert_eq!(op_symbol_to_arith("+").unwrap(), ArithOp::Add);
    assert_eq!(op_symbol_to_arith("-").unwrap(), ArithOp::Sub);
    assert_eq!(op_symbol_to_arith("*").unwrap(), ArithOp::Mul);
    assert_eq!(op_symbol_to_arith("/").unwrap(), ArithOp::Div);
}

#[test]
fn op_symbol_to_arith_rejects_unknown() {
    ensure_python_initialized();
    let err = op_symbol_to_arith("%").unwrap_err();
    assert!(err.to_string().contains("Unsupported arithmetic"));
}

#[test]
fn op_symbol_to_cmp_maps_core_operators() {
    assert_eq!(op_symbol_to_cmp("==").unwrap(), CmpOp::Eq);
    assert_eq!(op_symbol_to_cmp("!=").unwrap(), CmpOp::Ne);
    assert_eq!(op_symbol_to_cmp("<").unwrap(), CmpOp::Lt);
    assert_eq!(op_symbol_to_cmp("<=").unwrap(), CmpOp::Le);
    assert_eq!(op_symbol_to_cmp(">").unwrap(), CmpOp::Gt);
    assert_eq!(op_symbol_to_cmp(">=").unwrap(), CmpOp::Ge);
}

#[test]
fn op_symbol_to_cmp_rejects_unknown() {
    ensure_python_initialized();
    let err = op_symbol_to_cmp("<>").unwrap_err();
    assert!(err.to_string().contains("Unsupported comparison"));
}

#[test]
fn column_ref_referenced_columns_single() {
    let node = ExprNode::make_column_ref(
        "user_id".to_string(),
        DTypeDesc::non_nullable(BaseType::Int),
    )
    .expect("column ref");
    let cols: HashSet<_> = node.referenced_columns();
    assert_eq!(cols, HashSet::from(["user_id".to_string()]));
}

#[test]
fn compare_op_referenced_columns_union() {
    let a = ExprNode::make_column_ref("a".to_string(), DTypeDesc::non_nullable(BaseType::Int))
        .expect("a");
    let b = ExprNode::make_column_ref("b".to_string(), DTypeDesc::non_nullable(BaseType::Int))
        .expect("b");
    let cmp = ExprNode::make_compare_op(CmpOp::Gt, a, b).expect("a > b");
    let cols: HashSet<_> = cmp.referenced_columns();
    assert_eq!(cols, HashSet::from(["a".to_string(), "b".to_string()]));
}

#[test]
fn binary_op_dtype_matches_arithmetic_rules() {
    let left = ExprNode::make_column_ref("x".to_string(), DTypeDesc::non_nullable(BaseType::Int))
        .expect("x");
    let right = ExprNode::make_literal(
        Some(LiteralValue::Int(1)),
        DTypeDesc::non_nullable(BaseType::Int),
    )
    .expect("1");
    let sum = ExprNode::make_binary_op(ArithOp::Add, left, right).expect("x + 1");
    assert_eq!(
        sum.dtype().as_scalar_base_field().flatten(),
        Some(BaseType::Int)
    );
}

#[test]
fn map_get_rejects_non_map_and_never_panics() {
    ensure_python_initialized();
    let inner = ExprNode::make_column_ref("x".to_string(), DTypeDesc::non_nullable(BaseType::Int))
        .expect("x");
    let err = ExprNode::make_map_get(inner, "k".to_string()).unwrap_err();
    assert!(err.to_string().contains("map_get() requires a map column"));
}

#[test]
fn exprnode_to_serializable_column_ref_roundtrip_shape() {
    ensure_python_initialized();
    Python::with_gil(|py| {
        let node = ExprNode::make_column_ref(
            "amount".to_string(),
            DTypeDesc::scalar_nullable(BaseType::Float),
        )
        .expect("amount");
        let obj = exprnode_to_serializable(py, &node).expect("serialize");
        let d: &Bound<'_, PyDict> = obj.downcast_bound(py).expect("dict");
        assert_eq!(
            d.get_item("kind")
                .unwrap()
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "column_ref"
        );
        assert_eq!(
            d.get_item("name")
                .unwrap()
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "amount"
        );
    });
}

#[test]
fn window_range_frame_serializes_with_kind_and_bounds() {
    ensure_python_initialized();
    Python::with_gil(|py| {
        let inner =
            ExprNode::make_column_ref("v".to_string(), DTypeDesc::non_nullable(BaseType::Int))
                .expect("column");
        let node = ExprNode::make_window_sum(
            inner,
            vec!["g".to_string()],
            vec![("v".to_string(), true, false)],
            Some("range".to_string()),
            Some(-2),
            Some(0),
        )
        .expect("window");
        let obj = exprnode_to_serializable(py, &node).expect("serialize");
        let d: &Bound<'_, PyDict> = obj.downcast_bound(py).expect("dict");
        let frame_obj = d
            .get_item("frame")
            .expect("frame key")
            .expect("frame value");
        let frame: &Bound<'_, PyDict> = frame_obj.downcast().expect("frame dict");
        assert_eq!(
            frame
                .get_item("kind")
                .unwrap()
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "range"
        );
        assert_eq!(
            frame
                .get_item("start")
                .unwrap()
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            -2
        );
        assert_eq!(
            frame
                .get_item("end")
                .unwrap()
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            0
        );
    });
}
