use std::collections::HashMap;

use pyo3::prelude::*;

use super::*;
use crate::dtype::{BaseType, DTypeDesc};
use crate::expr::{ArithOp, CmpOp, ExprNode, LiteralValue};
use pyo3::prepare_freethreaded_python;
use pyo3::types::{PyDict, PyList};
use std::sync::Once;

static INIT_PYO3: Once = Once::new();

fn ensure_python_initialized() {
    INIT_PYO3.call_once(|| {
        prepare_freethreaded_python();
    });
}

#[test]
fn plan_select_rejects_empty_projection() {
    ensure_python_initialized();
    let mut schema = HashMap::new();
    schema.insert("id".to_string(), DTypeDesc::non_nullable(BaseType::Int));
    let plan = make_plan(schema);
    let err = plan_select(&plan, Vec::new()).unwrap_err();
    assert!(err.to_string().contains("requires at least one column"));
}

#[test]
fn schema_descriptors_encode_base_and_nullable() {
    ensure_python_initialized();
    Python::with_gil(|py| {
        let mut schema = HashMap::new();
        schema.insert("id".to_string(), DTypeDesc::non_nullable(BaseType::Int));
        schema.insert("age".to_string(), DTypeDesc::nullable(BaseType::Int));
        let obj = schema_descriptors_as_py(py, &schema).unwrap();
        let dict = obj.bind(py).downcast::<PyDict>().unwrap();

        let id = dict.get_item("id").unwrap().unwrap();
        let age = dict.get_item("age").unwrap().unwrap();
        assert_eq!(
            id.get_item("base").unwrap().extract::<String>().unwrap(),
            "int"
        );
        assert!(!id.get_item("nullable").unwrap().extract::<bool>().unwrap());
        assert_eq!(
            age.get_item("base").unwrap().extract::<String>().unwrap(),
            "int"
        );
        assert!(age.get_item("nullable").unwrap().extract::<bool>().unwrap());
    });
}

#[test]
fn planinner_to_serializable_smoke() {
    ensure_python_initialized();

    Python::with_gil(|py| {
        // Base schema
        let mut schema = HashMap::new();
        schema.insert("id".to_string(), DTypeDesc::non_nullable(BaseType::Int));
        schema.insert("age".to_string(), DTypeDesc::nullable(BaseType::Int));

        let plan0 = make_plan(schema);

        // select(id, age)
        let plan1 = plan_select(&plan0, vec!["id".to_string(), "age".to_string()])
            .expect("plan_select should succeed");

        // with_columns(age2 = age + 2)
        let age_ref =
            ExprNode::make_column_ref("age".to_string(), DTypeDesc::nullable(BaseType::Int))
                .expect("age column ref");
        let lit_two = ExprNode::make_literal(
            Some(LiteralValue::Int(2)),
            DTypeDesc::non_nullable(BaseType::Int),
        )
        .expect("literal 2");
        let age_plus_two =
            ExprNode::make_binary_op(ArithOp::Add, age_ref, lit_two).expect("age + 2");

        let mut with_cols: HashMap<String, ExprNode> = HashMap::new();
        with_cols.insert("age2".to_string(), age_plus_two);
        let plan2 = plan_with_columns(&plan1, with_cols).expect("plan_with_columns");

        // filter(age2 > 10)
        let age2_dtype = plan2.schema.get("age2").expect("age2 in derived schema");
        let age2_ref =
            ExprNode::make_column_ref("age2".to_string(), *age2_dtype).expect("age2 column ref");
        let lit_10 = ExprNode::make_literal(
            Some(LiteralValue::Int(10)),
            DTypeDesc::non_nullable(BaseType::Int),
        )
        .expect("literal 10");
        let cond = ExprNode::make_compare_op(CmpOp::Gt, age2_ref, lit_10).expect("age2 > 10");
        let plan3 = plan_filter(&plan2, cond).expect("plan_filter");

        let serial = planinner_to_serializable(py, &plan3).expect("serialize plan");
        let dict: &Bound<'_, PyDict> = serial
            .downcast_bound(py)
            .expect("plan serialization is a dict");

        assert_eq!(
            dict.get_item("version")
                .unwrap()
                .unwrap()
                .extract::<i64>()
                .unwrap(),
            1
        );

        let steps_obj = dict.get_item("steps").unwrap().unwrap();
        let steps = steps_obj.downcast::<PyList>().unwrap();
        assert_eq!(steps.len(), 3);

        let step0_any = steps.get_item(0).unwrap();
        let step0 = step0_any.downcast::<PyDict>().unwrap();
        assert_eq!(
            step0
                .get_item("kind")
                .unwrap()
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "select"
        );

        let step1_any = steps.get_item(1).unwrap();
        let step1 = step1_any.downcast::<PyDict>().unwrap();
        assert_eq!(
            step1
                .get_item("kind")
                .unwrap()
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "with_columns"
        );

        let step2_any = steps.get_item(2).unwrap();
        let step2 = step2_any.downcast::<PyDict>().unwrap();
        assert_eq!(
            step2
                .get_item("kind")
                .unwrap()
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "filter"
        );
    });
}
