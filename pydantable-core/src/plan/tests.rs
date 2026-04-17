#![allow(clippy::unwrap_used, clippy::expect_used)]

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
        schema.insert("age".to_string(), DTypeDesc::scalar_nullable(BaseType::Int));
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
        schema.insert("age".to_string(), DTypeDesc::scalar_nullable(BaseType::Int));

        let plan0 = make_plan(schema);

        // select(id, age)
        let plan1 = plan_select(&plan0, vec!["id".to_string(), "age".to_string()])
            .expect("plan_select should succeed");

        // with_columns(age2 = age + 2)
        let age_ref =
            ExprNode::make_column_ref("age".to_string(), DTypeDesc::scalar_nullable(BaseType::Int))
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
        let age2_ref = ExprNode::make_column_ref("age2".to_string(), age2_dtype.clone())
            .expect("age2 column ref");
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

/// Polars-backed execution: IPC default path, list path, and groupby sum null masking.
#[cfg(feature = "polars_engine")]
mod polars_engine_tests {
    use std::collections::HashMap;

    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyList};

    use super::ensure_python_initialized;
    use crate::dtype::{BaseType, DTypeDesc};
    use crate::plan::execute_polars::execute_groupby_agg_polars;
    use crate::plan::{execute_plan, make_plan};

    fn sample_kv_schema() -> HashMap<String, DTypeDesc> {
        let mut schema = HashMap::new();
        schema.insert("k".to_string(), DTypeDesc::non_nullable(BaseType::Int));
        schema.insert("v".to_string(), DTypeDesc::scalar_nullable(BaseType::Int));
        schema
    }

    #[test]
    fn execute_plan_as_python_lists_returns_dict_of_lists() {
        ensure_python_initialized();
        Python::with_gil(|py| {
            let mut schema = HashMap::new();
            schema.insert("id".to_string(), DTypeDesc::non_nullable(BaseType::Int));
            schema.insert("age".to_string(), DTypeDesc::scalar_nullable(BaseType::Int));
            let plan = make_plan(schema);

            let root = PyDict::new_bound(py);
            root.set_item("id", PyList::new_bound(py, [1_i64, 2_i64]))
                .unwrap();
            let ages = PyList::empty_bound(py);
            ages.append(20_i64).unwrap();
            ages.append(py.None()).unwrap();
            root.set_item("age", ages).unwrap();

            let out = execute_plan(py, &plan, root.as_any(), true, false).unwrap();
            let dict = out.bind(py).downcast::<PyDict>().expect("dict of lists");
            let ids: Vec<i64> = dict.get_item("id").unwrap().unwrap().extract().unwrap();
            let age0 = dict
                .get_item("age")
                .unwrap()
                .unwrap()
                .get_item(0)
                .unwrap()
                .extract::<i64>()
                .unwrap();
            assert!(dict
                .get_item("age")
                .unwrap()
                .unwrap()
                .get_item(1)
                .unwrap()
                .is_none());
            assert_eq!(ids, vec![1, 2]);
            assert_eq!(age0, 20);
        });
    }

    // `as_python_lists=false` uses IPC to Python `polars` (optional dep); the
    // default Python API uses `as_python_lists=true`.
    #[test]
    fn execute_plan_default_returns_polars_dataframe() {
        ensure_python_initialized();
        Python::with_gil(|py| {
            // `polars` is an optional Python dependency; skip this IPC-shape test
            // when it is not installed in the active interpreter.
            let Ok(polars) = py.import("polars") else {
                return;
            };
            let mut schema = HashMap::new();
            schema.insert("id".to_string(), DTypeDesc::non_nullable(BaseType::Int));
            schema.insert("age".to_string(), DTypeDesc::scalar_nullable(BaseType::Int));
            let plan = make_plan(schema);

            let root = PyDict::new_bound(py);
            root.set_item("id", PyList::new_bound(py, [1_i64, 2_i64]))
                .unwrap();
            let ages = PyList::empty_bound(py);
            ages.append(20_i64).unwrap();
            ages.append(py.None()).unwrap();
            root.set_item("age", ages).unwrap();

            let out = execute_plan(py, &plan, root.as_any(), false, false).unwrap();
            let df_class = polars.getattr("DataFrame").unwrap();
            let builtins = py.import("builtins").unwrap();
            let isinstance = builtins.getattr("isinstance").unwrap();
            let is_df: bool = isinstance
                .call1((out.bind(py), df_class.as_any()))
                .unwrap()
                .extract()
                .unwrap();
            assert!(is_df, "expected polars.DataFrame from IPC path");

            let ids: Vec<i64> = out
                .bind(py)
                .getattr("get_column")
                .unwrap()
                .call1(("id",))
                .unwrap()
                .call_method0("to_list")
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(ids, vec![1, 2]);
        });
    }

    #[test]
    fn groupby_sum_masks_all_null_group_to_none() {
        ensure_python_initialized();
        Python::with_gil(|py| {
            let plan = make_plan(sample_kv_schema());
            let root = PyDict::new_bound(py);
            root.set_item("k", PyList::new_bound(py, [1_i64, 1_i64, 2_i64]))
                .unwrap();
            let vs = PyList::empty_bound(py);
            vs.append(py.None()).unwrap();
            vs.append(py.None()).unwrap();
            vs.append(5_i64).unwrap();
            root.set_item("v", vs).unwrap();

            let (data, _desc) = execute_groupby_agg_polars(
                py,
                &plan,
                root.as_any(),
                vec!["k".to_string()],
                vec![("s".to_string(), "sum".to_string(), "v".to_string())],
                true,
                false,
                true,
                false,
            )
            .unwrap();

            let dict = data.bind(py).downcast::<PyDict>().unwrap();
            let keys: Vec<i64> = dict.get_item("k").unwrap().unwrap().extract().unwrap();
            let sums: Vec<Option<i64>> = dict.get_item("s").unwrap().unwrap().extract().unwrap();

            let mut pairs: Vec<(i64, Option<i64>)> = keys.into_iter().zip(sums).collect();
            pairs.sort_by_key(|(k, _)| *k);
            assert_eq!(pairs, vec![(1, None), (2, Some(5))]);
        });
    }

    #[test]
    fn groupby_sum_ipc_dataframe_aligns_with_lists_columns() {
        ensure_python_initialized();
        Python::with_gil(|py| {
            let plan = make_plan(sample_kv_schema());
            let root = PyDict::new_bound(py);
            root.set_item("k", PyList::new_bound(py, [1_i64, 1_i64, 2_i64]))
                .unwrap();
            let vs = PyList::empty_bound(py);
            vs.append(py.None()).unwrap();
            vs.append(py.None()).unwrap();
            vs.append(5_i64).unwrap();
            root.set_item("v", vs).unwrap();

            let (data_lists, _) = execute_groupby_agg_polars(
                py,
                &plan,
                root.as_any(),
                vec!["k".to_string()],
                vec![("s".to_string(), "sum".to_string(), "v".to_string())],
                true,
                false,
                true,
                false,
            )
            .unwrap();

            let root2 = PyDict::new_bound(py);
            root2
                .set_item("k", PyList::new_bound(py, [1_i64, 1_i64, 2_i64]))
                .unwrap();
            let vs2 = PyList::empty_bound(py);
            vs2.append(py.None()).unwrap();
            vs2.append(py.None()).unwrap();
            vs2.append(5_i64).unwrap();
            root2.set_item("v", vs2).unwrap();

            // `polars` is an optional Python dependency; skip IPC path assertions
            // when it is not installed in the active interpreter.
            if py.import("polars").is_err() {
                return;
            }

            let (data_ipc, _) = execute_groupby_agg_polars(
                py,
                &plan,
                root2.as_any(),
                vec!["k".to_string()],
                vec![("s".to_string(), "sum".to_string(), "v".to_string())],
                false,
                false,
                false,
                false,
            )
            .unwrap();

            // `polars` is an optional Python dependency; skip IPC-shape assertion
            // when it is not installed in the active interpreter.
            let Ok(polars) = py.import("polars") else {
                return;
            };
            let df_class = polars.getattr("DataFrame").unwrap();
            let builtins = py.import("builtins").unwrap();
            let isinstance = builtins.getattr("isinstance").unwrap();
            assert!(isinstance
                .call1((data_ipc.bind(py), df_class.as_any()))
                .unwrap()
                .extract::<bool>()
                .unwrap());

            let dict_lists = data_lists.bind(py).downcast::<PyDict>().unwrap();
            let k_lists: Vec<i64> = dict_lists
                .get_item("k")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            let s_lists: Vec<Option<i64>> = dict_lists
                .get_item("s")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();

            let k_ipc: Vec<i64> = data_ipc
                .bind(py)
                .getattr("get_column")
                .unwrap()
                .call1(("k",))
                .unwrap()
                .call_method0("to_list")
                .unwrap()
                .extract()
                .unwrap();
            let s_ipc: Vec<Option<i64>> = data_ipc
                .bind(py)
                .getattr("get_column")
                .unwrap()
                .call1(("s",))
                .unwrap()
                .call_method0("to_list")
                .unwrap()
                .extract()
                .unwrap();

            let mut pairs_lists: Vec<(i64, Option<i64>)> =
                k_lists.into_iter().zip(s_lists).collect();
            pairs_lists.sort_by_key(|(k, _)| *k);
            let mut pairs_ipc: Vec<(i64, Option<i64>)> = k_ipc.into_iter().zip(s_ipc).collect();
            pairs_ipc.sort_by_key(|(k, _)| *k);
            assert_eq!(
                pairs_ipc, pairs_lists,
                "IPC DataFrame columns should match dict-of-lists groupby output"
            );
        });
    }
}
