use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList};

use crate::dtype::{dtype_to_python_type, DTypeDesc};
use crate::expr::{ExprNode, LiteralValue};

#[derive(Clone, Debug)]
pub enum PlanStep {
    Select { columns: Vec<String> },
    WithColumns { columns: HashMap<String, ExprNode> },
    Filter { condition: ExprNode },
}

#[derive(Clone, Debug)]
pub struct PlanInner {
    pub steps: Vec<PlanStep>,
    pub schema: HashMap<String, DTypeDesc>,
    pub root_schema: HashMap<String, DTypeDesc>,
}

fn ctx_len(ctx: &HashMap<String, Vec<Option<LiteralValue>>>) -> PyResult<usize> {
    ctx.values()
        .next()
        .map(|v| v.len())
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Cannot execute plan with an empty input context.",
        ))
}

pub fn make_plan(schema: HashMap<String, DTypeDesc>) -> PlanInner {
    let root_schema = schema.clone();
    PlanInner {
        steps: Vec::new(),
        schema,
        root_schema,
    }
}

pub fn schema_fields_as_py(py: Python<'_>, schema: &HashMap<String, DTypeDesc>) -> PyResult<PyObject> {
    let dict = pyo3::types::PyDict::new_bound(py);
    for (name, dtype) in schema.iter() {
        let t = dtype_to_python_type(py, *dtype)?;
        dict.set_item(name, t)?;
    }
    Ok(dict.into_py(py))
}

pub fn root_data_to_ctx(
    py: Python<'_>,
    root_schema: &HashMap<String, DTypeDesc>,
    root_data: &Bound<'_, PyAny>,
) -> PyResult<HashMap<String, Vec<Option<LiteralValue>>>> {
    let dict: &Bound<'_, PyDict> = root_data.downcast()?;
    let mut ctx: HashMap<String, Vec<Option<LiteralValue>>> = HashMap::new();

    for (k, v) in dict.iter() {
        let name: String = k.extract()?;
        let values_any = v;
        let list: &Bound<'_, PyList> = values_any.downcast()?;

        let expected = root_schema.get(&name).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "Root data contains unknown column '{}'.",
                name
            ))
        })?;

        let mut out: Vec<Option<LiteralValue>> = Vec::with_capacity(list.len());
        for item in list.iter() {
            let item = item;
            if item.is_none() {
                out.push(None);
            } else {
                let lit = match expected.base {
                    Some(crate::dtype::BaseType::Int) => LiteralValue::Int(item.extract::<i64>()?),
                    Some(crate::dtype::BaseType::Float) => {
                        LiteralValue::Float(item.extract::<f64>()?)
                    }
                    Some(crate::dtype::BaseType::Bool) => LiteralValue::Bool(item.extract::<bool>()?),
                    Some(crate::dtype::BaseType::Str) => LiteralValue::Str(item.extract::<String>()?),
                    None => {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "Root schema cannot have unknown-base dtype.",
                        ))
                    }
                };
                out.push(Some(lit));
            }
        }
        ctx.insert(name, out);
    }

    Ok(ctx)
}

pub fn plan_select(plan: &PlanInner, columns: Vec<String>) -> PyResult<PlanInner> {
    for c in columns.iter() {
        if !plan.schema.contains_key(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "select() unknown column '{}'.",
                c
            )));
        }
    }

    let mut new_schema = HashMap::new();
    for c in columns.iter() {
        new_schema.insert(c.clone(), *plan.schema.get(c).unwrap());
    }

    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::Select { columns });

    Ok(PlanInner {
        steps: new_steps,
        schema: new_schema,
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_with_columns(
    plan: &PlanInner,
    columns: HashMap<String, ExprNode>,
) -> PyResult<PlanInner> {
    let mut new_schema = plan.schema.clone();
    let mut new_steps = plan.steps.clone();

    // Type-check and compute derived schema.
    for (name, expr) in columns.iter() {
        let referenced = expr.referenced_columns();
        for c in referenced.iter() {
            if !plan.schema.contains_key(c) {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Expression for '{}' references unknown column '{}'.",
                    name,
                    c
                )));
            }
        }

        let mut expr_dtype = expr.dtype();
        if expr_dtype.base.is_none() {
            // Literal(None) assigned directly needs destination type inference.
            if let Some(dest) = plan.schema.get(name) {
                let base = dest.base.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Destination schema base type is unknown.",
                    )
                })?;
                expr_dtype = DTypeDesc {
                    base: Some(base),
                    nullable: true,
                };
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                    "with_columns({}=None) cannot infer destination type; combine None with a typed expression or replace an existing column.",
                    name
                )));
            }
        }

        new_schema.insert(name.clone(), expr_dtype);
    }

    new_steps.push(PlanStep::WithColumns { columns });

    Ok(PlanInner {
        steps: new_steps,
        schema: new_schema,
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_filter(plan: &PlanInner, condition: ExprNode) -> PyResult<PlanInner> {
    let cond_dtype = condition.dtype();
    if cond_dtype.base != Some(crate::dtype::BaseType::Bool) {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "filter(condition) expects condition typed as bool or Optional[bool].",
        ));
    }

    // referenced column validation.
    let referenced = condition.referenced_columns();
    for c in referenced.iter() {
        if !plan.schema.contains_key(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Filter expression references unknown column '{}'.",
                c
            )));
        }
    }

    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::Filter { condition });

    Ok(PlanInner {
        steps: new_steps,
        schema: plan.schema.clone(),
        root_schema: plan.root_schema.clone(),
    })
}

pub fn execute_plan(py: Python<'_>, plan: &PlanInner, root_data: &Bound<'_, PyAny>) -> PyResult<PyObject> {
    let mut ctx = root_data_to_ctx(py, &plan.root_schema, root_data)?;
    let mut n = ctx_len(&ctx)?;

    for step in plan.steps.iter() {
        match step {
            PlanStep::Select { columns } => {
                ctx.retain(|k, _| columns.contains(k));
                n = ctx_len(&ctx)?;
            }
            PlanStep::WithColumns { columns } => {
                for (name, expr) in columns.iter() {
                    let out = expr.eval(&ctx, n)?;
                    ctx.insert(name.clone(), out);
                }
                n = ctx_len(&ctx)?;
            }
            PlanStep::Filter { condition } => {
                let mask = condition.eval(&ctx, n)?;
                let mut keep: Vec<usize> = Vec::new();
                for (i, m) in mask.into_iter().enumerate() {
                    if matches!(m, Some(LiteralValue::Bool(true))) {
                        keep.push(i);
                    }
                }

                for (_, col) in ctx.iter_mut() {
                    let new_col = keep.iter().map(|&i| col[i].clone()).collect();
                    *col = new_col;
                }
                n = ctx_len(&ctx)?;
            }
        }
    }

    // Convert context back to Python dict[str, list[Any]].
    let out_dict = PyDict::new_bound(py);
    for (name, col) in ctx.iter() {
        let mut values: Vec<PyObject> = Vec::with_capacity(col.len());
        for item in col.iter() {
            match item {
                None => values.push(py.None()),
                Some(LiteralValue::Int(i)) => values.push(i.into_py(py)),
                Some(LiteralValue::Float(f)) => values.push(f.into_py(py)),
                Some(LiteralValue::Bool(b)) => values.push(b.into_py(py)),
                Some(LiteralValue::Str(s)) => values.push(s.clone().into_py(py)),
            }
        }
        let py_list = PyList::new_bound(py, values);
        out_dict.set_item(name, py_list)?;
    }

    Ok(out_dict.into_py(py))
}

