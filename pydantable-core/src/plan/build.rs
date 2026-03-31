//! Pure planner transforms: `PlanInner` → `PlanInner`.

use std::collections::HashMap;

use pyo3::prelude::*;

use crate::dtype::{BaseType, DTypeDesc};
use crate::expr::{ExprNode, LiteralValue};

use super::ir::{PlanInner, PlanStep};

pub fn plan_global_select(plan: &PlanInner, items: Vec<(String, ExprNode)>) -> PyResult<PlanInner> {
    if items.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "global select requires at least one aggregate.",
        ));
    }

    let mut new_schema = HashMap::new();
    for (name, expr) in &items {
        if !matches!(
            expr,
            ExprNode::GlobalAgg { .. } | ExprNode::GlobalRowCount { .. }
        ) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "global select only supports global aggregate expressions (e.g. functions.sum / functions.avg) or global_row_count().",
            ));
        }
        let referenced = expr.referenced_columns();
        for c in referenced.iter() {
            if !plan.schema.contains_key(c) {
                let mut available: Vec<String> = plan.schema.keys().cloned().collect();
                available.sort();
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Aggregate references unknown column '{}'. Available columns: [{}].",
                    c,
                    available.join(", ")
                )));
            }
        }
        new_schema.insert(name.clone(), expr.dtype());
    }

    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::GlobalSelect { items });

    Ok(PlanInner {
        steps: new_steps,
        schema: new_schema,
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_select(plan: &PlanInner, columns: Vec<String>) -> PyResult<PlanInner> {
    if columns.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "select() requires at least one column.",
        ));
    }

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
        let dtype = plan.schema.get(c).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "internal: plan schema missing column '{c}' after validation.",
            ))
        })?;
        new_schema.insert(c.clone(), dtype.clone());
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
                let mut available: Vec<String> = plan.schema.keys().cloned().collect();
                available.sort();
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Expression for '{}' references unknown column '{}'. Available columns: [{}].",
                    name,
                    c,
                    available.join(", ")
                )));
            }
        }

        let mut expr_dtype = expr.dtype();
        if expr_dtype.is_scalar_unknown_nullable() {
            // Literal(None) assigned directly needs destination type inference.
            if let Some(dest) = plan.schema.get(name) {
                expr_dtype = dest.clone().with_assigned_none_nullability();
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
    if cond_dtype.as_scalar_base_field().flatten() != Some(crate::dtype::BaseType::Bool) {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            format!(
                "filter(condition) expects condition typed as bool or Optional[bool]. Got dtype={cond_dtype:?}.",
            ),
        ));
    }

    // referenced column validation.
    let referenced = condition.referenced_columns();
    for c in referenced.iter() {
        if !plan.schema.contains_key(c) {
            let mut available: Vec<String> = plan.schema.keys().cloned().collect();
            available.sort();
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Filter expression references unknown column '{}'. Available columns: [{}].",
                c,
                available.join(", ")
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

pub fn plan_sort(
    plan: &PlanInner,
    by: Vec<String>,
    descending: Vec<bool>,
    nulls_last: Vec<bool>,
    maintain_order: bool,
) -> PyResult<PlanInner> {
    if by.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "sort(by=...) requires at least one key.",
        ));
    }
    if !descending.is_empty() && descending.len() != by.len() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "sort(descending=...) must be empty or the same length as by.",
        ));
    }
    if !nulls_last.is_empty() && nulls_last.len() != by.len() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "sort(nulls_last=...) must be empty or the same length as by.",
        ));
    }
    for key in by.iter() {
        if !plan.schema.contains_key(key) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "sort() unknown key '{}'.",
                key
            )));
        }
    }
    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::Sort {
        by,
        descending,
        nulls_last,
        maintain_order,
    });
    Ok(PlanInner {
        steps: new_steps,
        schema: plan.schema.clone(),
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_unique(
    plan: &PlanInner,
    subset: Option<Vec<String>>,
    keep: String,
    maintain_order: bool,
) -> PyResult<PlanInner> {
    if let Some(keys) = subset.as_ref() {
        if keys.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "unique(subset=...) cannot be empty.",
            ));
        }
        for key in keys.iter() {
            if !plan.schema.contains_key(key) {
                return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                    "unique() unknown subset column '{}'.",
                    key
                )));
            }
        }
    }
    match keep.as_str() {
        "first" | "last" | "any" => {}
        other => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "unique(keep=...) unsupported value '{}'. Use one of: first, last, any.",
                other
            )))
        }
    }
    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::Unique {
        subset,
        keep,
        maintain_order,
    });
    Ok(PlanInner {
        steps: new_steps,
        schema: plan.schema.clone(),
        root_schema: plan.root_schema.clone(),
    })
}

fn resolve_duplicate_subset(
    plan: &PlanInner,
    subset: Option<Vec<String>>,
) -> PyResult<Vec<String>> {
    match subset {
        None => {
            let mut keys: Vec<String> = plan.schema.keys().cloned().collect();
            if keys.is_empty() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "duplicated() requires at least one column in the schema.",
                ));
            }
            keys.sort();
            Ok(keys)
        }
        Some(keys) => {
            if keys.is_empty() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "duplicated(subset=...) cannot be empty.",
                ));
            }
            for key in keys.iter() {
                if !plan.schema.contains_key(key) {
                    return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                        "duplicated() unknown subset column '{key}'.",
                    )));
                }
            }
            Ok(keys)
        }
    }
}

/// Output: single column `duplicated` (bool), same length as input rows.
pub fn plan_duplicate_mask(
    plan: &PlanInner,
    subset: Option<Vec<String>>,
    keep: String,
) -> PyResult<PlanInner> {
    match keep.as_str() {
        "first" | "last" | "none" => {}
        other => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "duplicated(keep=...) unsupported value {other:?}. Use 'first', 'last', or False (internal: 'none').",
            )));
        }
    }
    let subset = resolve_duplicate_subset(plan, subset)?;
    let mut new_schema = HashMap::new();
    new_schema.insert(
        "duplicated".to_string(),
        DTypeDesc::non_nullable(BaseType::Bool),
    );
    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::DuplicateMask {
        subset,
        keep: keep.clone(),
    });
    Ok(PlanInner {
        steps: new_steps,
        schema: new_schema,
        root_schema: plan.root_schema.clone(),
    })
}

/// Drop all rows that appear in a duplicate group (pandas `drop_duplicates(keep=False)`).
pub fn plan_drop_duplicate_groups(
    plan: &PlanInner,
    subset: Option<Vec<String>>,
) -> PyResult<PlanInner> {
    let subset = resolve_duplicate_subset(plan, subset)?;
    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::DropDuplicateGroups {
        subset: subset.clone(),
    });
    Ok(PlanInner {
        steps: new_steps,
        schema: plan.schema.clone(),
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_drop(plan: &PlanInner, columns: Vec<String>) -> PyResult<PlanInner> {
    if columns.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "drop(...) requires at least one column.",
        ));
    }
    let mut new_schema = plan.schema.clone();
    for col in columns.iter() {
        if !new_schema.contains_key(col) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "drop() unknown column '{}'.",
                col
            )));
        }
        new_schema.remove(col);
    }
    if new_schema.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "drop(...) cannot remove all columns.",
        ));
    }
    let mut new_steps = plan.steps.clone();
    let kept = new_schema.keys().cloned().collect::<Vec<_>>();
    new_steps.push(PlanStep::Select { columns: kept });
    Ok(PlanInner {
        steps: new_steps,
        schema: new_schema,
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_rename(plan: &PlanInner, columns: HashMap<String, String>) -> PyResult<PlanInner> {
    if columns.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "rename(...) requires at least one mapping.",
        ));
    }
    let mut new_schema = plan.schema.clone();
    for (old, new) in columns.iter() {
        if !plan.schema.contains_key(old) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "rename() unknown column '{}'.",
                old
            )));
        }
        if old != new && new_schema.contains_key(new) && !columns.contains_key(new) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "rename() target '{}' already exists.",
                new
            )));
        }
        let dtype = plan.schema.get(old).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "internal: plan schema missing column '{old}' after validation.",
            ))
        })?;
        new_schema.remove(old);
        new_schema.insert(new.clone(), dtype.clone());
    }
    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::Rename { columns });
    Ok(PlanInner {
        steps: new_steps,
        schema: new_schema,
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_slice(plan: &PlanInner, offset: i64, length: usize) -> PyResult<PlanInner> {
    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::Slice { offset, length });
    Ok(PlanInner {
        steps: new_steps,
        schema: plan.schema.clone(),
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_fill_null(
    plan: &PlanInner,
    subset: Option<Vec<String>>,
    value: Option<LiteralValue>,
    strategy: Option<String>,
) -> PyResult<PlanInner> {
    if value.is_none() && strategy.is_none() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "fill_null() requires either a scalar value or a strategy.",
        ));
    }
    if value.is_some() && strategy.is_some() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "fill_null() accepts either value or strategy, not both.",
        ));
    }
    if let Some(cols) = subset.as_ref() {
        if cols.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "fill_null(subset=...) cannot be empty.",
            ));
        }
        for c in cols.iter() {
            if !plan.schema.contains_key(c) {
                return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                    "fill_null() unknown subset column '{}'.",
                    c
                )));
            }
        }
    }
    if let Some(s) = strategy.as_ref() {
        match s.as_str() {
            "forward" | "backward" | "min" | "max" | "mean" | "zero" | "one" => {}
            other => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "fill_null(strategy=...) unsupported value '{}'.",
                    other
                )))
            }
        }
    }

    let mut new_schema = plan.schema.clone();
    if value.is_some() {
        let targets = subset
            .clone()
            .unwrap_or_else(|| new_schema.keys().cloned().collect());
        for c in targets.iter() {
            if let Some(d) = new_schema.get(c).cloned() {
                let updated = match d {
                    DTypeDesc::Scalar { base, .. } => DTypeDesc::Scalar {
                        base,
                        nullable: false,
                        literals: None,
                    },
                    DTypeDesc::Struct { fields, .. } => DTypeDesc::Struct {
                        fields,
                        nullable: false,
                    },
                    DTypeDesc::List { inner, .. } => DTypeDesc::List {
                        inner,
                        nullable: false,
                    },
                    DTypeDesc::Map { value, .. } => DTypeDesc::Map {
                        value,
                        nullable: false,
                    },
                };
                new_schema.insert(c.clone(), updated);
            }
        }
    }

    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::FillNull {
        subset,
        value,
        strategy,
    });
    Ok(PlanInner {
        steps: new_steps,
        schema: new_schema,
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_drop_nulls(plan: &PlanInner, subset: Option<Vec<String>>) -> PyResult<PlanInner> {
    if let Some(cols) = subset.as_ref() {
        if cols.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "drop_nulls(subset=...) cannot be empty.",
            ));
        }
        for c in cols.iter() {
            if !plan.schema.contains_key(c) {
                return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                    "drop_nulls() unknown subset column '{}'.",
                    c
                )));
            }
        }
    }
    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::DropNulls { subset });
    Ok(PlanInner {
        steps: new_steps,
        schema: plan.schema.clone(),
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_melt(
    plan: &PlanInner,
    id_vars: Vec<String>,
    value_vars: Option<Vec<String>>,
    variable_name: String,
    value_name: String,
) -> PyResult<PlanInner> {
    if variable_name == value_name {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "melt(variable_name=..., value_name=...) must be different.",
        ));
    }
    if plan.schema.contains_key(&variable_name) || plan.schema.contains_key(&value_name) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "melt variable/value names must not collide with existing columns.",
        ));
    }
    if id_vars.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "melt(id_vars=...) must be non-empty.",
        ));
    }
    for c in id_vars.iter() {
        if !plan.schema.contains_key(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "melt() unknown id_var '{}'.",
                c
            )));
        }
    }

    let value_vars = if let Some(v) = value_vars {
        if v.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "melt(value_vars=...) cannot be empty when provided.",
            ));
        }
        v
    } else {
        let id_set: std::collections::HashSet<String> = id_vars.iter().cloned().collect();
        let mut cols: Vec<String> = plan
            .schema
            .keys()
            .filter(|c| !id_set.contains(*c))
            .cloned()
            .collect();
        cols.sort();
        cols
    };
    if value_vars.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "melt() requires at least one value column.",
        ));
    }
    for c in value_vars.iter() {
        if !plan.schema.contains_key(c) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "melt() unknown value_var '{}'.",
                c
            )));
        }
    }

    let mut base: Option<crate::dtype::BaseType> = None;
    let mut nullable_any = false;
    for c in value_vars.iter() {
        let dt = plan.schema.get(c).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "internal: plan schema missing column '{c}' after validation.",
            ))
        })?;
        let Some(b) = dt.as_scalar_base_field().flatten() else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "melt only supports scalar value columns.",
            ));
        };
        if let Some(prev) = base {
            if prev != b {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "melt(value_vars=...) requires compatible scalar dtypes across value columns.",
                ));
            }
        } else {
            base = Some(b);
        }
        match dt {
            DTypeDesc::Scalar { nullable, .. }
            | DTypeDesc::Struct { nullable, .. }
            | DTypeDesc::List { nullable, .. }
            | DTypeDesc::Map { nullable, .. } => {
                if *nullable {
                    nullable_any = true;
                }
            }
        }
    }
    let base = base.ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("internal: melt base dtype not computed.")
    })?;

    let mut new_schema = HashMap::new();
    for c in id_vars.iter() {
        let dt = plan.schema.get(c).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "internal: plan schema missing column '{c}' after validation.",
            ))
        })?;
        new_schema.insert(c.clone(), dt.clone());
    }
    new_schema.insert(
        variable_name.clone(),
        DTypeDesc::Scalar {
            base: Some(crate::dtype::BaseType::Str),
            nullable: false,
            literals: None,
        },
    );
    new_schema.insert(
        value_name.clone(),
        DTypeDesc::Scalar {
            base: Some(base),
            nullable: nullable_any,
            literals: None,
        },
    );

    let mut new_steps = plan.steps.clone();
    new_steps.push(PlanStep::Melt {
        id_vars,
        value_vars,
        variable_name,
        value_name,
    });
    Ok(PlanInner {
        steps: new_steps,
        schema: new_schema,
        root_schema: plan.root_schema.clone(),
    })
}

pub fn plan_rolling_agg(
    plan: &PlanInner,
    column: String,
    window_size: usize,
    min_periods: usize,
    op: String,
    out_name: String,
    partition_by: Vec<String>,
) -> PyResult<PlanInner> {
    if window_size == 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "rolling(window_size=...) must be >= 1.",
        ));
    }
    if !plan.schema.contains_key(&column) {
        return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
            "rolling() unknown column '{}'.",
            column
        )));
    }
    if plan.schema.contains_key(&out_name) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "rolling(out_name=...) '{}' already exists.",
            out_name
        )));
    }
    let Some(base) = plan
        .schema
        .get(&column)
        .and_then(|d| d.as_scalar_base_field().flatten())
    else {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "rolling only supports scalar input columns.",
        ));
    };
    let (out_base, out_nullable) = match op.as_str() {
        "count" => (crate::dtype::BaseType::Int, true),
        "mean" => (crate::dtype::BaseType::Float, true),
        "sum" | "min" | "max" => (base, true),
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "rolling op must be one of: sum, mean, min, max, count.",
            ))
        }
    };
    let mut new_schema = plan.schema.clone();
    new_schema.insert(
        out_name.clone(),
        DTypeDesc::Scalar {
            base: Some(out_base),
            nullable: out_nullable,
            literals: None,
        },
    );
    let mut new_steps = plan.steps.clone();
    for p in partition_by.iter() {
        if !plan.schema.contains_key(p) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "rolling() partition column '{}' not in schema.",
                p
            )));
        }
    }
    if !partition_by.is_empty() {
        let mut sort_by = partition_by.clone();
        let mut rest: Vec<String> = plan
            .schema
            .keys()
            .filter(|k| !partition_by.iter().any(|p| p == *k))
            .cloned()
            .collect();
        rest.sort();
        sort_by.extend(rest);
        let n = sort_by.len();
        new_steps.push(PlanStep::Sort {
            by: sort_by,
            descending: vec![false; n],
            nulls_last: vec![false; n],
            maintain_order: false,
        });
    }
    new_steps.push(PlanStep::RollingAgg {
        column,
        window_size,
        min_periods,
        op,
        out_name,
        partition_by,
    });
    Ok(PlanInner {
        steps: new_steps,
        schema: new_schema,
        root_schema: plan.root_schema.clone(),
    })
}
