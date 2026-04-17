//! Serialization of `PlanInner` to Python dict structures.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use pyo3::IntoPyObjectExt;

use crate::expr::{exprnode_to_serializable, LiteralValue};

use super::ir::{PlanInner, PlanStep};
use super::schema_py::schema_descriptors_as_py;

pub fn planinner_to_serializable(py: Python<'_>, inner: &PlanInner) -> PyResult<PyObject> {
    let out = PyDict::new(py);
    out.set_item("version", 1)?;

    out.set_item(
        "schema_descriptors",
        schema_descriptors_as_py(py, &inner.schema)?,
    )?;
    out.set_item(
        "root_schema_descriptors",
        schema_descriptors_as_py(py, &inner.root_schema)?,
    )?;

    let steps = PyList::empty(py);
    for step in inner.steps.iter() {
        let step_out = PyDict::new(py);
        match step {
            PlanStep::Select { columns } => {
                step_out.set_item("kind", "select")?;
                step_out.set_item("columns", columns)?;
            }
            PlanStep::GlobalSelect { items } => {
                step_out.set_item("kind", "global_select")?;
                let cols = PyDict::new(py);
                for (name, expr) in items {
                    cols.set_item(name, exprnode_to_serializable(py, expr)?)?;
                }
                step_out.set_item("columns", cols)?;
            }
            PlanStep::WithColumns { columns } => {
                step_out.set_item("kind", "with_columns")?;
                let cols = PyDict::new(py);
                for (name, expr) in columns.iter() {
                    cols.set_item(name, exprnode_to_serializable(py, expr)?)?;
                }
                step_out.set_item("columns", cols)?;
            }
            PlanStep::Filter { condition } => {
                step_out.set_item("kind", "filter")?;
                step_out.set_item("condition", exprnode_to_serializable(py, condition)?)?;
            }
            PlanStep::Sort {
                by,
                descending,
                nulls_last,
                maintain_order,
            } => {
                step_out.set_item("kind", "sort")?;
                step_out.set_item("by", by)?;
                step_out.set_item("descending", descending)?;
                step_out.set_item("nulls_last", nulls_last)?;
                step_out.set_item("maintain_order", maintain_order)?;
            }
            PlanStep::Unique {
                subset,
                keep,
                maintain_order,
            } => {
                step_out.set_item("kind", "unique")?;
                step_out.set_item("subset", subset)?;
                step_out.set_item("keep", keep)?;
                step_out.set_item("maintain_order", maintain_order)?;
            }
            PlanStep::Rename { columns } => {
                step_out.set_item("kind", "rename")?;
                step_out.set_item("columns", columns)?;
            }
            PlanStep::Slice { offset, length } => {
                step_out.set_item("kind", "slice")?;
                step_out.set_item("offset", offset)?;
                step_out.set_item("length", length)?;
            }
            PlanStep::FillNull {
                subset,
                value,
                strategy,
            } => {
                step_out.set_item("kind", "fill_null")?;
                step_out.set_item("subset", subset)?;
                let value_obj = match value {
                    None => py.None(),
                    Some(LiteralValue::Int(v)) => v.into_py_any(py)?,
                    Some(LiteralValue::Float(v)) => v.into_py_any(py)?,
                    Some(LiteralValue::Bool(v)) => v.into_py_any(py)?,
                    Some(LiteralValue::Str(v)) => v.clone().into_py_any(py)?,
                    Some(LiteralValue::Uuid(v)) => v.clone().into_py_any(py)?,
                    Some(LiteralValue::Decimal(v)) => v.into_py_any(py)?,
                    Some(LiteralValue::EnumStr(v)) => v.clone().into_py_any(py)?,
                    Some(LiteralValue::DateTimeMicros(v)) => v.into_py_any(py)?,
                    Some(LiteralValue::DateDays(v)) => v.into_py_any(py)?,
                    Some(LiteralValue::DurationMicros(v)) => v.into_py_any(py)?,
                    Some(LiteralValue::TimeNanos(v)) => v.into_py_any(py)?,
                    Some(LiteralValue::Binary(b)) => PyBytes::new(py, b).into_py_any(py)?,
                };
                step_out.set_item("value", value_obj)?;
                step_out.set_item("strategy", strategy)?;
            }
            PlanStep::DropNulls {
                subset,
                how,
                threshold,
            } => {
                step_out.set_item("kind", "drop_nulls")?;
                step_out.set_item("subset", subset)?;
                step_out.set_item("how", how)?;
                step_out.set_item("threshold", threshold)?;
            }
            PlanStep::WithRowCount { name, offset } => {
                step_out.set_item("kind", "with_row_count")?;
                step_out.set_item("name", name)?;
                step_out.set_item("offset", offset)?;
            }
            PlanStep::Melt {
                id_vars,
                value_vars,
                variable_name,
                value_name,
            } => {
                step_out.set_item("kind", "melt")?;
                step_out.set_item("id_vars", id_vars)?;
                step_out.set_item("value_vars", value_vars)?;
                step_out.set_item("variable_name", variable_name)?;
                step_out.set_item("value_name", value_name)?;
            }
            PlanStep::RollingAgg {
                column,
                window_size,
                min_periods,
                op,
                out_name,
                partition_by,
            } => {
                step_out.set_item("kind", "rolling_agg")?;
                step_out.set_item("column", column)?;
                step_out.set_item("window_size", window_size)?;
                step_out.set_item("min_periods", min_periods)?;
                step_out.set_item("op", op)?;
                step_out.set_item("out_name", out_name)?;
                step_out.set_item("partition_by", partition_by)?;
            }
            PlanStep::DuplicateMask { subset, keep } => {
                step_out.set_item("kind", "duplicate_mask")?;
                step_out.set_item("subset", subset)?;
                step_out.set_item("keep", keep)?;
            }
            PlanStep::DropDuplicateGroups { subset } => {
                step_out.set_item("kind", "drop_duplicate_groups")?;
                step_out.set_item("subset", subset)?;
            }
        }
        steps.append(step_out)?;
    }

    out.set_item("steps", steps)?;
    Ok(out.unbind().into())
}
