//! Logical plans, serialization, and execution backends.

mod build;
#[cfg(not(feature = "polars_engine"))]
mod context;
#[cfg(feature = "polars_engine")]
mod execute_polars;
#[cfg(not(feature = "polars_engine"))]
mod execute_rowwise;
mod executor;
mod ir;
mod schema_py;
mod serialize;

pub use build::*;
#[cfg(feature = "polars_engine")]
#[allow(unused_imports)]
pub use execute_polars::{
    execute_concat_polars, execute_explode_polars, execute_groupby_agg_polars,
    execute_groupby_dynamic_agg_polars, execute_join_polars, execute_melt_polars,
    execute_pivot_polars, execute_unnest_polars, PolarsPlanRunner,
};
pub use executor::PhysicalPlanExecutor;
#[allow(unused_imports)]
pub use ir::{make_plan, PlanInner, PlanStep};
pub use schema_py::{schema_descriptors_as_py, schema_fields_as_py};
pub use serialize::planinner_to_serializable;

use pyo3::prelude::*;
use pyo3::types::PyAny;

#[cfg(feature = "polars_engine")]
use crate::plan::executor::PolarsExecutor;

#[cfg(not(feature = "polars_engine"))]
use crate::plan::executor::RowwiseExecutor;

/// Entry point: Polars engine when enabled, otherwise row-wise Python evaluation.
pub fn execute_plan(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    as_python_lists: bool,
) -> PyResult<PyObject> {
    #[cfg(feature = "polars_engine")]
    {
        PolarsExecutor.execute_plan(py, plan, root_data, as_python_lists)
    }
    #[cfg(not(feature = "polars_engine"))]
    {
        RowwiseExecutor.execute_plan(py, plan, root_data, as_python_lists)
    }
}

#[cfg(test)]
mod tests;
