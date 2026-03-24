//! Polars-backed physical execution for logical plans.

mod common;
mod runner;
mod materialize;
mod join_exec;
mod groupby_exec;
mod concat_exec;
mod literal_agg;
mod reshape_exec;

pub use runner::PolarsPlanRunner;
pub(crate) use materialize::execute_plan_polars;
pub use join_exec::execute_join_polars;
pub use groupby_exec::execute_groupby_agg_polars;
pub use concat_exec::execute_concat_polars;
pub use reshape_exec::{
    execute_explode_polars, execute_groupby_dynamic_agg_polars, execute_melt_polars,
    execute_pivot_polars, execute_unnest_polars,
};
