//! Polars-backed physical execution for logical plans.

mod common;
mod concat_exec;
mod groupby_exec;
mod join_exec;
mod literal_agg;
mod materialize;
mod reshape_exec;
mod root_lazy;
mod runner;
mod scan_kw;
mod setops_exec;

pub(crate) use concat_exec::execute_concat_polars;
pub(crate) use groupby_exec::execute_groupby_agg_polars;
pub(crate) use join_exec::execute_join_polars;
#[cfg(all(feature = "polars_engine", feature = "bench"))]
pub use materialize::bench_series_to_py_list;
pub(crate) use materialize::execute_plan_polars;
pub(crate) use materialize::{dtype_from_polars, series_to_py_list};
pub(crate) use reshape_exec::{
    execute_explode_polars, execute_groupby_dynamic_agg_polars, execute_melt_polars,
    execute_pivot_polars, execute_posexplode_polars, execute_unnest_polars,
};
#[cfg(all(feature = "polars_engine", feature = "bench"))]
pub use root_lazy::bench_collect_lazyframe;
pub(crate) use root_lazy::{
    collect_plan_batches_polars, sink_csv_polars, sink_ipc_polars, sink_ndjson_polars,
    sink_parquet_polars,
};
pub(crate) use setops_exec::{execute_except_all_polars, execute_intersect_all_polars};
