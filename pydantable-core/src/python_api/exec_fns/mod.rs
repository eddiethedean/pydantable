#![cfg_attr(not(feature = "polars_engine"), allow(unused_variables))]

//! Physical execution and sink PyO3 bindings (`execute_*`, `sink_*`, `collect_plan_batches`).

mod execute;
mod physical;
mod sinks;

use pyo3::prelude::*;

pub(super) fn register_functions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(execute::execute_plan, m)?)?;
    m.add_function(wrap_pyfunction!(sinks::sink_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(sinks::sink_csv, m)?)?;
    m.add_function(wrap_pyfunction!(sinks::sink_ipc, m)?)?;
    m.add_function(wrap_pyfunction!(sinks::sink_ndjson, m)?)?;
    m.add_function(wrap_pyfunction!(sinks::collect_plan_batches, m)?)?;
    m.add_function(wrap_pyfunction!(physical::execute_join, m)?)?;
    m.add_function(wrap_pyfunction!(physical::execute_groupby_agg, m)?)?;
    m.add_function(wrap_pyfunction!(physical::execute_concat, m)?)?;
    m.add_function(wrap_pyfunction!(physical::execute_except_all, m)?)?;
    m.add_function(wrap_pyfunction!(physical::execute_intersect_all, m)?)?;
    m.add_function(wrap_pyfunction!(physical::execute_melt, m)?)?;
    m.add_function(wrap_pyfunction!(physical::execute_pivot, m)?)?;
    m.add_function(wrap_pyfunction!(physical::execute_explode, m)?)?;
    m.add_function(wrap_pyfunction!(physical::execute_posexplode, m)?)?;
    m.add_function(wrap_pyfunction!(physical::execute_unnest, m)?)?;
    m.add_function(wrap_pyfunction!(physical::execute_rolling_agg, m)?)?;
    m.add_function(wrap_pyfunction!(physical::execute_groupby_dynamic_agg, m)?)?;
    Ok(())
}
