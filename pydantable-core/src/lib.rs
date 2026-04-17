//! Native execution core for PydanTable: PyO3 extension `pydantable_native._core`.
//!
//! This crate is built as a **cdylib** (see [`pydantable-native`](../../pydantable-native)) and
//! is not published on crates.io on its own.
//!
//! - **`polars_engine` (default):** Polars-backed plan execution, I/O helpers, and expression
//!   lowering. Disable for minimal builds (`cargo check --no-default-features`).
//! - **Python surface:** [`python_api::register`] binds classes and functions to the extension
//!   module. [`async_fns`](python_api::async_fns) initializes the Tokio runtime via
//!   `pyo3_async_runtimes` on module import.
//!
//! Contributor setup and CI expectations: see the repo **Developer** guide (`docs/project/developer.md`).
//!
//! **PyO3 conventions:** use `Python::import` (PyO3 0.24 rename from `import_bound`) for imports;
//! prefer `IntoPyObject` / `IntoPyObjectExt::into_py_any` and `Bound::unbind` over deprecated
//! `IntoPy` / `.into_py(py)` — see <https://pyo3.rs/main/migration>.

mod dtype;
mod expr;
#[cfg(feature = "polars_engine")]
mod io_polars;
mod plan;
#[cfg(feature = "polars_engine")]
mod polars_dtype;
mod py_datetime;
mod python_api;

use pyo3::prelude::*;
use pyo3::types::PyModule;

// Bench-only re-exports (not part of the Python extension API).
#[cfg(feature = "bench")]
pub use crate::dtype::{BaseType, DTypeDesc};
#[cfg(all(feature = "polars_engine", feature = "bench"))]
pub use crate::plan::{bench_collect_lazyframe, bench_series_to_py_list};

/// PyO3 extension module `pydantable_native._core` (built via maturin).
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Optional: customize Tokio multi-thread pool before first `pyo3_async_runtimes` use.
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    pyo3_async_runtimes::tokio::init(builder);
    python_api::register(m)
}
