//! Lazy `LazyFrame` from `ScanFileRoot` or in-memory Python columns, plus file sinks.

use std::fs::File;
use std::path::{Path, PathBuf};

use polars::prelude::{Engine, *};
use polars_io::ipc::IpcCompression;
use polars_io::prelude::IpcWriter;
use polars_io::SerWriter;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};

use crate::plan::ir::PlanInner;

use super::common::{polars_dataframe_to_python_via_ipc, polars_err, root_data_to_polars_df};
use super::runner::PolarsPlanRunner;
use super::scan_kw::{dispatch_file_scan, write_csv_file, write_ndjson_file, write_parquet_file};

pub(crate) const SCAN_FILE_ROOT_NAME: &str = "ScanFileRoot";

/// True if `obj` is `pydantable._core.ScanFileRoot`.
pub(crate) fn is_scan_file_root(obj: &Bound<'_, PyAny>) -> PyResult<bool> {
    let cls = obj.get_type();
    let name: String = cls.getattr("__name__")?.extract()?;
    if name != SCAN_FILE_ROOT_NAME {
        return Ok(false);
    }
    let module: String = cls.getattr("__module__")?.extract()?;
    Ok(module == "pydantable._core")
}

fn scan_kw_from_root(root_data: &Bound<'_, PyAny>) -> PyResult<Option<Py<PyDict>>> {
    if !root_data.hasattr("scan_kwargs")? {
        return Ok(None);
    }
    let b = root_data.getattr("scan_kwargs")?;
    if b.is_none() {
        return Ok(None);
    }
    let d = b.downcast::<PyDict>()?;
    Ok(Some(d.clone().unbind()))
}

/// Where physical rows for a plan root come from (in-memory columns vs on-disk scan).
enum RootSource {
    InMemory,
    Scan {
        path: String,
        format_lower: String,
        columns: Option<Vec<String>>,
        scan_kw: Option<Py<PyDict>>,
    },
}

fn root_source_from_py(_py: Python<'_>, root_data: &Bound<'_, PyAny>) -> PyResult<RootSource> {
    if is_scan_file_root(root_data)? {
        let path: String = root_data.getattr("path")?.extract()?;
        let format: String = root_data.getattr("format")?.extract()?;
        let format_lower = format.to_ascii_lowercase();
        let columns: Option<Vec<String>> = root_data
            .getattr("columns")
            .ok()
            .and_then(|c| c.extract().ok());
        let scan_kw = scan_kw_from_root(root_data)?;
        Ok(RootSource::Scan {
            path,
            format_lower,
            columns,
            scan_kw,
        })
    } else {
        Ok(RootSource::InMemory)
    }
}

/// Build the base `LazyFrame` for `plan` (no plan steps applied yet).
pub(crate) fn base_lazy_frame(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
) -> PyResult<LazyFrame> {
    match root_source_from_py(py, root_data)? {
        RootSource::Scan {
            path,
            format_lower,
            columns,
            scan_kw,
        } => {
            let kw_bound: Option<Bound<'_, PyDict>> = scan_kw.as_ref().map(|p| p.bind(py).clone());
            dispatch_file_scan(py, path, format_lower.as_str(), columns, kw_bound.as_ref())
        }
        RootSource::InMemory => {
            let df = root_data_to_polars_df(py, &plan.root_schema, root_data)?;
            Ok(df.lazy())
        }
    }
}

/// Apply `plan.steps` on top of the base lazy frame from `root_data`.
pub(crate) fn plan_to_lazyframe(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
) -> PyResult<LazyFrame> {
    let base = base_lazy_frame(py, plan, root_data)?;
    PolarsPlanRunner::apply_steps(base, &plan.steps)
}

/// Collect a `LazyFrame` with the requested Polars engine (streaming vs in-memory).
pub(crate) fn collect_lazyframe(
    py: Python<'_>,
    lf: LazyFrame,
    streaming: bool,
) -> PyResult<DataFrame> {
    let engine = if streaming {
        Engine::Streaming
    } else {
        Engine::InMemory
    };
    py.allow_threads(move || lf.collect_with_engine(engine).map_err(polars_err))
}

pub(crate) fn ipc_compression_from_str(s: Option<&str>) -> PyResult<Option<IpcCompression>> {
    let s = s.map(str::trim).filter(|x| !x.is_empty());
    match s {
        None | Some("none") | Some("uncompressed") => Ok(None),
        Some("lz4") => Ok(Some(IpcCompression::LZ4)),
        Some("zstd") => Ok(Some(IpcCompression::ZSTD(Default::default()))),
        Some(other) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Unknown IPC compression {other:?}; use None, 'lz4', or 'zstd'."
        ))),
    }
}

fn write_kw_as_dict<'py>(
    write_kw: Option<&'py Bound<'py, PyAny>>,
) -> PyResult<Option<&'py Bound<'py, PyDict>>> {
    match write_kw {
        None => Ok(None),
        Some(b) => {
            if b.is_none() {
                Ok(None)
            } else {
                Ok(Some(b.downcast::<PyDict>()?))
            }
        }
    }
}

fn hive_value_str(av: AnyValue<'_>) -> String {
    match av {
        AnyValue::String(s) => s.to_string(),
        AnyValue::StringOwned(s) => s.to_string(),
        _ => format!("{av}"),
    }
}

fn hive_segment_for_cell(av: AnyValue<'_>, col_name: &str) -> String {
    let mut value = hive_value_str(av);
    value = value.replace(['/', '\\'], "_");
    format!("{col_name}={value}")
}

/// Execute plan and write Parquet without materializing to Python `dict[str, list]`.
///
/// When `partition_by` is non-empty, `path` is the **dataset root directory**; rows are split with
/// [`DataFrame::partition_by_stable`], written under hive-style `col=value/.../00000000.parquet`
/// shards (partition columns are omitted from each file, matching common hive layouts).
#[allow(clippy::too_many_arguments)]
pub(crate) fn sink_parquet_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    path: String,
    streaming: bool,
    write_kw: Option<&Bound<'_, PyAny>>,
    partition_by: Option<Vec<String>>,
    mkdir: bool,
) -> PyResult<()> {
    let lf = plan_to_lazyframe(py, plan, root_data)?;
    let mut df = collect_lazyframe(py, lf, streaming)?;
    let d = write_kw_as_dict(write_kw)?;

    let cols: Vec<String> = partition_by
        .into_iter()
        .flatten()
        .filter(|s| !s.is_empty())
        .collect();
    if cols.is_empty() {
        return write_parquet_file(py, path.as_str(), &mut df, d);
    }

    let root = Path::new(path.as_str());
    if root.exists() && root.is_file() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "sink_parquet with partition_by requires path to be a directory (or not yet existing), not an existing file",
        ));
    }
    if mkdir {
        std::fs::create_dir_all(root).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyOSError, _>(format!(
                "sink_parquet partition root mkdir: {e}"
            ))
        })?;
    } else if !root.exists() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "sink_parquet with partition_by and mkdir=False requires an existing directory path",
        ));
    }

    let schema = df.schema();
    for c in &cols {
        if schema.get(c.as_str()).is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "sink_parquet partition_by: unknown column {c:?}"
            )));
        }
    }

    let parts = df.partition_by_stable(cols.clone(), true).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("sink_parquet partition_by: {e}"))
    })?;

    for part_df in parts {
        let mut dir = PathBuf::from(path.as_str());
        for c in &cols {
            let col = part_df.column(c.as_str()).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "sink_parquet partition shard: {e}"
                ))
            })?;
            let av = col.as_materialized_series().get(0).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "sink_parquet partition column {c:?}: {e}"
                ))
            })?;
            let seg = hive_segment_for_cell(av, c);
            dir.push(seg);
        }
        if mkdir {
            std::fs::create_dir_all(&dir).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyOSError, _>(format!(
                    "sink_parquet partition shard mkdir: {e}"
                ))
            })?;
        }
        let out_path = dir.join("00000000.parquet");
        let out_str = out_path.to_str().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "sink_parquet partition path is not valid UTF-8",
            )
        })?;
        let mut to_write = part_df.drop_many(cols.iter().cloned());
        write_parquet_file(py, out_str, &mut to_write, d)?;
    }

    Ok(())
}

pub(crate) fn sink_csv_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    path: String,
    streaming: bool,
    separator: u8,
    write_kw: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    let lf = plan_to_lazyframe(py, plan, root_data)?;
    let mut df = collect_lazyframe(py, lf, streaming)?;
    let d = write_kw_as_dict(write_kw)?;
    write_csv_file(py, path.as_str(), &mut df, separator, d)
}

pub(crate) fn sink_ipc_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    path: String,
    streaming: bool,
    compression: Option<String>,
    write_kw: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    if let Some(w) = write_kw {
        if !w.is_none() {
            let d = write_kw_as_dict(Some(w))?;
            if let Some(dict) = d {
                if dict.len() > 0 {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "sink_ipc does not accept write_kwargs yet; use compression= only",
                    ));
                }
            }
        }
    }
    let lf = plan_to_lazyframe(py, plan, root_data)?;
    let mut df = collect_lazyframe(py, lf, streaming)?;
    let comp = ipc_compression_from_str(compression.as_deref())?;
    let mut file = File::create(path.as_str())
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("sink_ipc: {e}")))?;
    let mut writer = IpcWriter::new(&mut file).with_compression(comp);
    writer.finish(&mut df).map_err(polars_err)?;
    Ok(())
}

pub(crate) fn sink_ndjson_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    path: String,
    streaming: bool,
    write_kw: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    let lf = plan_to_lazyframe(py, plan, root_data)?;
    let mut df = collect_lazyframe(py, lf, streaming)?;
    let d = write_kw_as_dict(write_kw)?;
    write_ndjson_file(py, path.as_str(), &mut df, d)
}

/// Materialize the plan, then split rows into Polars `DataFrame` chunks (via IPC to Python).
///
/// This uses a full `collect` first, then slices—true lazy batch collection would need Polars async APIs.
pub(crate) fn collect_plan_batches_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    batch_size: usize,
    streaming: bool,
) -> PyResult<Vec<PyObject>> {
    let lf = plan_to_lazyframe(py, plan, root_data)?;
    let df = collect_lazyframe(py, lf, streaming)?;
    let h = df.height();
    let bs = batch_size.max(1);
    let mut out: Vec<PyObject> = Vec::new();
    let mut offset = 0usize;
    while offset < h {
        let take = (h - offset).min(bs);
        let mut chunk = df.slice(offset as i64, take);
        out.push(polars_dataframe_to_python_via_ipc(py, &mut chunk)?);
        offset += take;
    }
    Ok(out)
}
