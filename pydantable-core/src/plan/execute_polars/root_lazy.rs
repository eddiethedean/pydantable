//! Lazy `LazyFrame` from `ScanFileRoot` or in-memory Python columns, plus file sinks.

use std::fs::File;

use polars::lazy::dsl::col;
use polars::prelude::{Engine, *};
use polars_io::ipc::IpcCompression;
use polars_io::ipc::IpcScanOptions;
use polars_io::prelude::{CsvWriter, IpcWriter, JsonFormat, JsonWriter, ParquetWriter};
use polars_io::SerWriter;
use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::plan::ir::PlanInner;

use super::common::{polars_dataframe_to_python_via_ipc, polars_err, root_data_to_polars_df};
use super::runner::PolarsPlanRunner;

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

/// Where physical rows for a plan root come from (in-memory columns vs on-disk scan).
enum RootSource {
    InMemory,
    Scan {
        path: String,
        format_lower: String,
        columns: Option<Vec<String>>,
    },
}

fn root_source_from_py(root_data: &Bound<'_, PyAny>) -> PyResult<RootSource> {
    if is_scan_file_root(root_data)? {
        let path: String = root_data.getattr("path")?.extract()?;
        let format: String = root_data.getattr("format")?.extract()?;
        let format_lower = format.to_ascii_lowercase();
        let columns: Option<Vec<String>> = root_data
            .getattr("columns")
            .ok()
            .and_then(|c| c.extract().ok());
        Ok(RootSource::Scan {
            path,
            format_lower,
            columns,
        })
    } else {
        Ok(RootSource::InMemory)
    }
}

fn apply_column_projection(mut lf: LazyFrame, columns: Option<Vec<String>>) -> LazyFrame {
    if let Some(cols) = columns {
        let exprs: Vec<Expr> = cols.iter().map(|c| col(c.as_str())).collect();
        lf = lf.select(exprs);
    }
    lf
}

fn dispatch_scan(
    py: Python<'_>,
    path: String,
    format: &str,
    columns: Option<Vec<String>>,
) -> PyResult<LazyFrame> {
    let lf = match format {
        "parquet" => py.allow_threads(move || {
            LazyFrame::scan_parquet(PlRefPath::new(path.as_str()), ScanArgsParquet::default())
        }),
        "csv" => py.allow_threads(move || {
            LazyCsvReader::new(PlRefPath::new(path.as_str())).finish()
        }),
        "ndjson" => py.allow_threads(move || {
            LazyJsonLineReader::new(PlRefPath::new(path.as_str())).finish()
        }),
        "ipc" => py.allow_threads(move || {
            LazyFrame::scan_ipc(
                PlRefPath::new(path.as_str()),
                IpcScanOptions::default(),
                polars::prelude::UnifiedScanArgs::default(),
            )
        }),
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unknown scan format {format:?}; expected 'parquet', 'csv', 'ndjson', or 'ipc'."
            )));
        }
    }
    .map_err(polars_err)?;
    Ok(apply_column_projection(lf, columns))
}

/// Build the base `LazyFrame` for `plan` (no plan steps applied yet).
pub(crate) fn base_lazy_frame(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
) -> PyResult<LazyFrame> {
    match root_source_from_py(root_data)? {
        RootSource::Scan {
            path,
            format_lower,
            columns,
        } => dispatch_scan(py, path, format_lower.as_str(), columns),
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

fn ipc_compression_from_str(s: Option<&str>) -> PyResult<Option<IpcCompression>> {
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

/// Execute plan and write Parquet without materializing to Python `dict[str, list]`.
pub(crate) fn sink_parquet_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    path: String,
    streaming: bool,
) -> PyResult<()> {
    let lf = plan_to_lazyframe(py, plan, root_data)?;
    let mut df = collect_lazyframe(py, lf, streaming)?;
    let mut file = File::create(path.as_str()).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("sink_parquet: {e}"))
    })?;
    ParquetWriter::new(&mut file)
        .finish(&mut df)
        .map_err(polars_err)?;
    Ok(())
}

pub(crate) fn sink_csv_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    path: String,
    streaming: bool,
    separator: u8,
) -> PyResult<()> {
    let lf = plan_to_lazyframe(py, plan, root_data)?;
    let mut df = collect_lazyframe(py, lf, streaming)?;
    let mut file = File::create(path.as_str()).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("sink_csv: {e}"))
    })?;
    CsvWriter::new(&mut file)
        .with_separator(separator)
        .finish(&mut df)
        .map_err(polars_err)?;
    Ok(())
}

pub(crate) fn sink_ipc_polars(
    py: Python<'_>,
    plan: &PlanInner,
    root_data: &Bound<'_, PyAny>,
    path: String,
    streaming: bool,
    compression: Option<String>,
) -> PyResult<()> {
    let lf = plan_to_lazyframe(py, plan, root_data)?;
    let mut df = collect_lazyframe(py, lf, streaming)?;
    let comp = ipc_compression_from_str(compression.as_deref())?;
    let mut file = File::create(path.as_str()).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("sink_ipc: {e}"))
    })?;
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
) -> PyResult<()> {
    let lf = plan_to_lazyframe(py, plan, root_data)?;
    let mut df = collect_lazyframe(py, lf, streaming)?;
    let mut file = File::create(path.as_str()).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("sink_ndjson: {e}"))
    })?;
    JsonWriter::new(&mut file)
        .with_json_format(JsonFormat::JsonLines)
        .finish(&mut df)
        .map_err(polars_err)?;
    Ok(())
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
