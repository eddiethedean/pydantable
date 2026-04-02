//! Optional scan/write kwargs as Python dicts mapped to Polars options.

use std::fs::File;
use std::num::NonZeroUsize;

use polars::lazy::dsl::col;
use polars::prelude::{
    DataFrame, Expr, LazyCsvReader, LazyFileListReader, LazyFrame, LazyJsonLineReader, PlRefPath,
    ScanArgsParquet, UnifiedScanArgs,
};
use polars_io::ipc::IpcScanOptions;
use polars_io::parquet::read::ParallelStrategy;
use polars_io::parquet::write::{ParquetCompression, ParquetWriteOptions, StatisticsOptions};
use polars_io::prelude::{CsvWriter, JsonFormat, JsonWriter, ParquetWriter};
use polars_io::RowIndex;
use polars_io::SerWriter;
use polars_utils::pl_str::PlSmallStr;
use polars_utils::IdxSize;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyDictMethods};

use super::common::polars_err;

fn unknown_scan_keys(_py: Python<'_>, d: &Bound<'_, PyDict>, allowed: &[&str]) -> PyResult<()> {
    for (k, _) in d.iter() {
        let key: String = k.extract()?;
        if !allowed.contains(&key.as_str()) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "unknown scan_kw key {key:?} for this format; allowed: {}",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
}

fn get_bool(d: &Bound<'_, PyDict>, key: &str) -> PyResult<Option<bool>> {
    match d.get_item(key)? {
        None => Ok(None),
        Some(v) => {
            if v.is_none() {
                Ok(None)
            } else {
                Ok(Some(v.extract::<bool>()?))
            }
        }
    }
}

fn get_usize(d: &Bound<'_, PyDict>, key: &str) -> PyResult<Option<usize>> {
    match d.get_item(key)? {
        None => Ok(None),
        Some(v) => {
            if v.is_none() {
                Ok(None)
            } else {
                Ok(Some(v.extract::<usize>()?))
            }
        }
    }
}

fn get_str(d: &Bound<'_, PyDict>, key: &str) -> PyResult<Option<String>> {
    match d.get_item(key)? {
        None => Ok(None),
        Some(v) => {
            if v.is_none() {
                Ok(None)
            } else {
                Ok(Some(v.extract::<String>()?))
            }
        }
    }
}

fn parse_parallel_strategy(s: &str) -> PyResult<ParallelStrategy> {
    match s.to_ascii_lowercase().as_str() {
        "none" => Ok(ParallelStrategy::None),
        "columns" => Ok(ParallelStrategy::Columns),
        "row_groups" | "rowgroups" => Ok(ParallelStrategy::RowGroups),
        "prefiltered" => Ok(ParallelStrategy::Prefiltered),
        "auto" => Ok(ParallelStrategy::Auto),
        _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "parallel must be one of: none, columns, row_groups, prefiltered, auto (got {s:?})"
        ))),
    }
}

fn idx_size_from_usize(n: usize) -> PyResult<IdxSize> {
    IdxSize::try_from(n).map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "row_index_offset must fit in IdxSize (got {n})"
        ))
    })
}

/// Shared `row_index_name` / `row_index_offset` handling for Parquet and CSV lazy scans.
/// - `Ok(None)` — do not change row index.
/// - `Ok(Some(None))` — clear row index.
/// - `Ok(Some(Some(RowIndex)))` — set row index column.
fn row_index_update_from_kwargs(kw: &Bound<'_, PyDict>) -> PyResult<Option<Option<RowIndex>>> {
    let has_name_key = kw.contains("row_index_name")?;
    let has_offset_only = kw.contains("row_index_offset")? && !has_name_key;
    if has_offset_only {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "row_index_offset requires row_index_name",
        ));
    }
    if !has_name_key {
        return Ok(None);
    }
    match kw.get_item("row_index_name")? {
        None => Ok(None),
        Some(v) if v.is_none() => Ok(Some(None)),
        Some(v) => {
            let name: String = v.extract()?;
            if name.is_empty() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "row_index_name must be non-empty when set",
                ));
            }
            let off = get_usize(kw, "row_index_offset")?.unwrap_or(0);
            Ok(Some(Some(RowIndex {
                name: PlSmallStr::from_str(&name),
                offset: idx_size_from_usize(off)?,
            })))
        }
    }
}

fn scan_args_parquet_from_kwargs(
    py: Python<'_>,
    mut args: ScanArgsParquet,
    kw: &Bound<'_, PyDict>,
) -> PyResult<ScanArgsParquet> {
    const ALLOWED: &[&str] = &[
        "n_rows",
        "low_memory",
        "rechunk",
        "use_statistics",
        "cache",
        "glob",
        "allow_missing_columns",
        "parallel",
        "hive_partitioning",
        "hive_start_idx",
        "try_parse_hive_dates",
        "include_file_paths",
        "row_index_name",
        "row_index_offset",
    ];
    unknown_scan_keys(py, kw, ALLOWED)?;
    if let Some(n) = get_usize(kw, "n_rows")? {
        args.n_rows = Some(n);
    }
    if let Some(v) = get_bool(kw, "low_memory")? {
        args.low_memory = v;
    }
    if let Some(v) = get_bool(kw, "rechunk")? {
        args.rechunk = v;
    }
    if let Some(v) = get_bool(kw, "use_statistics")? {
        args.use_statistics = v;
    }
    if let Some(v) = get_bool(kw, "cache")? {
        args.cache = v;
    }
    if let Some(v) = get_bool(kw, "glob")? {
        args.glob = v;
    }
    if let Some(v) = get_bool(kw, "allow_missing_columns")? {
        args.allow_missing_columns = v;
    }
    if let Some(s) = get_str(kw, "parallel")? {
        args.parallel = parse_parallel_strategy(s.trim())?;
    }

    // Hive partitioning (ScanArgsParquet.hive_options)
    if kw.contains("hive_partitioning")? {
        match kw.get_item("hive_partitioning")? {
            None => {}
            Some(v) if v.is_none() => args.hive_options.enabled = None,
            Some(v) => args.hive_options.enabled = Some(v.extract::<bool>()?),
        }
    }
    if let Some(n) = get_usize(kw, "hive_start_idx")? {
        args.hive_options.hive_start_idx = n;
    }
    if let Some(v) = get_bool(kw, "try_parse_hive_dates")? {
        args.hive_options.try_parse_dates = v;
    }

    // Lineage column for source file paths
    if kw.contains("include_file_paths")? {
        match kw.get_item("include_file_paths")? {
            None => {}
            Some(v) if v.is_none() => args.include_file_paths = None,
            Some(v) => {
                let s: String = v.extract()?;
                args.include_file_paths = Some(PlSmallStr::from_str(&s));
            }
        }
    }

    match row_index_update_from_kwargs(kw)? {
        None => {}
        Some(ri) => {
            args.row_index = ri;
        }
    }

    Ok(args)
}

fn lazy_csv_with_kwargs(
    py: Python<'_>,
    path: PlRefPath,
    kw: &Bound<'_, PyDict>,
) -> PyResult<LazyCsvReader> {
    let mut r = LazyCsvReader::new(path);
    const ALLOWED: &[&str] = &[
        "has_header",
        "separator",
        "skip_rows",
        "skip_lines",
        "n_rows",
        "infer_schema_length",
        "ignore_errors",
        "low_memory",
        "rechunk",
        "glob",
        "cache",
        "quote_char",
        "eol_char",
        "include_file_paths",
        "row_index_name",
        "row_index_offset",
        "raise_if_empty",
        "truncate_ragged_lines",
        "decimal_comma",
        "try_parse_dates",
    ];
    unknown_scan_keys(py, kw, ALLOWED)?;
    if let Some(v) = get_bool(kw, "has_header")? {
        r = r.with_has_header(v);
    }
    if let Some(sep) = get_str(kw, "separator")? {
        let b = sep.as_bytes();
        if b.len() != 1 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "separator must be a one-character string",
            ));
        }
        r = r.with_separator(b[0]);
    }
    if let Some(n) = get_usize(kw, "skip_rows")? {
        r = r.with_skip_rows(n);
    }
    if let Some(n) = get_usize(kw, "skip_lines")? {
        r = r.with_skip_lines(n);
    }
    if let Some(n) = get_usize(kw, "n_rows")? {
        r = r.with_n_rows(Some(n));
    }
    if kw.contains("infer_schema_length")? {
        if let Some(v) = kw.get_item("infer_schema_length")? {
            if v.is_none() {
                r = r.with_infer_schema_length(None);
            } else {
                let n: usize = v.extract()?;
                r = r.with_infer_schema_length(Some(n));
            }
        }
    }
    if let Some(v) = get_bool(kw, "ignore_errors")? {
        r = r.with_ignore_errors(v);
    }
    if let Some(v) = get_bool(kw, "low_memory")? {
        r = r.with_low_memory(v);
    }
    if let Some(v) = get_bool(kw, "rechunk")? {
        r = LazyFileListReader::with_rechunk(r, v);
    }
    if let Some(v) = get_bool(kw, "glob")? {
        r = r.with_glob(v);
    }
    if let Some(v) = get_bool(kw, "cache")? {
        r = r.with_cache(v);
    }
    if kw.contains("quote_char")? {
        if let Some(v) = kw.get_item("quote_char")? {
            let qc: Option<u8> = if v.is_none() {
                None
            } else {
                let s: String = v.extract()?;
                let b = s.as_bytes();
                if b.len() != 1 {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "quote_char must be a one-character string or None",
                    ));
                }
                Some(b[0])
            };
            r = r.with_quote_char(qc);
        }
    }
    if let Some(n) = get_usize(kw, "eol_char")? {
        if n > 255 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "eol_char must be <= 255",
            ));
        }
        r = r.with_eol_char(n as u8);
    }

    if kw.contains("include_file_paths")? {
        match kw.get_item("include_file_paths")? {
            None => {}
            Some(v) if v.is_none() => {
                r = r.with_include_file_paths(None);
            }
            Some(v) => {
                let s: String = v.extract()?;
                r = r.with_include_file_paths(Some(PlSmallStr::from_str(&s)));
            }
        }
    }

    match row_index_update_from_kwargs(kw)? {
        None => {}
        Some(ri) => {
            r = r.with_row_index(ri);
        }
    }

    if let Some(v) = get_bool(kw, "raise_if_empty")? {
        r = r.with_raise_if_empty(v);
    }
    if let Some(v) = get_bool(kw, "truncate_ragged_lines")? {
        r = r.with_truncate_ragged_lines(v);
    }
    if let Some(v) = get_bool(kw, "decimal_comma")? {
        r = r.with_decimal_comma(v);
    }
    if let Some(v) = get_bool(kw, "try_parse_dates")? {
        r = r.with_try_parse_dates(v);
    }

    Ok(r)
}

fn ipc_scan_options_from_kwargs(
    py: Python<'_>,
    kw: &Bound<'_, PyDict>,
) -> PyResult<IpcScanOptions> {
    let mut o = IpcScanOptions::default();
    const ALLOWED: &[&str] = &["record_batch_statistics"];
    unknown_scan_keys(py, kw, ALLOWED)?;
    if let Some(v) = get_bool(kw, "record_batch_statistics")? {
        o.record_batch_statistics = v;
    }
    Ok(o)
}

fn lazy_ndjson_with_kwargs(
    py: Python<'_>,
    path: PlRefPath,
    kw: &Bound<'_, PyDict>,
) -> PyResult<LazyJsonLineReader> {
    let mut r = LazyJsonLineReader::new(path);
    const ALLOWED: &[&str] = &[
        "glob",
        "low_memory",
        "rechunk",
        "ignore_errors",
        "n_rows",
        "infer_schema_length",
        "include_file_paths",
        "row_index_name",
        "row_index_offset",
    ];
    unknown_scan_keys(py, kw, ALLOWED)?;
    // Polars 0.53: LazyJsonLineReader::finish hardcodes UnifiedScanArgs { glob: true, ... };
    // there is no with_glob on the reader—disabling expansion is unsupported.
    if let Some(v) = get_bool(kw, "glob")? {
        if !v {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "ndjson scan: glob=False is not supported; Polars 0.53 NDJSON scans always expand paths (UnifiedScanArgs.glob is fixed to true)",
            ));
        }
    }
    if let Some(v) = get_bool(kw, "low_memory")? {
        r = r.low_memory(v);
    }
    if let Some(v) = get_bool(kw, "rechunk")? {
        r = LazyFileListReader::with_rechunk(r, v);
    }
    if let Some(v) = get_bool(kw, "ignore_errors")? {
        r = r.with_ignore_errors(v);
    }
    if let Some(n) = get_usize(kw, "n_rows")? {
        r = r.with_n_rows(Some(n));
    }
    if kw.contains("infer_schema_length")? {
        if let Some(v) = kw.get_item("infer_schema_length")? {
            if v.is_none() {
                r = r.with_infer_schema_length(None);
            } else {
                let n: usize = v.extract()?;
                let nz = NonZeroUsize::new(n).ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "infer_schema_length must be >= 1 or None",
                    )
                })?;
                r = r.with_infer_schema_length(Some(nz));
            }
        }
    }

    if kw.contains("include_file_paths")? {
        match kw.get_item("include_file_paths")? {
            None => {}
            Some(v) if v.is_none() => {
                r = r.with_include_file_paths(None);
            }
            Some(v) => {
                let s: String = v.extract()?;
                r = r.with_include_file_paths(Some(PlSmallStr::from_str(&s)));
            }
        }
    }

    match row_index_update_from_kwargs(kw)? {
        None => {}
        Some(ri) => {
            r = r.with_row_index(ri);
        }
    }

    Ok(r)
}

fn apply_column_projection(mut lf: LazyFrame, columns: Option<Vec<String>>) -> LazyFrame {
    if let Some(cols) = columns {
        let exprs: Vec<Expr> = cols.iter().map(|c| col(c.as_str())).collect();
        lf = lf.select(exprs);
    }
    lf
}

/// Build a lazy scan `LazyFrame` from path + format + optional column projection + optional kwargs dict.
pub(crate) fn dispatch_file_scan(
    py: Python<'_>,
    path: String,
    format: &str,
    columns: Option<Vec<String>>,
    scan_kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<LazyFrame> {
    let lf = match format {
        "parquet" => {
            let args = match scan_kwargs {
                None => ScanArgsParquet::default(),
                Some(kw) => scan_args_parquet_from_kwargs(py, ScanArgsParquet::default(), kw)?,
            };
            let p = path.clone();
            py.allow_threads(move || LazyFrame::scan_parquet(PlRefPath::new(p.as_str()), args))
        }
        "csv" => {
            let reader = match scan_kwargs {
                None => LazyCsvReader::new(PlRefPath::new(path.as_str())),
                Some(kw) => lazy_csv_with_kwargs(py, PlRefPath::new(path.as_str()), kw)?,
            };
            py.allow_threads(move || reader.finish())
        }
        "ndjson" => {
            let reader = match scan_kwargs {
                None => LazyJsonLineReader::new(PlRefPath::new(path.as_str())),
                Some(kw) => lazy_ndjson_with_kwargs(py, PlRefPath::new(path.as_str()), kw)?,
            };
            py.allow_threads(move || reader.finish())
        }
        "ipc" => {
            let ipc_opts = match scan_kwargs {
                None => IpcScanOptions::default(),
                Some(kw) => ipc_scan_options_from_kwargs(py, kw)?,
            };
            let p = path.clone();
            py.allow_threads(move || {
                LazyFrame::scan_ipc(
                    PlRefPath::new(p.as_str()),
                    ipc_opts,
                    UnifiedScanArgs::default(),
                )
            })
        }
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Unknown scan format {format:?}; expected 'parquet', 'csv', 'ndjson', or 'ipc'."
            )));
        }
    }
    .map_err(polars_err)?;
    Ok(apply_column_projection(lf, columns))
}

fn unknown_write_keys(_py: Python<'_>, d: &Bound<'_, PyDict>, allowed: &[&str]) -> PyResult<()> {
    for (k, _) in d.iter() {
        let key: String = k.extract()?;
        if !allowed.contains(&key.as_str()) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "unknown write_kw key {key:?}; allowed: {}",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
}

fn parse_parquet_compression(s: &str) -> PyResult<ParquetCompression> {
    match s.to_ascii_lowercase().as_str() {
        "uncompressed" | "none" => Ok(ParquetCompression::Uncompressed),
        "snappy" => Ok(ParquetCompression::Snappy),
        "gzip" => Ok(ParquetCompression::Gzip(None)),
        "zstd" => Ok(ParquetCompression::Zstd(None)),
        "lz4" | "lz4_raw" => Ok(ParquetCompression::Lz4Raw),
        "brotli" => Ok(ParquetCompression::Brotli(None)),
        _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "unknown parquet compression {s:?} (try snappy, zstd, gzip, lz4_raw, brotli, uncompressed)"
        ))),
    }
}

/// Write collected `df` to Parquet; `write_kw` selects compression, row groups, etc.
pub(crate) fn write_parquet_file(
    py: Python<'_>,
    path: &str,
    df: &mut DataFrame,
    write_kw: Option<&Bound<'_, PyDict>>,
) -> PyResult<()> {
    let mut file = File::create(path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("sink_parquet: {e}")))?;
    match write_kw {
        None => {
            let _n: u64 = ParquetWriter::new(&mut file)
                .finish(df)
                .map_err(polars_err)?;
            Ok(())
        }
        Some(d) => {
            const ALLOWED: &[&str] = &[
                "compression",
                "row_group_size",
                "data_page_size",
                "statistics",
                "parallel",
            ];
            unknown_write_keys(py, d, ALLOWED)?;
            let mut opts = ParquetWriteOptions::default();
            if let Some(s) = get_str(d, "compression")? {
                opts.compression = parse_parquet_compression(s.trim())?;
            }
            if let Some(n) = get_usize(d, "row_group_size")? {
                opts.row_group_size = Some(n);
            }
            if let Some(n) = get_usize(d, "data_page_size")? {
                opts.data_page_size = Some(n);
            }
            if let Some(v) = get_bool(d, "statistics")? {
                opts.statistics = if v {
                    StatisticsOptions::default()
                } else {
                    StatisticsOptions {
                        min_value: false,
                        max_value: false,
                        distinct_count: false,
                        null_count: false,
                    }
                };
            }
            let mut writer = opts.to_writer(&mut file);
            if let Some(v) = get_bool(d, "parallel")? {
                writer = writer.set_parallel(v);
            }
            let _n: u64 = writer.finish(df).map_err(polars_err)?;
            Ok(())
        }
    }
}

pub(crate) fn write_csv_file(
    py: Python<'_>,
    path: &str,
    df: &mut DataFrame,
    separator: u8,
    write_kw: Option<&Bound<'_, PyDict>>,
) -> PyResult<()> {
    let mut file = File::create(path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("sink_csv: {e}")))?;
    let mut w = CsvWriter::new(&mut file).with_separator(separator);
    if let Some(d) = write_kw {
        const ALLOWED: &[&str] = &["include_header", "include_bom"];
        unknown_write_keys(py, d, ALLOWED)?;
        if let Some(v) = get_bool(d, "include_header")? {
            w = w.include_header(v);
        }
        if let Some(v) = get_bool(d, "include_bom")? {
            w = w.include_bom(v);
        }
    }
    w.finish(df).map_err(polars_err)?;
    Ok(())
}

pub(crate) fn write_ndjson_file(
    py: Python<'_>,
    path: &str,
    df: &mut DataFrame,
    write_kw: Option<&Bound<'_, PyDict>>,
) -> PyResult<()> {
    let mut file = File::create(path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("sink_ndjson: {e}")))?;
    let fmt = match write_kw {
        None => JsonFormat::JsonLines,
        Some(d) => {
            const ALLOWED: &[&str] = &["json_format"];
            unknown_write_keys(py, d, ALLOWED)?;
            match get_str(d, "json_format")?.as_deref() {
                None | Some("lines") | Some("ndjson") | Some("jsonl") => JsonFormat::JsonLines,
                Some("json") | Some("array") => JsonFormat::Json,
                Some(other) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "json_format must be 'lines' or 'json' (got {other:?})"
                    )));
                }
            }
        }
    };
    JsonWriter::new(&mut file)
        .with_json_format(fmt)
        .finish(df)
        .map_err(polars_err)?;
    Ok(())
}
