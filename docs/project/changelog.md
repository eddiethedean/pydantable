# Changelog

All notable changes to this project are documented here. The format is inspired by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

(nothing yet)

## [2.0.0] — 2026-04-20

### Added

- **Multi-engine defaults:** SQL / Mongo / Spark readers respect **`engine_mode="auto"|"default"`** (explicit **`engine=`** still wins). See [SQL engine](../integrations/engines/sql.md), [Mongo engine](../integrations/engines/mongo.md), [Spark engine](../integrations/engines/spark.md), and [Upgrading to v2](../getting-started/upgrading-to-v2.md).
- **Engine handoff:** **`to_engine(...)`**, **`to_native()`**, plus **`to_sql_engine()`** / **`to_mongo_engine()`** / **`to_spark_engine()`** for explicit materialize-and-re-root workflows ([Execution](../user-guide/execution.md)).
- **Execution policy:** **`execution_policy=`** on **`collect()`** / **`to_dict()`** / **`acollect()`** / **`ato_dict()`** / **`submit()`**; exported **`ExecutionPolicy`**, **`EngineMode`**, **`HandoffMaterialize`** ([Engine policy](../user-guide/engine-policy.md)).
- **Observability:** **`engine_report()`** and **`explain_execution()`** on **`DataFrame`** and **`DataFrameModel`**.
- **Protocol:** **`EngineCapabilities`** extended with optional **`expression_dialect`**, transform/sink category tuples for third-party engines ([pydantable-protocol](https://pypi.org/project/pydantable-protocol/)).

### Removed

- Legacy string-SQL I/O aliases (**`fetch_sql`**, **`write_sql`**, …) — use **`*_sql_raw`** or **`*_sqlmodel`** ([IO_SQL](../io/sql.md)).
- **`as_polars=`** on collection APIs — use **`to_polars()`** / **`ato_polars()`**.
- **`moltres_engine=`** on lazy SQL frames — use **`sql_engine=`**.
- **`Entei*`** Mongo compatibility names and **`pydantable.mongo_entei`** — use **`Mongo*`** ([Versioning](../semantics/versioning.md)).

### Version bump

- Align **pydantable**, **pydantable-protocol**, **pydantable-native**, **pydantable-core**, and published **`__version__`** values to **2.0.0**.

## [1.19.0] — 2026-04-20

### Added

- **Engine pushdown methods (typed):** SQL, Mongo, and Spark DataFrame surfaces add typed “native/pushdown” methods (and explicit guardrails) so engine-specific operations are available when the frame is backed by that engine, with clear errors when not supported.
- **Engine parity matrix + tests:** a dedicated engine parity table plus tests/guardrails to keep engine capabilities and error messages consistent.
- **UI wrappers (optional engines):** pandas- and PySpark-shaped wrapper modules for the Spark and Mongo DataFrame surfaces.
- **pydantable-rag:** FastAPI backend for a documentation-grounded assistant (RAG), including a `/chat-app` UI, extractive mode, optional OpenAI-backed generation, and a committed SQLite vector index for deployment.

### Documentation

- **Engines:** updated SQL/Mongo/Spark engine docs plus new parity/guardrails documentation.
- **Docs assistant:** floating docs chat launcher (MkDocs overrides + CSS/JS) and README pointers for the RAG backend and deployment.

### Performance

- **Native engine (Rust / Polars execution):** reduced materialization overhead and added benchmarks for native materialization hot paths.

### CI / deployment

- **Docs assistant workflows:** FastAPI Cloud deploy workflow for `pydantable-rag`, plus a workflow to verify the committed vector index and optionally rebuild it in CI when `OPENAI_API_KEY` is configured.

### Version bump

- Align **pydantable**, **pydantable-protocol**, **pydantable-native**, **pydantable-core**, and published **`__version__`** values to **1.19.0**.

## [1.18.1] — 2026-04-16

### Documentation

- **Versioning:** Consolidated **2.0.0** migration pointers ( **`as_polars=`**, legacy SQL I/O names, **`moltres_engine=`**, **`Entei*`** → **`Mongo*`** ) in [VERSIONING](../semantics/versioning.md) **Planned removals**.

### Maintenance

- **pydantable-core (Rust / PyO3):** Migrated off deprecated **`IntoPy`** / **`.into_py(py)`** to **`IntoPyObject`** / **`into_py_any`**, updated tuple/list/dict constructors for PyO3 0.24, replaced **`PyErr::value_bound`** with **`value`**, and dropped crate-level **`allow(deprecated)`**. Introduced shared **`literal_value_to_pyobject`** for **`LiteralValue`** → Python and moved datetime construction helpers to **`py_datetime.rs`** (used from expression serialization and Polars execution).
- Narrowed optional-import **`except`** clauses (**`ImportError`** / **`OSError`**) where appropriate; documented intentional broad catches in **I/O** materialization fallbacks, **`Expr` repr**, and **`io.mongo`**.

### Version bump

- Align **pydantable**, **pydantable-protocol**, **pydantable-native**, **pydantable-core**, and published **`__version__`** values to **1.18.1**.

## [1.18.0] — 2026-04-15

### Added

- **PySpark (optional `pydantable[spark]`):** **`SparkDataFrame`** / **`SparkDataFrameModel`** façade on **raikou-core**, with lazy **`pydantable.pyspark.sparkdantic`** integration for **SparkDantic**-based JVM schemas. Documentation and tests: **SPARK_ENGINE**, **PYSPARK_UI**, **PYSPARK_PARITY**, and related coverage.

### Documentation

- **MkDocs + Material for MkDocs** replace Sphinx for the published manual; **Read the Docs** and CI use **`mkdocs build --strict`** (`mkdocs.yml`, **`docs/mkdocs_hooks.py`**).
- Lazy-SQL user guide file renamed **`MOLTRES_SQL.md` → `SQL_ENGINE.md`** (links and nav updated; content unchanged).

### CI

- Test installs use **`pip install -e ".[dev]"`** plus **raikou-core** / **pyspark** where needed; **Windows** skips **`@pytest.mark.spark`** (JVM / **winutils** constraints). Docs jobs include Spark stack for mkdocstrings imports.

### Version bump

- Align **pydantable**, **pydantable-protocol**, **pydantable-native**, **pydantable-core**, and published **`__version__`** values to **1.18.0**.

## [1.17.0] — 2026-04-14

### Added

- **Mongo engine (entei-core):** **`pydantable.mongo_dataframe`** — **`MongoDataFrame`** / **`MongoDataFrameModel`** facades (same pattern as **`pydantable.sql_dataframe`** + **moltres-core**). **`entei-core`** supplies **`MongoRoot`** (and materialization helpers); **`MongoPydantableEngine`** is implemented in **`pydantable.mongo_dataframe_engine`**.
- **Mongo / Beanie:** **`pydantable.mongo_beanie.sync_pymongo_collection`** (also a lazy import from **`pydantable`**), **`MongoDataFrame.from_beanie`**, and **`MongoDataFrameModel.from_beanie`** for [Beanie](https://github.com/BeanieODM/beanie) **`Document`** models with a **sync** **`pymongo.database.Database`** (Beanie uses async PyMongo). See **`MONGO_ENGINE`**.
- **Mongo I/O:** **`fetch_mongo`**, **`iter_mongo`**, **`write_mongo`** and async **`afetch_mongo`**, **`aiter_mongo`**, **`awrite_mongo`** — eager **`dict[str, list]`** reads/writes against a PyMongo **`Collection`** (same import pattern as SQL I/O). **`pydantable[mongo]`** includes **pymongo**.

### Documentation

- **`MONGO_ENGINE`** — user guide for the optional Mongo engine (**`MongoDataFrame`** / **`MongoDataFrameModel`**, **`pydantable[mongo]`**).
- **Mongo:** position [Beanie](https://github.com/BeanieODM/beanie) **`Document`** models as the **primary** way to use pydantable with MongoDB (**`pydantable[mongo]`**, **`from_beanie`**, **`sync_pymongo_collection`**); Pydantic **`Schema`** + **`from_collection`** documented as a supported alternative. See **`MONGO_ENGINE`**, **README**, and **IO_DECISION_TREE**.

### Changed

- **Public API naming (SQL / Mongo):** **`MongoDataFrame`**, **`MongoDataFrameModel`**, **`MongoPydantableEngine`** replace **`Entei*`**; modules **`pydantable.mongo_dataframe`**, **`pydantable.sql_dataframe`** are canonical (legacy paths **`mongo_entei`**, **`sql_moltres`** re-export). **`sql_engine_from_config`** and **`sql_engine=`** replace **`moltres_engine_*`**. **`pydantable[sql]`** includes eager SQLModel I/O, **moltres-core**, and the lazy **`SqlDataFrame`** stack (the separate **`lazy-sql`** and **`moltres`** extras are removed). **`Entei*`** and **`moltres_engine`** remain available with **`DeprecationWarning`**. User docs describe **SQLAlchemy**, **PyMongo**, and **Beanie** without third-party stack marketing names.
- **Optional dependencies:** **`pydantable[mongo]`** includes **Beanie** (`beanie>=1.24,<3`) and pins **`entei-core`** to **`>=0.2.0,<0.3`** (PyPI **`entei-core`** is versioned independently of **pydantable** **1.x**). The separate **`mongo-beanie`** extra is removed — use **`pip install "pydantable[mongo]"`**.
- **Optional dependencies:** **`rapsqlite`** is not bundled in **`pydantable[sql]`** — install a DB-API driver for your database URL yourself (``pip install rapsqlite`` only if you want the ``sqlite+rapsqlite`` dialect).

### Version bump

- Align **pydantable**, **pydantable-protocol**, **pydantable-native**, **pydantable-core**, and published **`__version__`** values to **1.17.0**.

## [1.16.1] — 2026-04-13

### Changed

- **Python:** Narrow `ImportError` handling for optional dtype registry and native capabilities; debug logging on intentional broad I/O / `Expr` repr fallbacks; optional scan-column recovery iteration bound in `_materialize_scan_fallback`.
- **Rust:** Split row-wise calendar/string helpers into `expr/rowwise_support.rs`.

### Tests

- **DataFrame:** Shared retail-style columnar scenarios (`tests/_support/scenarios.py`), multi-step workflow coverage (`tests/dataframe/test_dataframe_realistic_workflows.py`), Hypothesis strategies with Python oracles for discounted line totals and global sum/mean, and a global aggregate check on the scenario payload (`tests/dataframe/test_global_agg.py`).

### Version bump

- Align **pydantable**, **pydantable-protocol**, **pydantable-native**, **pydantable-core**, and published **`__version__`** values to **1.16.1**.

## [1.16.0] — 2026-04-06

### Added

- **Typing (DataFrame[Schema]):** `as_schema` / `try_as_schema` / `assert_schema` with `as_model` / `try_as_model` / `assert_model` aliases.
- **Typing escape hatches (DataFrame[Schema]):** `*_as_schema` (and `*_as_model` aliases) for `join`, `melt`, `unpivot`, and `rolling_agg`.
- **Typing contract tests:** new Pyright + Astral `ty` snippet tests for the generic `DataFrame` API.
- **Docs:** `DataFrameModel` “Pyright/ty golden path” section documenting the explicit `*_as_model` helpers.

### Version bump

- Align **pydantable**, **pydantable-protocol**, **pydantable-native**, **pydantable-core**, and published **`__version__`** values to **1.16.0**.

## [1.15.2] — 2026-04-05

### Fixed

- **`_dtype_repr`:** On Python 3.9-3.10, builtin generics such as ``list[int]`` were shown as ``list`` because they are ``isinstance(..., type)``; resolve via ``get_origin`` / ``get_args`` like 3.11+.
- **Tooling:** ``pyrightconfig.json`` ``include`` path points at ``tests/typing/test_pyright_dataframe_model_return_types.py`` (Pyright contract check).

### Changed

- **Docs:** Typing guide (`ty` vs mypy/Pyright, `Any` policy), troubleshooting (`engine="auto"` Rust fallback, async cancellation), I/O overview callouts, FASTAPI/DATAFRAMEMODEL non-deprecated SQL examples; DEVELOPER Rust `unwrap` note.
- **Typing:** Astral `ty` rules (`invalid-return-type`, `unsupported-operator` at error); grouped-frame protocol; SQL `afetch_sqlmodel` / `aiter_sqlmodel` `Sequence` signatures; optional `pyrightconfig-strict.json`.
- **Tests:** Scan-column engine-error regex regression tests; remove flaky wall-clock skip stub in async materialization tests; extra coverage for ``_describe_dtype``, grouped agg, selectors, PySpark shims, and ``rust_engine`` delegation; Ruff-formatted typing stubs.

### Version bump

- Align **pydantable**, **pydantable-protocol**, **pydantable-native**, **pydantable-core**, and published **`__version__`** values to **1.15.2**.

## [1.15.1] — 2026-04-04

### Fixed

- **Typing:** `sql_moltres` and `DataFrameModel` stubs aligned so mypy/pyright contract tests pass; `pyrightconfig.json` sets `extraPaths` for snippet imports.

### Changed

- **Rust:** Set-ops column assembly returns `PyRuntimeError` instead of panicking on unexpected internal map keys (`exceptAll` / `intersectAll` materialization).
- **Docs:** `DataFrame.join` / `DataFrameModel.join` document that `allow_parallel` / `force_parallel` are not implemented; TYPING notes `as_polars` on `collect` / `acollect` deprecation for 2.0.
- **Version bump:** Align **pydantable**, **pydantable-protocol**, **pydantable-native**, **pydantable-core**, and published **`__version__`** values to **1.15.1**.

### Tests

- **`aiter_sqlmodel`:** early break, `ThreadPoolExecutor`, and `batch_size=1` coverage.

## [1.15.0] — 2026-04-03

### Added

- **Optional Moltres integration:** extra **`pydantable[lazy-sql]`** *(install **`pydantable[sql]`** today — optional extras were consolidated)* pulls [**moltres-core**](https://pypi.org/project/moltres-core/). New **`SqlDataFrame`** and **`SqlDataFrameModel`** in **`pydantable.sql_dataframe`** (also available as **`pydantable.SqlDataFrame`** / **`SqlDataFrameModel`** via lazy import) bind **`moltres_core.MoltresPydantableEngine`** using **`sql_config=`** (**`moltres_core.EngineConfig`**) or **`moltres_engine=`**. Helper **`moltres_engine_from_sql_config`**. User guide: [SQL_ENGINE](../integrations/engines/sql.md); protocol story: [CUSTOM_ENGINE_PACKAGE](../integrations/engines/custom-engine-package.md).

### Changed

- **`pydantable[lazy-sql]`** *(today **`pydantable[sql]`**)* and **`[dev]`** now include **`greenlet`** so SQLAlchemy **`create_async_engine`** (for example **`sqlite+rapsqlite`** smoke tests and async SQLAlchemy workflows) works without an extra manual install.

- **Version bump:** Align Python package metadata (**`pydantable`**, **`pydantable-protocol`**, **`pydantable-native`**), Rust crate **`pydantable-core`**, and published **`__version__`** values to **1.15.0**.

## [1.14.1] — 2026-04-03

### Changed

- **`pydantable`** now requires **`pydantable-native`** at the same version, so **`pip install pydantable`** installs the Rust engine. **Removed** the **`pydantable-meta`** package and its release job.
- **Version bump:** Align Python package metadata (**`pydantable`**, **`pydantable-protocol`**, **`pydantable-native`**), Rust crate **`pydantable-core`**, and published **`__version__`** values to **1.14.1**.

## [1.14.0] — 2026-04-03

### Added

- **Documentation:** [CUSTOM_ENGINE_PACKAGE](../integrations/engines/custom-engine-package.md) — guide for authors publishing a separate engine package (dependencies, protocol implementation, wiring, expressions, I/O boundaries, testing, PyPI).
- **`pydantable-protocol`:** zero-dependency distribution defining **`ExecutionEngine`** / **`PlanExecutor`** / **`SinkWriter`**, **`EngineCapabilities`**, **`UnsupportedEngineOperationError`**, and **`MissingRustExtensionError`**. Third-party engines (for example SQL backends) can **`pip install pydantable-protocol`** for typing and shared errors without depending on **`pydantable`**. **`pydantable`** pins the same version and re-exports protocols from **`pydantable.engine.protocols`** (and **`MissingRustExtensionError`** from **`pydantable._extension`**).
- **`pydantable-native`** now depends only on **`pydantable-protocol`** (not **`pydantable`**). **`native_engine_capabilities`** lives in **`pydantable_native.capabilities`**. Tracing in the native engine uses **`pydantable.observe.span`** when **`pydantable`** is installed, otherwise a local **`pydantable_native._trace`** implementation (**`PYDANTABLE_TRACE`** behaves the same).

### Changed

- **Internal:** Introduced `pydantable.engine` (`NativePolarsEngine`, `get_default_engine`, `get_expression_runtime`) so execution is routed through a single abstraction; `rust_engine` remains a thin delegating module. See [ADR-engines](../project/adrs/engines.md) and [DEVELOPER](../project/developer.md).
- **Version bump:** Align Python package metadata ( **`pydantable`**, **`pydantable-protocol`**, **`pydantable-native`**, **`pydantable-meta`**), Rust crate **`pydantable-core`**, and published **`__version__`** values to **1.14.0**.

## [1.13.0] — 2026-04-02

### Added

- **SQLModel read I/O (Phase 0–1):** **`pydantable[sql]`** includes **sqlmodel**. New APIs **`fetch_sqlmodel`**, **`iter_sqlmodel`**, **`afetch_sqlmodel`**, and **`aiter_sqlmodel`** in **`pydantable.io`** (also re-exported from **`pydantable`**), sharing batching and **`StreamingColumns`** semantics with **`fetch_sql_raw`** / **`iter_sql_raw`**. **`MissingOptionalDependency`** when **sqlmodel** is required but not installed.
- **SQLModel write I/O (Phase 2):** **`write_sqlmodel`**, **`write_sqlmodel_batches`**, **`awrite_sqlmodel`**, **`awrite_sqlmodel_batches`** — DDL from **`SQLModel.__table__`**, **`replace_ok`** guard for **`if_exists="replace"`**, optional **`validate_rows`**, strict column alignment. **`python/pydantable/io/sqlmodel_write.py`**; tests **`tests/test_sqlmodel_io_phase02.py`**.
- **SQLModel + `DataFrameModel` (Phase 3):** classmethods **`fetch_sqlmodel`**, **`afetch_sqlmodel`**, **`iter_sqlmodel`**, **`aiter_sqlmodel`**, **`write_sqlmodel_data` / `awrite_sqlmodel_data`**; instance **`write_sqlmodel` / `awrite_sqlmodel`**; **`MyModel.Async.write_sqlmodel`** → **`awrite_sqlmodel_data`**. **`python/pydantable/dataframe_model.py`**; stubs in **`python/pydantable/dataframe_model.pyi`** / **`typings/`**; tests **`tests/test_sqlmodel_dataframe_model.py`**.
- **Explicit string SQL (Phase 4):** **`fetch_sql_raw`**, **`iter_sql_raw`**, **`write_sql_raw`**, **`afetch_sql_raw`**, **`aiter_sql_raw`**, **`awrite_sql_raw`** in **`pydantable.io`** (**`fetch_sql_raw`** / **`afetch_sql_raw`** also re-exported from **`pydantable`** root).
- **Schema bridging (Phase 5):** **`sqlmodel_columns`**, **`DataFrameModel.assert_sqlmodel_compatible`** — **`python/pydantable/io/sqlmodel_schema.py`**; tests **`tests/test_sqlmodel_bridge_phase05.py`**; docs [IO_SQL](../io/sql.md), [DATAFRAMEMODEL](../user-guide/dataframemodel.md), [SQLMODEL_SQL_ROADMAP](../project/sqlmodel-sql-roadmap.md).
- **Documentation + examples + testing gate (Phase 6):** SQLModel-first SQLite examples **`docs/examples/io/sql_sqlite_sqlmodel_roundtrip.py`**, **`docs/examples/io/sql_sqlite_sqlmodel_streaming.py`**; [IO_SQL](../io/sql.md) sections for raw vs SQLModel-first examples; **`tests/test_doc_io_examples.py`** runs **`sql_sqlite_streaming.py`** and the SQLModel scripts alongside existing **`sql_sqlite_*`** examples.

### Deprecated

- **Legacy string-SQL names (Phase 4):** **`fetch_sql`**, **`iter_sql`**, **`write_sql`**, **`afetch_sql`**, **`aiter_sql`**, **`awrite_sql`**, **`write_sql_batches`**, **`awrite_sql_batches`** — emit **`DeprecationWarning`**; migrate to **`*_raw`** or SQLModel helpers. **`DataFrameModel.write_sql`** / **`awrite_sql`** delegate to the same deprecated **`pydantable.io`** entrypoints. Removal no earlier than **`2.0.0`** ([VERSIONING](../semantics/versioning.md)). Tests: **`tests/test_sql_string_deprecation.py`**; default test run filters these warnings in **`pyproject.toml`** for backward-compatible suites.

### Docs

- **README / site index / I/O guides:** align **current release** (**1.13.0**), SQL I/O naming (**`fetch_sqlmodel`**, **`fetch_sql_raw`**, deprecations), and pointers to [IO_SQL](../io/sql.md) / [SQLMODEL_SQL_ROADMAP](../project/sqlmodel-sql-roadmap.md) across **README**, [index](../index.md), [IO_OVERVIEW](../io/overview.md), [IO_DECISION_TREE](../io/decision-tree.md), [EXECUTION](../user-guide/execution.md), [DATA_IO_SOURCES](../io/data-io-sources.md), [DOCS_MAP](../getting-started/docs-map.md), [POLARS_TRANSFORMATIONS_ROADMAP](../user-guide/polars-alignment/transformations-roadmap.md), [ROADMAP](../project/roadmap.md), and the SQLModel roadmap introduction.
- **SQL I/O:** [IO_SQL](../io/sql.md), [SQLMODEL_SQL_ROADMAP](../project/sqlmodel-sql-roadmap.md), [VERSIONING](../semantics/versioning.md) — SQLModel-first default, **`*_raw`** for explicit string SQL, deprecation policy. Runnable examples: raw **`sql_sqlite_roundtrip.py`** / **`sql_sqlite_streaming.py`** and SQLModel-first **`sql_sqlite_sqlmodel_*.py`** (see [IO_SQL](../io/sql.md)).

### Changed

- **Version bump:** Align Python package metadata, Rust crate, and published **`__version__`** to **1.13.0**. (This release includes all SQLModel-first SQL I/O work since **v1.12.0** — Phases 0–6 of [SQLMODEL_SQL_ROADMAP](../project/sqlmodel-sql-roadmap.md) — in one minor version.)

## [1.12.0] — 2026-04-02

### Changed

- **Version bump:** Align Python package metadata, Rust crate, and published `__version__` to **1.12.0**.

## [1.11.0] — 2026-04-01

### Added

- **Tests (Phase E):** **`tests/test_parquet_allow_missing_columns_e.py`** — directory scan with mismatched Parquet columns and **`allow_missing_columns=True`**.
- **Example (Phase E):** **`docs/examples/io/parquet_allow_missing_columns.py`** ( **`tests/test_doc_io_examples.py`** ).
- **Parquet lazy `scan_kwargs` (1.11.0 Phase B1):** **`hive_partitioning`**, **`hive_start_idx`**, **`try_parse_hive_dates`**, **`include_file_paths`**, **`row_index_name`**, **`row_index_offset`** forwarded to Polars **`ScanArgsParquet`** in **`pydantable-core`** (`scan_kw.rs`). Tests: **`tests/test_parquet_scan_hive_b1.py`**.
- **CSV lazy `scan_kwargs` (1.11.0 Phase B2):** **`include_file_paths`**, **`row_index_name`**, **`row_index_offset`**, **`raise_if_empty`**, **`truncate_ragged_lines`**, **`decimal_comma`**, **`try_parse_dates`** forwarded to Polars **`LazyCsvReader`** in **`pydantable-core`** (`lazy_csv_with_kwargs`); shared **`row_index_*`** parsing with Parquet. Tests: **`tests/test_csv_scan_directory_b2.py`** (directory / **`*.csv`** glob, hive path behavior, unknown kw).
- **NDJSON lazy `scan_kwargs` (1.11.0 Phase B3):** **`glob`** ( **`glob=False`** raises **`ValueError`**; Polars 0.53 NDJSON scans always expand paths), **`include_file_paths`**, **`row_index_name`**, **`row_index_offset`** in **`lazy_ndjson_with_kwargs`** (`scan_kw.rs`). Tests: **`tests/test_ndjson_scan_directory_b3.py`**.
- **IPC lazy `scan_kwargs` (1.11.0 Phase B4):** **`record_batch_statistics`** plus **`UnifiedScanArgs`** fields (**`glob`**, **`cache`**, **`rechunk`**, **`n_rows`**, **`hive_partitioning`**, **`hive_start_idx`**, **`try_parse_hive_dates`**, **`include_file_paths`**, **`row_index_name`**, **`row_index_offset`**) via **`ipc_scan_from_kwargs`** (`scan_kw.rs`). Tests: **`tests/test_ipc_scan_directory_b4.py`**.
- **`read_json` (1.11.0 Phase B5):** Confirmed alias of **`read_ndjson`** (JSON Lines lazy scan only); documentation for paths, **`glob`**, and **`scan_kwargs`** vs **`materialize_json`** for JSON **array** files. Tests: **`tests/test_read_json_paths_b5.py`**.
- **`iter_chain_batches`** (`pydantable.io` / `pydantable.io.batches`): chain per-file **`iter_*`** iterators over an explicit path list. Test: **`tests/test_batches_chain.py`**.
- **Partitioned Parquet writes (1.11.0 Phase D1):** **`DataFrame.write_parquet`** / **`DataFrameModel.write_parquet`** — **`partition_by`** (column names), **`mkdir`**, hive-style **`col=value/.../00000000.parquet`** shards via **`pydantable-core`** (`sink_parquet_polars`, Polars **`partition_by_stable`**). Tests: **`tests/test_write_parquet_partition_d1.py`**.
- **`write_*_batches` (Phase D2):** reject an existing **directory** path; CSV/NDJSON **`mode`** documented. Tests: **`tests/test_write_batches_phase_d2.py`**.
- **Tests (Phase F):** **`test_write_parquet_unknown_write_kw_raises`** in **`tests/test_write_parquet_partition_d1.py`** — invalid **`write_kwargs`** keys raise **`ValueError`** with **`unknown write_kw key`** (parity with **`scan_kwargs`** allowlist tests).

### Docs

- **Changelog page:** source file is **`docs/project/changelog.md`**; Sphinx / Read the Docs page is **`CHANGELOG`** (**`CHANGELOG.html`**). Update any bookmarks from **`changelog.html`**.
- **Local I/O (1.11.0) — release narrative:** Directory/glob/hive lazy reads, **`scan_kwargs`** / **`write_kwargs`** allowlists, eager **`iter_*`** / **`materialize_*`** guidance, partitioned Parquet writes, multi-file Parquet **`allow_missing_columns`** and observability — details in this **1.11.0** section; ongoing I/O work in [ROADMAP](../project/roadmap.md). **`pydantable.__version__`** / **`rust_version()`** alignment per [VERSIONING](../semantics/versioning.md) (**`tests/test_version_alignment.py`**).
- **Local I/O Phase E (1.11.0):** Multi-file Parquet — **`allow_missing_columns`**, Polars schema union, cast / optional-field patterns — [IO_PARQUET](../io/parquet.md); pointers in [DATA_IO_SOURCES](../io/data-io-sources.md), [IO_DECISION_TREE](../io/decision-tree.md), [SUPPORTED_TYPES](../user-guide/supported-types.md), [INTERFACE_CONTRACT](../semantics/interface-contract.md), [PLAN_AND_PLUGINS](../user-guide/plan-and-plugins.md). Contributor note: `pydantable-core/.../scan_kw.rs`, [DEVELOPER](../project/developer.md).
- **Writes Phase D:** partitioned **`write_parquet`**, batch-writer file vs directory—[IO_PARQUET](../io/parquet.md), [IO_OVERVIEW](../io/overview.md), [IO_DECISION_TREE](../io/decision-tree.md), [DATA_IO_SOURCES](../io/data-io-sources.md), [INTERFACE_CONTRACT](../semantics/interface-contract.md); example **`docs/examples/io/parquet_partitioned_write.py`**.
- **Eager / batched multi-file clarity (1.11.0 Phase C):** **`materialize_*`** single-file contract; **`iter_*` / `aiter_*`** one path per call and Python-side glob/directory expansion; **`iter_chain_batches`**; bounded-memory notes vs **`iter_concat_batches`** and lazy **`read_*`**—[IO_OVERVIEW](../io/overview.md), [IO_DECISION_TREE](../io/decision-tree.md), [DATA_IO_SOURCES](../io/data-io-sources.md), [INTERFACE_CONTRACT](../semantics/interface-contract.md); example **`docs/examples/io/iter_glob_parquet_batches.py`**.
- **Local I/O audit (1.11.0 Phase A):** Polars **0.53.0** vs pydantable **`scan_kwargs`** matrix, directory/glob/hive notes—see [Polars 0.53 vs pydantable scan audit](../io/data-io-sources.md#audit-polars-053x-vs-pydantable-1110-phase-a); multi-file entrypoint table—[IO_DECISION_TREE](../io/decision-tree.md); **Local lazy file scans**—[INTERFACE_CONTRACT](../semantics/interface-contract.md); path/glob subsections on [IO_PARQUET](../io/parquet.md), [IO_CSV](../io/csv.md), [IO_NDJSON](../io/ndjson.md), [IO_IPC](../io/ipc.md), [IO_JSON](../io/json.md); link from [IO_OVERVIEW](../io/overview.md).
- **Parquet B1:** [DATA_IO_SOURCES](../io/data-io-sources.md) audit + summary table; [IO_PARQUET](../io/parquet.md); [INTERFACE_CONTRACT](../semantics/interface-contract.md) (Parquet lineage / hive kwargs).
- **CSV B2:** [DATA_IO_SOURCES](../io/data-io-sources.md) audit + summary table; [IO_CSV](../io/csv.md); [INTERFACE_CONTRACT](../semantics/interface-contract.md) (CSV **`include_file_paths`** / **`row_index_*`**).
- **NDJSON B3:** [DATA_IO_SOURCES](../io/data-io-sources.md) audit + summary table; [IO_NDJSON](../io/ndjson.md); [INTERFACE_CONTRACT](../semantics/interface-contract.md) (NDJSON **`glob`** / **`include_file_paths`** / **`row_index_*`**).
- **IPC B4:** [DATA_IO_SOURCES](../io/data-io-sources.md) audit + summary table; [IO_IPC](../io/ipc.md); [INTERFACE_CONTRACT](../semantics/interface-contract.md) (IPC **`scan_kwargs`**).
- **JSON `read_json` B5:** [IO_JSON](../io/json.md); [DATA_IO_SOURCES](../io/data-io-sources.md) (lazy vs array); **`pydantable.io.read_json`** docstring.

### Changed

- **Versioning:** Python package metadata and Rust crate aligned at **1.11.0** for this release; docs “current release” strings ([index](../index.md), [CHANGELOG](../project/changelog.md), [ROADMAP](../project/roadmap.md), [POLARS_TRANSFORMATIONS_ROADMAP](../user-guide/polars-alignment/transformations-roadmap.md)) aligned.

## [1.10.0] — 2026-04-01

### Added

- **Struct expressions (Polars):** **`Expr.struct_json_encode`**, **`struct_json_path_match`**, **`struct_rename_fields`**, **`struct_with_fields`** (Rust **`ExprNode`** + lowering); PySpark façade **`pydantable.pyspark.sql.functions.struct_json_encode`** / **`struct_json_path_match`**. Tests: **`tests/test_struct_expr_phase_b.py`**.
- **JSON decode (Polars):** **`Expr.str_json_decode(dtype)`** → struct or **`dict[str, T]`** via **`StringJsonDecode`** and shared **`polars_dtype`** mapping. Tests: **`tests/test_str_json_decode_phase_c.py`**.
- **Tests:** **`tests/test_json_io_phase_a.py`** — nested **`materialize_json`** (array vs NDJSON), **`export_json`** round-trip and **`default=str`** for **`datetime`** / **`Decimal`** / **`UUID`**, lazy **`read_ndjson`** / **`read_json`** alias with nested struct + list, eager path with **`dict[str, T]`** map column.

### Docs

- **JSON modeling:** **JSON (RFC 8259) vs column types** in [SUPPORTED_TYPES](../user-guide/supported-types.md) (heterogeneous arrays, arbitrary JSON, link from [IO_JSON](../io/json.md)).
- **I/O:** **Eager `export_json` serialization** in [IO_JSON](../io/json.md); extended **`export_json`** docstring in **`pydantable.io`** (`json.dump` + `default=str`); struct → JSON text pointer (**`struct_json_encode`**).
- **Structs:** **`SUPPORTED_TYPES`** and **`INTERFACE_CONTRACT`** — struct JSON / **`with_fields`** / **`rename_fields`** semantics and row-wise limits.
- **FastAPI:** columnar **map** / nested field notes with links to [SUPPORTED_TYPES](../user-guide/supported-types.md) and [IO_JSON](../io/json.md).
- **Roadmap:** Phase A + B + C JSON/struct work summarized in this **1.10.0** section and [ROADMAP](../project/roadmap.md) (**Shipped in 1.10.0**); **`str_json_decode`** / error semantics in [SUPPORTED_TYPES](../user-guide/supported-types.md) and [INTERFACE_CONTRACT](../semantics/interface-contract.md); [IO_JSON](../io/json.md) cross-link.
- **Phase D (I/O):** [IO_JSON](../io/json.md) — **`read_json`** vs **`read_ndjson`** vs **`materialize_json`**, large-file / **`streaming`** patterns, NDJSON **`scan_kwargs`** presets; example **`docs/examples/io/large_ndjson_patterns.py`**; cross-links from [DATA_IO_SOURCES](../io/data-io-sources.md), [EXECUTION](../user-guide/execution.md), [IO_NDJSON](../io/ndjson.md).
- **Phase E (UX) & 1.10.0 JSON/struct summary:** [SELECTORS](../user-guide/selectors.md) — **`s.structs()`**, **`unnest`**, **`struct_field`** pipeline; cookbook [json_logs_unnest_export](../cookbook/json_logs_unnest_export.md) (NDJSON → unnest → **`export_json`**); [DOCS_MAP](../getting-started/docs-map.md) link. **Release narrative:** JSON ↔ schema matrix and I/O tests; struct expressions (**`struct_json_encode`**, path/rename/with-fields); **`str_json_decode`**; Phase D large-file NDJSON docs; Phase E selectors + cookbook + this page.

### Changed

- **Versioning:** Python package metadata and Rust crate aligned at **1.10.0** for this release.

## [1.9.0] — 2026-04-01

### Added

- **PySpark UI parity:** **`groupBy`** returning **`PySparkGroupedDataFrame`** / **`PySparkGroupedDataFrameModel`** (aggregations stay Spark-flavored), **`sort`**, **`crossJoin`**, frame action **`count()` → int** (via **`global_row_count()`**), **`unionByName`** (optional **`allowMissingColumns`**), **`intersect`** / **`subtract`** / **`exceptAll`** (join-layer semantics; **`exceptAll`** aliases **`subtract`**, not Spark multiset **`EXCEPT ALL`**), **`fillna`** / **`dropna`** / **`.na`**, **`printSchema`**, **`explain`**, **`toPandas`**, and the same methods on **`DataFrameModel`**. See [PYSPARK_UI](../integrations/alternate-surfaces/pyspark-ui.md), [PYSPARK_PARITY](../integrations/alternate-surfaces/pyspark-parity.md), and [INTERFACE_CONTRACT](../semantics/interface-contract.md).
- **Engine:** **`cast_expr`** / **`Expr.cast`** now accepts **`Literal(None)`** (unknown-base SQL NULL) and casts it to a **nullable** scalar dtype, enabling typed null padding (e.g. **`unionByName(..., allowMissingColumns=True)`**).
- **Temporal:** **`Expr.dt_dayofyear`**, **`Expr.from_unix_time`**, PySpark **`F.dayofyear`** / **`F.from_unixtime`** (numeric epoch → UTC-naive **`datetime`**; Spark’s optional **`from_unixtime` format** string is not modeled—use parsing helpers on strings). Rust: **`TemporalPart::DayOfYear`**, **`ExprNode::FromUnixTime`**.
- **Introspection:** **`DataFrame.describe()`** (and PySpark **`summary()`**) now includes **`date`** and **`datetime`** columns: non-null **count**, **min**, **max**, and **null** count (one **`to_dict()`** materialization). Tests: **`tests/test_dataframe_discovery.py`**.

### Docs / tooling

- **Versioning:** bump to **1.9.0** across Python package metadata, Rust crate, and shipped stubs; docs “current release” strings ([index](../index.md), [ROADMAP](../project/roadmap.md), [POLARS_TRANSFORMATIONS_ROADMAP](../user-guide/polars-alignment/transformations-roadmap.md)) aligned.
- **Docs:** **`describe()`** / **`summary()`**, **`SUPPORTED_TYPES`** temporal helpers, **[INTERFACE_CONTRACT](../semantics/interface-contract.md)**, **[EXECUTION](../user-guide/execution.md)**, **[PYSPARK_PARITY](../integrations/alternate-surfaces/pyspark-parity.md)**, **[PARITY_SCORECARD](../user-guide/polars-alignment/parity-scorecard.md)**, **[DEVELOPER](../project/developer.md)**, **[PANDAS_UI](../integrations/alternate-surfaces/pandas-ui.md)**, and **[POLARS_TRANSFORMATIONS_ROADMAP](../user-guide/polars-alignment/transformations-roadmap.md)** updated for **1.9.0** behavior.

## [1.8.0] — 2026-03-31

### Added

- **Selectors:** selector-driven column and rename helpers (see `pydantable.selectors` and [SELECTORS](../user-guide/selectors.md)).
- **Core DataFrame ergonomics:** `row_count`, `clip`, and `drop_nulls` arguments and convenience behavior aligned with the 1.8 parity push (see [POLARS_PARITY_1_8](../user-guide/polars-alignment/polars-parity-1-8.md) and [PARITY_SCORECARD](../user-guide/polars-alignment/parity-scorecard.md)).
- **Joins:** additional join argument parity including `join_nulls` and `maintain_order` (typed contract preserved; see [INTERFACE_CONTRACT](../semantics/interface-contract.md)).
- **Reshape:** `pivot_longer` / `pivot_wider` and related reshape ergonomics (see [POLARS_WORKFLOWS](../user-guide/polars-alignment/workflows.md) and [INTERFACE_CONTRACT](../semantics/interface-contract.md) reshape notes).

### Docs

- Add explicit **1.8.0** release entry and align “current release” references with the changelog.

## [1.7.0] — 2026-03-30

### Added

- **Pandas UI (schema-first):** `duplicated` / `drop_duplicates(keep=False)` backed by engine plan steps where Polars is enabled; typed **`get_dummies`** with cardinality guard; eager **`cut`** / **`qcut`**, **`factorize_column`**, narrow **`ewm(...).mean()`** (may require pandas at runtime); façade **`pivot`** delegating to core. See [PANDAS_UI](../integrations/alternate-surfaces/pandas-ui.md) and [PARITY_SCORECARD](../user-guide/polars-alignment/parity-scorecard.md).
- **Tests:** `tests/test_pandas_ui_popular_features.py` — extended coverage for duplicates, dummies, binning, factorize, ewm, and pivot.

### Docs

- Refreshed **PANDAS_UI** (correct **`pivot`** façade, **`get_dummies`** null/boolean behavior, naming map, test links), **INTERFACE_CONTRACT** (duplicate detection, **`value_counts`** `dict` note), **DOCS_MAP**, **QUICKSTART**, **TROUBLESHOOTING** (optional **pandas** for eager helpers), **EXECUTION**, **DEVELOPER**, **DATAFRAMEMODEL**, **POLARS_TRANSFORMATIONS_ROADMAP**, **PARITY_SCORECARD**, and root **README** cross-links.

### Docs / tooling

- **Versioning:** bump to **1.8.0** across Python package metadata, Rust crate, and shipped stubs; docs “current release” strings aligned.

## [1.6.1] — 2026-03-30

### Fixed

- **Async iterator bridge:** hardened `pydantable.io.aiter_sql()` and internal `_aiter_from_iter()` against deadlocks when the async consumer stops early (e.g. client disconnect / early return). Producer threads now exit cleanly and do not block forever on a bounded queue.
- **Deferred materialization:** `ExecutionHandle.result()` now shields the underlying `concurrent.futures.Future` so cancelling the awaiting task cancels the **wait** but does not cancel the background engine work.
- **Submit cancellation race:** `DataFrame.submit()` now avoids `InvalidStateError` when a handle is cancelled before work starts (mirrors `ThreadPoolExecutor` semantics).
- **Lazy scan missing optional columns:** recovery for missing optional scan columns is now tolerant to error-message variants (not coupled to one brittle regex).

### Docs / tooling

- **Read the Docs build:** install `pydata-sphinx-theme` during RTD builds to match the configured `html_theme` (`docs/conf.py`).
- **Versioning:** bump to **1.6.1** across Python package metadata, Rust crate, and shipped stubs; docs “current release” strings aligned.

## [1.6.0] — 2026-03-30

Summary: **FastAPI** helpers (columnar OpenAPI bodies, NDJSON, **`register_exception_handlers`**), **`pydantable.errors`**, **`submit` / `stream` / `astream`**, **`PlanMaterialization`**, awaitable lazy reads (**`AwaitableDataFrameModel`**), Rust **async** plan execution when available, and docs/cookbooks for services. **Breaking:** removed legacy **`DataFrameModel`** eager SQL / **`materialize_*`** shims—use **`from pydantable import …`** (eager SQL / **`materialize_*`**) and **`read_*` / `aread_*`** as described under **Removed** below.

### Removed

- **`DataFrameModel`** eager I/O shims: **`materialize_*`**, **`amaterialize_*`**, **`fetch_sql`**, **`afetch_sql`**, **`iter_sql`**, **`aiter_sql`**, **`from_sql`**, **`afrom_sql`**. Use **`from pydantable import …`** for eager **`dict[str, list]`** loads and **`SQL`** streaming, then **`MyModel(cols, ...)`**; keep lazy scans on **`read_*`** / **`aread_*`**.

### Added

- **`pydantable.errors`:** **`PydantableUserError`**, **`ColumnLengthMismatchError`** (column length mismatch at schema ingest). **`register_exception_handlers`** maps **`ColumnLengthMismatchError`** → **400** with JSON **`detail`**.
- **`pydantable.fastapi`:** **`columnar_body_model`**, **`columnar_body_model_from_dataframe_model`**, **`columnar_dependency`**, **`rows_dependency`** — OpenAPI-friendly columnar bodies and **`Depends`** factories for **`DataFrameModel`**; see [FASTAPI](../integrations/fastapi/fastapi.md).
- **`pydantable.testing.fastapi`:** **`fastapi_app_with_executor`**, **`fastapi_test_client`** (lifespan-aware **`TestClient`** for **`executor_lifespan`** / **`get_executor`**).
- **`pydantable.fastapi`:** **`ndjson_streaming_response`** / **`ndjson_chunk_bytes`** for NDJSON **`StreamingResponse`** from **`astream()`** without hand-rolling encoders.
- **`pydantable.fastapi`** (optional **`pip install 'pydantable[fastapi]'`**): **`executor_lifespan`**, **`get_executor`** (``Depends``), **`register_exception_handlers`** for **`MissingRustExtensionError`** / **`pydantic.ValidationError`**. See [GOLDEN_PATH_FASTAPI](../integrations/fastapi/golden-path.md) and [FASTAPI](../integrations/fastapi/fastapi.md).
- **`pydantable.typing.SupportsLazyAsyncMaterialize`:** structural ``Protocol`` for objects with async terminal materialization via **`acollect`** (``DataFrameModel`` and ``AwaitableDataFrameModel``).
- **`AwaitableDataFrameModel`:** **`aread_parquet`**, **`aread_ipc`**, **`aread_csv`**, **`aread_ndjson`**, and **`aread_json`** return a chainable awaitable (``select`` / ``filter`` / … then ``await …acollect()``) so async routes avoid nested ``await`` on the read. **Lazy metadata:** ``await …columns`` / ``shape`` / ``empty`` / ``dtypes``; **`then`** for custom sync/async steps; **`concat`** to merge multiple pending chains or concrete models. **Async-first names:** unprefixed terminals on the chain — **`collect`**, **`to_dict`**, **`to_polars`**, **`to_arrow`**, **`rows`**, **`to_dicts`**, **`stream`** (aliases of the ``a*`` methods); **`DataFrameModel.Async.read_*`** / **`Async.write_sql`** / **`Async.export_*`** mirror **`aread_*`** / **`awrite_sql`** / **`aexport_*`** without the ``a`` prefix (``read_parquet`` cannot replace **`aread_parquet`** on the class itself because **`read_parquet`** is the sync lazy reader). Pending chains show a **descriptive ``repr``** (read path + chained transforms).
- **`DataFrameModel.aexport_parquet`**, **`aexport_csv`**, **`aexport_ndjson`**, **`aexport_ipc`**, **`aexport_json`**: async eager exports via the same **`aexport_*`** implementation module as **`pydantable.io`** (prefer **`DataFrameModel`** classmethods in application code).
- **Rust async bridge:** **`async_execute_plan`** and **`async_collect_plan_batches`** on **`pydantable_native._core`** (Tokio + **`pyo3-async-runtimes`**); **`acollect`** / **`ato_*`** prefer this awaitable when present.
- **`DataFrame.submit`** / **`DataFrameModel.submit`** and **`ExecutionHandle`** (**`result`**, **`done`**, **`cancel`**) for background **`collect`**.
- **`DataFrame.astream`** / **`DataFrameModel.astream`**: async iteration of column **`dict`** chunks after one engine collect (see [EXECUTION](../user-guide/execution.md)).
- **`DataFrame.stream`** / **`DataFrameModel.stream`**: synchronous **`dict[str, list]`** chunk iterator (same semantics as **`astream`**); **`PlanMaterialization`** and **`plan_materialization_summary()`** label the four terminal modes (blocking, async, deferred, chunked).

### Docs

- New [FASTAPI_ENHANCEMENTS](../integrations/fastapi/enhancements.md) (roadmap + “when to use what” matrix); links from [GOLDEN_PATH_FASTAPI](../integrations/fastapi/golden-path.md), [FASTAPI](../integrations/fastapi/fastapi.md), [DOCS_MAP](../getting-started/docs-map.md).
- [FASTAPI_ENHANCEMENTS](../integrations/fastapi/enhancements.md): production **lifespan** snippet (**`executor_lifespan`**, **`get_executor`**, **`register_exception_handlers`**), NDJSON helper semantics, troubleshooting table (422 vs 503, empty streams, executor tuning); **`tests/test_pydantable_fastapi_integration.py`** covers empty NDJSON, Unicode/null, custom **`media_type`**, **`astream`** batching, and golden-path stream parsing.
- [FASTAPI](../integrations/fastapi/fastapi.md) **Columnar OpenAPI and Depends**; [fastapi_columnar_bodies](../cookbook/fastapi_columnar_bodies.md) uses generated models; **`tests/test_pydantable_fastapi_columnar.py`** covers OpenAPI schema, aliases, **`rows_dependency`**, and **`pydantable.testing.fastapi`**.
- [FASTAPI](../integrations/fastapi/fastapi.md) / [FASTAPI_ENHANCEMENTS](../integrations/fastapi/enhancements.md) / cookbook: columnar **422** vs **`ValueError`** (**500**), nested **`list[NestedModel]`**, **`TestClient(raise_server_exceptions=False)`**; expanded **`tests/test_pydantable_fastapi_columnar.py`** (cache, nested routes, length mismatch, **`register_handlers`**).
- [fastapi_observability](../cookbook/fastapi_observability.md), [fastapi_background_tasks](../cookbook/fastapi_background_tasks.md) (end-to-end-style examples); example **`docs/examples/fastapi/service_layout/`** (`UserBatch`, health metadata, **400** on length mismatch); **`tests/test_pydantable_errors.py`**, **`tests/test_pydantable_fastapi_service_layout.py`**, broader columnar / handler tests; [FASTAPI_ENHANCEMENTS](../integrations/fastapi/enhancements.md) Phases 4–6 and 8 marked shipped where applicable.
- [TYPING](../user-guide/typing.md): expanded **`SupportsLazyAsyncMaterialize`** (when to use vs **`DataFrameModelWithRow`**, runtime **`isinstance`** caveats, examples); [DATAFRAMEMODEL](../user-guide/dataframemodel.md) cross-link from async lazy I/O.
- New [MATERIALIZATION](../user-guide/materialization.md) page; [EXECUTION](../user-guide/execution.md), [INTERFACE_CONTRACT](../semantics/interface-contract.md), [DATAFRAMEMODEL](../user-guide/dataframemodel.md), [DOCS_MAP](../getting-started/docs-map.md) cross-links.
- [DATAFRAMEMODEL](../user-guide/dataframemodel.md) **Three layers** (ASCII diagram + rule of thumb + lazy-shape warning); [async_lazy_pipeline](../cookbook/async_lazy_pipeline.md); [fastapi_async_materialization](../cookbook/fastapi_async_materialization.md) prefers **`collect`** / **`to_dict`**.
- [ROADMAP](../project/roadmap.md), [DATA_IO_SOURCES](../io/data-io-sources.md), and **`docs/async_ideas/`** aligned with async/submit/stream work where applicable.
- **README**, [index](../index.md), [DOCS_MAP](../getting-started/docs-map.md), [GOLDEN_PATH_FASTAPI](../integrations/fastapi/golden-path.md), [TROUBLESHOOTING](../getting-started/troubleshooting.md): FastAPI helpers, **`pydantable.errors`**, cookbooks, **`service_layout`**, and testing helpers cross-linked; troubleshooting bullets repaired.

## [1.5.0] — 2026-03-29

### Added

- **Batched column-dict I/O:** `iter_*` / `aiter_*` readers and `write_*_batches` writers for core formats (Parquet, IPC, CSV, NDJSON, JSON array/lines) plus selected extras (Excel/Delta/Avro/ORC/BigQuery/Snowflake/Kafka where supported).
- **Engine streaming default propagation:** `engine_streaming=` alias and per-frame defaults set by lazy `read_*` / `aread_*`, applied to later `collect()` / `to_*` / lazy `write_*` unless overridden.

### Fixed

- **IPC batch iteration:** `iter_ipc(..., as_stream=False)` now works with PyArrow `RecordBatchFileReader` (file format readers are not iterable in some versions).

## [1.4.0] — 2026-03-29

### Added

- **SQL streaming (SQLAlchemy):** `iter_sql` / `aiter_sql` for batch iteration of `SELECT` results; `DataFrameModel.iter_sql` / `aiter_sql` yield typed batch models.
- **SQL batch sinks:** `write_sql_batches` / `awrite_sql_batches` for end-to-end streaming (consume batches without building one giant in-memory dict).

### Changed

- **`fetch_sql` / `afetch_sql`:** support `batch_size=` and automatic streaming behavior for large results (may return a streaming container with `.to_dict()`).
- **`write_sql` / `awrite_sql`:** support `chunk_size=` and stream inserts in chunks to reduce peak memory use.

## [1.3.0] — 2026-03-29

### Added

- **Expr (type-specific):** `list_join`, `list_sort`, and `list_unique` on homogeneous
  lists; `dt_week` (ISO week, `date` / `datetime`); `str_reverse`, `str_pad_start` /
  `str_pad_end`, `str_zfill`, `str_extract_regex`, and `str_json_path_match` (Polars
  engine; semantics in [SUPPORTED_TYPES](../user-guide/supported-types.md) and [INTERFACE_CONTRACT](../semantics/interface-contract.md)).
- **Docs / tests:** expanded expression contracts in [SUPPORTED_TYPES](../user-guide/supported-types.md) and
  [TYPING](../user-guide/typing.md); integration coverage in `tests/test_type_specific_expr.py`.

### Removed

- **CI / Release:** CycloneDX SBOM generation and upload jobs (too fragile for default
  automation); generate SBOMs locally if required (see [DEVELOPER](../project/developer.md) **Optional CycloneDX SBOMs**).

## [1.2.0] — 2026-03-28

### Added

- **Column types (see [SUPPORTED_TYPES](../user-guide/supported-types.md)):**
  - **`typing.Literal[...]`** — homogeneous **`str`**, **`int`**, or **`bool`** members only; dtype descriptors include an optional **`literals`** list; invalid **`filter(col == ...)`** constants are rejected when the expression is built.
  - **`ipaddress.IPv4Address`** / **`IPv6Address`** — Polars **Utf8**, canonical string form; string cells coerce on ingest.
  - **`pydantable.types.WKB`** — **`bytes`** subclass for Well-Known Binary geometry; Polars **Binary** (same **`Expr`** surface as **`bytes`** where applicable).
  - **`Annotated[str, ...]`** — logical **`str`** in the Rust plan; Pydantic applies metadata on **`collect()`** / **`RowModel`**.
- **Tests:** `tests/test_extended_scalar_dtypes_v12.py`, typing-engine parity for these scalars, mypy/pyright **DataFrameModel** chain snippets.
- **Docs:** practical notes for **`Expr`** comparisons (IP/WKB operands), [TYPING](../user-guide/typing.md) (1.2 scalars), [DATAFRAMEMODEL](../user-guide/dataframemodel.md) field list.

### Fixed

- **`cargo check -p pydantable-core --no-default-features`:** exhaustive **`DTypeDesc::Scalar`** matches and row-wise **`CompareOp`** / **`cast_literal_value`** coverage for **IPv4** / **IPv6** / **WKB** when **`polars_engine`** is off.

### Typing / lint

- **`__all__`** and Ruff-driven cleanups on **`schema`**, **`types`**, and tests.

## [1.1.0] — 2026-03-27

### Added

- **Typing:** `DataFrameModel` transform methods return derived model types so mypy/pyright
  can verify the schema after `select`, `drop`, `rename`, `with_columns`, `join`, and
  `group_by(...).agg(...)` without materializing between steps (see GitHub issue #1).
- **Tests:** expanded coverage (I/O fallbacks, PySpark/expr edges, schema helpers) and mypy
  regression updates in `tests/test_mypy_dataframe_model_return_types.py`.

## [1.0.0] — 2026-03-26

### Scope

- Production-ready major release focused on API stability and semver contract clarity.
- No large new execution-engine features are required for the tag.

### Added

- Ingest/docs consistency for missing optional behavior:
  - `fill_missing_optional` documented consistently across constructor and typed lazy-read materialization paths.
  - Explicit schema defaults on optional fields (for example `note: str | None = "n/a"` or `= None`) now take precedence when `fill_missing_optional=False` instead of raising.
- **1.0.0** readiness documentation:
  - explicit 1.x semver policy in [VERSIONING](../semantics/versioning.md),
  - release gate checklist and security-advisory handling in [DEVELOPER](../project/developer.md),
  - roadmap, README, and docs index updates for 1.0 communication and support matrix policy.
- **`[docs]`** extra includes **SQLAlchemy** so Sphinx (**`-W`**) and `sphinx-autodoc-typehints` resolve `DataFrameModel` **`Engine`** / **`Connection`** annotations in CI (matches Read the Docs).

### Changed

- Documentation includes migration guidance from earlier `missing_optional` string-style wording (`"fill_none"` / `"error"`) to boolean `fill_missing_optional=True/False`.

### Stability commitments

- 1.x patch/minor/major policy is defined in [VERSIONING](../semantics/versioning.md).
- Behavioral semantics continue to be defined in [INTERFACE_CONTRACT](../semantics/interface-contract.md).

### Upgrade guidance

- Canonical upgrade path from 0.20.x/0.23.x is documented in `README.md` and linked from the docs index; I/O renames from 0.22.x/0.23.x are summarized under **0.23.0** below.

## [0.23.0] — 2026-03-25

### Highlights

- **Out-of-core file workflows:** **`read_parquet`**, **`read_csv`**, **`read_ndjson`**, **`read_ipc`**, **`read_json`** (and **`aread_*`**) return a **`ScanFileRoot`** so **`DataFrame` / `DataFrameModel`** can run transforms on a Polars **`LazyFrame`** without loading the full file into Python lists first.
- **`DataFrame.write_parquet`** (and **`write_csv`**, **`write_ipc`**, **`write_ndjson`**): write the lazy pipeline from the Rust engine without building a giant **`dict[str, list]`** for the result.
- **Breaking — public I/O renames:** sync/async eager file reads into columns are **`materialize_*` / `amaterialize_*`**. Lazy local files use **`read_*` / `aread_*`**. Eager **`dict[str, list]` → file** uses **`export_*` / `aexport_*`**. SQL **`read_sql` / `aread_sql`** → **`fetch_sql` / `afetch_sql`**. Eager HTTP(S) column readers (0.22 **`read_*_url`**) → **`fetch_parquet_url`**, **`fetch_csv_url`**, **`fetch_ndjson_url`**. **Lazy** HTTP Parquet (temp file on disk) stays **`read_parquet_url` / `aread_parquet_url`** — use **`read_parquet_url_ctx` / `aread_parquet_url_ctx`** to delete the temp file when done ([IO_HTTP](../io/http.md)). Top-level **`pydantable`** exports and **`DataFrameModel`** classmethods follow the same vocabulary.
- **Pre-release / internal names:** development builds that still exposed **`scan_*` / `ascan_*`** or **`sink_*`** for lazy I/O now align with the public **`read_*` / `write_*`** names; **`pydantable`** re-exports **`read_parquet`**, **`read_parquet_url`**, **`aread_parquet`**, **`aread_parquet_url`**, **`export_parquet`** (replacing **`scan_*` / `write_parquet`** on the package root).

### Added

- **JSON (array of objects):** **`read_json`**, **`materialize_json`**, **`export_json`**, **`aread_json`**, **`amaterialize_json`**, **`aexport_json`** — local lazy scan and eager column dicts (see [IO_JSON](../io/json.md)).
- **`read_parquet_url_ctx` / `aread_parquet_url_ctx`:** context managers that delete the temporary Parquet file when the block exits (see [IO_HTTP](../io/http.md)).
- **`DataFrameModel`:** classmethods **`export_*`**, **`write_sql`** / **`awrite_sql`**, **`from_sql`** / **`afrom_sql`** delegating to **`pydantable.io`**.
- **`MissingRustExtensionError`:** subclass of **`NotImplementedError`** when the native extension is missing or incomplete on lazy scan/sink paths and **`execute_plan`** (still catchable as **`NotImplementedError`**).
- **HTTP / object store safety:** **`max_bytes`** on **`fetch_bytes`** and **`read_from_object_store`**; chunked reads with **`ValueError`** when exceeded.
- **Docs:** [IO_DECISION_TREE](../io/decision-tree.md), [IO_JSON](../io/json.md), [IO_HTTP](../io/http.md) updates, engine matrix in [IO_OVERVIEW](../io/overview.md), FASTAPI executor guidance; README and manual pages refreshed for **0.23.x** I/O.

### Details

- **Rust:** **`ScanFileRoot`**, **`plan_to_lazyframe`**, internal sink exports for lazy writes; join/groupby/reshape entrypoints work with lazy file roots where implemented (see [EXECUTION](../user-guide/execution.md) matrix).
- **Python:** **`read_csv_stdin`** uses **`materialize_csv`** internally.
- **Docs:** [EXECUTION](../user-guide/execution.md) memory model and streaming/collect compatibility matrix (**`PYDANTABLE_ENGINE_STREAMING`** reserved); [DATA_IO_SOURCES](../io/data-io-sources.md), [FASTAPI](../integrations/fastapi/fastapi.md), [INTERFACE_CONTRACT](../semantics/interface-contract.md), [ROADMAP](../project/roadmap.md), [README](../project/repository-readme.md).

### Migration (from 0.22.x)

| Old (0.22.x) | Use instead (0.23.0) |
|--------------|----------------------|
| Eager file → **`dict[str, list]`** via **`read_parquet`**, **`aread_parquet`**, … | **`materialize_parquet`**, **`amaterialize_parquet`**, … |
| **`read_sql`**, **`aread_sql`** | **`fetch_sql`**, **`afetch_sql`** |
| Eager URL → columns via **`read_parquet_url`** / **`read_csv_url`** / **`read_ndjson_url`** | **`fetch_parquet_url`**, **`fetch_csv_url`**, **`fetch_ndjson_url`** |
| Lazy HTTP Parquet (unchanged name, new cleanup helpers) | Still **`read_parquet_url`** / **`aread_parquet_url`**; prefer **`read_parquet_url_ctx`** / **`aread_parquet_url_ctx`** for automatic temp-file removal |
| Large local file, filter → Parquet out | **`read_parquet`** + transforms + **`DataFrame.write_parquet`** |

## [0.22.0] — 2026-03-25

### Highlights

- **Comprehensive I/O:** the **`pydantable.io`** package adds Rust-backed (Polars) **`read_*` / `write_*`** for **Parquet**, **Arrow IPC**, **CSV**, and **NDJSON** into **`dict[str, list]`**, with **`Python::allow_threads`** on read hot paths; **PyArrow** remains the default for **buffers**, **column projection**, and **streaming IPC**. Async mirrors **`aread_*` / `awrite_*`** use **`asyncio.to_thread`** (optional **`executor=`**), matching **`acollect`** / **`ato_arrow`**.
- **SQLAlchemy bridge:** **`read_sql`** / **`write_sql`** ( **`pip install 'pydantable[sql]'`** + your DB driver) for URL/engine **`SELECT`** → column dict and append/replace inserts across **SQLAlchemy-supported** databases.
- **Transports (experimental):** HTTP(S) **`fetch_bytes`**, **`read_parquet_url`**, **`read_csv_url`**, **`read_ndjson_url`**, and **`fsspec`**-based **`read_from_object_store`** — opt in with **`experimental=True`** or **`PYDANTABLE_IO_EXPERIMENTAL=1`**.
- **Tier-2/3 extras (best-effort):** **`[excel]`**, **`[kafka]`**, **`[bq]`**, **`[snowflake]`**, **`[cloud]`**; helpers such as **`read_excel`**, **`read_delta`**, **`read_kafka_json_batch`**, **`read_csv_stdin` / `write_csv_stdout`** (see **`docs/io/data-io-sources.md`**).
- **Optional `[rap]`:** true-async CSV via **`aread_csv_rap`** when **`rapcsv`** + **`rapfiles`** are installed.
- **Engine override:** set **`PYDANTABLE_IO_ENGINE=rust`** or **`pyarrow`** to force file readers/writers.
- **Release quality bar:** the **`v0.22.0`** tag is cut from a commit that passes **`make check-full`**, full **`pytest`**, and Rust checks including **`--no-default-features`**.
- **Supply chain:** the release workflow publishes **CycloneDX SBOMs** (Python + Rust) alongside wheels/sdist.
- **Support matrix:** Python **3.10–3.13**.

### Details

- **Rust:** new **`pydantable_native._core`** exports **`io_read_*_path`** / **`io_write_*_path`**; column-dict writes round-trip through Python **`polars.DataFrame`** → IPC → Rust writers (install **`pydantable[polars]`** for writes).
- **Tests:** **`tests/test_io_comprehensive.py`** (round-trips, SQLite SQL, local HTTP server for URL Parquet).
- **CI:** Python test job installs **`sqlalchemy`** with other dev deps.

## [0.21.0] — 2026-03-25

### Highlights

- **Streamlit:** `DataFrame` and `DataFrameModel` implement the **Python DataFrame Interchange Protocol** (`__dataframe__`) via PyArrow so `st.dataframe(df)` can render a typed `pydantable` frame directly when `pyarrow` is installed (`pip install 'pydantable[arrow]'`). For editing, use `st.data_editor(df.to_arrow())` (or `to_polars()`). See [STREAMLIT](../integrations/streamlit.md) and [EXECUTION](../user-guide/execution.md) (**interchange**).

## [0.20.0] — 2026-03-25

**Supersedes:** 0.19.0.

### Highlights

- **UX / discovery:** Core **`DataFrame`** and **`DataFrameModel`** expose **`columns`**, **`shape`**, **`empty`**, **`dtypes`**, **`info()`**, and **`describe()`** for **int**, **float**, **bool**, and **str** columns (one **`to_dict()`** materialization). **`shape[0]`** follows **root-buffer** semantics—see [INTERFACE_CONTRACT](../semantics/interface-contract.md) **Introspection**, [EXECUTION](../user-guide/execution.md).
- **Docs:** [QUICKSTART](../getting-started/quickstart.md) (five-minute tour), repository **`notebooks/five_minute_tour.ipynb`**, [EXECUTION](../user-guide/execution.md) sections on **materialization costs**, **import styles**, **copy-as / interchange**; **naming map** in [PANDAS_UI](../integrations/alternate-surfaces/pandas-ui.md) / [PYSPARK_UI](../integrations/alternate-surfaces/pyspark-ui.md).
- **Display:** **`pydantable.display`** — **`get_repr_html_limits`**, **`set_display_options`**, **`reset_display_options`**; env **`PYDANTABLE_REPR_HTML_*`** for Jupyter HTML preview bounds.
- **`DataFrame.value_counts`** / **`DataFrameModel.value_counts`** (group-by path); **`_repr_mimebundle_`** on **`DataFrame`** and **`DataFrameModel`** (`text/plain` + `text/html`).
- **Debugging:** **`PYDANTABLE_VERBOSE_ERRORS=1`** appends schema context to **`ValueError`** from **`execute_plan`**.
- **Expressions:** **`Expr`**, **`ColumnRef`**, **`WhenChain`**, and pending window helpers implement readable **`__repr__`**. Tests: **`tests/test_expr_repr.py`**.
- **PySpark façade:** **`DataFrame.show()`** and **`summary()`** (alias of **`describe()`**). See [PYSPARK_UI](../integrations/alternate-surfaces/pyspark-ui.md), [PYSPARK_PARITY](../integrations/alternate-surfaces/pyspark-parity.md).
- **Documentation:** [README](../project/repository-readme.md), [index](../index.md), [ROADMAP](../project/roadmap.md), [PARITY_SCORECARD](../user-guide/polars-alignment/parity-scorecard.md), [PANDAS_UI](../integrations/alternate-surfaces/pandas-ui.md), [DEVELOPER](../project/developer.md).

### Details

- **Repr / HTML:** Multi-line **`DataFrame.__repr__`** and **`_repr_html_`** (card-style HTML; grouped/model banners). See [EXECUTION](../user-guide/execution.md), **`tests/test_dataframe_repr.py`**.
- **Tests:** **`tests/test_display_options.py`**, **`tests/test_dataframe_discovery.py`**, **`tests/test_rust_engine_verbose_errors.py`**. See [EXECUTION](../user-guide/execution.md), [INTERFACE_CONTRACT](../semantics/interface-contract.md).
- **Release hygiene:** **`make check-full`**, full **pytest**, **`cargo test --all-features`** per [DEVELOPER](../project/developer.md).

## [0.19.0] — 2026-03-24

### Highlights

- **Pre-1.0 consolidation:** [VERSIONING](../semantics/versioning.md) documents **0.x** patch vs minor expectations; [INTERFACE_CONTRACT](../semantics/interface-contract.md) links there for semver scope while staying the behavioral source of truth.
- **Roadmap to 1.0:** [ROADMAP](../project/roadmap.md) **Shipped in 0.19.0** replaces the planned checklist; **Planned v1.0.0** items that belong on the **1.0.0** tag (full **1.x** semver policy, SBOM, comms) remain explicitly deferred there with rationale below.
- **Parity docs:** [POLARS_TRANSFORMATIONS_ROADMAP](../user-guide/polars-alignment/transformations-roadmap.md), [PARITY_SCORECARD](../user-guide/polars-alignment/parity-scorecard.md), [PYSPARK_PARITY](../integrations/alternate-surfaces/pyspark-parity.md), [README](../project/repository-readme.md), and [index](../index.md) updated for **current release** and **0.19 → 1.0** clarity—no new table methods or PySpark `functions` rows.
- **Performance:** [PERFORMANCE](../project/performance.md) adds an **0.19.0 validation** note (key scripts spot-checked; no headline number refresh vs **0.18.x** paths).
- **CI / tests:** Grouped output comparisons in **`tests/test_v018_features.py`** sort by group key where row order is not API-guaranteed (stable **`pytest-xdist`** on Linux).

### Details

See [ROADMAP](../project/roadmap.md) **Shipped in 0.19.0**. Release hygiene: **`make check-full`**, **`cargo test --all-features`**, **`cargo check --no-default-features`**, full **pytest** before tag; GitHub Actions install deps aligned with [DEVELOPER](../project/developer.md) / **`pyproject.toml`** **`[dev]`**.

**Deferred to v1.0.0 tag (not blocking 0.19.0):** formal **1.x** semver publication, PyPI packaging dry-run narrative, SBOM/supply-chain notes, support matrix as a **1.0.x** commitment, and README/index “1.0 leads” copy—see [ROADMAP](../project/roadmap.md) **Planned v1.0.0**.

## [0.18.0] — 2026-03-22

### Highlights

- **Grouped execution errors:** Polars **`collect()`** failures during **`group_by().agg()`** may include **`(group_by().agg())`** in the **`ValueError`** text (via **`polars_err_ctx`**) so they are identifiable as grouped aggregation runtime errors. See [EXECUTION](../user-guide/execution.md).
- **Maps:** **Non-string** map keys (**`dict[int, T]`**, non-UTF-8 Arrow map keys) remain **unsupported** and are **explicitly deferred** for this release ([SUPPORTED_TYPES](../user-guide/supported-types.md), [ROADMAP](../project/roadmap.md) **Later**).
- **Documentation:** Post–**P7** note in [POLARS_TRANSFORMATIONS_ROADMAP](../user-guide/polars-alignment/transformations-roadmap.md) (phases complete; further parity is additive). [PARITY_SCORECARD](../user-guide/polars-alignment/parity-scorecard.md), [PYSPARK_PARITY](../integrations/alternate-surfaces/pyspark-parity.md), [DEVELOPER](../project/developer.md), [ROADMAP](../project/roadmap.md) updated. **No** new PySpark **`sql.functions`** wrappers or table API changes.
- **Tests:** Hypothesis + integration coverage for **`group_by`** / **`join`** (`tests/test_hypothesis_properties.py`, **`tests/test_v018_features.py`**); Rust **`polars_err_ctx`** message format (`execute_polars/common.rs`, **`polars_err_format_tests`**).

### Details

See [ROADMAP](../project/roadmap.md) **Shipped in 0.18.0**. [INTERFACE_CONTRACT](../semantics/interface-contract.md) aggregation rules are unchanged; the doc notes optional **`group_by().agg()`** error-message context.

## [0.17.0] — 2026-03-18

### Highlights

- **Maps (string keys):** Documented and tested **Expr** behavior for **`map_get`** / **`map_contains_key`** on columns ingested from PyArrow **`map<utf8, …>`** (missing key → null). **Non-string** Python **`dict[int, T]`** map keys remain **unsupported** (deferred); see [ROADMAP](../project/roadmap.md) **Later**.
- **PySpark façade:** [`PYSPARK_PARITY.md`](../integrations/alternate-surfaces/pyspark-parity.md) — new thin **`pydantable.pyspark.sql.functions`** wrappers: **`str_replace`**, **`regexp_replace`** (alias, literal replace), **`strip_prefix`**, **`strip_suffix`**, **`strip_chars`**, **`strptime`**, **`binary_len`**, **`list_len`**, **`list_get`**, **`list_contains`**, **`list_min`**, **`list_max`**, **`list_sum`** (core **`Expr`** / Rust lowering unchanged). Tests: **`tests/test_pyspark_sql.py`**.
- **Docs:** Refreshed [`PARITY_SCORECARD.md`](../user-guide/polars-alignment/parity-scorecard.md), [`POLARS_TRANSFORMATIONS_ROADMAP.md`](../user-guide/polars-alignment/transformations-roadmap.md), [`SUPPORTED_TYPES.md`](../user-guide/supported-types.md) (map + Arrow ingest note).

### Details

See [ROADMAP](../project/roadmap.md) **Shipped in 0.17.0**.

## [0.16.1] — 2026-03-27

### Fixed

- **Expression typing:** Binary arithmetic on **`dict[str, T]`** map columns (for example `df.m + 1` or `df.m + df.m`) now raises **`TypeError`** in Rust (`infer_arith_dtype`) instead of panicking on an internal unwrap. Regression test: **`tests/test_expr_070_surfaces.py`**.
- **Constructors:** **`validate_columns_strict`** (and therefore **`DataFrame[Schema](pa.Table)`** / **`RecordBatch`**) imported Arrow conversion helpers from the wrong submodule (`pydantable.schema.io`, which does not exist). Imports now use **`pydantable.io`**, matching **`DataFrameModel`**. Regression: **`tests/test_arrow_interchange.py`** (`test_dataframe_generic_accepts_pa_table`).

## [0.16.0] — 2026-03-26

### Highlights

- **Arrow interchange:** **`read_parquet`** and **`read_ipc`** (optional **`as_stream`** for streaming IPC) return **`dict[str, list]`** for **`DataFrame` / `DataFrameModel`**. **`to_arrow`** / **`ato_arrow`** materialize a PyArrow **`Table`** after the same engine path as **`to_dict`** (not zero-copy). Optional extra **`pydantable[arrow]`** (**`pyarrow>=14`**). Constructors accept **`pa.Table`** / **`RecordBatch`** when **`pyarrow`** is installed.
- **FastAPI:** [`FASTAPI.md`](../integrations/fastapi/fastapi.md) — multipart Parquet upload, **`Depends`** executor pattern, background-task notes, **422** vs application error guidance. **`python-multipart`** in **`[dev]`** and CI workflows. Tests: **`tests/test_fastapi_recipes.py`** (multipart + invalid body **422**), **`tests/test_arrow_interchange.py`**; **`scripts/verify_doc_examples.py`** extended.
- **Docs:** [`EXECUTION.md`](../user-guide/execution.md), [`SUPPORTED_TYPES.md`](../user-guide/supported-types.md), [`INTERFACE_CONTRACT.md`](../semantics/interface-contract.md), [`ROADMAP.md`](../project/roadmap.md), [`README.md`](../project/repository-readme.md), [`index.md`](../index.md).

### Details

See [ROADMAP](../project/roadmap.md) **Shipped in 0.16.0**. Sync **`read_parquet` / `read_ipc`** are blocking; use **`asyncio.to_thread`** or an executor from **`async def`** routes for large files if loop latency matters.

- **CI / release:** **`actions/cache@v5`** in **`ci.yml`** and **`release.yml`** (clears GitHub Actions Node 20 deprecation warnings for **`actions/cache@v4`**). **`release.yml`** uses **`maturin build`** + **`twine upload --skip-existing`** per platform instead of deprecated **`maturin publish`** (see [PyO3/maturin#2334](https://github.com/PyO3/maturin/issues/2334)); **`TWINE_USERNAME=__token__`** and **`PYPI_API_TOKEN`** unchanged.

## [0.15.0] — 2026-03-25

### Highlights

- **Async materialization:** **`acollect`**, **`ato_dict`**, **`ato_polars`** on **`DataFrame`**; **`DataFrameModel`** adds the same plus **`arows`** and **`ato_dicts`**. Work runs in **`asyncio.to_thread`** or an optional **`executor=`**. See [EXECUTION](../user-guide/execution.md), [FASTAPI](../integrations/fastapi/fastapi.md).
- **FastAPI:** **`async def`** route examples, **`lifespan`** + **`ThreadPoolExecutor`**, and **`StreamingResponse`** guidance (manual chunking; no built-in row iterator yet). **`tests/test_fastapi_recipes.py`** and **`scripts/verify_doc_examples.py`** extended.
- **Arrow-native maps:** PyArrow **`map<utf8, …>`** arrays (and chunked) ingest for **`dict[str, T]`** columns; convert to Python **`dict`** cells. String keys only; **`strict`** checks scalar value types (nested map values: best-effort). Tests: **`tests/test_pyarrow_map_ingest.py`**. [SUPPORTED_TYPES](../user-guide/supported-types.md) updated.
- **PySpark façade:** **`trim`**, **`abs`**, **`round`**, **`floor`**, **`ceil`** in **`pydantable.pyspark.sql.functions`** (and package **`__all__`**). [PYSPARK_PARITY](../integrations/alternate-surfaces/pyspark-parity.md) updated.
- **Constructor cleanup:** **`validate_data`** removed from **`DataFrame`** and **`DataFrameModel`**. Ingest depth uses **`trusted_mode`** only (`off` / `shape_only` / `strict`; omit for full per-element validation). Passing **`validate_data=...`** raises **`TypeError`**. Removed internal schema helpers **`_VALIDATE_DATA_KW_UNSET`**, **`_warn_validate_data_kw_deprecated`**, and **`_coerce_validate_data_kw`**. Direct callers of **`validate_columns_strict`** may still use **`validate_elements`** as a legacy bridge. Docs (**`DATAFRAMEMODEL`**, **`FASTAPI`**, **`SUPPORTED_TYPES`**, **`PERFORMANCE`**, etc.) describe **`trusted_mode`** only on constructors.
- **Dev:** **`pytest-asyncio`** in **`[dev]`**; **`asyncio_mode = auto`** in **`pyproject.toml`**.

### Tests

- **`tests/test_async_materialization.py`**, **`tests/test_pyarrow_map_ingest.py`**, **`tests/test_v015_features.py`**, **`tests/test_v015_constructor_api.py`**; extended **`tests/test_fastapi_recipes.py`**. **`tests/test_v014_features.py`**, **`tests/test_dataframe_model.py`**, **`tests/test_dataframe_ops.py`**: **`TypeError`** when **`validate_data`** is passed; trusted paths use **`trusted_mode`** only.

### Details

See [ROADMAP](../project/roadmap.md) **Shipped in 0.15.0**. Sync **`collect` / `to_dict` / `to_polars`** are unchanged aside from constructor kwargs (drop **`validate_data`**; use **`trusted_mode`**). You may replace manual **`asyncio.to_thread`** wrappers with **`acollect`** / **`ato_*`**.

**`rust_version()`** in the extension reports **`env!("CARGO_PKG_VERSION")`** so it matches **`pyproject.toml`** / **`Cargo.toml`**.

## [0.14.0] — 2026-03-23

### Highlights

- **Window `orderBy` null placement:** **`nulls_last`** on **`Window.partitionBy(...).orderBy(...)`** (per-column list or bool); framed windows use all keys; unframed Polars **`.over`** uses the first key for **`SortOptions`**. Docs: [WINDOW_SQL_SEMANTICS](../semantics/window-sql-semantics.md), [INTERFACE_CONTRACT](../semantics/interface-contract.md).
- **Trusted `shape_only`:** **`pydantable.DtypeDriftWarning`** when data would fail **`strict`**; env **`PYDANTABLE_SUPPRESS_SHAPE_ONLY_DRIFT_WARNINGS=1`** to silence. See [SUPPORTED_TYPES](../user-guide/supported-types.md).
- **`validate_data` deprecation:** explicit **`validate_data=`** without **`trusted_mode`** raises **`DeprecationWarning`** (removal shipped in **0.15.0**). See [DATAFRAMEMODEL](../user-guide/dataframemodel.md), [FASTAPI](../integrations/fastapi/fastapi.md).
- **PySpark façade:** **`dayofmonth`**, **`lower`**, **`upper`** in **`pydantable.pyspark.sql.functions`**. [PYSPARK_PARITY](../integrations/alternate-surfaces/pyspark-parity.md) updated.
- **FastAPI DX:** **`TestClient`** recipes and OpenAPI notes in [FASTAPI](../integrations/fastapi/fastapi.md); **`tests/test_fastapi_recipes.py`**; **`fastapi`** / **`httpx`** in **`[dev]`** and CI.
- **Hypothesis:** extra property test for **`with_columns`** identity; [DEVELOPER](../project/developer.md) documents running property tests.
- **Tests:** **`tests/test_v014_features.py`** covers **`DtypeDriftWarning`** (including multi-column drift), window **`lag`** / **`row_number`** with null sort order, FastAPI **422** / OpenAPI **`requestBody`**, and PySpark **`dayofmonth` / `lower` / `upper`**. (Constructor **`validate_data`** was deprecated here and removed in **0.15.0** — see changelog **\[0.15.0\]**.)

### Details

See [ROADMAP](../project/roadmap.md) **Shipped in 0.14.0**.

## [0.13.0] — 2026-03-23

### Highlights

- **Stabilization + combined scope:** [FASTAPI](../integrations/fastapi/fastapi.md) — **`trusted_mode`** / **`validate_data`**, column-shaped **`dict[str, list]`** bodies, **sync** materialization and pointers forward to async work (shipped in **0.15.0**); trust-boundary guidance for large / pre-validated tables and **Polars** / **Arrow**; install notes for PyPI wheels vs git builds.
- **Sync-only I/O (at time of 0.13.0):** [EXECUTION](../user-guide/execution.md) and [PERFORMANCE](../project/performance.md) described **blocking** materialization; **async** APIs arrived in **0.15.0** ([EXECUTION](../user-guide/execution.md), [FASTAPI](../integrations/fastapi/fastapi.md)). Tuning text prefers **`trusted_mode`** alongside **`validate_data`**.
- **Window semantics (docs):** null ordering and **`CURRENT ROW`** / peer framing in [WINDOW_SQL_SEMANTICS](../semantics/window-sql-semantics.md) and [INTERFACE_CONTRACT](../semantics/interface-contract.md); `Window` docstring in `window_spec.py`. (**User-facing `NULLS FIRST` / `LAST`** shipped in **0.14.0**.)
- **Trusted `strict` + PyArrow:** `isinstance(..., pa.Array | pa.ChunkedArray)` in trusted buffers (concrete array types such as `Int64Array`); stricter scalars for **int**, **float**, **decimal**, **enum**, **uuid**, and temporal Arrow types. Tests in `tests/test_trusted_strict_pyarrow.py`; **`pyarrow>=14`** in **`[dev]`** and CI. (**`shape_only` drift warnings** shipped in **0.14.0**.)
- **Performance:** `benchmarks/framed_window_bench.py`, `benchmarks/trusted_polars_ingest_bench.py`; [PERFORMANCE](../project/performance.md) table and cross-links.
- **Discoverability:** [index](../index.md), [README](../project/repository-readme.md) roadmap table, and cross-links across [INTERFACE_CONTRACT](../semantics/interface-contract.md), [WINDOW_SQL_SEMANTICS](../semantics/window-sql-semantics.md), [POLARS_TRANSFORMATIONS_ROADMAP](../user-guide/polars-alignment/transformations-roadmap.md).
- **CI:** **`RUSTSEC-2025-0141`** audit-step comment; GitHub Actions versions reviewed (**`actions/cache@v4`**, etc.).
- **Examples:** `scripts/verify_doc_examples.py` covers new FastAPI patterns (trusted ingest + columnar body).

### Details

Release audit: `make check-full` and full **pytest** green with a **release** `maturin` build. No regressions to **`rangeBetween`**, trusted **`strict`**, or **`map_from_entries`** beyond documentation and **PyArrow** **`strict`** hardening above.

**Roadmap (editorial):** **0.13.0** ships the documentation-first stabilization track together with the scope formerly planned as **Remaining in 0.13.x** / **0.14.0**. Async materialization shipped in **0.15.0** (see changelog **\[0.15.0\]**).

See [FASTAPI](../integrations/fastapi/fastapi.md), [EXECUTION](../user-guide/execution.md), [PERFORMANCE](../project/performance.md), [ROADMAP](../project/roadmap.md), and [INTERFACE_CONTRACT](../semantics/interface-contract.md).

## [0.12.0] — 2026-03-22

### Highlights

- **Multi-key `rangeBetween`:** aggregate window frames may use multiple `orderBy` columns; sort is lexicographic and **range bounds apply to the first** sort key (PostgreSQL-style). Documented in [WINDOW_SQL_SEMANTICS](../semantics/window-sql-semantics.md).
- **Trusted `strict` ingest:** Polars columns are matched structurally to nested annotations (`list` / `dict[str, T]` / nested `Schema` structs); columnar Python paths get the same nested shape checks.
- **Contracts and parity docs:** refresh `INTERFACE_CONTRACT`, PySpark UI/scorecard, roadmap; add duplicate-key policy for `map_from_entries` ([SUPPORTED_TYPES](../user-guide/supported-types.md)).
- **Regression tests:** broader coverage for multi-key `rangeBetween` (desc/mixed `orderBy`, partitions, `date`/`datetime` axis, `window_mean`/`window_min`), PySpark window mirrors, trusted `strict` nested paths (Python + Polars), `map_from_entries` duplicate keys, and `DataFrame` / `DataFrameModel` strict parity.

### Details

See [INTERFACE_CONTRACT](../semantics/interface-contract.md), [PYSPARK_PARITY](../integrations/alternate-surfaces/pyspark-parity.md), [PARITY_SCORECARD](../user-guide/polars-alignment/parity-scorecard.md), [WINDOW_SQL_SEMANTICS](../semantics/window-sql-semantics.md), and [ROADMAP](../project/roadmap.md).

## [0.11.0] — 2026-03-23

### Highlights

- **Window range semantics v2:** `rangeBetween` supports numeric, `date`, `datetime`, and `duration` order keys (single `orderBy` key), with deterministic boundary-inclusive behavior.
- **Map ergonomics expanded:** add `map_from_entries()` and PySpark-compatible `element_at()` alias; map entry roundtrip coverage expanded.
- **Trusted ingest modes:** add explicit trusted modes (`shape_only`, `strict`) alongside compatibility with `validate_data`, including stricter nullability and dtype checks for trusted columnar paths.
- **Parity coverage expansion:** add dedicated DataFrame/DataFrameModel parity tests and additional PySpark map parity contracts.
- **Release hardening:** update docs/contracts and version metadata for the 0.11.0 line.

### Details

See [INTERFACE_CONTRACT](../semantics/interface-contract.md), [PYSPARK_PARITY](../integrations/alternate-surfaces/pyspark-parity.md), [SUPPORTED_TYPES](../user-guide/supported-types.md), and [ROADMAP](../project/roadmap.md).

## [0.10.0] — 2026-03-23

### Highlights

- **Framed windows expanded:** framed execution now covers `window_mean`, `window_min`, `window_max`, `lag`, `lead`, `rank`, and `dense_rank` in addition to `row_number` / `window_sum`.
- **Map utilities:** add `map_keys()` and `map_values()` to complement `map_len`, `map_get`, and `map_contains_key`.
- **Parity and interop hardening:** PySpark parity tests for framed windows/map utilities plus trusted constructor coverage for Polars DataFrame input (`validate_data=False`).
- **Window range contracts tightened:** `rangeBetween` now enforces exactly one `orderBy` key for supported aggregate frames, with explicit typed errors.
- **Map v2 parity expanded:** add `map_entries()` and PySpark wrappers for `map_len`, `map_get`, and `map_contains_key`.
- **Trusted ingest hardening:** Polars trusted constructor path rejects nulls in non-nullable schema fields when `validate_data=False`.

### Details

See [INTERFACE_CONTRACT](../semantics/interface-contract.md), [PYSPARK_PARITY](../integrations/alternate-surfaces/pyspark-parity.md), [SUPPORTED_TYPES](../user-guide/supported-types.md), and [ROADMAP](../project/roadmap.md).

## [0.9.0] — 2026-03-23

### Highlights

- **Bad-input ingest controls:** `ignore_errors=True` with `on_validation_errors=...` (`row_index`, `row`, `errors`) across `DataFrameModel` and `DataFrame` constructor paths.
- **Framed windows:** `rowsBetween` / `rangeBetween` frame metadata is wired through Python/PySpark/Rust; framed execution is supported for `row_number` / `window_sum` (`rangeBetween` on integer order keys, range offset computed from first `orderBy` key).
- **Map v2 values:** `dict[str, T]` map columns now support nested JSON-like value dtypes (lists/maps/structs and nullable unions), with `map_len`, `map_get`, and `map_contains_key` behavior preserved.
- **Release hardening:** expanded 0.9.0 edge-case tests and full quality-gate coverage (`make check-full`, docs example validation, Sphinx warnings-as-errors build).

### Details

See [DATAFRAMEMODEL](../user-guide/dataframemodel.md), [INTERFACE_CONTRACT](../semantics/interface-contract.md), [SUPPORTED_TYPES](../user-guide/supported-types.md), [PYSPARK_PARITY](../integrations/alternate-surfaces/pyspark-parity.md), and [ROADMAP](../project/roadmap.md).

## [0.8.0] — 2026-03-23

### Highlights

- **Global row count:** `global_row_count()` and PySpark `functions.count()` with no column (`count(*)`-style) for `DataFrame.select`.
- **Casts:** `str` → `date` / `datetime` via `Expr.cast(...)` (Polars parsing); use `strptime` for fixed formats.
- **Maps:** `Expr.map_get(key)` / `map_contains_key(key)` on `dict[str, T]` columns (list-of-struct encoding).
- **Windows:** `window_min` / `window_max`; IR carries optional `WindowFrame::Rows` for future Spark-style `rowsBetween` (lowering not yet implemented).
- **Docs:** `INTERFACE_CONTRACT`, `PYSPARK_PARITY`, `SUPPORTED_TYPES` updates.

### Details

See [SUPPORTED_TYPES](../user-guide/supported-types.md), [PYSPARK_PARITY](../integrations/alternate-surfaces/pyspark-parity.md), [ROADMAP](../project/roadmap.md), and [INTERFACE_CONTRACT](../semantics/interface-contract.md).

### Testing

- Broader integration tests for 0.7.0 / 0.8.0 surfaces (`test_v070_features`, `test_v080_features`), including PySpark `F.count()` with no column.

### Documentation

- README feature bullets; [INTERFACE_CONTRACT](../semantics/interface-contract.md) (global `select`); [POLARS_WORKFLOWS](../user-guide/polars-alignment/workflows.md) (single-row globals example); [index](../index.md), [EXECUTION](../user-guide/execution.md), [PYSPARK_UI](../integrations/alternate-surfaces/pyspark-ui.md), [PYSPARK_PARITY](../integrations/alternate-surfaces/pyspark-parity.md), [PYSPARK_INTERFACE](../integrations/alternate-surfaces/pyspark-interface.md).

## [0.7.0] — 2026-03-23

### Highlights

- **Global aggregates:** `global_count`, `global_min`, `global_max` for `DataFrame.select` (non-null `count`; Polars `min`/`max`); PySpark `functions.count` / `min` / `max` on typed columns.
- **Windows:** `lag` / `lead` (Polars `shift` + `.over(...)`); still no SQL-style `rowsBetween` / `rangeBetween` in the IR (see `INTERFACE_CONTRACT.md`).
- **Temporal:** `Expr.strptime` / `Expr.unix_timestamp`, PySpark `to_date(..., format=...)` and `unix_timestamp`; `dt_nanosecond` for `datetime` and `time`.
- **Maps / binary:** `Expr.map_len()`, `Expr.binary_len()` (byte length of `bytes` columns).

### Details

See [SUPPORTED_TYPES](../user-guide/supported-types.md), [PYSPARK_PARITY](../integrations/alternate-surfaces/pyspark-parity.md), and [ROADMAP](../project/roadmap.md).

## [0.6.0] — 2026-03-22

### Highlights

- **Scalar types:** `datetime.time`, `bytes`, and homogeneous `dict[str, T]` map columns (Polars-backed I/O; map execution surface is intentionally small).
- **Windows:** `row_number`, `rank`, `dense_rank`, `window_sum`, `window_mean` with `Window.partitionBy(...).orderBy(...)` / `.spec()`; Rust `ExprNode::Window` + Polars `.over(...)`.
- **Global aggregates (Phase D):** `DataFrame.select(...)` with `global_sum` / `global_mean` or PySpark `functions.sum` / `avg` / `mean` on typed columns — single-row results.
- **PySpark façade:** `dropDuplicates(subset=...)`, date helpers (`year`, `month`, …, `to_date`), documentation parity fixes (`union`, types).

### Development and testing

- **`[dev]` optional dependencies** include `numpy` (for `collect(as_numpy=...)` tests), `pytest-cov`, `coverage`, `pytest-xdist`, and `polars`.
- **CI** runs parallel `pytest` on Linux, optional **coverage** XML on Ubuntu + Python 3.11, installs **Polars** on that leg so `to_polars()` tests run, runs **`scripts/verify_doc_examples.py`**, and uses **`GITHUB_ACTIONS`-scaled** performance guardrails.

### Details

See [SUPPORTED_TYPES](../user-guide/supported-types.md), [PYSPARK_PARITY](../integrations/alternate-surfaces/pyspark-parity.md), and [ROADMAP](../project/roadmap.md).

## [0.5.0] — 2026

### Highlights

- **PydanTable** naming and docs alignment with the [ROADMAP](../project/roadmap.md) (0.5.x line).
- **Typed `DataFrameModel`** and **`DataFrame[Schema]`** with a Rust execution core (Polars-backed in the native extension).
- **Materialization:** `collect()` returns Pydantic row models; `to_dict()` / `collect(as_lists=True)` for columnar data; optional `to_polars()` with the `[polars]` extra.
- **Rich column types:** nested Pydantic models (structs), homogeneous `list[T]`, `uuid.UUID`, `decimal.Decimal`, `enum.Enum`, plus `explode`, `unnest`, and extended `Expr` helpers (see [SUPPORTED_TYPES](../user-guide/supported-types.md)).

### Details

For phase history and future direction, see [ROADMAP](../project/roadmap.md) and [POLARS_TRANSFORMATIONS_ROADMAP](../user-guide/polars-alignment/transformations-roadmap.md).
