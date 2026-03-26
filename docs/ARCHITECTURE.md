# Architecture

This page is the **mental model** for how pydantable works in production.

## Core execution flow

```
flowchart LR
  userCode[UserCode] --> pythonApi[PythonAPI_DataFrame_DataFrameModel]
  pythonApi --> rustPlan[RustPlan_PyPlan]
  rustPlan --> polarsExec[PolarsExecutionInExtension]
  polarsExec --> materialize[MaterializeToPythonColumns]
  materialize --> outputs[Outputs_RowsOrDictOrArrowOrPolars]
```

## I/O vocabulary (lazy vs eager)

```
flowchart LR
  readRoots[read_*_aread_*] --> scanRoot[ScanFileRoot]
  scanRoot --> transforms[Transforms_filter_select_with_columns]
  transforms --> writeLazy[write_*]
  transforms --> materialize[to_dict_collect_to_arrow]

  eagerRead[materialize_*_fetch_sql_fetch_*_url] --> columnDict[Columns_dictStrList]
  columnDict --> constructor[DataFrameModel_or_DataFrameConstructor]
  columnDict --> export[export_*_write_sql]
```

## Validation timing

```
flowchart LR
  constructor[Constructor] -->|"trusted_mode_off_shape_only_strict"| ingestValidate[IngestValidationNow]
  lazyRead[read_*] --> scan[LazyScanRoot]
  scan --> plan[PlanTransforms]
  plan --> materialize2[Materialize]
  materialize2 --> validateLater[OptionalValidationAtMaterialization]
```

## Notes

- **`read_*`** returns a lazy scan root (`ScanFileRoot`) and defers ingest validation until materialization.\n+- **`materialize_*`** returns a Python column dict immediately and can be validated on construction.\n+- **Row order is not a stable guarantee** unless explicitly documented; compare on keys when testing (see {doc}`INTERFACE_CONTRACT`).\n+
