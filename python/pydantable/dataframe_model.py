from __future__ import annotations

import sys
import typing
from typing import Any, Dict, Mapping, Sequence, Type, cast

from pydantic import BaseModel, create_model

from .dataframe import DataFrame
from .schema import Schema, schema_field_types


def _field_defs_from_annotations(annotations: Mapping[str, Any]) -> Dict[str, tuple[Any, Any]]:
    return {name: (dtype, ...) for name, dtype in annotations.items()}


def _normalize_input(
    *,
    data: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    row_model: Type[BaseModel],
) -> Dict[str, list[Any]]:
    expected_fields = list(row_model.model_fields.keys())

    if isinstance(data, Mapping):
        # Columnar input path; downstream DataFrame strict validation handles
        # required/extra keys, length, and value type checks.
        return {k: list(v) if isinstance(v, (list, tuple)) else v for k, v in data.items()}  # type: ignore[return-value]

    if isinstance(data, Sequence):
        rows = list(data)
        if not rows:
            return {name: [] for name in expected_fields}

        normalized_rows: list[BaseModel] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise TypeError("Row input must be a sequence of mapping objects.")
            normalized_rows.append(row_model.model_validate(row))

        columns: Dict[str, list[Any]] = {name: [] for name in expected_fields}
        for row in normalized_rows:
            row_dict = row.model_dump()
            for name in expected_fields:
                columns[name].append(row_dict[name])
        return columns

    raise TypeError("DataFrameModel input must be a column mapping or row sequence.")


class DataFrameModel:
    """
    FastAPI-friendly DataFrame container abstraction.

    - user defines fields on subclass annotations
    - class auto-generates a per-row `RowModel`
    - accepts both row-list and column-dict inputs
    - composes the existing `DataFrame[Schema]` engine
    """

    RowModel: Type[BaseModel]
    _SchemaModel: Type[Schema]
    _df: DataFrame[Any]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if cls.__name__ == "DataFrameModel":
            return

        module = sys.modules.get(cls.__module__)
        globalns = vars(module) if module is not None else {}
        eval_ns = dict(vars(typing))
        eval_ns.update(globalns)

        raw_annotations = dict(getattr(cls, "__dict__", {}).get("__annotations__", {}))
        annotations: Dict[str, Any] = {}
        for field_name, field_type in raw_annotations.items():
            if isinstance(field_type, str):
                annotations[field_name] = eval(field_type, eval_ns, eval_ns)
            else:
                annotations[field_name] = field_type
        if not annotations:
            raise TypeError("DataFrameModel subclasses must define annotated fields.")

        field_defs = _field_defs_from_annotations(annotations)
        cls.RowModel = create_model(  # type: ignore[call-arg]
            f"{cls.__name__}RowModel",
            __base__=Schema,
            **field_defs,
        )
        cls._SchemaModel = create_model(  # type: ignore[call-arg]
            f"{cls.__name__}Schema",
            __base__=Schema,
            **field_defs,
        )

    def __init__(self, data: Mapping[str, Any] | Sequence[Mapping[str, Any]], *, strict: bool = True):
        normalized = _normalize_input(data=data, row_model=self.RowModel)
        self._df = DataFrame[self._SchemaModel](normalized, strict=strict)

    @classmethod
    def _derived_model_type(cls, field_types: Mapping[str, Any]) -> Type["DataFrameModel"]:
        name = f"{cls.__name__}Derived"
        annotations = dict(field_types)
        derived = type(name, (DataFrameModel,), {"__annotations__": annotations})
        return cast(Type["DataFrameModel"], derived)

    @classmethod
    def _from_dataframe(cls, df: DataFrame[Any]) -> "DataFrameModel":
        derived_type = cls._derived_model_type(df.schema_fields())
        obj = derived_type.__new__(derived_type)
        obj._df = df
        return obj

    def schema_fields(self) -> Dict[str, Any]:
        return self._df.schema_fields()

    def collect(self) -> Dict[str, list[Any]]:
        return self._df.collect()

    def to_dict(self) -> Dict[str, list[Any]]:
        return self._df.to_dict()

    def select(self, *cols: Any) -> "DataFrameModel":
        return self._from_dataframe(self._df.select(*cols))

    def with_columns(self, **new_columns: Any) -> "DataFrameModel":
        return self._from_dataframe(self._df.with_columns(**new_columns))

    def filter(self, condition: Any) -> "DataFrameModel":
        return self._from_dataframe(self._df.filter(condition))

    def __getattr__(self, item: str) -> Any:
        # Delegate column refs + API methods to wrapped DataFrame.
        return getattr(self._df, item)

    @classmethod
    def row_model(cls) -> Type[BaseModel]:
        return cls.RowModel

    @classmethod
    def schema_model(cls) -> Type[Schema]:
        return cls._SchemaModel

