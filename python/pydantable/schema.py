from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

from pydantic import BaseModel, ConfigDict, TypeAdapter, create_model

if TYPE_CHECKING:
    from collections.abc import Mapping


class Schema(BaseModel):
    """
    Base class for Pydantic-backed schemas used by `DataFrame[SchemaType]`.

    We keep `extra="forbid"` so runtime schema enforcement is strict by default.
    """

    model_config = ConfigDict(extra="forbid")


def schema_field_types(schema_type: type[BaseModel]) -> dict[str, Any]:
    """
    Extract `field_name -> python_type` from a Pydantic model.

    Notes:
    - We preserve Optional[T] / Union[T, None] in the returned annotation types so
      expression typing can propagate nullability into derived schemas.
    - This skeleton intentionally supports only a small set of expression dtypes;
      runtime validation of DataFrame column values is delegated to Pydantic.
    """

    # Pydantic v2: `model_fields` holds `FieldInfo` with `.annotation`.
    field_types: dict[str, Any] = {}
    for name, field in schema_type.model_fields.items():
        annotation = field.annotation
        field_types[name] = annotation
    return field_types


def _sequence_column_to_list(name: str, col: Any) -> list[Any]:
    """Normalize list/tuple/numpy.ndarray to a Python list."""
    if isinstance(col, (list, tuple)):
        return list(col)
    typ = type(col)
    if typ.__module__ == "numpy" and typ.__name__ == "ndarray":
        return col.tolist()
    raise TypeError(
        f"Column {name!r} must be a list, tuple, or numpy.ndarray (got {typ!r})."
    )


def _is_polars_dataframe(obj: Any) -> bool:
    t = type(obj)
    mod = getattr(t, "__module__", "") or ""
    return mod.startswith("polars") and t.__name__ == "DataFrame"


def _column_buffer_for_trusted(name: str, col: Any) -> Any:
    """
    Normalize column inputs when skipping per-element validation.

    Keeps numpy ndarray or pyarrow Array/ChunkedArray buffers for a Rust fast path;
    list/tuple are copied to ``list``.
    """
    if isinstance(col, (list, tuple)):
        return list(col)
    typ = type(col)
    if typ.__module__ == "numpy" and typ.__name__ == "ndarray":
        return col
    mod = getattr(typ, "__module__", "") or ""
    if mod.startswith("pyarrow") and typ.__name__ in ("Array", "ChunkedArray"):
        return col
    raise TypeError(
        f"Column {name!r} must be a list, tuple, numpy.ndarray, or pyarrow "
        f"Array/ChunkedArray (got {typ!r})."
    )


def validate_columns_strict(
    data: Mapping[str, Any] | Any,
    schema_type: type[BaseModel],
    *,
    validate_elements: bool = True,
) -> dict[str, Any] | Any:
    """
    Validate that `data` matches `schema_type` and return normalized columns.

    When ``validate_elements`` is True (default), each element is validated with
    Pydantic ``TypeAdapter``. Set to False for trusted bulk inputs (keys and row
    lengths are still checked; numpy columns are converted via ``tolist()``).

    With ``validate_elements=False``, a Polars ``DataFrame`` may be passed
    directly so the Rust engine can ingest it via Arrow IPC without per-cell
    Python materialization.
    """

    if _is_polars_dataframe(data):
        if validate_elements:
            raise TypeError(
                "Passing a Polars DataFrame requires validate_data=False "
                "(per-element validation is skipped for columnar buffers)."
            )
        field_types = schema_field_types(schema_type)
        cols = {str(c) for c in data.columns}
        field_keys = set(field_types.keys())
        missing = sorted(field_keys - cols)
        extra = sorted(cols - field_keys)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        if extra:
            raise ValueError(f"Unknown columns for schema: {extra}")
        return data

    field_types = schema_field_types(schema_type)
    data_keys = set(data.keys())
    field_keys = set(field_types.keys())

    missing = sorted(field_keys - data_keys)
    extra = sorted(data_keys - field_keys)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    if extra:
        raise ValueError(f"Unknown columns for schema: {extra}")

    normalized: dict[str, Any] = {}
    lengths = set()
    for name, expected_type in field_types.items():
        col = data[name]
        if validate_elements:
            values = _sequence_column_to_list(name, col)
            lengths.add(len(values))
            adapter = TypeAdapter(expected_type)
            for v in values:
                adapter.validate_python(v)
        else:
            values = _column_buffer_for_trusted(name, col)
            lengths.add(len(values))

        normalized[name] = values

    if len(lengths) != 1:
        raise ValueError(
            f"All columns must have the same length; got {sorted(lengths)}"
        )

    return normalized


def make_derived_schema_type(
    base_schema_type: type[BaseModel], field_types: Mapping[str, Any]
) -> type[BaseModel]:
    """
    Create a new Pydantic model type for derived DataFrames.
    """

    # Make derived schemas strict by inheriting from our `Schema` base.
    # If the user passed a custom BaseModel subclass, we still want strict
    # `extra="forbid"` behavior.
    # `create_model` stubs expose many keyword-only params (`__config__`, …). Unpacking
    # `**{ name: (type, ...) }` makes Pyright match each tuple against those params.
    # Widen the mapping for `**` so values are treated as dynamic field definitions.
    field_definitions: dict[str, Any] = {
        name: (t, ...) for name, t in field_types.items()
    }
    derived = create_model(
        f"{base_schema_type.__name__}Derived",
        __base__=Schema,
        **cast("Any", field_definitions),
    )
    return derived


def dtype_descriptor_to_annotation(descriptor: Mapping[str, Any]) -> Any:
    """
    Convert a Rust dtype descriptor into a Python type annotation.

    Descriptor format:
    - {"base": "int" | "float" | "bool" | "str", "nullable": bool}
    """
    base = descriptor.get("base")
    nullable = bool(descriptor.get("nullable", False))

    base_map: dict[str, Any] = {
        "int": int,
        "float": float,
        "bool": bool,
        "str": str,
        "datetime": datetime,
        "date": date,
        "duration": timedelta,
    }
    if base not in base_map:
        raise TypeError(f"Unsupported Rust dtype descriptor base: {base!r}")

    py_t = base_map[base]
    if nullable:
        return py_t | None
    return py_t


def schema_from_descriptors(
    descriptors: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """
    Convert rust plan schema descriptors into field annotations map.
    """
    return {name: dtype_descriptor_to_annotation(d) for name, d in descriptors.items()}
