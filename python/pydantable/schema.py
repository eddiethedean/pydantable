from __future__ import annotations

from typing import TYPE_CHECKING, Any

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


def validate_columns_strict(
    data: Mapping[str, Any], schema_type: type[BaseModel]
) -> dict[str, list[Any]]:
    """
    Validate that `data` matches `schema_type` and return normalized columns.

    This validates the column *values* at runtime (sufficient for early skeleton
    tests). For now, it validates each element using Pydantic `TypeAdapter`.
    """

    field_types = schema_field_types(schema_type)
    data_keys = set(data.keys())
    field_keys = set(field_types.keys())

    missing = sorted(field_keys - data_keys)
    extra = sorted(data_keys - field_keys)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    if extra:
        raise ValueError(f"Unknown columns for schema: {extra}")

    normalized: dict[str, list[Any]] = {}
    lengths = set()
    for name, expected_type in field_types.items():
        col = data[name]
        if not isinstance(col, (list, tuple)):
            raise TypeError(f"Column {name!r} must be a list/tuple")
        values = list(col)
        lengths.add(len(values))

        adapter = TypeAdapter(expected_type)
        for v in values:
            adapter.validate_python(v)

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
    derived = create_model(  # type: ignore[call-overload]
        f"{base_schema_type.__name__}Derived",
        __base__=Schema,
        **{name: (t, ...) for name, t in field_types.items()},
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
