from __future__ import annotations

from typing import Any, Dict, Mapping, Type

from pydantic import BaseModel, ConfigDict, TypeAdapter, create_model


class Schema(BaseModel):
    """
    Base class for Pydantic-backed schemas used by `DataFrame[SchemaType]`.

    We keep `extra="forbid"` so runtime schema enforcement is strict by default.
    """

    model_config = ConfigDict(extra="forbid")


def schema_field_types(schema_type: Type[BaseModel]) -> Dict[str, Any]:
    """
    Extract `field_name -> python_type` from a Pydantic model.

    Notes:
    - We preserve Optional[T] / Union[T, None] in the returned annotation types so
      expression typing can propagate nullability into derived schemas.
    - This skeleton intentionally supports only a small set of expression dtypes;
      runtime validation of DataFrame column values is delegated to Pydantic.
    """

    # Pydantic v2: `model_fields` holds `FieldInfo` with `.annotation`.
    field_types: Dict[str, Any] = {}
    for name, field in schema_type.model_fields.items():
        annotation = field.annotation
        field_types[name] = annotation
    return field_types


def validate_columns_strict(
    data: Mapping[str, Any], schema_type: Type[BaseModel]
) -> Dict[str, list[Any]]:
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

    normalized: Dict[str, list[Any]] = {}
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
        raise ValueError(f"All columns must have the same length; got {sorted(lengths)}")

    return normalized


def make_derived_schema_type(
    base_schema_type: Type[BaseModel], field_types: Mapping[str, Any]
) -> Type[BaseModel]:
    """
    Create a new Pydantic model type for derived DataFrames.
    """

    # Make derived schemas strict by inheriting from our `Schema` base.
    # If the user passed a custom BaseModel subclass, we still want strict
    # `extra="forbid"` behavior.
    derived = create_model(  # type: ignore[call-arg]
        f"{base_schema_type.__name__}Derived",
        __base__=Schema,
        **{name: (t, ...) for name, t in field_types.items()},
    )
    return derived

