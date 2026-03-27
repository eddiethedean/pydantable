"""Schema base, dtype rules, and column validation; see :mod:`._impl`."""

from __future__ import annotations

from pydantable.schema._impl import (
    DtypeDriftWarning,
    Schema,
    _annotation_nullable_inner,
    _is_polars_dataframe,
    descriptor_matches_column_annotation,
    dtype_descriptor_to_annotation,
    is_supported_column_annotation,
    is_supported_scalar_column_annotation,
    make_derived_schema_type,
    merge_field_types_preserving_identity,
    previous_field_types_for_join,
    schema_field_types,
    schema_from_descriptors,
    validate_columns_strict,
    validate_dataframe_model_field_annotations,
)

__all__ = [
    "DtypeDriftWarning",
    "Schema",
    "_annotation_nullable_inner",
    "_is_polars_dataframe",
    "descriptor_matches_column_annotation",
    "dtype_descriptor_to_annotation",
    "is_supported_column_annotation",
    "is_supported_scalar_column_annotation",
    "make_derived_schema_type",
    "merge_field_types_preserving_identity",
    "previous_field_types_for_join",
    "schema_field_types",
    "schema_from_descriptors",
    "validate_columns_strict",
    "validate_dataframe_model_field_annotations",
]
