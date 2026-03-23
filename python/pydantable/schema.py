"""Pydantic schema base, column dtype rules, and validation for :class:`DataFrame`.

Functions such as :func:`is_supported_column_annotation` define allowed field types.
:func:`validate_columns_strict` normalizes constructor input against a schema model.
"""

from __future__ import annotations

import enum
import types
import uuid
from collections.abc import Callable, Mapping
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import (
    Annotated,
    Any,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

from pydantic import BaseModel, ConfigDict, TypeAdapter, ValidationError, create_model

_NoneType = type(None)
_SUPPORTED_NON_NULL_SCALAR_TYPES = frozenset(
    {int, float, bool, str, bytes, uuid.UUID, Decimal, datetime, date, time, timedelta}
)


def _unwrap_annotated(annotation: Any) -> Any:
    """Strip ``Annotated[T, ...]`` wrappers to the inner ``T``."""
    ann = annotation
    origin = get_origin(ann)
    while origin is Annotated:
        ann = get_args(ann)[0]
        origin = get_origin(ann)
    return ann


def _is_supported_non_null_scalar_type(tp: Any) -> bool:
    """True if ``tp`` is one allowed non-null scalar or enum (before ``| None``)."""
    if tp in _SUPPORTED_NON_NULL_SCALAR_TYPES:
        return True
    return isinstance(tp, type) and issubclass(tp, enum.Enum) and tp is not enum.Enum


def is_supported_scalar_column_annotation(annotation: Any) -> bool:
    """
    Return True if ``annotation`` is a **scalar** pydantable column dtype (no nested
    ``BaseModel`` columns). For recursive validation including struct columns, use
    [`is_supported_column_annotation`][pydantable.schema.is_supported_column_annotation].
    """
    ann = _unwrap_annotated(annotation)
    if ann is Any:
        return False
    origin = get_origin(ann)
    if origin is Union or origin is types.UnionType:
        args = [a for a in get_args(ann) if a is not _NoneType]
        if len(args) != 1:
            return False
        inner = args[0]
        inner = _unwrap_annotated(inner)
        if get_origin(inner) is not None:
            return False
        return _is_supported_non_null_scalar_type(inner)
    if origin is not None:
        return False
    if isinstance(ann, type) and issubclass(ann, BaseModel):
        return False
    return _is_supported_non_null_scalar_type(ann)


def is_supported_column_annotation(annotation: Any) -> bool:
    """
    Return True if ``annotation`` is a pydantable column dtype: a supported scalar,
    ``Optional`` / ``| None`` around one of those, or a nested Pydantic ``BaseModel``
    whose fields (recursively) use only supported column annotations.

    For nested models, field types are resolved with :func:`typing.get_type_hints`
    so string forward references work when resolvable from the model's module.
    """
    return _is_supported_column_annotation_inner(annotation, _model_stack=set())


def _is_supported_column_annotation_inner(
    annotation: Any, *, _model_stack: set[type]
) -> bool:
    ann = _unwrap_annotated(annotation)
    if ann is Any:
        return False
    origin = get_origin(ann)
    if origin is Union or origin is types.UnionType:
        args = [a for a in get_args(ann) if a is not _NoneType]
        if len(args) != 1:
            return False
        return _is_supported_column_annotation_inner(
            _unwrap_annotated(args[0]), _model_stack=_model_stack
        )
    if origin is list:
        list_args = get_args(ann)
        if len(list_args) != 1:
            return False
        return _is_supported_column_annotation_inner(
            _unwrap_annotated(list_args[0]), _model_stack=_model_stack
        )
    if origin is dict:
        dict_args = get_args(ann)
        if len(dict_args) != 2:
            return False
        key_t, val_t = dict_args
        if key_t is not str:
            return False
        return _is_supported_column_annotation_inner(val_t, _model_stack=_model_stack)
    if origin is not None:
        return False
    if isinstance(ann, type) and issubclass(ann, BaseModel):
        if ann in _model_stack:
            return True
        stack = set(_model_stack)
        stack.add(ann)
        try:
            hints = get_type_hints(ann, include_extras=True)
        except Exception:
            return False
        for fname in ann.model_fields:
            fa = hints.get(fname)
            if fa is None:
                return False
            if not _is_supported_column_annotation_inner(fa, _model_stack=stack):
                return False
        return True
    return _is_supported_non_null_scalar_type(ann)


def validate_dataframe_model_field_annotations(
    model_name: str, annotations: dict[str, Any]
) -> None:
    """
    Ensure every field annotation is a supported column dtype (scalar, optional
    scalar, or nested Pydantic models with supported fields).

    Raises ``TypeError`` with a user-facing message when a ``DataFrameModel`` subclass
    is defined with an unsupported column type (e.g. ``dict[...]``, bare ``list``).
    """
    for field_name, field_type in annotations.items():
        if not is_supported_column_annotation(field_type):
            raise TypeError(
                f"DataFrameModel {model_name!r} field {field_name!r} has unsupported "
                f"type {field_type!r}. Column types must be pydantable dtypes: scalars "
                "(int, float, bool, str, datetime, date, timedelta) or Optional[T] / "
                "T | None over those, homogeneous ``list[T]`` / ``List[T]`` over those "
                "types, or nested Pydantic models whose fields are also supported. "
                "See docs/SUPPORTED_TYPES.md."
            )


class Schema(BaseModel):
    """Base model for ``DataFrame[YourSchema]`` column definitions.

    Uses ``extra="forbid"`` so unexpected fields fail validation at construction.
    """

    model_config = ConfigDict(extra="forbid")


def schema_field_types(schema_type: type[BaseModel]) -> dict[str, Any]:
    """Map field names to annotations from a Pydantic model (v2 ``model_fields``).

    Optional / ``| None`` wrappers are preserved so nullability flows into derived
    schemas and expression typing. Element validation uses Pydantic adapters.
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
    ignore_errors: bool = False,
    on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
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
        polars_df = cast("Any", data)
        cols = {str(c) for c in polars_df.columns}
        field_keys = set(field_types.keys())
        missing = sorted(field_keys - cols)
        extra = sorted(cols - field_keys)
        if missing or extra:
            expected = sorted(field_keys)
            raise ValueError(
                "Polars DataFrame columns must match schema columns exactly; "
                f"expected={expected}, missing={missing}, extra={extra}"
            )
        for name, annotation in field_types.items():
            _, nullable = _annotation_nullable_inner(annotation)
            if nullable:
                continue
            null_count = int(polars_df.get_column(name).null_count())
            if null_count > 0:
                raise ValueError(
                    f"Column '{name}' is non-nullable in schema "
                    "but contains null values."
                )
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
    for name, _expected_type in field_types.items():
        col = data[name]
        if validate_elements:
            values = _sequence_column_to_list(name, col)
            lengths.add(len(values))
        else:
            values = _column_buffer_for_trusted(name, col)
            lengths.add(len(values))

        normalized[name] = values

    if len(lengths) != 1:
        raise ValueError(
            f"All columns must have the same length; got {sorted(lengths)}"
        )

    if validate_elements and ignore_errors:
        n_rows = next(iter(lengths), 0)
        adapters = {name: TypeAdapter(field_types[name]) for name in field_types}
        valid_rows: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        for i in range(n_rows):
            row = {name: normalized[name][i] for name in field_types}
            typed_row: dict[str, Any] = {}
            row_errors: list[Any] = []
            for name, adapter in adapters.items():
                try:
                    typed_row[name] = adapter.validate_python(row[name])
                except ValidationError as exc:
                    row_errors.extend(exc.errors())
                except Exception as exc:  # pragma: no cover - defensive adapter path
                    row_errors.append(
                        {
                            "type": "validation_error",
                            "loc": (name,),
                            "msg": str(exc),
                            "input": row[name],
                        }
                    )
            if row_errors:
                failures.append({"row_index": i, "row": row, "errors": row_errors})
            else:
                valid_rows.append(typed_row)
        if failures and on_validation_errors is not None:
            on_validation_errors(failures)
        out: dict[str, list[Any]] = {name: [] for name in field_types}
        for row in valid_rows:
            for name in field_types:
                out[name].append(row[name])
        return out

    if validate_elements:
        for name, expected_type in field_types.items():
            adapter = TypeAdapter(expected_type)
            for v in normalized[name]:
                adapter.validate_python(v)

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

    Descriptor formats:
    - Scalar: ``{"base": "int" | ... | "unknown", "nullable": bool}``
    - Struct: ``{"kind": "struct", "nullable": bool, "fields": [...]}``
    - List: ``{"kind": "list", "nullable": bool, "inner": <descriptor>}``
    - Map: ``{"kind": "map", "nullable": bool, "value": <descriptor>}``
      for ``dict[str, V]``
    """
    if descriptor.get("kind") == "map":
        nullable = bool(descriptor.get("nullable", False))
        inner = descriptor.get("value")
        if not isinstance(inner, Mapping):
            raise TypeError(
                "Invalid map dtype descriptor (expected mapping 'value'): "
                f"{descriptor!r}"
            )
        val_ann = dtype_descriptor_to_annotation(inner)
        map_ann = dict[str, val_ann]  # type: ignore[valid-type,misc]
        if nullable:
            return map_ann | None
        return map_ann
    if descriptor.get("kind") == "list":
        nullable = bool(descriptor.get("nullable", False))
        inner = descriptor.get("inner")
        if not isinstance(inner, Mapping):
            raise TypeError(
                "Invalid list dtype descriptor (expected mapping 'inner'): "
                f"{descriptor!r}"
            )
        inner_ann = dtype_descriptor_to_annotation(inner)
        list_ann = list[inner_ann]  # type: ignore[valid-type]
        if nullable:
            return list_ann | None
        return list_ann
    if descriptor.get("kind") == "struct":
        nullable = bool(descriptor.get("nullable", False))
        fields_raw = descriptor.get("fields")
        if not isinstance(fields_raw, (list, tuple)):
            raise TypeError(
                "Invalid struct dtype descriptor (expected 'fields' list): "
                f"{descriptor!r}"
            )
        field_definitions: dict[str, Any] = {}
        for fe in fields_raw:
            if not isinstance(fe, Mapping):
                raise TypeError(f"Invalid struct field entry: {fe!r}")
            name = fe.get("name")
            sub = fe.get("dtype")
            if not isinstance(name, str):
                raise TypeError(f"Invalid struct field name: {name!r}")
            if not isinstance(sub, Mapping):
                raise TypeError(f"Invalid struct field dtype for {name!r}: {sub!r}")
            field_definitions[name] = (dtype_descriptor_to_annotation(sub), ...)
        model_name = f"PydantableStruct_{id(descriptor):x}"
        nested = create_model(
            model_name,
            __base__=Schema,
            **cast("Any", field_definitions),
        )
        if nullable:
            return nested | None
        return nested

    base = descriptor.get("base")
    nullable = bool(descriptor.get("nullable", False))

    base_map: dict[str, Any] = {
        "int": int,
        "float": float,
        "bool": bool,
        "str": str,
        "enum": Any,
        "uuid": uuid.UUID,
        "decimal": Decimal,
        "datetime": datetime,
        "date": date,
        "duration": timedelta,
        "time": time,
        "binary": bytes,
        "unknown": Any,
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


def _annotation_nullable_inner(annotation: Any) -> tuple[Any, bool]:
    """Strip ``Annotated`` / optional union; return ``(inner, nullable)``."""
    ann = _unwrap_annotated(annotation)
    origin = get_origin(ann)
    if origin is Union or origin is types.UnionType:
        args = get_args(ann)
        non_none = tuple(a for a in args if a is not _NoneType)
        if _NoneType in args and len(non_none) == 1:
            return _unwrap_annotated(non_none[0]), True
    return ann, False


_RUST_BASE_FOR_PY_SCALAR: dict[type, str] = {
    int: "int",
    float: "float",
    bool: "bool",
    str: "str",
    uuid.UUID: "uuid",
    Decimal: "decimal",
    datetime: "datetime",
    date: "date",
    timedelta: "duration",
    time: "time",
    bytes: "binary",
}


def descriptor_matches_column_annotation(
    descriptor: Mapping[str, Any], annotation: Any
) -> bool:
    """
    Return True if a Rust dtype descriptor describes the same shape as ``annotation``
    (scalar, optional scalar, nested ``BaseModel``, or optional nested model).
    """
    if not isinstance(descriptor, Mapping):
        return False
    inner, nullable = _annotation_nullable_inner(annotation)
    if bool(descriptor.get("nullable", False)) != nullable:
        return False

    if descriptor.get("kind") == "struct":
        if not isinstance(inner, type) or not issubclass(inner, BaseModel):
            return False
        fields_raw = descriptor.get("fields")
        if not isinstance(fields_raw, (list, tuple)):
            return False
        by_name: dict[str, Mapping[str, Any]] = {}
        for fe in fields_raw:
            if not isinstance(fe, Mapping):
                return False
            n = fe.get("name")
            sub = fe.get("dtype")
            if not isinstance(n, str) or not isinstance(sub, Mapping):
                return False
            by_name[n] = sub
        mf = inner.model_fields
        if set(by_name.keys()) != set(mf.keys()):
            return False
        for fname, finfo in mf.items():
            ann_f = finfo.annotation
            if ann_f is None:
                return False
            if not descriptor_matches_column_annotation(by_name[fname], ann_f):
                return False
        return True

    if descriptor.get("kind") == "list":
        inner_d = descriptor.get("inner")
        if not isinstance(inner_d, Mapping):
            return False
        origin = get_origin(inner)
        if origin is not list:
            return False
        la = get_args(inner)
        if len(la) != 1:
            return False
        return descriptor_matches_column_annotation(inner_d, la[0])

    if descriptor.get("kind") == "map":
        val_d = descriptor.get("value")
        if not isinstance(val_d, Mapping):
            return False
        origin = get_origin(inner)
        if origin is not dict:
            return False
        la = get_args(inner)
        if len(la) != 2 or la[0] is not str:
            return False
        return descriptor_matches_column_annotation(val_d, la[1])

    exp_base = descriptor.get("base")
    if not isinstance(exp_base, str):
        return False
    if get_origin(inner) is not None:
        return False
    if isinstance(inner, type) and issubclass(inner, BaseModel):
        return False
    if exp_base == "unknown":
        return inner is Any
    if exp_base == "enum":
        return (
            isinstance(inner, type)
            and issubclass(inner, enum.Enum)
            and inner is not enum.Enum
        )
    expected_py = None
    for py_t, rust_s in _RUST_BASE_FOR_PY_SCALAR.items():
        if rust_s == exp_base:
            expected_py = py_t
            break
    if expected_py is None:
        return False
    return inner is expected_py


def merge_field_types_preserving_identity(
    previous: Mapping[str, Any],
    descriptors: Mapping[str, Mapping[str, Any]],
    derived_types: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Where a column name existed in the previous schema and the new Rust descriptor
    matches that annotation, keep the **original** Python type (including user nested
    ``BaseModel`` classes). Otherwise use ``derived_types`` from
    :func:`schema_from_descriptors`.
    """
    out: dict[str, Any] = {}
    for name, dt in derived_types.items():
        desc = descriptors.get(name)
        prev_ann = previous.get(name)
        if (
            prev_ann is not None
            and isinstance(desc, Mapping)
            and descriptor_matches_column_annotation(desc, prev_ann)
        ):
            out[name] = prev_ann
        else:
            out[name] = dt
    return out


def previous_field_types_for_join(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    *,
    suffix: str,
    output_columns: list[str],
) -> dict[str, Any]:
    """
    Map join output column names to pre-join Python annotations (left wins on bare
    name; right non-key collisions appear as ``stem + suffix``).
    """
    prev: dict[str, Any] = {}
    left_names = set(left.keys())
    for name in output_columns:
        if name in left:
            prev[name] = left[name]
        elif name in right:
            prev[name] = right[name]
        elif suffix and name.endswith(suffix):
            stem = name[: -len(suffix)]
            if stem in right and stem in left_names:
                prev[name] = right[stem]
    return prev
