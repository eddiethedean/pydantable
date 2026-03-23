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
    Literal,
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


def _trusted_mode_from_legacy(
    *,
    validate_elements: bool | None,
    trusted_mode: Literal["off", "shape_only", "strict"] | None,
) -> Literal["off", "shape_only", "strict"]:
    if trusted_mode is None:
        if validate_elements is None:
            return "off"
        return "off" if validate_elements else "shape_only"
    if validate_elements is not None:
        if validate_elements and trusted_mode != "off":
            raise ValueError(
                "validate_elements=True conflicts with trusted_mode; "
                "use trusted_mode='off'."
            )
        if not validate_elements and trusted_mode == "off":
            raise ValueError(
                "validate_elements=False conflicts with trusted_mode='off'."
            )
    return trusted_mode


def _trusted_scalar_compatible(annotation: Any, value: Any) -> bool:
    inner, nullable = _annotation_nullable_inner(annotation)
    if value is None:
        return nullable
    origin = get_origin(inner)
    if origin is not None:
        return True
    if inner is Any:
        return True
    if isinstance(inner, type) and issubclass(inner, BaseModel):
        return isinstance(value, (Mapping, BaseModel))
    if isinstance(inner, type):
        return isinstance(value, inner)
    return True


def _trusted_column_has_nulls(col: Any) -> bool:
    if isinstance(col, (list, tuple)):
        return any(v is None for v in col)
    if hasattr(col, "null_count"):
        try:
            return int(col.null_count) > 0
        except Exception:
            pass
    typ = type(col)
    if typ.__module__ == "numpy" and typ.__name__ == "ndarray":
        return bool((col == None).any())  # noqa: E711
    return False


def _polars_integer_dtype_classes(pl: Any) -> frozenset[type]:
    names = (
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "Int128",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "UInt128",
    )
    return frozenset(c for n in names if (c := getattr(pl, n, None)) is not None)


def _polars_float_dtype_classes(pl: Any) -> frozenset[type]:
    names = ("Float32", "Float64")
    return frozenset(c for n in names if (c := getattr(pl, n, None)) is not None)


def _polars_scalar_dtype_matches(inner: Any, dt: Any, pl: Any) -> bool:
    if inner is int:
        return dt.__class__ in _polars_integer_dtype_classes(pl)
    if inner is float:
        return dt.__class__ in _polars_float_dtype_classes(pl)
    if inner is bool:
        return dt == pl.Boolean
    if inner is str:
        return dt == pl.Utf8 or dt == pl.String
    if inner is bytes:
        return dt == pl.Binary
    if inner is datetime:
        return isinstance(dt, pl.Datetime)
    if inner is date:
        return dt == pl.Date
    if inner is time:
        return dt == pl.Time
    if inner is timedelta:
        return isinstance(dt, pl.Duration)
    if inner is uuid.UUID:
        return dt == pl.Object
    if inner is Decimal:
        return isinstance(dt, pl.Decimal)
    if (
        isinstance(inner, type)
        and issubclass(inner, enum.Enum)
        and inner is not enum.Enum
    ):
        return dt.__class__ in _polars_integer_dtype_classes(pl)
    return True


def _polars_dtype_matches_annotation(dt: Any, annotation: Any) -> bool:
    """Check Polars dtype against a pydantable column annotation (strict trusted)."""
    try:
        import polars as pl
    except ImportError:
        return True

    inner, _nullable = _annotation_nullable_inner(annotation)
    origin = get_origin(inner)

    if origin is list:
        la = get_args(inner)
        if len(la) != 1:
            return False
        if not isinstance(dt, pl.List):
            return False
        return _polars_dtype_matches_annotation(dt.inner, la[0])

    if origin is dict:
        la = get_args(inner)
        if len(la) != 2 or la[0] is not str:
            return False
        val_ann = la[1]
        if isinstance(dt, pl.List):
            idt = dt.inner
            if isinstance(idt, pl.Struct):
                by_field = {f.name: f.dtype for f in idt.fields}
                if set(by_field.keys()) != {"key", "value"}:
                    return False
                if not _polars_dtype_matches_annotation(by_field["key"], str):
                    return False
                return _polars_dtype_matches_annotation(by_field["value"], val_ann)
        return False

    if isinstance(inner, type) and issubclass(inner, BaseModel):
        if not isinstance(dt, pl.Struct):
            return False
        by_name = {f.name: f.dtype for f in dt.fields}
        try:
            get_type_hints(inner, include_extras=True)
        except Exception:
            return False
        if set(by_name.keys()) != set(inner.model_fields.keys()):
            return False
        for fname, finfo in inner.model_fields.items():
            ann_f = finfo.annotation
            if ann_f is None:
                return False
            if not _polars_dtype_matches_annotation(by_name[fname], ann_f):
                return False
        return True

    if get_origin(inner) is not None:
        return False
    if inner is Any:
        return True
    return _polars_scalar_dtype_matches(inner, dt, pl)


def _trusted_nested_value_strict(annotation: Any, value: Any) -> bool:
    inner, nullable = _annotation_nullable_inner(annotation)
    if value is None:
        return nullable
    origin = get_origin(inner)
    if origin is list:
        la = get_args(inner)
        if len(la) != 1:
            return False
        if not isinstance(value, (list, tuple)):
            return False
        el_a = la[0]
        for item in value:
            if item is not None and not _trusted_nested_value_strict(el_a, item):
                return False
        return True
    if origin is dict:
        la = get_args(inner)
        if len(la) != 2 or la[0] is not str:
            return False
        if not isinstance(value, Mapping):
            return False
        val_a = la[1]
        for kk, vv in value.items():
            if not isinstance(kk, str):
                return False
            if vv is not None and not _trusted_nested_value_strict(val_a, vv):
                return False
        return True
    if isinstance(inner, type) and issubclass(inner, BaseModel):
        if not isinstance(value, Mapping):
            return False
        try:
            hints = get_type_hints(inner, include_extras=True)
        except Exception:
            return False
        keys = set(value.keys())
        if keys != set(inner.model_fields.keys()):
            return False
        for fname in inner.model_fields:
            fa = hints.get(fname)
            if fa is None:
                return False
            if not _trusted_nested_value_strict(fa, value[fname]):
                return False
        return True
    return _trusted_scalar_compatible(annotation, value)


def _trusted_column_strict_compatible(annotation: Any, col: Any) -> bool:
    inner, _nullable = _annotation_nullable_inner(annotation)
    origin = get_origin(inner)

    if isinstance(col, (list, tuple)):
        if origin is list or origin is dict:
            for v in col:
                if v is not None and not _trusted_nested_value_strict(annotation, v):
                    return False
            return True
        if isinstance(inner, type) and issubclass(inner, BaseModel):
            for v in col:
                if v is not None and not _trusted_nested_value_strict(annotation, v):
                    return False
            return True
        for v in col:
            if v is not None:
                return _trusted_scalar_compatible(annotation, v)
        return True
    typ = type(col)
    if typ.__module__ == "numpy" and typ.__name__ == "ndarray":
        dtype_obj = getattr(col, "dtype", None)
        kind = str(getattr(dtype_obj, "kind", ""))
        inner, _nullable = _annotation_nullable_inner(annotation)
        if inner in (int,):
            return kind in ("i", "u")
        if inner in (float,):
            return kind in ("i", "u", "f")
        if inner in (bool,):
            return kind == "b"
        if inner in (str,):
            return kind in ("U", "S", "O")
        return True
    mod = getattr(typ, "__module__", "") or ""
    if mod.startswith("pyarrow"):
        dt = str(getattr(col, "type", "")).lower()
        inner, _nullable = _annotation_nullable_inner(annotation)
        if inner in (int,):
            return "int" in dt or "uint" in dt
        if inner in (float,):
            return "float" in dt or "double" in dt or "int" in dt
        if inner in (bool,):
            return "bool" in dt
        if inner in (str,):
            return "string" in dt or "large_string" in dt
        return True
    return True


def validate_columns_strict(
    data: Mapping[str, Any] | Any,
    schema_type: type[BaseModel],
    *,
    validate_elements: bool | None = None,
    trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
    ignore_errors: bool = False,
    on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
) -> dict[str, Any] | Any:
    """
    Validate that `data` matches `schema_type` and return normalized columns.

    ``trusted_mode`` controls validation depth:
    - ``off``: full per-element validation.
    - ``shape_only``: schema/shape/nullability checks only.
    - ``strict``: shape checks plus dtype-compatibility checks (scalars; for Polars,
      nested ``list`` / ``dict[str, T]`` / struct columns vs annotations).

    ``validate_elements`` remains as a compatibility alias:
    ``True`` maps to ``trusted_mode='off'`` and ``False`` maps to
    ``trusted_mode='shape_only'``.

    With ``validate_elements=False``, a Polars ``DataFrame`` may be passed
    directly so the Rust engine can ingest it via Arrow IPC without per-cell
    Python materialization.
    """

    mode = _trusted_mode_from_legacy(
        validate_elements=validate_elements, trusted_mode=trusted_mode
    )

    if _is_polars_dataframe(data):
        if mode == "off":
            raise TypeError(
                "Passing a Polars DataFrame requires trusted ingest mode "
                "('shape_only' or 'strict')."
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
                pass
            elif int(polars_df.get_column(name).null_count()) > 0:
                raise ValueError(
                    f"Column '{name}' is non-nullable in schema "
                    "but contains null values."
                )
            if mode == "strict":
                series = polars_df.get_column(name)
                if not _polars_dtype_matches_annotation(series.dtype, annotation):
                    raise ValueError(
                        f"Column '{name}' is incompatible with schema annotation "
                        f"{annotation!r} in strict trusted mode."
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
        if mode == "off":
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

    if mode == "off" and ignore_errors:
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

    if mode == "off":
        for name, expected_type in field_types.items():
            adapter = TypeAdapter(expected_type)
            for v in normalized[name]:
                adapter.validate_python(v)
    else:
        for name, annotation in field_types.items():
            _, nullable = _annotation_nullable_inner(annotation)
            col = normalized[name]
            if not nullable and _trusted_column_has_nulls(col):
                raise ValueError(
                    f"Column '{name}' is non-nullable in schema "
                    "but contains null values."
                )
            if mode == "strict" and not _trusted_column_strict_compatible(
                annotation, col
            ):
                raise ValueError(
                    f"Column '{name}' is incompatible with schema annotation "
                    f"{annotation!r} in strict trusted mode."
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
