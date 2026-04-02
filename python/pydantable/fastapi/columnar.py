"""OpenAPI-friendly Pydantic models for columnar JSON (``dict[str, list]``)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Literal, cast, get_origin

from pydantic import (
    AliasChoices,
    AliasPath,
    BaseModel,
    ConfigDict,
    Field,
    create_model,
    model_validator,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from pydantable.dataframe_model import DataFrameModel

_COLUMNAR_MODEL_CACHE: dict[tuple[int, str, str, bool, str], type[BaseModel]] = {}


def _column_list_annotation(cell_annotation: Any) -> Any:
    return list[cell_annotation]


def _example_list_for_annotation(annotation: Any) -> list[Any] | None:
    """Best-effort example list for one column cell annotation."""
    ann = annotation
    origin = get_origin(ann)
    if origin is not None:
        return None
    if ann is int:
        return [1, 2]
    if ann is float:
        return [1.0, 2.0]
    if ann is bool:
        return [True, False]
    if ann is str:
        return ["a", "b"]
    return None


def _merge_column_examples(*, finfo: Any, cell_annotation: Any) -> list[Any] | None:
    """
    Prefer explicit `Field(examples=[...])` values; otherwise use a small type-based
    default catalog.
    """
    ex = getattr(finfo, "examples", None)
    if isinstance(ex, (list, tuple)) and ex:
        return list(ex)
    return _example_list_for_annotation(cell_annotation)


def columnar_body_model(
    row_model: type[BaseModel],
    *,
    model_name: str | None = None,
    json_schema_extra: dict[str, Any] | None = None,
    example: dict[str, list[Any]] | None = None,
    generate_examples: bool = False,
    input_key_mode: Literal["python", "aliases", "both"] = "aliases",
) -> type[BaseModel]:
    """Return a Pydantic model describing columnar JSON for ``row_model``.

    Each row field ``name: T`` becomes ``name: list[T]`` (nested models become
    ``list[Nested]`` per index). Use as a FastAPI ``Body`` model or
    ``response_model`` for shapes matching
    :meth:`~pydantable.dataframe_model.DataFrameModel.to_dict`.

    Validation aliases on row fields (``Field(validation_alias=...)`` or ``alias=``)
    are preserved on the column fields so incoming JSON keys match your API contract;
    :meth:`~pydantic.main.BaseModel.model_dump` uses Python field names, matching
    :class:`~pydantable.dataframe_model.DataFrameModel` column keys.

    If ``example`` is set, it is merged into the JSON Schema (``example`` key) for
    OpenAPI.

    The same class is returned for the same ``row_model``, ``model_name``, and
    ``json_schema_extra`` (cached).
    """
    merged_extra: dict[str, Any] | None = (
        dict(json_schema_extra) if json_schema_extra else None
    )
    if example is not None:
        if merged_extra is None:
            merged_extra = {}
        merged_extra.setdefault("example", example)
    name = model_name or f"{row_model.__name__}ColumnarBody"
    extra_key = json.dumps(merged_extra, sort_keys=True) if merged_extra else ""
    key = (id(row_model), name, extra_key, bool(generate_examples), str(input_key_mode))
    cached = _COLUMNAR_MODEL_CACHE.get(key)
    if cached is not None:
        return cached

    field_defs: dict[str, Any] = {}
    alias_conflicts: dict[str, str] = {}
    for py_name, finfo in row_model.model_fields.items():
        ann: Any = finfo.annotation if finfo.annotation is not None else Any
        list_ann = _column_list_annotation(ann)
        desc = finfo.description
        examples = _merge_column_examples(finfo=finfo, cell_annotation=ann)
        if finfo.validation_alias is not None and input_key_mode != "python":
            validation_alias: Any
            if input_key_mode == "aliases":
                validation_alias = finfo.validation_alias
            else:
                if isinstance(finfo.validation_alias, (str, AliasPath)):
                    validation_alias = AliasChoices(py_name, finfo.validation_alias)
                else:
                    # Already a compound alias; preserve as-is.
                    validation_alias = finfo.validation_alias
                if isinstance(finfo.validation_alias, str):
                    alias_conflicts[py_name] = finfo.validation_alias
            field_defs[py_name] = (
                list_ann,
                Field(
                    ...,
                    validation_alias=validation_alias,
                    description=desc,
                    examples=examples if generate_examples else None,
                ),
            )
        else:
            field_defs[py_name] = (
                list_ann,
                Field(
                    ...,
                    description=desc,
                    examples=examples if generate_examples else None,
                ),
            )

    if merged_extra is not None:
        body_config = ConfigDict(json_schema_extra=merged_extra)
    else:
        body_config = ConfigDict()

    class _ColumnarBodyBase(BaseModel):
        __alias_conflicts__: dict[str, str] = {}

        @model_validator(mode="before")
        @classmethod
        def _check_alias_conflicts(cls, data: Any) -> Any:
            if not isinstance(data, dict):
                return data
            if not cls.__alias_conflicts__:
                return data
            keys = set(data.keys())
            for py_name, alias in cls.__alias_conflicts__.items():
                if py_name in keys and alias in keys:
                    raise ValueError(
                        "Columnar body includes both alias and python keys for the "
                        "same field."
                    )
            return data

    model_cls = create_model(  # type: ignore[call-overload]
        name,
        __base__=_ColumnarBodyBase,
        __config__=body_config,
        **field_defs,
    )
    model_cls.__alias_conflicts__ = alias_conflicts
    _COLUMNAR_MODEL_CACHE[key] = model_cls
    return model_cls


def columnar_body_model_from_dataframe_model(
    model_cls: type[DataFrameModel[Any]],
    *,
    model_name: str | None = None,
    json_schema_extra: dict[str, Any] | None = None,
    example: dict[str, list[Any]] | None = None,
    generate_examples: bool = False,
    input_key_mode: Literal["python", "aliases", "both"] = "aliases",
) -> type[BaseModel]:
    """Like :func:`columnar_body_model` but uses ``model_cls.RowModel``."""
    row_model: type[BaseModel] = model_cls.RowModel
    name = model_name or f"{model_cls.__name__}ColumnarBody"
    return columnar_body_model(
        row_model,
        model_name=name,
        json_schema_extra=json_schema_extra,
        example=example,
        generate_examples=generate_examples,
        input_key_mode=input_key_mode,
    )


def columnar_dependency(
    model_cls: type[DataFrameModel[Any]],
    *,
    trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
    fill_missing_optional: bool = True,
    ignore_errors: bool = False,
    validation_profile: str | None = None,
    json_schema_extra: dict[str, Any] | None = None,
    example: dict[str, list[Any]] | None = None,
    generate_examples: bool = False,
    input_key_mode: Literal["python", "aliases", "both"] = "aliases",
) -> Any:
    """Return a FastAPI dependency that parses columnar JSON into ``model_cls``.

    Use with ``Annotated[..., Depends(columnar_dependency(MyDF))]``.

    The request body is validated with :func:`columnar_body_model_from_dataframe_model`;
    :meth:`~pydantic.main.BaseModel.model_dump` yields ``dict[str, list]`` keys
    suitable for :class:`~pydantable.dataframe_model.DataFrameModel`.
    """
    Col = columnar_body_model_from_dataframe_model(
        model_cls,
        json_schema_extra=json_schema_extra,
        example=example,
        generate_examples=generate_examples,
        input_key_mode=input_key_mode,
    )

    def dep(body: Any) -> Any:
        data = body.model_dump()
        return model_cls(
            data,
            trusted_mode=trusted_mode,
            fill_missing_optional=fill_missing_optional,
            ignore_errors=ignore_errors,
            validation_profile=validation_profile,
        )

    dep.__annotations__ = {"body": Col, "return": model_cls}
    return dep


def rows_dependency(
    model_cls: type[DataFrameModel[Any]],
    *,
    trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
    fill_missing_optional: bool = True,
    ignore_errors: bool = False,
    validation_profile: str | None = None,
    on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
) -> Any:
    """Return a FastAPI dependency that parses a JSON array of rows into ``model_cls``.

    The parameter type is ``list[model_cls.RowModel]`` (validated per row).
    """
    row_model: type[BaseModel] = model_cls.RowModel

    def dep(rows: Any) -> Any:
        return cast(
            "Any",
            model_cls(
                rows,
                trusted_mode=trusted_mode,
                fill_missing_optional=fill_missing_optional,
                ignore_errors=ignore_errors,
                validation_profile=validation_profile,
                on_validation_errors=on_validation_errors,
            ),
        )

    # Dynamic annotation uses runtime ``row_model`` (not a static type alias).
    dep.__annotations__ = {"rows": list[row_model], "return": model_cls}  # type: ignore[valid-type]
    return dep
