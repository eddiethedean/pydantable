import typing as _t
from enum import StrEnum

import pydantic


Record = dict | pydantic.BaseModel
Json = str | bytes | bytearray


class ErrorOption(StrEnum):
    RETURN = 'return'
    RAISE = 'raise'
    SKIP = 'skip'


class RecordValidationResult(_t.NamedTuple):
    error: pydantic.ValidationError | None
    result: pydantic.BaseModel | None
    value: Record


class JsonValidationResult(_t.NamedTuple):
    error: pydantic.ValidationError | None
    result: pydantic.BaseModel | None
    value: Json


def validate_record(
    record: Record,
    model: pydantic.BaseModel,
    *,
    from_attributes: bool | None = None,
    strict: bool | None = None,
    raise_errors: bool = False
) -> RecordValidationResult:
    try:
        validated_record: pydantic.BaseModel = model.model_validate(
            record, from_attributes=from_attributes, strict=strict
        )
    except pydantic.ValidationError as e:
        if raise_errors:
            raise e
        return RecordValidationResult(e, None, record)
    return RecordValidationResult(None, validated_record, record)


def validate_records(
    records: _t.Iterator[Record],
    model: pydantic.BaseModel,
    *,
    from_attributes: bool | None = None,
    strict: bool | None = None,
    error_option: ErrorOption = ErrorOption.RETURN
) -> _t.Generator[RecordValidationResult, None, None]:
    for record in records:
        result: RecordValidationResult = validate_record(
            record, model,
            from_attributes=from_attributes,
            strict=strict,
            raise_errors=error_option == ErrorOption.RAISE
        )
        if result.error and error_option == ErrorOption.SKIP:
            continue
        yield result


def validate_json(
    json: Json,
    model: pydantic.BaseModel,
    *,
    strict: bool | None = None,
    raise_errors: bool = False
) -> JsonValidationResult:
    try:
        validated_record: pydantic.BaseModel = model.model_validate_json(
            json, strict=strict
        )
    except pydantic.ValidationError as e:
        if raise_errors:
            raise e
        return JsonValidationResult(e, None, json)
    return JsonValidationResult(None, validated_record, json)


def validate_jsons(
    records: _t.Iterator[Json],
    model: pydantic.BaseModel,
    *,
    strict: bool | None = None,
    error_option: ErrorOption = ErrorOption.RETURN
) -> _t.Generator[JsonValidationResult, None, None]:
    for record in records:
        result: JsonValidationResult = validate_json(
            record, model,
            strict=strict,
            raise_errors=error_option == ErrorOption.RAISE
        )
        if result.error and error_option == ErrorOption.SKIP:
            continue
        yield result


def validate(
    records: _t.Iterator[Record | Json],
    model: pydantic.BaseModel,
    *,
    from_attributes: bool | None = None,
    strict: bool | None = None,
    error_option: ErrorOption = ErrorOption.RETURN
) -> _t.Generator[RecordValidationResult | JsonValidationResult, None, None]:
    result: RecordValidationResult | JsonValidationResult
    for record in records:
        if isinstance(record, Json):
            result = validate_json(
                record, model,
                strict=strict,
                raise_errors=error_option == ErrorOption.RAISE
            )
        else:
            result = validate_record(
                record, model,
                from_attributes=from_attributes,
                strict=strict,
                raise_errors=error_option == ErrorOption.RAISE
            )
        if result.error and error_option == ErrorOption.SKIP:
            continue
        yield result