from typing import Any
import pydantic_core
import pydantic 


def add_column(
    Model: pydantic.BaseModel,
    column_name: str,
    annotation: type,
    default=pydantic_core.PydanticUndefined
) -> type[pydantic.BaseModel]:
    cols: dict[str, tuple[type[Any] | None, Any]] = {
        name: (info.annotation, info.default) for
           name, info in Model.model_fields.items()
    }
    cols[column_name] = (annotation, default)
    return pydantic.create_model(
        Model.__name__,
        **cols # type: ignore
    ) # type: ignore


def drop_column(
    Model: pydantic.BaseModel,
    column_name: str
) -> type[pydantic.BaseModel]:
    cols: dict[str, tuple[type[Any] | None, Any]] = {
        name: (info.annotation, info.default) for
           name, info in Model.model_fields.items() if name != column_name
    }
    return pydantic.create_model(
        Model.__name__,
        **cols # type: ignore
    ) # type: ignore