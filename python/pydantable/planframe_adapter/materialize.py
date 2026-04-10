from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast

from pydantable.planframe_adapter.errors import require_planframe

if TYPE_CHECKING:
    from pydantable.dataframe_model import DataFrameModel

AfterModelT = TypeVar("AfterModelT", bound="DataFrameModel[Any]")


def materialize_dataframe_model(
    frame: Any,
    model: type[AfterModelT],
    *,
    streaming: bool | None = None,
    engine_streaming: bool | None = None,
    trusted_mode: Literal["off", "shape_only", "strict"] | None = "shape_only",
    fill_missing_optional: bool = True,
    ignore_errors: bool = False,
    on_validation_errors: Callable[[list[dict[str, Any]]], None] | None = None,
) -> AfterModelT:
    """
    Materialize a PlanFrame ``Frame`` into a concrete pydantable ``DataFrameModel``.

    This is the supported boundary recipe for PlanFrame-first typing chains:

    - PlanFrame chain: ``pf_out = df.planframe....`` (typed by PlanFrame stubs)
    - Boundary: ``materialize_dataframe_model(pf_out, AfterModel)``

    Validation semantics live on the pydantable constructor; this helper routes
    execution-time hints through PlanFrame's ``ExecutionOptions``.
    """

    require_planframe()
    from planframe.execution import ExecutionOptions

    from pydantable.dataframe_model import DataFrameModel

    if not isinstance(model, type) or not issubclass(model, DataFrameModel):
        raise TypeError("model must be a DataFrameModel subclass.")

    opts = ExecutionOptions(streaming=streaming, engine_streaming=engine_streaming)
    cols = frame.to_dict(options=opts)
    return model(
        cols,
        trusted_mode=trusted_mode,
        fill_missing_optional=fill_missing_optional,
        ignore_errors=ignore_errors,
        on_validation_errors=cast("Any", on_validation_errors),
    )

