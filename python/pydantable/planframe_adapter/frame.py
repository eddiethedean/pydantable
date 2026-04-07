from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from pydantable.planframe_adapter.errors import require_planframe

SchemaT = TypeVar("SchemaT")


@dataclass(frozen=True)
class PydantableFrame(Generic[SchemaT]):
    """
    Internal backing object for a PlanFrame-based pipeline.

    This is intentionally minimal at first; it will be expanded as we implement
    the adapter execution surface (select/filter/join/collect/etc).
    """

    plan: Any
    adapter: Any
    schema_type: type[SchemaT]

    @staticmethod
    def create(
        *, plan: Any, adapter: Any, schema_type: type[SchemaT]
    ) -> PydantableFrame[SchemaT]:
        require_planframe()
        return PydantableFrame(plan=plan, adapter=adapter, schema_type=schema_type)
