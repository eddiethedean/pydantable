from __future__ import annotations

from enum import Enum

class PlanMaterialization(str, Enum):
    BLOCKING: str
    ASYNC: str
    DEFERRED: str
    CHUNKED: str

def plan_materialization_summary(kind: PlanMaterialization) -> str: ...
