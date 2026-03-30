from __future__ import annotations

from enum import Enum

class PlanMaterialization(str, Enum):
    BLOCKING = "blocking"
    ASYNC = "async"
    DEFERRED = "deferred"
    CHUNKED = "chunked"

def plan_materialization_summary(kind: PlanMaterialization) -> str: ...
