from __future__ import annotations

from typing import Any

from pydantable.planframe_adapter.errors import require_planframe


def execute_plan(*, adapter: Any, plan: Any, source: Any, schema: Any) -> Any:
    """
    Execute a PlanFrame plan via the adapter and return the backend *lazy frame*.

    For pydantable, the backend frame is a `pydantable.dataframe.DataFrame` which
    remains lazy; this function *does not materialize*.
    """

    require_planframe()
    from planframe.execution import execute_plan as _pf_execute_plan

    return _pf_execute_plan(adapter=adapter, plan=plan, root_data=source, schema=schema)


def execute_frame(frame: Any) -> Any:
    """
    Compile a PlanFrame `Frame` into the backend lazy frame.
    """

    require_planframe()
    adapter = getattr(frame, "_adapter", None)
    plan = getattr(frame, "_plan", None)
    data = getattr(frame, "_data", None)
    schema = getattr(frame, "_schema", None)
    if adapter is None or plan is None or data is None or schema is None:
        raise TypeError("Invalid PlanFrame Frame object.")
    return execute_plan(adapter=adapter, plan=plan, source=data, schema=schema)
