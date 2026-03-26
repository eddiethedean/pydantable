from __future__ import annotations

from pydantable import DataFrame, Schema
from pydantable.observe import set_observer


class Row(Schema):
    x: int


def test_observer_receives_execute_plan_event() -> None:
    events: list[dict] = []

    def obs(e: dict) -> None:
        events.append(e)

    set_observer(obs)
    try:
        df = DataFrame[Row]({"x": [1, 2, 3]})
        _ = df.to_dict()
    finally:
        set_observer(None)

    assert any(e.get("op") == "execute_plan" for e in events)
    ev = next(e for e in events if e.get("op") == "execute_plan")
    assert "duration_ms" in ev
    assert ev.get("ok") is True

