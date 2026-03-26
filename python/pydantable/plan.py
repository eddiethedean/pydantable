"""Stable plan introspection helpers for `DataFrame.explain()`."""

from __future__ import annotations

from typing import Any, Literal


def render_plan_text(plan: dict[str, Any]) -> str:
    """
    Render a compact, stable string for a plan dict.

    The `plan` dict is expected to match `pydantable._core.PyPlan.to_serializable()`,
    optionally with extra top-level keys added by the Python layer.
    """
    version = plan.get("version", "?")
    streaming = plan.get("engine_streaming")
    root_kind = plan.get("root_data_kind")

    lines: list[str] = [f"Plan(version={version})"]
    if streaming is not None:
        lines.append(f"  engine_streaming: {streaming}")
    if root_kind is not None:
        lines.append(f"  root_data: {root_kind}")

    steps = plan.get("steps", [])
    lines.append(f"  steps: {len(steps)}")
    for i, s in enumerate(steps):
        if not isinstance(s, dict):
            lines.append(f"    {i}: <invalid step>")
            continue
        kind = s.get("kind", "?")
        if kind == "select":
            cols = s.get("columns", [])
            lines.append(f"    {i}: select({cols})")
        elif kind == "with_columns":
            cols = sorted((s.get("columns") or {}).keys())
            lines.append(f"    {i}: with_columns({cols})")
        elif kind == "filter":
            lines.append(f"    {i}: filter(...)")
        elif kind == "sort":
            by = s.get("by", [])
            desc = s.get("descending", [])
            lines.append(f"    {i}: sort(by={by}, descending={desc})")
        elif kind == "unique":
            subset = s.get("subset", None)
            keep = s.get("keep", None)
            lines.append(f"    {i}: unique(subset={subset}, keep={keep})")
        elif kind == "rename":
            cols = s.get("columns", {})
            lines.append(f"    {i}: rename({cols})")
        elif kind == "slice":
            off = s.get("offset", None)
            ln = s.get("length", None)
            lines.append(f"    {i}: slice(offset={off}, length={ln})")
        elif kind == "fill_null":
            subset = s.get("subset", None)
            val = s.get("value", None)
            strat = s.get("strategy", None)
            lines.append(
                f"    {i}: fill_null(subset={subset}, value={val}, strategy={strat})"
            )
        elif kind == "drop_nulls":
            subset = s.get("subset", None)
            lines.append(f"    {i}: drop_nulls(subset={subset})")
        elif kind == "global_select":
            cols = sorted((s.get("columns") or {}).keys())
            lines.append(f"    {i}: global_select({cols})")
        else:
            lines.append(f"    {i}: {kind}")
    return "\n".join(lines)


def explain(
    plan: Any,
    *,
    format: Literal["text", "json"] = "text",
    engine_streaming: bool | None = None,
    root_data_kind: str | None = None,
) -> str | dict[str, Any]:
    """
    Convert a Rust plan object to a stable representation.

    `plan` is typically `pydantable._core.PyPlan`.
    """
    d: dict[str, Any] = plan.to_serializable()  # type: ignore[attr-defined]
    if engine_streaming is not None:
        d["engine_streaming"] = bool(engine_streaming)
    if root_data_kind is not None:
        d["root_data_kind"] = root_data_kind
    if format == "json":
        return d
    if format == "text":
        return render_plan_text(d)
    raise ValueError(f"Unsupported format: {format!r}")
