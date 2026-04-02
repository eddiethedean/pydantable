from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from pydantable.policies import column_policies


def redaction_mask_for_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        return "***"
    if isinstance(v, (int, float)):
        return 0
    return None


def apply_redaction_to_row_dicts(
    schema_type: type[BaseModel],
    rows: list[dict[str, Any]],
    *,
    policy_key: str = "redact",
) -> list[dict[str, Any]]:
    """
    Apply top-level column redaction to already-serialized row dicts.

    Phase 5: policy reading is shallow (top-level only), using the existing
    `Field(json_schema_extra={"pydantable": {...}})` channel.
    """
    policies = column_policies(schema_type)
    to_redact = {k for k, p in policies.items() if p.get(policy_key) is True}
    if not to_redact:
        return rows
    out: list[dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        for k in to_redact:
            if k in rr:
                rr[k] = redaction_mask_for_value(rr[k])
        out.append(rr)
    return out


__all__ = [
    "apply_redaction_to_row_dicts",
    "redaction_mask_for_value",
]

