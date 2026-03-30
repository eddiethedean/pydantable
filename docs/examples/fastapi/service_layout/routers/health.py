"""Liveness for orchestrators (Kubernetes, etc.)."""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter()

SERVICE_NAME = "pydantable-example-layout"


@router.get("/live")
def live() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_NAME}
