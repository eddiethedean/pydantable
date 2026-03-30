"""Batch ingest: columnar JSON for a small user table (realistic field names)."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from pydantable import DataFrameModel
from pydantable.fastapi import columnar_dependency

router = APIRouter()


class UserBatch(DataFrameModel):
    user_id: int
    email: str
    score: float | None = None


@router.post("/columnar")
def ingest_users_columnar(
    df: Annotated[UserBatch, Depends(columnar_dependency(UserBatch))],
) -> dict[str, list]:
    """Columnar body: parallel lists per column (equal lengths)."""
    return df.to_dict()
