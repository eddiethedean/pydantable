"""Taxonomy enum for the four terminal materialization modes."""

from __future__ import annotations

import pydantable
from pydantable import PlanMaterialization, plan_materialization_summary


def test_plan_materialization_values_and_str_enum() -> None:
    assert PlanMaterialization.BLOCKING.value == "blocking"
    assert PlanMaterialization.ASYNC.value == "async"
    assert PlanMaterialization.DEFERRED.value == "deferred"
    assert PlanMaterialization.CHUNKED.value == "chunked"
    assert isinstance(PlanMaterialization.BLOCKING, str)
    assert len(PlanMaterialization) == 4


def test_plan_materialization_summary_covers_all() -> None:
    for k in PlanMaterialization:
        s = plan_materialization_summary(k)
        assert isinstance(s, str) and len(s) > 20


def test_exported_from_package() -> None:
    assert pydantable.PlanMaterialization is PlanMaterialization
    assert pydantable.plan_materialization_summary is plan_materialization_summary
