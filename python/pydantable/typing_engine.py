"""Python helpers that mirror Rust logical-plan schema inference for tests and tooling.

Functions delegate to :mod:`pydantable._core` to compute output
``schema_descriptors`` after transforms such as select, drop, rename, and
``with_columns``. Used by :mod:`tests.test_typing_engine_parity` and similar checks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

from pydantable.expressions import Expr
from pydantable.rust_engine import _require_rust_core


def infer_schema_descriptors_select(
    schema_fields: Mapping[str, Any], columns: Sequence[str]
) -> dict[str, dict[str, Any]]:
    rust = _require_rust_core()
    plan = rust.make_plan(dict(schema_fields))
    plan2 = rust.plan_select(plan, list(columns))
    return plan2.schema_descriptors()


def infer_schema_descriptors_drop(
    schema_fields: Mapping[str, Any], columns: Sequence[str]
) -> dict[str, dict[str, Any]]:
    rust = _require_rust_core()
    plan = rust.make_plan(dict(schema_fields))
    plan2 = rust.plan_drop(plan, list(columns))
    return plan2.schema_descriptors()


def infer_schema_descriptors_rename(
    schema_fields: Mapping[str, Any], mapping: Mapping[str, str]
) -> dict[str, dict[str, Any]]:
    rust = _require_rust_core()
    plan = rust.make_plan(dict(schema_fields))
    plan2 = rust.plan_rename(plan, dict(mapping))
    return plan2.schema_descriptors()


def infer_schema_descriptors_with_columns(
    schema_fields: Mapping[str, Any], new_columns: Mapping[str, Expr | Any]
) -> dict[str, dict[str, Any]]:
    rust = _require_rust_core()
    plan = rust.make_plan(dict(schema_fields))
    rust_cols: dict[str, Any] = {}
    for name, value in new_columns.items():
        if isinstance(value, Expr):
            rust_cols[name] = value._rust_expr
        else:
            rust_cols[name] = rust.make_literal(value=value)
    plan2 = rust.plan_with_columns(plan, rust_cols)
    return plan2.schema_descriptors()
