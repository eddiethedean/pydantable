from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MethodParity:
    method: str
    core_expr: str  # "typed"
    sql_expr: str  # "typed"
    mongo_expr: str  # "typed"
    spark_expr: str  # "typed" | "native"


# High-signal core transform surface for parity drift detection.
#
# Note: Spark's engine-backed DataFrame expects engine-native expressions for some
# methods; typed-Expr parity is provided by the pyspark-shaped wrapper.
MATRIX: tuple[MethodParity, ...] = (
    MethodParity("select", "typed", "typed", "typed", "typed"),
    MethodParity("with_columns", "typed", "typed", "typed", "native"),
    MethodParity("filter", "typed", "typed", "typed", "native"),
    MethodParity("join", "typed", "typed", "typed", "typed"),
    MethodParity("group_by", "typed", "typed", "typed", "typed"),
    MethodParity("sort", "typed", "typed", "typed", "typed"),
    MethodParity("limit", "typed", "typed", "typed", "typed"),
    MethodParity("distinct", "typed", "typed", "typed", "typed"),
    MethodParity("unique", "typed", "typed", "typed", "typed"),
    MethodParity("pivot", "typed", "typed", "typed", "typed"),
    MethodParity("explode", "typed", "typed", "typed", "typed"),
    MethodParity("concat", "typed", "typed", "typed", "typed"),
)
