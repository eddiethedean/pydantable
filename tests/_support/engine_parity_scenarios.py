"""Shared behavioral scenarios for cross-engine DataFrame parity."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

from pydantable.schema import Schema


class ParityRow(Schema):
    id: int
    v: int


class ParityJoinLeft(Schema):
    k: int
    v: int


class ParityJoinRight(Schema):
    k: int
    w: int


class ParityGroup(Schema):
    g: str
    n: int


class ParityExplode(Schema):
    id: int
    items: list[int]


class ParityPivot(Schema):
    i: int
    c: str
    val: int


# ``engines``: subset of ``core``, ``mongo``, ``sql`` that must agree on output.
# Methods omitted from SQL when Moltres rejects typed Expr transforms.
BEHAVIORAL_SCENARIOS: dict[str, dict[str, Any]] = {
    "select": {
        "engines": ("core", "mongo", "sql"),
        "apply": lambda df: df.select("id"),
        "expected": {"id": [3, 1, 2]},
    },
    "filter": {
        "engines": ("core", "mongo"),
        "apply": lambda df: df.filter(df.id > 1),
        "expected": {"id": [3, 2], "v": [30, 20]},
    },
    "with_columns": {
        "engines": ("core", "mongo"),
        "apply": lambda df: df.with_columns(dbl=df.v * 2),
        "expected": {"id": [3, 1, 2], "v": [30, 10, 20], "dbl": [60, 20, 40]},
    },
    "sort": {
        "engines": ("core", "mongo", "sql"),
        "apply": lambda df: df.sort("id"),
        "expected": {"id": [1, 2, 3], "v": [10, 20, 30]},
    },
    "limit": {
        "engines": ("core", "mongo", "sql"),
        "apply": lambda df: df.head(2),
        "expected": {"id": [3, 1], "v": [30, 10]},
    },
    "unique": {
        "engines": ("core", "mongo"),
        "apply": lambda df: df.unique(subset=["v"], maintain_order=True),
        "expected": {"id": [1, 2, 4], "v": [10, 20, 30]},
    },
    "distinct": {
        "engines": ("core", "mongo"),
        "apply": lambda df: df.distinct(subset=["v"]),
        "expected": {"id": [1, 2, 4], "v": [10, 20, 30]},
    },
    "join": {
        "engines": ("core", "mongo"),
        "apply": None,  # special-cased: needs two frames
        "expected": {"k": [1, 2], "v": [10, 20], "w": [100, 200]},
    },
    "group_by": {
        "engines": ("core", "mongo"),
        "apply": None,  # special-cased below
        "expected": {"g": ["A", "B"], "n_sum": [3, 10]},
    },
    "explode": {
        "engines": ("core", "mongo"),
        "apply": None,
        "expected": {"id": [1, 1, 2], "items": [10, 20, 30]},
    },
    "pivot": {
        "engines": ("core", "mongo"),
        "apply": None,
        "expected": {"i": [1, 2], "x_first": [1, 3], "y_first": [2, None]},
    },
    "concat": {
        "engines": ("core", "mongo"),
        "apply": None,
        "expected": {"id": [1, 2], "v": [10, 20]},
    },
}


def make_in_memory_frame(
    cls: type,
    payload: dict[str, list[Any]],
    *,
    sql_config: Any | None = None,
) -> Any:
    if sql_config is not None:
        return cls(payload, sql_config=sql_config)
    return cls(payload)


def apply_parity_transform(
    method: str,
    df: Any,
    *,
    join_right: Any | None = None,
) -> Any:
    if method == "join":
        assert join_right is not None
        return df.join(join_right, on="k", how="inner")
    if method == "group_by":
        return df.group_by("g", maintain_order=True).agg(n_sum=("sum", "n"))
    if method == "explode":
        return df.explode("items")
    if method == "pivot":
        return df.pivot(
            index="i",
            columns="c",
            values="val",
            aggregate_function="first",
        )
    if method == "concat":
        raise RuntimeError("concat handled in parity test module")
    scenario = BEHAVIORAL_SCENARIOS[method]
    apply_fn: Callable[[Any], Any] = scenario["apply"]
    return apply_fn(df)


def parity_payload(method: str) -> dict[str, list[Any]]:
    if method in ("join",):
        return {"k": [2, 1], "v": [20, 10]}
    if method == "group_by":
        return {"g": ["A", "A", "B"], "n": [1, 2, 10]}
    if method == "explode":
        return {"id": [1, 2], "items": [[10, 20], [30]]}
    if method == "pivot":
        return {"i": [1, 1, 2], "c": ["x", "y", "x"], "val": [1, 2, 3]}
    if method == "concat":
        return {"id": [1], "v": [10]}
    if method in ("unique", "distinct"):
        return {"id": [1, 2, 3, 4], "v": [10, 20, 10, 30]}
    return {"id": [3, 1, 2], "v": [30, 10, 20]}


def parity_schema(method: str) -> type[Schema]:
    if method == "join":
        return ParityJoinLeft  # type: ignore[return-value]
    if method == "group_by":
        return ParityGroup  # type: ignore[return-value]
    if method == "explode":
        return ParityExplode  # type: ignore[return-value]
    if method == "pivot":
        return ParityPivot  # type: ignore[return-value]
    return ParityRow  # type: ignore[return-value]
