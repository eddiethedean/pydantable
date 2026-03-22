from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel


def _materialized_table_to_dict(table: Any) -> dict[str, list[Any]]:
    if isinstance(table, dict):
        return table
    if isinstance(table, list):
        if not table:
            return {}
        first = table[0]
        if isinstance(first, BaseModel):
            keys = list(first.model_dump().keys())
            out: dict[str, list[Any]] = {k: [] for k in keys}
            for row in table:
                d = row.model_dump()
                for k in keys:
                    out[k].append(d[k])
            return out
        raise TypeError(
            f"Unsupported list table type (expected rows of BaseModel): {type(first)!r}"
        )
    typ = type(table)
    mod = getattr(typ, "__module__", "") or ""
    if mod.startswith("polars") and typ.__name__ == "DataFrame":
        return table.to_dict(as_series=False)
    raise TypeError(f"Unsupported table type for comparison: {typ!r}")


def _sort_key_for_value(value: Any) -> tuple[int, Any]:
    # Ensure Python3 can compare keys even when values contain `None`.
    # We sort `None` last to keep "normal" values ordered first.
    return (1, None) if value is None else (0, value)


def sort_rows_by_keys(
    table: dict[str, list[Any]],
    keys: list[str],
) -> dict[str, list[Any]]:
    if not table:
        return {}

    n = len(next(iter(table.values())))
    order = list(range(n))
    order.sort(
        key=lambda i: tuple(_sort_key_for_value(table[k][i]) for k in keys),
    )

    out: dict[str, list[Any]] = {}
    for col_name, col_values in table.items():
        out[col_name] = [col_values[i] for i in order]
    return out


def assert_table_eq_sorted(
    got: dict[str, list[Any]] | Any,
    expected: dict[str, list[Any]] | Any,
    keys: list[str],
) -> None:
    got = _materialized_table_to_dict(got)
    expected = _materialized_table_to_dict(expected)
    got_sorted = sort_rows_by_keys(got, keys)
    expected_sorted = sort_rows_by_keys(expected, keys)
    assert got_sorted == expected_sorted


@pytest.fixture
def _keys_for_sort() -> list[str]:
    # Small fixture for future reuse.
    return ["id"]
