from __future__ import annotations

from pydantable import DataFrame, Schema


class Wide(Schema):
    id: int
    a: int
    b: int


class ForPivot(Schema):
    id: int
    key: str
    x: int


class Long(Schema):
    id: int
    variable: str
    value: int


class WideOut(Schema):
    id: int
    A_first: int | None
    B_first: int | None


def main() -> None:
    wide = DataFrame[Wide]({"id": [1, 2], "a": [10, 11], "b": [20, 21]})
    long = wide.melt_as(
        Long, id_vars=[wide.col.id], value_vars=[wide.col.a, wide.col.b]
    )
    print(long.to_dict())

    df = DataFrame[ForPivot]({"id": [1, 1], "key": ["A", "B"], "x": [10, 20]})
    out = df.pivot_as(
        WideOut,
        index=[df.col.id],
        columns=df.col.key,
        values=[df.col.x],
        pivot_values=["A", "B"],
        separator="_",
    )
    print(out.to_dict())


if __name__ == "__main__":
    main()
