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


def main() -> None:
    wide = DataFrame[Wide]({"id": [1, 2], "a": [10, 11], "b": [20, 21]})
    long = wide.pivot_longer(id_vars="id", value_vars=["a", "b"])
    d1 = long.to_dict()
    print({k: d1[k] for k in sorted(d1)})

    df = DataFrame[ForPivot]({"id": [1, 1], "key": ["A", "B"], "x": [10, 20]})
    out = df.pivot_wider(index="id", names_from="key", values_from="x")
    d2 = out.to_dict()
    print({k: d2[k] for k in sorted(d2)})


if __name__ == "__main__":
    main()
