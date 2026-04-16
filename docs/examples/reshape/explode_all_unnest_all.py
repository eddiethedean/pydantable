from __future__ import annotations

from pydantable import DataFrame, Schema


class _Addr(Schema):
    street: str


class WithList(Schema):
    id: int
    tags: list[int]


class WithStruct(Schema):
    id: int
    addr: _Addr


def main() -> None:
    df = DataFrame[WithList]({"id": [1, 2], "tags": [[1, 2], [3]]})
    d1 = df.explode_all().to_dict()
    print({k: d1[k] for k in sorted(d1)})

    df2 = DataFrame[WithStruct]({"id": [1], "addr": [{"street": "x"}]})
    d2 = df2.unnest_all().to_dict()
    print({k: d2[k] for k in sorted(d2)})


if __name__ == "__main__":
    main()
