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
    print(df.explode_all().to_dict())

    df2 = DataFrame[WithStruct]({"id": [1], "addr": [{"street": "x"}]})
    print(df2.unnest_all().to_dict())


if __name__ == "__main__":
    main()

