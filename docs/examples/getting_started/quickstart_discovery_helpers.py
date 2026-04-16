from __future__ import annotations

from pydantable import DataFrame
from pydantic import BaseModel


class Row(BaseModel):
    id: int
    score: float
    label: str


def main() -> None:
    df = DataFrame[Row](
        {
            "id": [1, 2, 3],
            "score": [10.0, 20.5, 7.0],
            "label": ["a", "b", "a"],
        }
    )

    print("df.columns =", df.columns)
    print("df.shape =", df.shape)
    print()
    print(df.info())
    print()
    print(df.describe())


if __name__ == "__main__":
    main()
