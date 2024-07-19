from __future__ import annotations
import typing as _t
import pydantic as _pydantic

class IterTable:
    def __init__(self, table: Table) -> None:
        self.table: Table = table
        self.i = 0

    def __next__(self) -> _pydantic.BaseModel:
        if self.i >= len(self.table):
            raise StopIteration
        row: dict[str, _t.Any] =  {
            col: values[self.i]
                for col, values in self.table.data.items()
        }
        return self.table.model.model_construct(**row)


class Table:
    def __init__(
        self,
        data: dict[str, _t.Any],
        model: _pydantic.BaseModel
    ) -> None:
        self.data: dict[str, _t.Any] = data
        self.model: _pydantic.BaseModel = model

    def columns(self) -> list[str]:
        return list(self.data.keys())

    def __len__(self) -> int:
        column_names: list[str] = self.columns()
        if len(column_names) == 0:
            return 0
        return len(self.data[column_names[0]])

    def __iter__(self) -> IterTable:
        return IterTable(self)

    def validate(self):
        ...
