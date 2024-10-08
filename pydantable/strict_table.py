from typing import Any, Generator
import tinytim as tt
from tabulate import tabulate
import pydantic


class PydanTable:
    def __init__(
        self,
        data: dict[str, list],
        model: pydantic.BaseModel
    ) -> None:
        self.model = model
        model.model_config['validate_assignment'] = True
        self.rows = [model(**row) for _, row in tt.rows.iterrows(data)]

    def __getitem__(self, index: int) -> pydantic.BaseModel:
        return self.rows[index]

    def __iter__(self):
        self._i = 0
        return self

    def __next__(self) -> pydantic.BaseModel:
        if self._i > len(self.rows) - 1:
            raise StopIteration
        row = self.rows[self._i]
        self._i += 1
        return row

    def itervalues(self) -> Generator[tuple[Any, ...], Any, None]:
        for row in self:
            yield tuple(value[1] for value in row)

    def __repr__(self):
        return f'{self.model.__name__} {self.__class__.__name__}{"\n"}' + tabulate(
            self.itervalues(), headers=self.headers, tablefmt='grid', showindex=True)

    @property
    def columns(self) -> list[str]:
        return list(self.model.model_fields.keys())

    @property
    def dtypes(self) -> tuple[tuple[str, str], ...]:
        return tuple((col, t.annotation.__name__) for col, t in self.model.model_fields.items())

    @property
    def headers(self) -> list[str]:
        return [f'{col}[{dtype}]' for col, dtype in self.dtypes]