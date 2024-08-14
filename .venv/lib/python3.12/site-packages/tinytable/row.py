from typing import Any, Generator, List, Dict, Tuple

from tabulate import tabulate

from tinytable.functional.utils import row_values_generator
from tinytable.types import DataMapping


class Row:
    def __init__(self, data: Dict[str, Any], index: int, parent=None, label=None):
        self.data = data
        self.index = index
        self.parent = parent
        self.label = label
        
    def __len__(self) -> int:
        return len(self.data)
    
    def __iter__(self):
        return row_values_generator(self.data)
    
    def __repr__(self) -> str:
        index = self.index if self.label is None else self.label
        return tabulate({col: [value] for col, value in self.data.items()}, headers=self.columns, tablefmt='grid', showindex=[index])
    
    def __getitem__(self, column: str) -> Any:
        return self.data[column]
    
    def __setitem__(self, column: str, value: Any) -> None:
        self.data[column] = value
        if self.parent is not None:
            self.parent.edit_value(column, self.index, value)
    
    @property
    def columns(self) -> List[str]:
        return list(self.data.keys())

    def keys(self) -> List[str]:
        return list(self.data.keys())

    def drop(self) -> None:
        """drop Row from parent"""
        if self.parent is not None:
            self.parent.drop_row(self.index)
            self.parent = None

    def values(self) -> List[Any]:
        return list(self.data.values())


def iterrows(data: DataMapping, parent, labels=None) -> Generator[Tuple[int, Row], None, None]:
    if len(data) == 0:
        return
    i = 0
    while True:
        try:
            label = None if labels is None else labels[i]
            yield i, Row({col: data[col][i] for col in data}, i, parent, label)
        except IndexError:
            return
        i += 1


