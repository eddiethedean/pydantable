from __future__ import annotations
from mailbox import NotEmptyError
from typing import Any, Callable, MutableMapping, MutableSequence, Generator, Sequence, Union

from tabulate import tabulate

from tinytable.filter import Filter
from tinytable.group import Group
from tinytable.functional.group import groupby
from tinytable.functional.copy import copy_table
import tinytable.functional.columns as columns
from tinytable.types import DataDict, data_dict


class Column:
    def __init__(self, data: Sequence, name: Union[str, None], parent=None, labels=None):
        self.data = list(data)
        self.name = name
        self.parent = parent
        self.labels = labels

    def __len__(self) -> int:
        return len(self.data)
    
    def __repr__(self) -> str:
        header = 'index' if self.name is None else self.name
        index = True if self.labels is None else self.labels
        return tabulate({header: self.data}, headers=[header], tablefmt='grid', showindex=index)
    
    def __iter__(self):
        return iter(self.data)
    
    def __getitem__(self, index: int) -> Any:
        return self.data[index]
    
    def __setitem__(self, index: int, value: Any) -> None:
        self.data[index] = value
        if self.parent is not None:
            self.parent.edit_value(self.name, index, value)

    def __eq__(self, value: Any) -> Filter:
        return Filter(self, lambda x: x == value)

    def __ne__(self, value: Any) -> Filter:
        return Filter(self, lambda x: x != value)

    def __gt__(self, value: Any) -> Filter:
        return Filter(self, lambda x: x > value)

    def __lt__(self, value: Any) -> Filter:
        return Filter(self, lambda x: x < value)

    def __ge__(self, value: Any) -> Filter:
        return Filter(self, lambda x: x >= value)

    def __le__(self, value: Any) -> Filter:
        return Filter(self, lambda x: x <= value)

    def __add__(self, other) -> Column:
        data = columns.add_to_column(self.data, other)
        return Column(data, self.name)

    def __sub__(self, other) -> Column:
        data = columns.subtract_from_column(self.data, other)
        return Column(data, self.name, self.parent, self.labels)

    def __mul__(self, other) -> Column:
        data = columns.multiply_column(self.data, other)
        return Column(data, self.name, self.parent, self.labels)

    def __truediv__(self, other) -> Column:
        data = columns.divide_column(self.data, other)
        return Column(data, self.name, self.parent, self.labels)

    def __mod__(self, other) -> Column:
        data = columns.mod_column(self.data, other)
        return Column(data, self.name, self.parent, self.labels)

    def __floordiv__(self, other) -> Column:
        data = columns.floor_column(self.data, other)
        return Column(data, self.name, self.parent, self.labels)

    def __pow__(self, other) -> Column:
        data = columns.exponent_column(self.data, other)
        return Column(data, self.name, self.parent, self.labels)

    def isin(self, values: MutableSequence) -> Filter:
        return Filter(self, lambda x: x in values)

    def notin(self, values: MutableSequence) -> Filter:
        return Filter(self, lambda x: x not in values)

    def drop(self):
        """drop Column from parent"""
        if self.parent is not None:
            self.parent.drop_column(self.name)
            self.parent = None

    def cast_as(self, data_type: Callable) -> None:
        self.data = [data_type(item) for item in self.data]
        if self.parent is not None:
            self.parent.cast_column_as(self.name, data_type)

    def value_counts(self) -> dict:
        return {value: self.data.count(value) for value in self.data}

    def sum(self) -> Union[float, int]:
        return sum(self.data)

    def groupby(self) -> Group:
        name = str(self.name)
        groups = groupby({name: self.data}, by=str(name))
        return Group(groups, by=name)

    def __reversed__(self) -> Column:
        data = list(reversed(self.data))
        return Column(data, self.name, self.parent, self.labels)

    def __delitem__(self, i) -> None:
        raise NotImplemented('deleting items from columns is not implemented')

    def __contains__(self, value) -> bool:
        return value in self.data

    def index(self, value) -> int:
        return list(self.data).index(value)

    def count(self, value) -> int:
        return list(self.data).count(value)


def itercolumns(data: MutableMapping, parent, labels=None) -> Generator[Column, None, None]:
    for col in data.keys():
        yield Column(data[col], col, parent, labels)


def iteritems(data: MutableMapping, parent) -> Generator[tuple[str, Column], None, None]:
    for col in data.keys():
        yield col, Column(data[col], col, parent)


def cast_column_as(data: MutableMapping, column_name: str, data_type: Callable) -> DataDict:
    """Return a new dict with named column cast as data_type."""
    new_data = data_dict(data)
    new_data[column_name] = [data_type(value) for value in new_data[column_name]]
    return new_data