"""Purely integer-location based indexing for Table selection by position."""
from tinytable.functional.utils import slice_to_range
from tinytable.functional.features import table_value

from typing import List, Sequence


class Iloc:
    def __init__(self, parent):
        self.parent = parent

    def __getitem__(self, key):
        # With a scalar integer. tbl.iloc[0]
        if isinstance(key, int):
            return self.parent[key]
        
        # With a slice object. tbl.iloc[:3]
        if isinstance(key, slice):
            if is_int_slice(key):
                return self.parent[key]
            else:
                raise TypeError('Cannot index by location index with a non-integer key')

        # With a list of integers. tbl.iloc[[0, 1]]
        if isinstance(key, list):
            if is_int_list(key):
                return self.parent.filter_by_indexes(key)
            else:
                raise TypeError('Cannot index by location index with a non-integer key')

        if isinstance(key, tuple):
            if len(key) > 2:
                raise IndexError('Too many indexers')

            # With scalar integers. tbl.iloc[0, 1]
            if is_int_tuple(key):
                data = self.parent.data
                column = self.parent.columns[key[1]]
                return table_value(data, column, key[0])
            
            # With lists of integers. tbl.iloc[[0, 2], [1, 3]]
            if is_two_int_lists(key):
                cols = self.parent.columns
                return self.parent[[cols[i] for i in key[1]]].filter_by_indexes(key[0])

            # With slice objects. tbl.iloc[1:3, 0:3]
            if is_two_int_slices(key):
                index_range = slice_to_range(key[0])
                column_range = slice_to_range(key[1])
                cols = self.parent.columns
                return self.parent[[cols[i] for i in column_range]].filter_by_indexes(index_range)

        raise TypeError('Cannot index by location index with a non-integer key')

    def __setitem__(self, key, value) -> None:
        # With a scalar integer. tbl.iloc[0] = {'age': 22}
        if isinstance(key, int):
            self.parent[key] = value
            return
        
        # With a slice object. tbl.iloc[:3] = [{'age': 22}, {'age': 21}, {'age': 20}]
        if isinstance(key, slice):
            if is_int_slice(key):
                self.parent[key] = value
                return
            else:
                raise TypeError('Cannot index by location index with a non-integer key')

        # With a list of integers. tbl.iloc[[0, 1]] = [{'age': 22}, {'age': 21}]
        #                          tbl.iloc[[0, 1]] = [[1, 22], [2, 21]]
        if isinstance(key, list):
            if is_int_list(key):
                for i, v in zip(key, value):
                    self.parent.edit_row(i, v)
                return
            else:
                raise TypeError('Cannot index by location index with a non-integer key')

        if isinstance(key, tuple):
            if len(key) > 2:
                raise IndexError('Too many indexers')

            # With scalar integers. tbl.iloc[0, 1] = 22
            if is_int_tuple(key):
                column = self.parent.columns[key[1]]
                self.parent[column][key[0]] = value
                return

            # With lists of integers. tbl.iloc[[0, 2], [1, 3]] = []
            if is_two_int_lists(key):
                raise NotImplemented('Setting value of Table subset is not implemented.')

            # With slice objects. tbl.iloc[1:3, 0:3]
            if is_two_int_slices(key):
                raise NotImplemented('Setting value of Table subset is not implemented.')

        raise TypeError('Cannot index by location index with a non-integer key')


def is_int_slice(s: slice) -> bool:
    if not isinstance(s.start, int) and not s.start is None:
        return False
    if not isinstance(s.stop, int) and not s.stop is None:
        return False
    if not isinstance(s.step, int) and not s.step is None:
        return False
    return True


def is_int_list(l: list) -> bool:
    return all(isinstance(v, int) for v in l)


def is_int_tuple(t: tuple) -> bool:
    return all(isinstance(v, int) for v in t)


def is_two_int_lists(s: Sequence[List[int]]) -> bool:
    if isinstance(s[0], list) and isinstance(s[1], list):
        if is_int_list(s[0]) and is_int_list(s[1]):
            return True
    return False


def is_two_int_slices(s: Sequence[slice]) -> bool:
    if isinstance(s[0], slice) and isinstance(s[1], slice):
        if is_int_slice(s[0]) and is_int_slice(s[1]):
            return True
    return False