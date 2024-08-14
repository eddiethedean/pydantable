from typing import Iterable

from dictanykey.default_dictanykey import DefaultDictAnyKey
from dictanykey.dictanykey import DictAnyKey


def value_counts(
   values: Iterable,
   sort=True,
   ascending=True
) -> DictAnyKey:
    """
    Count up each value.
    Return a DictAnyKey[value] -> count
    Allows for unhashable values.

    Parameters
    ----------
    values :
        values to be counted up
    sort : default True, sort results by counts
    ascending: default True, sort highest to lowest


    Returns
    -------
    DictAnyKey[Any, int]
        {value: value_count}

    Example
    -------
    >>> values = [4, 1, 1, 4, 5, 1]
    >>> value_counts(values)
    DictAnyKey((1, 3), (4, 2), (5, 1))
    """
    d = DefaultDictAnyKey(int)
    for value in values:
        d[value] += 1
    if sort:
        return DictAnyKey(sorted(d.items(),  # type: ignore
                                 key=lambda item: item[1],  # type: ignore
                                 reverse=ascending))
    else:
        return DictAnyKey(d)