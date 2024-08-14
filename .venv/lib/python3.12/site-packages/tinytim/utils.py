from typing import Any, Collection, Dict, Generator, Iterable, List, Mapping
from typing import Optional, Sequence, Tuple

from tinytim.types import DataMapping, RowMapping


def uniques(values: Iterable) -> List:
    """
    Return a list of the unique items in values.

    Parameters
    ----------
    values : iterable
        iterable of objects of any type

    Returns
    -------
    list
        a list of unique objects from values

    Example
    -------
    >>> values = [1, 1, 2, 4, 5, 2, 0, 6, 1]
    >>> uniques(values)
    [1, 2, 4, 5, 0, 6]
    """
    out = []
    for value in values:
        if value not in out:
            out.append(value)
    return out


def nuniques(values: Sequence) -> int:
    """
    Count up number of unique items in values.

    Parameters
    ----------
    values : iterable
        iterable of objects of any type

    Returns
    -------
    int
        count of unique objects in values

    Example
    -------
    >>> values = [1, 1, 2, 4, 5, 2, 0, 6, 1]
    >>> nuniques(values)
    6
    """
    return len(uniques(values))


def row_value_tuples(
    data: DataMapping,
    column_names: Sequence[str]
) -> Tuple[tuple]:
    """
    Return row value tuples for only columns in column_names.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}
    column_names : Sequence[str]
        sequence of column names

    Returns
    -------
    tuple[tuple]
        row value tuples for only columns in column_names

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8], 'z': [9, 10, 11]}
    >>> row_value_tuples(data, ['x', 'z'])
    ((1, 9), (2, 10), (3, 11))
    """
    return tuple(zip(*[data[col] for col in column_names]))


def all_keys(dicts: Sequence[Mapping]) -> List:
    """
    Return all the unique keys from a collection of dicts.

    Parameters
    ----------
    dicts : Sequence[Mapping]
        sequence of mappings

    Returns
    -------
    list
        list of unique keys from all dicts

    Example
    -------
    >>> dicts = [{'x': 1, 'y': 2}, {'x': 4, 'z': 7}]
    >>> all_keys(dicts)
    ['x', 'y', 'z']
    """
    keys = []
    for d in dicts:
        for key in d:
            if key not in keys:
                keys.append(key)
    return keys


def all_bool(values: Collection) -> bool:
    """
    Return if all items in values are bool or not.

    Parameters
    ----------
    values : Collection
        collection of any values

    Returns
    -------
    bool
        True, if all values are bool type
        False, if not all values are bool type

    Examples
    --------
    >>> values = [1, True, False, True]
    >>> all_bool(values)
    False
    
    >>> values = [True, True, False, True]
    >>> all_bool(values)
    True
    """
    return all(isinstance(item, bool) for item in values)


def row_values_generator(row: RowMapping) -> Generator[Any, None, None]:
    """
    Return a generator that yields values from a row dict.

    Parameters
    ----------
    row : Mapping[str, Any]
        mapping of row values {column name: row value}

    Returns
    -------
    generator
        generator of row values

    Example
    -------
    >>> row = {'x': 1, 'y': 4, 'z': 8}
    >>> generator = row_values_generator(row)
    >>> next(generator)
    1
    >>> next(generator)
    4
    >>> next(generator)
    8
    >>> next(generator)
    ...
    StopIteration
    """
    for key in row:
        yield row[key]


def slice_to_range(s: slice, stop: Optional[int] = None) -> range:
    """
    Convert an int:int:int slice object to a range object.
    Needs stop if s.stop is None since range is not allowed to have stop=None.

    Parameters
    ----------
    s : slice
        slice object
    stop : int, optional
        stop is needed for range but not slice
        pass in if slice is missing stop

    Returns
    -------
    range
        range object with corresponding start, stop, step

    Examples
    --------
    >>> s = slice(1, 4, 0)
    >>> slice_to_range(s)
    range(0, 3, 1)

    >>> s = slice(0, 3, 1)
    >>> slice_to_range(s)
    range(0, 3, 1)
    """
    step = 1 if s.step is None else s.step
    if step == 0:
        raise ValueError('step must not be zero')

    if step > 0:
        start = 0 if s.start is None else s.start
        stop = s.stop if s.stop is not None else stop
    else:
        start = stop if s.start is None else s.start
        if isinstance(start, int):
            start -= 1
        stop = -1 if s.stop is None else s.stop

        if start is None:
            raise ValueError('start cannot be None is range with negative step')

    if stop is None:
        raise ValueError('stop cannot be None in range')
    
    return range(start, stop, step)


def combine_names_rows(
    column_names: Sequence[str],
    rows: Sequence[Sequence]
) -> Dict[str, List]:
    """
    Convert a sequence of column names and a sequence of row values
    into dict[column_name: values] format.

    Parameters
    ----------
    column_names : Sequence[str]
        sequence of column names
    rows : Sequence[Sequence]
        sequence of different row values

    Returns
    -------
    dict[str, list]
        data table formatted {column name: column values}

    Example
    -------
    >>> column_names = ['x', 'y']
    >>> rows = ((1, 2), (4, 5), (8, 10))
    >>> combine_names_rows(column_names, rows)
    {'x': [1, 4, 8], 'y': [2, 5, 10]}
    """
    return dict(zip(column_names, map(list, zip(*rows))))


def nunique(data: DataMapping) -> Dict[str, int]:
    """
    Count number of distinct values in each column.
    Return dict with count of distinct values.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}

    Returns
    -------
    dict[str, int]
        dict formatted {column name: unique value count}

    Example
    -------
    >>> data = {'x': [1, 2, 2], 'y': [6, 7, 8], 'z': [9, 9, 9]}
    >>> nunique(data)
    {'x': 2, 'y': 3, 'z': 1}
    """
    return {col: len(uniques(values)) for col, values in data.items()}


def set_values_to_many(s: list, values: Sequence) -> None:
    if len(s) != len(values):
        raise AttributeError('s and values must be same len')
    for i, value in enumerate(values):
        s[i] = value


def set_values_to_one(s: list, value: Any) -> None:
    for i in range(len(s)):
        s[i] = value