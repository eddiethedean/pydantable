from collections import defaultdict
from itertools import zip_longest
from typing import Any, Dict, Generator, Optional, Sequence, Tuple

import tinytim.data as data_functions
import tinytim.edit as edit_functions
import tinytim.utils as utils_functions
from tinytim.types import DataDict, DataMapping, RowMapping, RowDict


def row_dict(
    data: DataMapping,
    index: int
) -> RowDict: 
    """
    Return one row from data at index.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}
    index : int
        row index

    Returns
    -------
    dict
        one row from data at index

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> row_dict(data, 1)
    {'x': 2, 'y': 7}
    """
    return {col: data_functions.table_value(data, col, index)
                for col in data_functions.column_names(data)}


def row_values(
    data: DataMapping,
    index: int
) -> tuple:
    """
    Return a tuple of the values at row index.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}
    index : int
        row index

    Returns
    -------
    tuple
        values at row index

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> row_values(data, 0)
    (1, 6)
    """
    return tuple(values[index] for values in data.values())


def iterrows(
    data: DataMapping,
    reverse: bool = False
) -> Generator[Tuple[int, RowDict], None, None]:
    """
    Return a generator of tuple row index, row dict values.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}

    Returns
    -------
    generator[tuple[int, dict]]
        generator of tuple (row index, row dict values)

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> generator = iterrows(data)
    >>> next(generator)
    (0, {'x': 1, 'y': 6})
    >>> next(generator)
    (1, {'x': 2, 'y': 7})
    >>> next(generator)
    (2, {'x': 3, 'y': 8})
    >>> next(generator)
    ...
    StopIteration
    """
    indexes = data_functions.index(data)
    indexes = reversed(indexes) if reverse else indexes
    for i in indexes:
        yield i, row_dict(data, i)


def itertuples(
    data: DataMapping
) -> Generator[tuple, None, None]:
    """
    Return a generator of tuple row values.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}

    Returns
    -------
    generator[tuple]
        generator of tuple row values

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> generator = itertuples(data)
    >>> next(generator)
    (1, 6)
    >>> next(generator)
    (2, 7)
    >>> next(generator)
    (3, 8)
    >>> next(generator)
    ...
    StopIteration
    """
    for _, row in iterrows(data):
        yield tuple(row.values())


def itervalues(
    data: DataMapping
) -> Generator[tuple, None, None]:
    """
    Return a generator of tuple row values.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}

    Returns
    -------
    generator[tuple]
        generator of tuple row values

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> generator = itervalues(data)
    >>> next(generator)
    (1, 6)
    >>> next(generator)
    (2, 7)
    >>> next(generator)
    (3, 8)
    >>> next(generator)
    ...
    StopIteration
    """
    return itertuples(data)


def values(
    data: DataMapping
) -> Tuple[tuple]:
    """
    Return tuple of tuple row values.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}

    Returns
    -------
    tuple[tuple]
        tuple of tuple row values

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> values(data)
    ((1, 6), (2, 7), (3, 8))
    """
    return tuple(itervalues(data))


def row_value_counts(
    data: DataMapping,
    sort=True,
    ascending=True
) -> Dict[tuple, int]:
    """
    Count up the unique rows.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}
    sort : bool, optional
        sort the results by count
    ascending : bool, optional
        if sort=True, sort highest to lowest

    Returns
    -------
    dict[tuple, int]
        {(row values), count}

    Example
    -------
    >>> data = {'x': [1, 2, 3, 3], 'y': [6, 7, 3, 3]}
    >>> row_value_counts(data)
    {(3, 3): 2, (1, 6): 1, (2, 7): 1}
    """
    d = defaultdict(int)
    for row in itertuples(data):
        d[row] += 1
    if sort:
        return dict(sorted(d.items(),
                           key=lambda item: item[1],
                           reverse=ascending))
    else:
        return dict(d)


def records(d: DataMapping) -> Generator[RowDict, None, None]:
    """
    Yield each record (row) in d.
    
    Parameters
    ----------
    d : Mapping[str, Sequence]
        data mapping of {column name: column values}
    
    Example
    -------
    >>> d = {'x': [1, 2, 3, 4], 'y': [55, 66, 77, 88]}
    >>> generator = records(d)
    >>> next(generator)
    {'x': 1, 'y': 55}
    >>> next(generator)
    {'x': 2, 'y': 66}
    >>> next(generator)
    {'x': 3, 'y': 77}
    >>> next(generator)
    {'x': 4, 'y': 88}
    """
    for _, record in iterrows(d):
        yield record
    

def records_equal(d1: DataMapping, d2: DataMapping) -> bool:
    """
    Compare d1 and d2 records (rows) to see if they are equal.
    Order of records or columns does not matter.
    
    Parameters
    ----------
    d1 : Mapping[str, Sequence]
        data mapping of {column name: column values}
    d2 : Mapping[str, Sequence]
        data mapping of {column name: column values}
    
    Examples
    --------
    >>> d1 = {'x': [1, 2, 3, 4], 'y': [55, 66, 77, 88]}
    >>> d2 = {'x': [2, 1, 4, 3], 'y': [66, 55, 88, 77]}
    >>> records_equal(d1, d2)
    True
    
    >>> d1 = {'x': [1, 2, 3, 4], 'y': [55, 66, 77, 88]}
    >>> d2 = {'x': [2, 1, 4, 3], 'y': [55, 77, 88, 66]}
    >>> records_equal(d1, d2)
    False
    """
    if set(data_functions.column_names(d1)) != set(data_functions.column_names(d2)):
        return False
    
    if data_functions.row_count(d1) != data_functions.row_count(d2):
        return False
    
    d2_rows = [row for row in records(d2)]
    
    for row in records(d1):
        if row in d2_rows:
            d2_rows.remove(row)
        else:
            return False
    
    return True


def row_dicts_to_data(
    rows: Sequence[RowMapping],
    columns: Optional[Sequence[str]] = None,
    missing_value: Optional[Any] = None
) -> DataDict:
    """
    Convert a list of row dicts to dict[col_name: values] format.

    Parameters
    ----------
    rows : Sequence[Mapping[str, Any]]
        sequence of row mappings
    missing_value : Any, optional
        value to insert if column is missing values

    Returns
    -------
    dict[str, list]
        data table formatted: {column name: column values}

    Examples
    --------
    >>> rows = [{'x': 1, 'y': 20}, {'x': 2, 'y': 21}, {'x': 3, 'y': 22}]
    >>> row_dicts_to_data(rows)
    {'x': [1, 2, 3], 'y': [20, 21, 22]}

    >>> rows = [{'x': 1, 'y': 20}, {'x': 2}, {'x': 3, 'y': 22}]
    >>> row_dicts_to_data(rows)
    {'x': [1, 2, 3], 'y': [20, None, 22]}
    """
    keys = utils_functions.all_keys(rows)
    data = defaultdict(list)
    for row in rows:
        for col in keys:
            if col in row:
                data[col].append(row[col])
            else:
                data[col].append(missing_value)
    if columns:
        data = edit_functions.replace_column_names(dict(data), columns)
        return {col: list(values) for col, values in data.items()}
    return dict(data)
    

def row_values_to_data(
    rows: Sequence[Sequence],
    column_names: Sequence[str],
    missing_value: Optional[Any] = None
) -> DataDict:
    """
    Convert sequence of row values: [col1_value, col2_value, col3_value]
    and column names: [col1_name, col2_name, col3_name]
    to data dict: {column_name: column_values}
    
    Examples
    --------
    >>> rows = [[1, 20], [2, 21], [3, 22]]
    >>> columns = ['x', 'y']
    >>> row_values_to_data(rows, columns)
    {'x': [1, 2, 3], 'y': [20, 21, 22]}

    >>> rows = [[1, 20], [2], [3, 22]]
    >>> row_values_to_data(rows, columns)
    {'x': [1, 2, 3], 'y': [20, None, 22]}
    """
    data = defaultdict(list)
    col_count = len(column_names)
    for row in rows:
        if len(row) > col_count:
            raise ValueError('row values cannot be longer than column names.')
        for col, val in zip_longest(column_names, row, fillvalue=missing_value):
            data[col].append(val)
    return dict(data)