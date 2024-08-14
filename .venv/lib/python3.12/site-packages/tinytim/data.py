from typing import Any, Sized, Tuple

from tinytim.types import DataDict, DataMapping


def column_count(data: DataMapping) -> int:
    """
    Return the number of columns in data.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}

    Returns
    -------
    int
        number of columns in data

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> column_count(data)
    2
    """
    return len(data)


def first_column_name(data: DataMapping) -> str:
    """
    Return the name of the first column.
    Raises StopIteration if data has zero columns.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}

    Returns
    -------
    str
        name of first column

    Raises
    ------
    StopIteration
        if data has zero columns

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> first_column_name(data)
    'x'
    """
    return next(iter(data))


def row_count(data: DataMapping) -> int:
    """
    Return the number of rows in data.
    
    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}

    Returns
    -------
    int
        number of rows in data

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> row_count(data)
    3
    """
    if column_count(data) == 0: return 0
    return len(data[first_column_name(data)])


def shape(data: DataMapping) -> Tuple[int, int]:
    """
    Return data row count, column count tuple.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}

    Returns
    -------
    tuple[int, int]
        (row count, column count)

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> shape(data)
    (3, 2)
    """
    col_count = column_count(data)
    if col_count == 0: return 0, 0
    return row_count(data), col_count


def size(data: DataMapping) -> int:
    """
    Return data row count multiplied by column count.
        
    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}

    Returns
    -------
    int
        row count * column count

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> size(data)
    6
    """
    rows, columns = shape(data)
    return rows * columns


def column_names(data: DataMapping) -> Tuple[str]:
    """
    Return data column names.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}

    Returns
    -------
    tuple[str]
        tuple of data column names

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> column_names(data)
    ('x', 'y')
    """
    return tuple(data)


def head(data: DataMapping, n: int = 5) -> DataDict:
    """
    Return the first n rows of data.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}
    n : int, optional
        number of rows to return from top of data

    Returns
    -------
    dict[str, Sequence]
        {column name: top n column values}

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> head(data, 2)
    {'x': [1, 2], 'y': [6, 7]}
    """
    return {col: list(values[:n]) for col, values in data.items()}


def tail(data: DataMapping, n: int = 5) -> DataDict:
    """
    Return the last n rows of data.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}
    
    n : int, optional
        number of rows to return from bottom of data

    Returns
    -------
    dict
        {column name: bottom n column values}

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> tail(data, 2)
    {'x': [2, 3], 'y': [7, 8]}
    """
    return {col: list(values[-n:]) for col, values in data.items()}


def index(data: DataMapping) -> Tuple[int]:
    """
    Return tuple of data column indexes.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}

    Returns
    -------
    tuple[int]
        tuple of row indexes

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> index(data)
    (0, 1, 2)
    """
    return tuple(range(row_count(data)))


def table_value(data: DataMapping, column_name: str, index: int) -> Any:
    """
    Return one value from column at row index.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}
    column_name : str
        name of column
    index : int
        index of column value

    Returns
    -------
    Any
        value of column at index

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> table_value(data, 'x', 1)
    2
    """
    return data[column_name][index]


def column_values(data: DataMapping, column_name: str) -> tuple:
    """
    Return all the values from one column.
    
    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}
    column_name : str
        name of column

    Returns
    -------
    Sequence
        column values

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> column_values(data, 'y')
    [6, 7, 8]
    """
    return tuple(data[column_name])