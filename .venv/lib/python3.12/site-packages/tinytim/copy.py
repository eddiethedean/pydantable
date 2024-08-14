import copy

from tinytim.types import DataMapping


def copy_table(data: DataMapping) -> DataMapping:
    """
    Copy data and return the copy.

    Parameters
    ----------
    data : MutableMapping[str, MutableSequence]
        data mapping of {column name: column values}

    Returns
    -------
    MutableMapping[str, MutableSequence]
        copy of data

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> copy_table(data)
    {'x': [1, 2, 3], 'y': [6, 7, 8]}
    """
    return copy.deepcopy(data)


def deepcopy_table(data: DataMapping) -> dict:
    """
    Deep copy data and return the copy.

    Parameters
    ----------
    data : MutableMapping[str, MutableSequence]
        data mapping of {column name: column values}

    Returns
    -------
    MutableMapping[str, MutableSequence]
        copy of data

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> deepcopy_table(data)
    {'x': [1, 2, 3], 'y': [6, 7, 8]}
    """
    return {col: copy.deepcopy(values) for col, values in data.items()}


def copy_list(values: list) -> list:
    """
    Copy list and return the copy.

    Parameters
    ----------
    values : list
        list of values

    Returns
    -------
    list
        copy of list

    Example
    -------
    >>> values = [1, 2, 3, 6, 7, 8]
    >>> values_copy = copy_list(values)
    >>> values_copy
    [1, 2, 3, 6, 7, 8]
    >>> values_copy[0] = 11
    >>> values_copy
    [11, 2, 3, 6, 7, 8]
    >>> values
    [1, 2, 3, 6, 7, 8]
    """
    return copy.copy(values)


def deepcopy_list(values: list) -> list:
    """
    Deep copy list and return the copy.

    Parameters
    ----------
    values : list
        list of values

    Returns
    -------
    list
        deep copy of list

    Example
    -------
    >>> values = [1, 2, 3, 6, 7, 8]
    >>> values_copy = deepcopy_list(values)
    >>> values_copy
    [1, 2, 3, 6, 7, 8]
    >>> values_copy[0] = 11
    >>> values_copy
    [11, 2, 3, 6, 7, 8]
    >>> values
    [1, 2, 3, 6, 7, 8]
    """
    return copy.deepcopy(values)