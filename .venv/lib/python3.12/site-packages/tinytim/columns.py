from itertools import repeat
from numbers import Number
from typing import Any, Callable, Generator, Iterable, Sequence, Sized, Tuple, Union

from dictanykey import DefaultDictAnyKey, DictAnyKey
import tinytim.data as data_functions
from tinytim.types import DataMapping, DataDict


def column_dict(data: DataMapping, col: str) -> DataDict:
    """
    Return a dict of {col_name, col_values} from data.
        
    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}
    col : str
        column name to pull out of data.

    Returns
    -------
    dict[str, Sequence]
        {column_name: column_values}

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> column_dict(data, 'x')
    {'x': [1, 2, 3]}
    >>> column_dict(data, 'y')
    {'y': [6, 7, 8]}
    """
    return {col: list(data_functions.column_values(data, col))}


def itercolumns(data: DataMapping) -> Generator[Tuple[str, tuple], None, None]:
    """
    Return a generator of tuple column name, column values.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}
    
    Returns
    -------
    Generator[Tuple[str, tuple], None, None]
        generator that yields tuples(column_name, column_values)
        
    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> generator = itercolumns(data)
    >>> next(generator)
    ('x', (1, 2, 3)) 
    >>> next(generator)
    ('y', (6, 7, 8))
    """
    for col in data_functions.column_names(data):
        yield col, tuple(data_functions.column_values(data, col))


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


def operate_on_column(
    column: Sequence,
    values: Union[Iterable, str, Number],
    func: Callable[[Any, Any], Any]
) -> list:
    """
    Uses func operator on values in column.
    If values is a sequence, operate on each column value with values.
    values sequence must be same len as column.
    If values is not a sequence, operate on each column value with the single value.

    Parameters
    ----------
    column : MutableSequence
        sequence of values in column
    values : Sequence | str | Number
        values to operate on column values
    func : Callable[[Any, Any], Any]
        operator function to use to use values on column values

    Returns
    -------
    list

    Examples
    --------
    >>> column = [1, 2, 3, 4]
    >>> operate_on_columns(column, 1, lamda x, y : x + y)
    [2, 3, 4, 5]

    >>> column = [1, 2, 3, 4]
    >>> operate_on_columns(column, [2, 3, 4, 5], lamda x, y : x + y)
    [3, 5, 7, 9]
    """
    iterable_and_sized = isinstance(values, Iterable) and isinstance(values, Sized)
    if isinstance(values, str) or not iterable_and_sized:
        return [func(x, y) for x, y in zip(column, repeat(values, len(column)))]
    
    if iterable_and_sized and not isinstance(values, Number):
        if len(values) != len(column):  # type: ignore
            raise ValueError('values length must match data rows count.')
        return [func(x, y) for x, y in zip(column, values)]
    else:
        raise TypeError('values must either be a sequence or number to operate on column')


def add_to_column(column: Sequence, values: Union[Sequence, str, Number]) -> list:
    """
    Add a value or values to a sequence of column values.

    Parameters
    ----------
    column : Sequence
    values : Sequence | str | Number

    Returns
    -------
    list

    Examples
    --------
    Add a number to each value in a sequence.

    >>> column = (1, 2, 3, 4)
    >>> add_to_column(column, 1)
    [2, 3, 4, 5]

    Add a sequence of numbers to a sequence.

    >>> column = (1, 2, 3, 4)
    >>> add_to_column(column, (4, 5, 6, 7))
    [5, 7, 9, 11]

    Concatenate a string to each value in a sequence of strings.

    >>> column = ('a', 'b', 'c', 'd')
    >>> add_to_column(column, 'A')
    ['aA', 'bA', 'cA', 'dA']

    Concatenate a string to each value in a sequence of strings.

    >>> column = ('a', 'b', 'c', 'd')
    >>> add_to_column(column, ('A', 'B', 'C', 'D'))

    See Also
    --------
    tinytim.columns.subtract_from_column
    tinytim.columns.multiply_column
    tinytim.columns.divide_column
    """
    return operate_on_column(column, values, lambda x, y : x + y)


def subtract_from_column(column: Sequence, values: Union[Sequence, Number]) -> list:
    """
    Subtract a value or values from a sequence of column values.

    Parameters
    ----------
    column : Sequence
    values : Sequence | str | Number

    Returns
    -------
    list

    Examples
    --------
    Subtract a number from each value in a sequence of numbers.

    >>> column = (1, 2, 3, 4)
    >>> subtract_from_column(column, 1)
    [0, 1, 2, 3]

    Subtract a sequence of numbers from a sequence of numbers.

    >>> column = (1, 2, 3, 4)
    >>> subtract_from_column(column, (4, 5, 6, 7))
    [-3, -3, -3, -3]

    See Also
    --------
    tinytim.columns.add_to_column
    tinytim.columns.multiply_column
    tinytim.columns.divide_column
    """
    return operate_on_column(column, values, lambda x, y : x - y)


def multiply_column(column: Sequence, values: Union[Sequence, Number]) -> list:
    """
    Multiply a value or values with a sequence of column values.

    Parameters
    ----------
    column : Sequence
    values : Sequence | str | Number

    Returns
    -------
    list

    Examples
    --------
    Multiply a number with each value in a sequence of numbers.

    >>> column = (1, 2, 3, 4)
    >>> multiply_column(column, 2)
    [2, 4, 6, 8]

    Mutiply a sequence of numbers with a sequence of numbers.

    >>> column = (1, 2, 3, 4)
    >>> multiply_column(column, (4, 5, 6, 7))
    [4, 10, 18, 28]

    Multiply a sequence of strings with a sequence of numbers.
    >>> column = ['a', 'b', 'c', 'd']
    >>> multiply_column(column, 3)
    ['aaa', 'bbb', 'ccc', 'ddd']

    Multiply a sequence of numbers with a sequence of strings.
    >>> column = [1, 2, 3, 4]
    >>> multiply_column(column, ('z', 'q', 'y', 'q'))

    See Also
    --------
    tinytim.columns.add_to_column
    tinytim.columns.subtract_from_column
    tinytim.columns.divide_column
    """
    return operate_on_column(column, values, lambda x, y : x * y)


def divide_column(column: Sequence, values: Union[Sequence, Number]) -> list:
    """
    Divide a value or values from a sequence of column values.

    Parameters
    ----------
    column : Sequence
    values : Sequence | str | Number

    Returns
    -------
    list

    Examples
    --------
    Divide a number from each value in a sequence of numbers.

    >>> column = (1, 2, 3, 4)
    >>> divide_column(column, 2)
    [0.5, 1.0, 1.5, 2.0]

    Divide a sequence of numbers from a sequence of numbers.

    >>> column = (1, 2, 3, 4)
    >>> divide_column(column, (4, 5, 6, 7))
    [0.25, 0.4, 0.5, 0.5714285714285714]

    See Also
    --------
    tinytim.columns.add_to_column
    tinytim.columns.subtract_from_column
    tinytim.columns.multiply_column
    """
    return operate_on_column(column, values, lambda x, y : x / y)


def mod_column(column: Sequence, values: Union[Sequence, Number]) -> list:
    """
    Modulo a value or values from a sequence of column values.

    Parameters
    ----------
    column : Sequence
    values : Sequence | str | Number

    Returns
    -------
    list

    Examples
    --------
    Modulo a number from each value in a sequence of numbers.

    >>> column = (1, 2, 3, 4)
    >>> mod_column(column, 2)
    [1, 0, 1, 0]

    Modulo a sequence of numbers from a sequence of numbers.

    >>> column = (4, 67, 87, 65)
    >>> mod_column(column, (2, 3, 4, 5))
    [0, 1, 3, 0]

    See Also
    --------
    tinytim.columns.add_to_column
    tinytim.columns.subtract_from_column
    tinytim.columns.divide_column
    """
    return operate_on_column(column, values, lambda x, y : x % y)


def exponent_column(column: Sequence, values: Union[Sequence, Number]) -> list:
    """
    Exponent a value or values with a sequence of column values.

    Parameters
    ----------
    column : Sequence
    values : Sequence | str | Number

    Returns
    -------
    list

    Examples
    --------
    Exponent a number with each value in a sequence of numbers.

    >>> column = (1, 2, 3, 4)
    >>> exponent_column(column, 2)
    [1, 4, 9, 16]

    Exponent a sequence of numbers with a sequence of numbers.

    >>> column = (2, 3, 4, 5)
    >>> exponent_column(column, (2, 3, 4, 5))
    [4, 27, 256, 3125]

    See Also
    --------
    tinytim.columns.add_to_column
    tinytim.columns.subtract_from_column
    tinytim.columns.divide_column
    """
    return operate_on_column(column, values, lambda x, y : x ** y)


def floor_column(column: Sequence, values: Union[Sequence, Number]) -> list:
    """
    Floor divide a value or values from a sequence of column values.

    Parameters
    ----------
    column : Sequence
    values : Sequence | str | Number

    Returns
    -------
    list

    Examples
    --------
    Floor divide a number from each value in a sequence of numbers.

    >>> column = (1, 2, 3, 4)
    >>> floor_column(column, 2)
    [0, 1, 1, 2]

    Floor divide a sequence of numbers from a sequence of numbers.

    >>> column = (56, 77, 88, 55)
    >>> floor_column(column, (5, 6, 7, 8))
    [11, 12, 12, 6]

    See Also
    --------
    tinytim.columns.add_to_column
    tinytim.columns.subtract_from_column
    tinytim.columns.divide_column
    """
    return operate_on_column(column, values, lambda x, y : x // y)
