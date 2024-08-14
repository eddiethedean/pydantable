from typing import Callable, Mapping, Sequence, Tuple, Any, Union, Optional, List, Iterable
from itertools import repeat
from typing import NamedTuple

from tinytim.types import DataDict, DataMapping


class MatchIndexes(NamedTuple):
    value: Any
    left_index: Optional[int] = None
    right_index: Optional[int] = None


Matches = Tuple[MatchIndexes]
JoinStrategy = Callable[[Sequence, Sequence], Matches]


def inner_join(
    left: DataMapping,
    right: DataMapping,
    left_on: str,
    right_on: Optional[str] = None,
    select: Optional[Sequence[str]] = None
) -> DataDict:
    """
    Inner Join two data dict on a specified column name(s).
    If right_on is None, joins both on same column name (left_on).
    Parameters
    ----------
    left : Mapping[str, Sequence]
        left data mapping of {column name: column values}
    right : Mapping[str, Sequence]
        right data mapping of {column name: column values}
    left_on : str
        column name to join on in left
    right_on : str, optional
        column name to join on in right, join on left_on if None
    select : list[str], optional
        column names to return
        
    Returns
    -------
    DataDict
        resulting joined data table
        
    Example
    -------
    >>> left =  {'id': ['a', 'c', 'd', 'f', 'g'], 'x': [33, 44, 55, 66, 77]}
    >>> right = {'id': ['a', 'b', 'c', 'd'], 'y': [11, 22, 33, 44]}
    >>> inner_join(left, right, 'id')
    {'id': ['a', 'c', 'd'], 'x': [33, 44, 55], 'y': [11, 33, 44]}
    """
    return _join(left, right, left_on, right_on, select, _inner_matching_indexes)


def full_join(
    left: DataMapping,
    right: DataMapping,
    left_on: str,
    right_on: Optional[str] = None,
    select: Optional[Sequence[str]] = None
) -> DataDict:
    """
    Full Join two data dict on a specified column name(s).
    If right_on is None, joins both on same column name (left_on).
    Parameters
    ----------
    left : Mapping[str, Sequence]
        left data mapping of {column name: column values}
    right : Mapping[str, Sequence]
        right data mapping of {column name: column values}
    left_on : str
        column name to join on in left
    right_on : str, optional
        column name to join on in right, join on left_on if None
    select : list[str], optional
        column names to return
        
    Returns
    -------
    DataDict
        resulting joined data table
        
    Example
    -------
    >>> left = {'id': ['a', 'c', 'd', 'f', 'g'], 'x': [33, 44, 55, 66, 77]}
    >>> right = {'id': ['a', 'b', 'c', 'd'], 'y': [11, 22, 33, 44]}
    >>> full_join(left, right, 'id')
    {'id': ['a', 'c', 'd', 'f', 'g', 'b'],
     'x': [33, 44, 55, 66, 77, None],
     'y': [11, 33, 44, None, None, 22]}
    """
    return _join(left, right, left_on, right_on, select, _full_matching_indexes)


def left_join(
    left: DataMapping,
    right: DataMapping,
    left_on: str,
    right_on: Optional[str] = None,
    select: Optional[Sequence[str]] = None
) -> DataDict:
    """
    Left Join two data dict on a specified column name(s).
    If right_on is None, joins both on same column name (left_on).
    
    Parameters
    ----------
    left : Mapping[str, Sequence]
        left data mapping of {column name: column values}
    right : Mapping[str, Sequence]
        right data mapping of {column name: column values}
    left_on : str
        column name to join on in left
    right_on : str, optional
        column name to join on in right, join on left_on if None
    select : list[str], optional
        column names to return
        
    Returns
    -------
    DataDict
        resulting joined data table
        
    Example
    -------
    >>> left = {'id': ['a', 'c', 'd', 'f'], 'x': [33, 44, 55, 66]}
    >>> right = {'id': ['a', 'b', 'c', 'd'], 'y': [11, 22, 33, 44]}
    >>> left_join(left, right, 'id')
    {'id': ['a', 'c', 'd', 'f'], 'x': [33, 44, 55, 66], 'y': [11, 33, 44, None]}
    """
    return _join(left, right, left_on, right_on, select, _left_matching_indexes)


def right_join(
    left: DataMapping,
    right: DataMapping,
    left_on: str,
    right_on: Optional[str] = None,
    select: Optional[Sequence[str]] = None
) -> DataDict:
    """
    Right Join two data dict on a specified column name(s).
    If right_on is None, joins both on same column name (left_on).
    
    Parameters
    ----------
    left : Mapping[str, Sequence]
        left data mapping of {column name: column values}
    right : Mapping[str, Sequence]
        right data mapping of {column name: column values}
    left_on : str
        column name to join on in left
    right_on : str, optional
        column name to join on in right, join on left_on if None
    select : list[str], optional
        column names to return
        
    Returns
    -------
    DataDict
        resulting joined data table
        
    Example
    -------
    >>> left = {'id': ['a', 'c', 'd', 'f'], 'x': [33, 44, 55, 66]}
    >>> right = {'id': ['a', 'b', 'c', 'd'], 'y': [11, 22, 33, 44]}
    >>> right_join(left, right, 'id')
    {'id': ['a', 'b', 'c', 'd'], 'x': [33, None, 44, 55], 'y': [11, 22, 33, 44]}
    """
    return _join(left, right, left_on, right_on, select, _right_matching_indexes)
    

def locate(
    l: Sequence,
    value: Any
) -> List[int]:
    """
    Return list of all index of each matching item in list.
    
    Parameters
    ----------
    l : Sequence
        sequence of values to check for matches
    value : Any
        value to check if equals any values in l
        
    Returns
    -------
    List[int]
        index numbers of items in l that equal value
        
    Example
    -------
    >>> l = [1, 2, 1, 2, 4, 5, 1]
    >>> locate(l, 1)
    [0, 2, 6]
    """
    return [i for i, x in enumerate(l) if x == value]


def _name_matches(matches: Iterable[Tuple[Any, int, int]]) -> Tuple[MatchIndexes]:
    """
    Convert tuples into MatchIndexes namedtuples.
    
    Parameters
    ----------
    matches : Iterable[Tuple[Any, int, int]]
        iterable of matches tuples[value, left_index, right_index]
        
    Returns
    -------
    Tuple[MatchIndexes]
        tuple of MatchIndexes namedtuples[Any, Optional[int], Optional[int]]
        
    Example
    -------
    >>> matches = [('a', 1, 1), ('b', 2, None), ('c', None, 3)]
    >>> name_matches(matches)
    (MatchIndexes(value='a', left_index=1, right_index=1),
     MatchIndexes(value='b', left_index=2, right_index=None),
     MatchIndexes(value='c', left_index=None, right_index=3))
    """
    return tuple(MatchIndexes(value, left_index, right_index)
        for value, left_index, right_index in matches)


def _inner_matching_indexes(
    left: Sequence,
    right: Sequence
) -> Tuple[MatchIndexes]:
    """
    Find matching item value indexes in two lists.
    Returns tuple of tuples: namedtuple[value, left_index, right_index]
    
    Parameters
    ----------
    left : Sequence
        first sequence of values
    right : Sequence
        second sequence of values
        
    Returns
    -------
    Tuple[MatchIndexes]
        tuple of namedtuple[value, left_index, right_index]
    
    Example
    -------
    >>> l1 = ['a', 'c', 'd', 'f', 'a']
    >>> l2 = ['a', 'b', 'c', 'd', 'c']
    >>> inner_matching_indexes(l1, l2)
    (MatchIndexes(value='a', left_index=0, right_index=0),
     MatchIndexes(value='c', left_index=1, right_index=2),
     MatchIndexes(value='c', left_index=1, right_index=4),
     MatchIndexes(value='d', left_index=2, right_index=3),
     MatchIndexes(value='a', left_index=4, right_index=0))
    """
    out = []
    for i, value in enumerate(left):
        right_i = locate(right, value)
        count = len(right_i)
        out.extend(_name_matches(zip(repeat(value, count), repeat(i, count), right_i)))
    return tuple(out)


def _left_matching_indexes(
    left: Sequence,
    right: Sequence
) -> Tuple[MatchIndexes]:
    """
    Find matching item value indexes in two sequences.
    Returns tuple of tuple[value, left_index, right_index].
    Also, provide (value, left_index, None) pairs for unmatched values in left.
    
    Parameters
    ----------
    left : Sequence
    right : Sequence
    
    Returns
    -------
    Tuple[namedtuple[value, left_index, right_index]]
    
    Example
    -------
    >>> l1 = ['a', 'c', 'd', 'f', 'a', 'g']
    >>> l2 = ['a', 'b', 'c', 'd', 'c']
    >>> left_matching_indexes(l1, l2)
    (MatchIndexes(value='a', left_index=0, right_index=0),
     MatchIndexes(value='c', left_index=1, right_index=2),
     MatchIndexes(value='c', left_index=1, right_index=4),
     MatchIndexes(value='d', left_index=2, right_index=3),
     MatchIndexes(value='f', left_index=3, right_index=None),
     MatchIndexes(value='a', left_index=4, right_index=0),
     MatchIndexes(value='g', left_index=5, right_index=None))
    """
    out = []
    for i, value in enumerate(left):
        right_i = locate(right, value)
        if right_i:
            count = len(right_i)
            out.extend(_name_matches(zip(repeat(value, count), repeat(i, count), right_i)))
        else:
            out.append(MatchIndexes(value, left_index=i))
    return tuple(out)


def _right_matching_indexes(
    l1: Sequence,
    l2: Sequence
) -> Tuple[MatchIndexes]:
    """
    Find matching item value indexes in two lists.
    Also, provide (value, None, right_index) pairs for unmatched values in right.
    
    Parameters
    ----------
    left : Sequence
    right : Sequence
    
    Returns
    -------
    Tuple[namedtuple[value, left_index, right_index]]
    
    Example
    -------
    >>> l1 = ['a', 'c', 'd', 'f', 'a', 'g']
    >>> l2 = ['a', 'b', 'c', 'd', 'c']
    >>> right_matching_indexes(l1, l2)
    (MatchIndexes(value='a', left_index=0, right_index=0),
     MatchIndexes(value='a', left_index=4, right_index=0),
     MatchIndexes(value='b', left_index=None, right_index=1),
     MatchIndexes(value='c', left_index=1, right_index=2),
     MatchIndexes(value='d', left_index=2, right_index=3),
     MatchIndexes(value='c', left_index=1, right_index=4))
    """
    out = []
    for i, value in enumerate(l2):
        l1_i = locate(l1, value)
        if l1_i:
            count = len(l1_i)
            out.extend(_name_matches(zip(repeat(value, count), l1_i, repeat(i, count))))
        else:
            out.append(MatchIndexes(value, right_index=i))
    return tuple(out)


def _full_matching_indexes(
    left: Sequence,
    right: Sequence
) -> Tuple[MatchIndexes]:
    """
    Find matching item value indexes in two lists.
    Also, provide (value, left_index, None) pairs for unmatched values in left.
    Also, provide (value, None, right_index) pairs for unmatched values in right.
    
    Parameters
    ----------
    left : Sequence
    right : Sequence
    
    Returns
    -------
    Tuple[namedtuple[value, left_index, right_index]]
    
    Example
    -------
    >>> l1 = ['a', 'c', 'd', 'f', 'a', 'g']
    >>> l2 = ['a', 'b', 'c', 'd', 'c']
    >>> matching_indexes(l1, l2)
    (MatchIndexes(value='a', left_index=0, right_index=0),
     MatchIndexes(value='c', left_index=1, right_index=2),
     MatchIndexes(value='c', left_index=1, right_index=4),
     MatchIndexes(value='d', left_index=2, right_index=3),
     MatchIndexes(value='f', left_index=3, right_index=None),
     MatchIndexes(value='a', left_index=4, right_index=0),
     MatchIndexes(value='g', left_index=5, right_index=None),
     MatchIndexes(value='b', left_index=None, right_index=1))
    """
    out = []
    for i, value in enumerate(left):
        right_i = locate(right, value)
        if right_i:
            count = len(right_i)
            out.extend(_name_matches(zip(repeat(value, count), repeat(i, count), right_i)))
        else:
            out.append(MatchIndexes(value, left_index=i))    
    found_rights = set(x.right_index for x in out)
    unfound_rights = [MatchIndexes(value, right_index=i) for i, value in enumerate(right)
                          if i not in found_rights]
    out.extend(unfound_rights)
    return tuple(out)


def _filter_values_by_index_matches(
    values: Sequence,
    indexes: Sequence[Union[int, None]]
) -> list:
    """
    Filter a sequence by indexes.
    Returns a list of matched index values and Nones for Nones in indexes.
    
    Parameters
    ----------
    values : Sequence
        sequence of values
    indexes : Sequence[Union[int, None]]
         sequence of indexes or Nones
         
    Returns
    -------
    list
        list of values or Nones at given indexes
    
    Example
    -------
    >>> values = ['x', 'y', 'z']
    >>> indexes = [0, None, 1, 2, None, 1]
    >>> filter_values_by_index_matches(values, indexes)
    ['x', None, 'y', 'z', None, 'y']
    """
    return [None if i is None else values[i] for i in indexes]


def _missing_col_error(col: str, table_name: str) -> ValueError:
    return ValueError(f'column {col} is missing from {table_name} table')


def _sequence_of_str(value) -> bool:
    if len(value) == 0:
        return False
    if isinstance(value, str):
        return False
    return all(isinstance(x, str) for x in value)


def _check_on_types(left_on, right_on) -> None:
    error = ValueError('right_on and left_on must both be str or sequence of str.')
    right_is_str_sequence = _sequence_of_str(right_on)
    left_is_str = isinstance(left_on, str)
    left_is_str_sequence = _sequence_of_str(left_on)
    right_is_str = isinstance(right_on, str)
    if not ((right_is_str and left_is_str) or (right_is_str_sequence and left_is_str_sequence)):
        raise error
    if left_is_str_sequence and right_is_str_sequence:
        if len(left_on) != len(right_on):
            raise ValueError('left_on sequence must be same len as right_on sequence')


def _check_for_missing_on(
    table: Mapping[str, Sequence],
    on_name: Union[str, Sequence[str]],
    table_name: str
) -> None:
    if isinstance(on_name, str):
        if on_name not in table:
            raise _missing_col_error(on_name, table_name)
    else:
        for col in on_name:
            if col not in table:
                _missing_col_error(col, table_name)


def _tuple_keys(
    table: DataMapping,
    column_names: Sequence[str]
) -> Tuple[Tuple]:
    """
    Return tuple row values for just column_names.
    
    Parameters
    ----------
    table : DataMapping
        data mapping of {column name: column values}
    column_names : Sequence[str]
        column names to include in row values tuples
        
    Returns
    -------
    tuple[tuple]
        row values tuples
        
    Example
    -------
    >>> table = {'id': [1, 2, 3, 4],
                 'x': [3, 4, 2, 1],
                 'y': [4, 3, 2, 1]}
    >>> tuple_keys(table, ['x', 'y'])
    ((3, 4), (4, 3), (2, 2), (1, 1))
    """
    return tuple(zip(*[table[col] for col in column_names]))


def _join(
    left: DataMapping,
    right: DataMapping,
    left_on: str,
    right_on: Optional[str] = None,
    select: Optional[Sequence[str]] = None,
    join_strategy: JoinStrategy = _full_matching_indexes
) -> DataDict:
    """
    Join two data mappings on a specified column name(s)
    using a join strategy (inner, left, right or full).
    Default join strategy is full outer join if no
    join_strategy is passed.
    
    Parameters
    ----------
    left : Mapping[str, Sequence]
        left data mapping of {column name: column values}
    right : Mapping[str, Sequence]
        right data mapping of {column name: column values}
    left_on : str
        column name to join on in left
    right_on : str, optional
        column name to join on in right, join on left_on if None
    select : list[str], optional
        column names to return
    join_strategy : Callable[[Sequence, Sequence], Matches]
    
    Returns
    -------
    DataDict
        resulting joined data table
    
    Examples
    --------
    >>> left = {'id': ['a', 'c', 'd', 'f', 'g'], 'x': [33, 44, 55, 66, 77]}
    >>> right = {'id': ['a', 'b', 'c', 'd'], 'y': [11, 22, 33, 44]}
    >>> join(left, right, 'id', join_strategy=inner_matching_indexes)
    {'id': ['a', 'c', 'd'], 'x': [33, 44, 55], 'y': [11, 33, 44]}
    
    >>> left = {'id': ['a', 'c', 'd', 'f', 'g'], 'x': [33, 44, 55, 66, 77]}
    >>> right = {'id': ['a', 'b', 'c', 'd'], 'y': [11, 22, 33, 44]}
    >>> join(left, right, 'id', join_strategy=left_matching_indexes)
    {'id': ['a', 'c', 'd', 'f', 'g'],
     'x': [33, 44, 55, 66, 77],
     'y': [11, 33, 44, None, None]}
    
    >>> left = {'id': ['a', 'c', 'd', 'f', 'g'], 'x': [33, 44, 55, 66, 77]}
    >>> right = {'id': ['a', 'b', 'c', 'd'], 'y': [11, 22, 33, 44]}
    >>> join(left, right, 'id')
    {'id': ['a', 'c', 'd', 'f', 'g', 'b'],
     'x': [33, 44, 55, 66, 77, None],
     'y': [11, 33, 44, None, None, 22]}
    """
    right_on = left_on if right_on is None else right_on
    _check_on_types(left_on, right_on)
    _check_for_missing_on(left, left_on, 'left')
    _check_for_missing_on(right, right_on, 'right')
    
    if isinstance(left_on, str):
        indexes = join_strategy(left[left_on], right[right_on])
    else:
        indexes = join_strategy(_tuple_keys(left, left_on), _tuple_keys(right, right_on))

    values = [x.value for x in indexes]
    left_indexes = [x.left_index for x in indexes]
    right_indexes = [x.right_index for x in indexes]
    out = {col: _filter_values_by_index_matches(left[col], left_indexes) for col in left}
    
    for col in right:
        if col not in [left_on, right_on]:
            out[col] = _filter_values_by_index_matches(right[col], right_indexes)
            
    if isinstance(right_on, str):
        out[right_on] = values
        out[left_on] = values
    else:
        for i, col in enumerate(right_on):
            out[col] = [x[i] for x in values]
        for i, col in enumerate(left_on):
            out[col] = [x[i] for x in values]
    if select:
        return {col: out[col] for col in select}
    return out


