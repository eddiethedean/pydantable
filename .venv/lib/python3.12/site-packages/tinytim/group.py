from typing import Any, Callable, Dict, List, Mapping, Sequence, Tuple, Union
from statistics import mean, mode, stdev, pstdev

import tinytim.filter as filter_functions
import tinytim.utils as utils_functions
import tinytim.rows as rows_functions
from tinytim.types import DataDict, DataMapping, RowDict, RowMapping


RowDict = Dict[str, Any]
GroupbyValue = Union[Any, tuple]
Group = Tuple[GroupbyValue, DataDict]


def groupby(
    data: DataMapping,
    by: Union[str, Sequence[str]]
) -> List[Group]:
    """
    Group data by a column or sequence of columns.

    Parameters
    ----------
    data : Mapping[str, Sequence]
    by : str | Sequence[str]
        column name/s to group by

    Returns
    -------
    list[tuple[Any, dict[str, list]]]

    Examples
    --------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}

    Group data by a column.

    >>> groupby(data, 'Animal')
    [('Falcon', {'Animal': ['Falcon', 'Falcon'],
                 'Color': ['Brown', 'Brown'],
                 'Max Speed': [380, 370]}),
    ('Parrot', {'Animal': ['Parrot', 'Parrot'],
                'Color': ['Blue', 'Red'],
                'Max Speed': [24, 26]})]

    Group data by sequence of columns.

    >>> groupby(data, ['Animal', 'Color'])
    [(('Falcon', 'Brown'), {'Animal': ['Falcon', 'Falcon'],
                            'Color': ['Brown', 'Brown'],
                            'Max Speed': [380, 370]}),
     (('Parrot', 'Blue'), {'Animal': ['Parrot'],
                           'Color': ['Blue'],
                           'Max Speed': [24]}),
     (('Parrot', 'Red'), {'Animal': ['Parrot'],
                          'Color': ['Red'],
                          'Max Speed': [26]})]
    """
    if isinstance(by, str):
        return groupbyone(data, by)
    else:
        return groupbymulti(data, by)


def groupbycolumn(data: Mapping, column: Sequence) -> List[Group]:
    keys = utils_functions.uniques(column)
    return [(k, filter_functions.filter_data(data,
                                             filter_functions.column_filter(column, lambda x: x == k)))
                for k in keys]


def groupbyone(data: Mapping, column_name: str) -> List[Group]:
    return groupbycolumn(data, data[column_name])


def groupbymulti(data: Mapping, column_names: Sequence[str]) -> List[Group]:
    return groupbycolumn(data, utils_functions.row_value_tuples(data, column_names))


def aggregate_groups(
    groups: Sequence[Group],
    func: Callable[[DataMapping], RowMapping]
) -> Tuple[List, DataDict]:
    labels = []
    rows = []
    for key, data in groups:
        row = func(data)
        if len(row):
            labels.append(key)
            rows.append(row)
    return labels, rows_functions.row_dicts_to_data(rows)


def sum_groups(groups: List[Group]) -> Tuple[List, DataDict]:
    """
    Sum groups together

    Parameters
    ----------
    groups : list[tuple]
        list of groups to sum

    Returns
    -------
    tuple[list, dict]
        tuple of groupby values, data dict of sums

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> groups = groupby(data, 'Animal')
    >>> sum_groups(groups)
    (['Falcon', 'Parrot'], {'Max Speed': [750, 50]})
    """
    return aggregate_groups(groups, sum_data)


def count_groups(groups: List[tuple]) -> Tuple[List, DataDict]:
    """
    Get count of each group

    Parameters
    ----------
    groups : list[tuple]
        list of groups to count

    Returns
    -------
    tuple[list, dict]
        tuple of groupby values, data dict of counts

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> groups = groupby(data, 'Animal')
    >>> count_groups(groups)
    (['Falcon', 'Parrot'],
     {'Animal': [2, 2], 'Color': [2, 2], 'Max Speed': [2, 2]})
    """
    return aggregate_groups(groups, count_data)


def mean_groups(groups: List[tuple]) -> Tuple[List, DataDict]:
    """
    Mean groups together

    Parameters
    ----------
    groups : list[tuple]
        list of groups to mean

    Returns
    -------
    tuple[list, dict]
        tuple of groupby values, data dict of means

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> groups = groupby(data, 'Animal')
    >>> mean_groups(groups)
    (['Falcon', 'Parrot'], {'Max Speed': [375, 25]})
    """
    return aggregate_groups(groups, mean_data)


def min_groups(groups: List[tuple]) -> Tuple[List, DataDict]:
    """
    Get min values for each group

    Parameters
    ----------
    groups : list[tuple]
        list of groups to get min values from

    Returns
    -------
    tuple[list, dict]
        tuple of groupby values, data dict of min values

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> groups = groupby(data, 'Animal')
    >>> min_groups(groups)
    (['Falcon', 'Parrot'],
     {'Animal': ['Falcon', 'Parrot'],
     'Color': ['Brown', 'Blue'],
     'Max Speed': [370, 24]})
    """
    return aggregate_groups(groups, min_data)


def max_groups(groups: List[tuple]) -> Tuple[List, DataDict]:
    """
    Get max values for each group

    Parameters
    ----------
    groups : list[tuple]
        list of groups to get max values from

    Returns
    -------
    tuple[list, dict]
        tuple of groupby values, data dict of max values

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> groups = groupby(data, 'Animal')
    >>> max_groups(groups)
    (['Falcon', 'Parrot'],
     {'Animal': ['Falcon', 'Parrot'],
     'Color': ['Brown', 'Red'],
     'Max Speed': [380, 26]})
    """
    return aggregate_groups(groups, max_data)


def mode_groups(groups: List[tuple]) -> Tuple[List, DataDict]:
    """
    Get mode of each group

    Parameters
    ----------
    groups : list[tuple]
        list of groups to get mode from

    Returns
    -------
    tuple[list, dict]
        tuple of groupby values, data dict of mode values

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> groups = groupby(data, 'Animal')
    >>> mode_groups(groups)
    (['Falcon', 'Parrot'],
     {'Animal': ['Falcon', 'Parrot'],
     'Color': ['Brown', 'Blue'],
     'Max Speed': [380, 24]})
    """
    return aggregate_groups(groups, mode_data)


def stdev_groups(groups: List[tuple]) -> Tuple[List, DataDict]:
    """
    Get Standard Deviation of each group

    Parameters
    ----------
    groups : list[tuple]
        list of groups to get standard deviation from

    Returns
    -------
    tuple[list, dict]
        tuple of groupby values, data dict of standard deviations

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> groups = groupby(data, 'Animal')
    >>> stdev_groups(groups)
    (['Falcon', 'Parrot'], {'Max Speed': [7.0710678118654755, 1.4142135623730951]})
    """
    return aggregate_groups(groups, stdev_data)


def pstdev_groups(groups: List[tuple]) -> Tuple[List, DataDict]:
    """
    Get standard deviation from an entire population of each group

    Parameters
    ----------
    groups : list[tuple]
        list of groups to get standard deviation of population from

    Returns
    -------
    tuple[list, dict]
        tuple of groupby values, data dict of standard deviations of population

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> groups = groupby(data, 'Animal')
    >>> pstdev_groups(groups)
    (['Falcon', 'Parrot'], {'Max Speed': [5.0, 1.0]})
    """
    return aggregate_groups(groups, pstdev_data)


def nunique_groups(groups: List[tuple]) -> Tuple[List, DataDict]:
    """
    Count how many unique values are in each group

    Parameters
    ----------
    groups : list[tuple]
        list of groups to get unique values counts from

    Returns
    -------
    tuple[list, dict]
        tuple of groupby values, data dict of unique values counts

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> groups = groupby(data, 'Animal')
    >>> nunique_groups(groups)
    (['Falcon', 'Parrot'],
     {'Animal': [1, 1], 'Color': [1, 2], 'Max Speed': [2, 2]})
    """
    return aggregate_groups(groups, nunique_data)


def aggregate_data(data: Mapping, func: Callable) -> RowDict:
    out = {}
    for column_name in data.keys():
        try:
            col_sum = func(data[column_name])
        except TypeError:
            continue
        out[column_name] = col_sum
    return out


def sum_data(data: Mapping) -> RowDict:
    """
    Sum each column in data mapping if able

    Parameters
    ----------
    data : Mapping[str, Sequence]

    Returns
    -------
    dict[str, Any]
        {column name: sum of values}

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> sum_data(data)
    {'Max Speed': 800}
    """
    return aggregate_data(data, sum)


def count_data(data: Mapping) -> RowDict:
    """
    Count each column in data mapping if able

    Parameters
    ----------
    data : Mapping[str, Sequence]

    Returns
    -------
    dict[str, Any]
        {column name: values count}

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> count_data(data)
    {'Animal': 4, 'Color': 4, 'Max Speed': 4}
    """
    return aggregate_data(data, len)


def mean_data(data: Mapping) -> RowDict:
    """
    Calculate mean of each column in data mapping if able

    Parameters
    ----------
    data : Mapping[str, Sequence]

    Returns
    -------
    dict[str, Any]
        {column name: mean of values}

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> mean_data(data)
    {'Max Speed': 200}
    """
    return aggregate_data(data, mean)


def min_data(data: Mapping) -> RowDict:
    """
    Calculate min value of each column in data mapping if able

    Parameters
    ----------
    data : Mapping[str, Sequence]

    Returns
    -------
    dict[str, Any]
        {column name: min value}

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> min_data(data)
    {'Animal': 'Falcon', 'Color': 'Blue', 'Max Speed': 24}
    """
    return aggregate_data(data, min)


def max_data(data: Mapping) -> RowDict:
    """
    Calculate max value of each column in data mapping if able

    Parameters
    ----------
    data : Mapping[str, Sequence]

    Returns
    -------
    dict[str, Any]
        {column name: max value}

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> max_data(data)
    {'Animal': 'Parrot', 'Color': 'Red', 'Max Speed': 380}
    """
    return aggregate_data(data, max)


def mode_data(data: Mapping) -> RowDict:
    """
    Calculate mode of each column in data mapping if able

    Parameters
    ----------
    data : Mapping[str, Sequence]

    Returns
    -------
    dict[str, Any]
        {column name: mode of values}

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> mode_data(data)
    {'Animal': 'Falcon', 'Color': 'Brown', 'Max Speed': 380}
    """
    return aggregate_data(data, mode)


def stdev_data(data: Mapping) -> RowDict:
    """
    Calculate standard deviation of each column in data mapping if able

    Parameters
    ----------
    data : Mapping[str, Sequence]

    Returns
    -------
    dict[str, Any]
        {column name: standard deviation of values}

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> stdev_data(data)
    {'Max Speed': 202.1154785430019}
    """
    return aggregate_data(data, stdev)


def pstdev_data(data: Mapping) -> RowDict:
    """
    Calculate population standard deviation of each column in data mapping if able

    Parameters
    ----------
    data : Mapping[str, Sequence]

    Returns
    -------
    dict[str, Any]
        {column name: population standard deviation of values}

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> pstdev_data(data)
    {'Max Speed': 175.03713891628828}
    """
    return aggregate_data(data, pstdev)


def nunique_data(data: Mapping) -> RowDict:
    """
    Calculate count of unique values for each column in data mapping if able

    Parameters
    ----------
    data : Mapping[str, Sequence]

    Returns
    -------
    dict[str, Any]
        {column name: count of unique values}

    Example
    -------
    >>> data = {'Animal': ['Falcon', 'Falcon', 'Parrot', 'Parrot'],
                'Color': ['Brown', 'Brown', 'Blue', 'Red'],
                'Max Speed': [380, 370, 24, 26]}
    >>> nunique_data(data)
    {'Animal': 2, 'Color': 3, 'Max Speed': 4}
    """
    return aggregate_data(data, utils_functions.nuniques)