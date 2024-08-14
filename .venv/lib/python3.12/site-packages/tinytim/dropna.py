from typing import Any, List, Optional, Sequence, Union

import tinytim.data as data_functions
import tinytim.edit as edit_functions
import tinytim.rows as rows_functions
import tinytim.filter as filter_functions
import tinytim.isna as isna_functions
from tinytim.types import DataMapping, DataDict, RowMapping, data_dict


def dropna(
    data: DataDict,
    axis: Union[int, str] = 0,
    how: str = 'any',
    thresh: Optional[int] = None,
    subset: Optional[Sequence[str]] = None,
    inplace: bool = False,
    na_value: Optional[Any] = None,
    remaining: Optional[bool] = False
) -> Union[DataDict, None, List[int], List[str]]:
    """
    Remove missing values.

    Parameters
    ----------
    data : dict[str, List]
        data dict of {column name: column values}
    axis : {0 or 'rows', 1 or 'columns'}, default 0
        Determine if rows or columns which contain missing values are removed.
        0, or 'index' : Drop rows which contain missing values.
        1, or 'columns' : Drop columns which contain missing value.
    how : {'any', 'all'}, default 'any'
        Determine if row or column is removed from Table, when we have at least one missing or all missing.
        'any' : If any missing values are present, drop that row or column.
        'all' : If all values are missing, drop that row or column.
    thresh : int, optional
        Require that many not missing values. Cannot be combined with how.
    subset : Sequence[str]
        column names to consider when checking for row values
    inplace : bool, default False
        Whether to modify the original data rather than returning new data.
    na_value : Any, default None
        value to look for missing values
    remaining : bool, default False
        if True, return only remaining indexes/column names

    Returns
    -------
    dict[str, list] | None | List[int] | List[str]
        Data with missing values removed
        return None if inplace=True
        return list[int] of remaining indexes if remaining=True and axis=0 or 'rows'
        return list[str] of remaining column names if remaining=True and axis=1 or 'columns'

    See Also
    --------
    tinytim.fillna.fillna : fill missing values
    tinytim.isna.isna : mask of True/False if value is missing
    tinytim.isna.notna : mask of True/False if value not missing

    Examples
    --------
    >>> from tinytim.na import dropna
    >>> data = {"name": ['Alfred', 'Batman', 'Catwoman'],
                "toy": [None, 'Batmobile', 'Bullwhip'],
                "born": [None, "1940-04-25", None]}

    Drop the rows where at least one element is missing.

    >>> dropna(data)
    {'name': ['Batman'], 'toy': ['Batmobile'], 'born': ['1940-04-25']}

    Drop the columns where at least one element is missing.

    >>> dropna(data, axis='columns')
    {'name': ['Alfred', 'Batman', 'Catwoman']}

    Drop the rows where all elements are missing.

    >>> dropna(data, how='all')
    {'name': ['Alfred', 'Batman', 'Catwoman'],
     'toy': [None, 'Batmobile', 'Bullwhip'],
     'born': [None, '1940-04-25', None]}

    Keep only the rows with at least 2 non-NA values.

    >>> dropna(data, thresh=2)
    {'name': ['Batman', 'Catwoman'],
     'toy': ['Batmobile', 'Bullwhip'],
     'born': ['1940-04-25', None]}

    Define in which columns to look for missing values.

    >>> dropna(data, subset=['name', 'toy'])
    {'name': ['Batman', 'Catwoman'],
     'toy': ['Batmobile', 'Bullwhip'],
     'born': ['1940-04-25', None]}

    Keep the data with valid entries in the same variable.

    >>> dropna(data, inplace=True)
    >>> data
    {'name': ['Batman'], 'toy': ['Batmobile'], 'born': ['1940-04-25']}
    """
    if thresh is not None:
        if inplace:
            dropna_thresh_inplace(data, thresh, axis, subset, na_value)
        else:
            return dropna_thresh(data, thresh, axis, subset, na_value, remaining)
    elif how == 'any':
        if inplace:
            dropna_any_inplace(data, axis, subset, na_value)
        else:
            return dropna_any(data, axis, subset, na_value, remaining)
    elif how == 'all':
        if inplace:
            dropna_all_inplace(data, axis, subset, na_value)
        else:
            return dropna_all(data, axis, subset, na_value, remaining)


def dropna_any_inplace(
    data: DataDict,
    axis: Union[int, str] = 0,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None
) -> None:
    if axis in [1, 'columns']:
        dropna_columns_any_inplace(data, subset, na_value)
    elif axis in [0, 'rows']:
        dropna_rows_any_inplace(data, subset, na_value)
    else:
        raise ValueError('axis but be 0, 1, "columns", or "rows"')


def dropna_any(
    data: DataMapping,
    axis: Union[int, str] = 0,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None,
    remaining: Optional[bool] = False
) -> Union[DataDict, List[int], List[str]]:
    if axis in [1, 'columns']:
        return dropna_columns_any(data, subset, na_value, remaining)
    elif axis in [0, 'rows']:
        return dropna_rows_any(data, subset, na_value, remaining)
    else:
        raise ValueError('axis but be 0, 1, "columns", or "rows"')


def dropna_thresh_inplace(
    data: DataDict,
    thresh: int,
    axis: Union[int, str] = 0,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None
) -> None:
    if axis in [1, 'columns']:
        dropna_columns_thresh_inplace(data, thresh, subset, na_value)
    elif axis in [0, 'rows']:
        dropna_rows_thresh_inplace(data, thresh, subset, na_value)
    else:
        raise ValueError('axis but be 0, 1, "columns", or "rows"')


def dropna_thresh(
    data: DataMapping,
    thresh: int,
    axis: Union[int, str] = 0,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None,
    remaining: Optional[bool] = False
) -> Union[DataDict, List[int], List[str]]:
    if axis in [1, 'columns']:
        return dropna_columns_thresh(data, thresh, subset, na_value, remaining)
    elif axis in [0, 'rows']:
        return dropna_rows_thresh(data, thresh, subset, na_value, remaining)
    else:
        raise ValueError('axis but be 0, 1, "columns", or "rows"')


def dropna_columns_any_inplace(
    data: DataDict,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None
) -> None:
    columns = dropna_columns_any_names(data, subset, na_value)
    filter_functions.filter_by_columns_inplace(data, columns)


def dropna_column_any_inplace(
    data: DataDict,
    column_name: str,
    na_value: Optional[Any] = None
) -> None:
    if column_any_na(data[column_name], na_value):
        edit_functions.drop_column_inplace(data, column_name)


def dropna_columns_any_names(
    data: DataMapping,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None
) -> List[str]:
    columns = []
    for col in data_functions.column_names(data):
        if subset is not None and col not in subset:
            columns.append(col)
            continue
        if not column_any_na(data[col], na_value):
            columns.append(col)
    return columns


def dropna_column_any(
    data: DataMapping,
    column_name: str,
    na_value: Optional[Any] = None
) -> DataDict:
    data = data_dict(data)
    dropna_column_any_inplace(data, column_name, na_value)
    return data


def dropna_columns_any(
    data: DataMapping,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None,
    remaining_names: Optional[bool] = False
) -> Union[DataDict, List[str]]:
    columns = dropna_columns_any_names(data, subset, na_value)
    if remaining_names:
        return columns
    return filter_functions.filter_by_columns(data, columns)


def dropna_rows_any_inplace(
    data: DataDict,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None
) -> None:
    remaining_indexes = dropna_rows_any_indexes(data, subset, na_value)
    filter_functions.filter_by_indexes_inplace(data, remaining_indexes)
    

def dropna_rows_any_indexes(
    data: DataMapping,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None
) -> List[int]:
    remaining_indexes = []
    for i, row in rows_functions.iterrows(data):
        if not row_any_na(row, subset, na_value):
            remaining_indexes.append(i)
    return remaining_indexes


def dropna_rows_any(
    data: DataMapping,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None,
    remaining_indexes: Optional[bool] = False
) -> Union[DataDict, List[int]]:
    indexes = dropna_rows_any_indexes(data, subset, na_value)
    if remaining_indexes:
        return indexes
    return filter_functions.filter_by_indexes(data, indexes)


def column_any_na(
    column: Sequence,
    na_value: Optional[Any] = None
) -> bool:
    return any(isna_functions.is_missing(val, na_value) for val in column)


def subset_row_values(
    row: RowMapping,
    subset: Optional[Sequence[str]] = None
) -> list:
    return list(row.values()) if subset is None else [val for key, val in row.items() if key in subset]


def row_any_na(
    row: DataMapping,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None
) -> bool:
    values = subset_row_values(row, subset)
    return any(isna_functions.is_missing(val, na_value) for val in values)


def column_all_na(
    column: Sequence,
    na_value: Optional[Any] = None
) -> bool:
    return all(isna_functions.is_missing(val, na_value) for val in column)


def row_all_na(
    row: DataMapping,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None
) -> bool:
    values = subset_row_values(row, subset)
    return all(isna_functions.is_missing(val, na_value) for val in values)


def dropna_all_inplace(
    data: DataDict,
    axis: Union[int, str] = 0,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None
) -> None:
    if axis in [1, 'columns']:
        dropna_columns_all_inplace(data, subset, na_value)
    elif axis in [0, 'rows']:
        dropna_rows_all_inplace(data, subset, na_value)
    else:
        raise ValueError('axis but be 0, 1, "columns", or "rows"')


def dropna_all(
    data: DataDict,
    axis: Union[int, str] = 0,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None,
    remaining: Optional[bool] = False
) -> Union[DataDict, List[int]]:
    if axis in [1, 'columns']:
        return dropna_columns_all(data, subset, na_value)
    if axis in [0, 'rows']:
        return dropna_rows_all(data, subset, na_value, remaining)
    else:
        raise ValueError('axis but be 0, 1, "columns", or "rows"')


def dropna_columns_all_inplace(
    data: DataDict,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None
) -> None:
    columns = dropna_columns_all_names(data, subset, na_value)
    filter_functions.filter_by_columns_inplace(data, columns)


def dropna_columns_all(
    data: DataMapping,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None
) -> DataDict:
    columns = dropna_columns_all_names(data, subset, na_value)
    return filter_functions.filter_by_columns(data, columns)


def dropna_columns_all_names(
    data: DataMapping,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None
) -> List[str]:
    columns = []
    for col in data_functions.column_names(data):
        if subset is not None and col not in subset:
            columns.append(col)
            continue
        if not column_all_na(data[col], na_value):
            columns.append(col)
    return columns


def dropna_rows_all_inplace(
    data: DataDict,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None
) -> None:
    indexes = dropna_rows_all_indexes(data, subset, na_value)
    filter_functions.filter_by_indexes_inplace(data, indexes)


def dropna_rows_all(
    data: DataMapping,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None,
    remaining_indexes: Optional[bool] = False
) -> Union[DataDict, List[int]]:
    indexes = dropna_rows_all_indexes(data, subset, na_value)
    if remaining_indexes:
        return indexes
    return filter_functions.filter_by_indexes(data, indexes)


def dropna_rows_all_indexes(
    data: DataMapping,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None,
) -> List[int]:
    indexes = []
    for i, row in rows_functions.iterrows(data):
        if not row_all_na(row, subset, na_value):
            indexes.append(i)
    return indexes


def dropna_columns_thresh_inplace(
    data: DataDict,
    thresh: int,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None
) -> None:
    columns = dropna_columns_thresh_names(data, thresh, subset, na_value)
    filter_functions.filter_by_columns_inplace(data, columns)


def dropna_columns_thresh(
    data: DataMapping,
    thresh: int,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None,
    remaining_names: Optional[bool] = False
) -> Union[DataDict, List[str]]:
    columns = dropna_columns_thresh_names(data, thresh, subset, na_value)
    if remaining_names:
        return columns
    return filter_functions.filter_by_columns(data, columns)


def dropna_columns_thresh_names(
    data: DataMapping,
    thresh: int,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None
) -> List[str]:
    columns = []
    for col in data_functions.column_names(data):
        if subset is not None and col not in subset:
            columns.append(col)
            continue
        if column_na_thresh(data[col], thresh, na_value):
            columns.append(col)
    return columns


def dropna_column_thresh_inplace(
    data: DataDict,
    column_name: str,
    thresh: int,
    na_value: Optional[Any] = None
) -> None:
    if not column_na_thresh(data[column_name], thresh, na_value):
        edit_functions.drop_column_inplace(data, column_name)


def dropna_rows_thresh_inplace(
    data: DataDict,
    thresh: int,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None,
) -> None:
    indexes = dropna_rows_thresh_indexes(data, thresh, subset, na_value)
    filter_functions.filter_by_indexes_inplace(data, indexes)


def dropna_rows_thresh(
    data: DataMapping,
    thresh: int,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None,
    remaining_indexes: Optional[bool] = False
) -> Union[DataDict, List[int]]:
    indexes = dropna_rows_thresh_indexes(data, thresh, subset, na_value)
    if remaining_indexes:
        return indexes
    return filter_functions.filter_by_indexes(data, indexes)


def dropna_rows_thresh_indexes(
    data: DataMapping,
    thresh: int,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] = None
) -> List[int]:
    indexes = []
    for i, row in rows_functions.iterrows(data):
        if row_na_thresh(row, thresh, subset, na_value):
            indexes.append(i)
    return indexes


def column_na_thresh(
    column: Sequence,
    thresh: int,
    na_value: Optional[Any] = None
) -> bool:
    return sum(val != na_value for val in column) >= thresh


def row_na_thresh(
    row: RowMapping,
    thresh: int,
    subset: Optional[Sequence[str]] = None,
    na_value: Optional[Any] =None
) -> bool:
    items = row.values() if subset is None else [val for key, val in row.items() if key in subset]
    return sum(val != na_value for val in items) >= thresh

