from typing import Sequence

import tinytim.data as data_functions
from tinytim.types import DataDict, DataMapping, data_dict, RowDict, RowMapping, row_dict


def is_missing(value, missing_value) -> bool:
    return value == missing_value or value is missing_value


def isnull(data: DataMapping, na_value=None) -> DataDict:
    data = data_dict(data)
    isnull_inplace(data, na_value)
    return data


def notnull(data: DataMapping, na_value=None) -> DataDict:
    data = data_dict(data)
    notnull_inplace(data, na_value)
    return data


isna = isnull
notna = notnull


def isnull_inplace(data: DataDict, na_value=None) -> None:
    for col in data_functions.column_names(data):
        column_isnull_inplace(data[col], na_value)


def notnull_inplace(data: DataDict, na_value=None) -> None:
    for col in data_functions.column_names(data):
        column_notnull_inplace(data[col], na_value)


def column_isnull(column: Sequence, na_value=None) -> list:
    column = list(column)
    column_isnull_inplace(column, na_value)
    return column


def column_notnull(column: Sequence, na_value=None) -> list:
    column = list(column)
    column_notnull_inplace(column, na_value)
    return column


def column_isnull_inplace(column: list, na_value=None) -> None:
    for i, item in enumerate(column):
        column[i] =  is_missing(item, na_value)


def column_notnull_inplace(column: list, na_value=None) -> None:
    for i, item in enumerate(column):
        column[i] = not is_missing(item, na_value)


def row_isnull(row: RowMapping, na_value=None) -> RowDict:
    row = row_dict(row)
    row_isnull_inplace(row, na_value)
    return row


def row_notnull(row: DataMapping, na_value=None) -> RowDict:
    row = row_dict(row)
    row_notnull_inplace(row, na_value)
    return row


def row_isnull_inplace(row: RowDict, na_value=None) -> None:
    for key, item in row.items():
        row[key] = is_missing(item, na_value)


def row_notnull_inplace(row: RowDict, na_value=None) -> None:
    for key, item in row.items():
        row[key] = not is_missing(item, na_value)