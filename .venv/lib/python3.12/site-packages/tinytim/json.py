"""
Module used for converting data format to json and json to data format.
"""

from typing import Dict, List, Sequence
import json

import tinytim.rows as rows_functions
from tinytim.types import DataMapping, DataDict, RowMapping, RowDict


def data_to_json_list(data: DataMapping) -> List[RowDict]:
    """
    Convert data table to list of row dicts.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}

    Returns
    -------
    list[dict]
        list of row dicts

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> data_to_json_list(data)
    [{'x': 1, 'y': 6}, {'x': 2, 'y': 7}, {'x': 3, 'y': 8}]
    """
    return [row for _, row in rows_functions.iterrows(data)]


def json_list_to_data(l: Sequence[RowMapping]) -> DataDict:
    """
    Convert list of row dicts to data table format.

    Parameters
    ----------
    l : list[dict]
        list of row dicts

    Returns
    -------
    dict[str, list]
        data dict of {column name: column values}

    Example
    -------
    >>> json = [{'x': 1, 'y': 6}, {'x': 2, 'y': 7}, {'x': 3, 'y': 8}]
    >>> json_list_to_data(json)
    {'x': [1, 2, 3], 'y': [6, 7, 8]}
    """
    return rows_functions.row_dicts_to_data(l)


def data_to_json(data: DataMapping) -> str:
    """
    Convert data table to list of row dicts json string.

    Parameters
    ----------
    data : Mapping[str, Sequence]
        data mapping of {column name: column values}

    Returns
    -------
    str
        json string, list of row dicts

    Example
    -------
    >>> data = {'x': [1, 2, 3], 'y': [6, 7, 8]}
    >>> data_to_json(data)
    '[{"x": 1, "y": 6}, {"x": 2, "y": 7}, {"x": 3, "y": 8}]'
    """
    l: List[Dict] = data_to_json_list(data)
    return json.dumps(l)


def json_to_data(j: str) -> DataDict:
    """
    Convert row dicts json string to data dict table.

    Parameters
    ----------
    j : str
        json string, list of row dicts

    Returns
    -------
    Mapping[str, Sequence]
        data mapping of {column name: column values}

    Example
    -------
    >>> j = '[{"x": 1, "y": 6}, {"x": 2, "y": 7}, {"x": 3, "y": 8}]'
    >>> json_to_data(j)
    {'x': [1, 2, 3], 'y': [6, 7, 8]}
    """
    l = json.loads(j)
    return json_list_to_data(l)