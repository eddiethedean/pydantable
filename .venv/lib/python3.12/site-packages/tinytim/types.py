from typing import Any, Dict, Mapping, Sequence

DataMapping = Mapping[str, Sequence]
RowMapping = Mapping[str, Any]
DataDict = Dict[str, list]
RowDict = Dict[str, Any]


def data_dict(m: DataMapping) -> DataDict:
    return {str(col): list(values) for col, values in m.items()}


def row_dict(m: RowMapping) -> RowDict:
    return {str(col): value for col, value in m.items()}