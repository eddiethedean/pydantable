from typing import Any, Dict, Mapping, Sequence


DataDict = Dict[str, list]
DataMapping = Mapping[str, Sequence]
RowDict = Dict[str, Any]
RowMapping = Mapping[str, Any]


def data_dict(d: Any) -> DataDict:
    return {str(col): list(values) for col, values in d.items()}


def row_dict(r: Any) -> RowDict:
    return {str(col): values for col, values in r.items()}