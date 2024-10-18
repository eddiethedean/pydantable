"""
Reader Generator for reading a DataFrame.


"""
import typing as _t

import pydantic

from pydantable.results import dataframe as results


class DataFrameModelReader(results.DataFrameModelResultsGenerator):
    def __init__(
        self,
        data: _t.Generator[tuple[int, dict], None, None],
        model: pydantic.BaseModel
    ) -> None:
        self.data = data
        self.model: pydantic.BaseModel = model

    def process(self, result: _t.Mapping) -> pydantic.BaseModel:
        validated_model: pydantic.BaseModel = self.model.model_validate(result)
        return validated_model
    

