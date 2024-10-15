import typing as _t

from pydantable.results import dicts as results

TransformFunction = _t.Callable[[_t.Mapping], dict]


class DictTransformer(results.MappingResultsGenerator):
    def __init__(
        self,
        data: _t.Iterator[results.MappingResult],
        transformer_function: TransformFunction,
    ) -> None:
        self.data: _t.Iterator[results.MappingResult] = data
        self.transformer_function: TransformFunction = transformer_function
        
    def process(self, result: _t.Mapping) -> dict:
        return self.transformer_function(result)
        

