from types import MappingProxyType
from typing import Mapping, Iterator


class View:
    def __init__(self, parent: Mapping):
        self.map = MappingProxyType(parent)

    def __len__(self) -> int:
        return len(self.map)

    
class DictKeysView(View):
    def __repr__(self) -> str:
        return repr(self.map.keys())
    
    def __iter__(self) -> Iterator:
        return iter(self.map.keys())
    
    def __contains__(self, key) -> bool:
        return key in self.map.keys()
    
    def __reversed__(self) -> Iterator:
        return reversed(self.map.keys()) # type: ignore 


class DictValuesView(View):
    def __repr__(self) -> str:
        return repr(self.map.values())
    
    def __iter__(self) -> Iterator:
        return iter(self.map.values())
    
    def __contains__(self, value) -> bool:
        return value in self.map.values()
    
    def __reversed__(self) -> Iterator:
        return reversed(self.map.keys()) # type: ignore 


class DictItemsView(View):
    def __repr__(self) -> str:
        return repr(self.map.items())
    
    def __iter__(self) -> Iterator:
        return iter(self.map.items())
    
    def __contains__(self, item) -> bool:
        return item in self.map.items()
    
    def __reversed__(self) -> Iterator:
        return reversed(self.map.keys()) # type: ignore 