from typing import Iterable, Iterator, Any, Optional

from dictanykey.iterators import DictItemIterator, DictKeyIterator, DictValueIterator
from dictanykey.parent import Parent


class View:
    def __init__(self, parent: Parent) -> None:
        self.parent = parent

    def __len__(self) -> int:
        return len(self.parent._get_keys_list())


class DictKeys(View):

    def __contains__(self, key: Any) -> bool:
        return key in self.parent._get_keys_list()

    def __iter__(self) -> DictKeyIterator:
        return DictKeyIterator(self.parent)

    def __repr__(self) -> str:
        return f'DictKeys({self.parent._get_keys_list()})'

    
class DictValues(View):
    def __contains__(self, value):
        return value in self.parent._get_values_list()

    def __iter__(self):
        return DictValueIterator(self.parent)
    
    def __repr__(self):
        return f'DictValues({self.parent._get_values_list()})'
    
    
class DictItems(View):
    def __contains__(self, item):
        return item in self.parent._get_items_list()

    def __iter__(self):
        return DictItemIterator(self.parent)  
    
    def __repr__(self):
        return f'DictItems({self.parent._get_items_list()})'


class OrderedKeys:
    def __init__(self, keys: Optional[Iterable] = None) -> None:
        self.keys: list = list(keys) if keys is not None else []
        
    def add(self, key):
        if key not in self.keys:
            self.keys.append(key)
            
    def delete(self, key):
        if key in self.keys:
            i = self.keys.index(key)
            del self.keys[i]
            
    def __iter__(self) -> Iterator:
        return iter(self.keys)