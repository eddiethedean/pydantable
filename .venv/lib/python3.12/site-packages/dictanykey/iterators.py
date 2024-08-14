from typing import Any, Iterator

from dictanykey.parent import Parent


class DictKeyIterator:
    def __init__(self, parent: Parent) -> None:
        self.parent = parent
        self.len: int = len(parent)
        self.iterator: Iterator = iter(parent._get_keys_list())

    def __iter__(self):
        return self

    def __next__(self) -> Any:
        if len(self.parent) != self.len:
            raise RuntimeError('dictionary changed size during iteration')
        return next(self.iterator)
    
    
class DictValueIterator:
    def __init__(self, parent: Parent) -> None:
        self.parent = parent
        self.len: int = len(parent)
        self.iterator: Iterator = iter(parent._get_values_list())

    def __iter__(self):
        return self

    def __next__(self) -> Any:
        if len(self.parent) != self.len:
            raise RuntimeError('dictionary changed size during iteration')
        return next(self.iterator)
    
    
class DictItemIterator:
    def __init__(self, parent: Parent):
        self.parent = parent
        self.len: int = len(parent)
        self.iterator: Iterator = iter(parent._get_items_list())

    def __iter__(self):
        return self

    def __next__(self) -> Any:
        if len(self.parent) != self.len:
            raise RuntimeError('dictionary changed size during iteration')
        return next(self.iterator)