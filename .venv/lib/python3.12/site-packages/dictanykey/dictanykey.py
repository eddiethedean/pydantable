from typing import Any, Iterator, Mapping, Optional, Iterable, Union, List
from dictanykey.iterables import DictItems, DictKeys, DictValues, OrderedKeys

from dictanykey.unhashmap import UnHashMap
from dictanykey.utils import quote_string


class DictAnyKey:
    """A dictionary where the keys don't need to be hashable
       Stores hashable keys with values in _hashmap: dict
       Stores unhashable keys with values in _unhashmap: UnHashMap

       Maintains order of items inserted.

       Unhashable key lookups are slower than built in dict.
       Hashable key lookups are the same speed as built in dict.
    """
    def __init__(self, data: Optional[Union[Iterable, Mapping]] = None) -> None:
        self._hashmap: dict = {}
        self._unhashmap = UnHashMap()
        self._keys = OrderedKeys()
        self.update(data)

    def __getitem__(self, key: Any) -> Any:
        if key in self._get_keys_list():
            return self.get(key)
        else:
            raise KeyError(key)
        
    def __contains__(self, value: Any) -> bool:
        return value in self._keys
    
    def __setitem__(self, key: Any, value: Any) -> None:
        try:
            self._hashmap[key] = value
        except TypeError:
            self._unhashmap[key] = value
        self._keys.add(key)

    def __len__(self) -> int:
        return len(self._get_keys_list())

    def __str__(self) -> str:
        s = ', '.join(f'{quote_string(key)}: {quote_string(value)}' for key, value in self._get_items_list())
        return '{' + f'{s}' + '}'
        
    def __delitem__(self, key: Any) -> None:
        try:
            del self._hashmap[key]
        except (KeyError, TypeError):
            del self._unhashmap[key]
        self._keys.delete(key)

    def __repr__(self) -> str:
        return f'DictAnyKey({[(key, value) for key, value in self._get_items_list()]})'

    def __iter__(self) -> Iterator:
        return iter(self._get_keys_list())

    def __eq__(self, other: Mapping) -> bool:
        if not {'__len__', '__contains__', '__getitem__'}.issubset(dir(other)):
            return False
        if len(self) != len(other):
            return False
        for key in self._get_keys_list():
            if key not in other:
                return False
            if self[key] != other[key]:
                return False
        return True

    def _get_keys_list(self) -> List[Any]:
        return list(self._keys)

    def _get_values_list(self) -> List[Any]:
        return [self[key] for key in self._get_keys_list()]

    def _get_items_list(self) -> List[tuple]:
        return [(key, self[key]) for key in self._get_keys_list()]
    
    def keys(self) -> DictKeys:
        return DictKeys(self)  # type: ignore 
  
    def values(self) -> DictValues:
        return DictValues(self)  # type: ignore 
    
    def items(self) -> DictItems:
        return DictItems(self)  # type: ignore 
    
    def get(self, key: Any, default: Optional[Any] = None) -> Any:
        """Return the value for key if key is in the dictionary, else default."""
        if key not in self._get_keys_list():
            return default
        try:
            return self._hashmap[key]
        except (KeyError, TypeError):
            try:
                return self._unhashmap[key]
            except KeyError:
                return default

    def update(self, data: Optional[Union[Iterable, Mapping]] = None) -> None:
        """Update dict from dict/iterable data.
           If data is present and has a .keys() method, then does:  for k in data: self[k] = data[k]
           If data is present and lacks a .keys() method, then does:  for k, v in data: self[k] = v
        """
        if data is None:
            return

        if isinstance(data, Mapping):
            for k in data.keys():
                self[k] = data[k]
        else:
            for k, v in data:
                self[k] = v

    def clear(self):
        """Remove all items from self."""
        self._hashmap: dict = {}
        self._unhashmap = UnHashMap()
        self._keys = OrderedKeys()

    def copy(self):
        return type(self)(self._get_items_list())

    def setdefault(self, key: Any, default: Optional[Any] = None) -> Any:
        """Insert key with a value of default if key is not in the dictionary.

            Return the value for key if key is in the dictionary, else default.
        """
        if key not in self:
            self[key] = default
            return default
        return key

    # TODO: pop method tests
    def pop(self, key: Any, default=None):
        """Docstring:
        D.pop(k[,d]) -> v, remove specified key and return the corresponding value.

        If key is not found, default is returned if given, otherwise KeyError is raised
        """
        try:
            value = self[key]
        except KeyError as e:
            if default is None:
                raise e
            else:
                return default
        del self[key]

    # TODO: popitem tests
    def popitem(self):
        """Docstring:
        Remove and return a (key, value) pair as a 2-tuple.

        Pairs are returned in LIFO (last-in, first-out) order.
        Raises KeyError if the dict is empty.
        """
        if len(self) == 0:
            raise KeyError('popitem(): dictionary is empty')
        last_key = self._get_keys_list()[-1]
        item = self[last_key]
        del self[last_key]
        return item

    # TODO: fromkeys tests
    @classmethod
    def fromkeys(cls, iterable, value: Optional[Any] = None):
        """Signature: d.fromkeys(iterable, value=None, /)
        Docstring: Create a new dictionary with keys from iterable and values set to value.
        """
        new = cls()
        for key in iterable:
            new[key] = value
        return new

