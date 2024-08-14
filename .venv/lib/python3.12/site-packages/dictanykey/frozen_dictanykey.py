from typing import Any, Iterable, Mapping, Optional, Union

from dictanykey.dictanykey import DictAnyKey
from dictanykey.iterables import OrderedKeys
from dictanykey.unhashmap import UnHashMap


class FrozenDictAnyKey(DictAnyKey):
    """A DictAnyKey that cannot be edited."""
    def __init__(self, data: Optional[Union[Iterable, Mapping]] = None) -> None:
        self._hashmap: dict = {}
        self._unhashmap = UnHashMap()
        self._keys = OrderedKeys()
        if data is None:
            return
        if isinstance(data, Mapping):
            for k in data.keys():
                super().__setitem__(k, data[k])
        else:
            for k, v in data:
                super().__setitem__(k, v)

    def __setitem__(self, key: Any, value: Any) -> None:
        raise TypeError(f"'{self.__class__.__name__}' object doesn't support item assignment")

    def __delitem__(self, key: Any) -> None:
        raise TypeError(f"'{self.__class__.__name__}' object doesn't support item deletion")

    def clear(self) -> None:
        raise AttributeError(f"'{self.__class__.__name__}' object is read-only")

    def delete(self, key: Any) -> None:
        raise AttributeError(f"'{self.__class__.__name__}' object is read-only")

    def pop(self, key: Any) -> Any:
        raise AttributeError(f"'{self.__class__.__name__}' object is read-only")

    def popitem(self) -> Any:
        raise AttributeError(f"'{self.__class__.__name__}' object is read-only")

    def setdefault(self, key: Any, default: Optional[Any] = None) -> Any:
        raise AttributeError(f"'{self.__class__.__name__}' object is read-only")

    def update(self, data: Optional[Union[Iterable, Mapping]] = None) -> None:
        raise AttributeError(f"'{self.__class__.__name__}' object is read-only")