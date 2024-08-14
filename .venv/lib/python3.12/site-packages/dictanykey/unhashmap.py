from typing import Any, List, Optional
from dictanykey.iterables import DictItems, DictKeys, DictValues


class UnHashMap:
    """A dictionary where the keys don't need to be hashable.
       Stores keys in _keys: list
       Stores values in _values: list

       Uses == to compare keys rather than hash function

       Much slower key lookup speeds compared to dict but
       keys don't need to be hashable.
    """
    def __init__(self, data: Optional[List[tuple]] = None) -> None:
        """Initialize self.  See help(type(self)) for accurate signature."""
        self._keys: list = []
        self._values: list = []
        if data is not None:
            for key, value in data:
                self[key] = value

    def __contains__(self, value: Any) -> bool:
        """True if the dictionary has the specified key, else False."""
        return value in self._keys
            
    def __setitem__(self, key: Any, value: Any) -> None:
        """Set self[key] to value."""
        if key not in self._keys:
            self._keys.append(key)
            self._values.append(value)
        else:
            i = self._getindex(key)
            self._values[i] = value

    def __len__(self) -> int:
        return len(self._keys)

    def __getitem__(self, key: Any) -> Any:
        if key in self._keys:
            return self.get(key)
        else:
            raise KeyError(key)
            
    def _getindex(self, key: Any) -> int:
        """Use _keys.index method to look up and return index of key.
           Raises KeyError if key is not in _keys.
        """
        try:
            return self._keys.index(key)
        except ValueError as e:
            raise KeyError(key)

    def _get_keys_list(self) -> List[Any]:
        return self._keys

    def _get_values_list(self) -> List[Any]:
        return [self[key] for key in self._get_keys_list()]

    def _get_items_list(self) -> List[tuple]:
        return [(key, self[key]) for key in self._get_keys_list()]
        
    def __delitem__(self, key: Any) -> None:
        """Delete self[key]."""
        i = self._getindex(key)
        del self._keys[i]
        del self._values[i]

    def __repr__(self) -> str:
        """Return repr(self)."""
        return f'UnHashMap({[(key, value) for key, value in self._get_items_list()]})'
    
    def keys(self) -> DictKeys:
        """Returns a set-like object providing a view on self's keys"""
        return DictKeys(self)  # type: ignore 
    
    def values(self) -> DictValues:
        """Returns an object providing a view on self's values"""
        return DictValues(self)  # type: ignore 
    
    def items(self) -> DictItems:
        """Returns set-like object providing a view on self's items"""
        return DictItems(self)  # type: ignore 
    
    def get(self, key, default: Optional[Any] = None) -> Any:
        """Return the value for key if key is in the dictionary, else default."""
        try:
            i = self._getindex(key)
        except KeyError:
            return default
        return self._values[i]