from typing import Any, Callable, Iterable, Optional

from dictanykey.dictanykey import DictAnyKey


class DefaultDictAnyKey(DictAnyKey):
    """A dictionary that takes any key while also behaving like collections.defaultdict
       
       The default factory is called without arguments to produce
       a new value when a key is not present, in __getitem__ only.
       A defaultdict compares equal to a dict with the same items.
       All remaining arguments are treated the same as if they were
       passed to the dict constructor, including keyword arguments.
    """
    def __init__(
        self,
        default_factory: Optional[Callable] = None,
        data: Optional[Iterable] = None
    ) -> None:
        """Initialize self.  See help(type(self)) for accurate signature."""
        if default_factory is not None:
            try:
                default_factory()
            except TypeError:
                raise TypeError('first argument must be callable or None')
        super().__init__(data)
        self.default_factory = default_factory

    def __getitem__(self, key: Any) -> Any:
        try:
            return super().__getitem__(key)
        except KeyError as e:
            if self.default_factory is None:
                raise e
            self[key] = self.default_factory()
            return self[key]

    def __repr__(self) -> str:
        try:
            if self.default_factory is not None:
                name = self.default_factory.__name__
            else:
                name = None
        except AttributeError:
            name = self.default_factory
        return f'{type(self).__name__}({name}, {[(key, value) for key, value in self._get_items_list()]})'

    def copy(self):
        return type(self)(self.default_factory, self._get_items_list())