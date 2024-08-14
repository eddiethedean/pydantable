from typing import Any, Iterable, Type, Union

from hasattrs.attributes import CONTAINER, HASHABLE, ITERABLE, ITERATOR, REVERSABLE
from hasattrs.attributes import GENERATOR, SIZED, CALLABLE, COLLECTION, SEQUENCE
from hasattrs.attributes import MUTABLE_SEQUENCE, BYTE_STRING, SET, MUTABLE_SET
from hasattrs.attributes import MAPPING, MUTABLE_MAPPING, MAPPING_VIEW
from hasattrs.attributes import ITEMS_VIEW, KEYS_VIEW, VALUES_VIEW, AWAITABLE, COROUTINE
from hasattrs.attributes import ASYNC_ITERABLE, ASYNC_ITERATOR, ASYNC_GENERATOR
from hasattrs.types import ABC, Typing, abc_attrs


def has_attrs(obj: Any, attrs: Iterable[str]) -> bool:
    return all(hasattr(obj, a) for a in attrs)


def has_abc_attrs(obj: Any, abc: Union[Type[ABC], Type[Typing], str]) -> bool:
    return has_attrs(obj, abc_attrs[abc])


def has_container_attrs(obj: Any) -> bool:
    return has_attrs(obj, CONTAINER)


def has_hashable_attrs(obj: Any) -> bool:
    return has_attrs(obj, HASHABLE)


def has_iterable_attrs(obj: Any) -> bool:
    return has_attrs(obj, ITERABLE)


def has_iterator_attrs(obj: Any) -> bool:
    return has_attrs(obj, ITERATOR)


def has_reversible_attrs(obj: Any) -> bool:
    return has_attrs(obj, REVERSABLE)


def has_generator_attrs(obj: Any) -> bool:
    return has_attrs(obj, GENERATOR)


def has_sized_attrs(obj: Any) -> bool:
    return has_attrs(obj, SIZED)


def has_callable_attrs(obj: Any) -> bool:
    return has_attrs(obj, CALLABLE)


def has_collection_attrs(obj: Any) -> bool:
    return has_attrs(obj, COLLECTION)


def has_sequence_attrs(obj: Any) -> bool:
    return has_attrs(obj, SEQUENCE)


def has_mutable_sequence_attrs(obj: Any) -> bool:
    return has_attrs(obj, MUTABLE_SEQUENCE)


def has_byte_string_attrs(obj: Any) -> bool:
    return has_attrs(obj, BYTE_STRING)


def has_set_attrs(obj: Any) -> bool:
    return has_attrs(obj, SET)


def has_mutable_set_attrs(obj: Any) -> bool:
    return has_attrs(obj, MUTABLE_SET)


def has_mapping_attrs(obj: Any) -> bool:
    return has_attrs(obj, MAPPING)


def has_mutable_mapping_attrs(obj: Any) -> bool:
    return has_attrs(obj, MUTABLE_MAPPING)


def has_mapping_view_attrs(obj: Any) -> bool:
    return has_attrs(obj, MAPPING_VIEW)


def has_item_view_attrs(obj: Any) -> bool:
    return has_attrs(obj, ITEMS_VIEW)


def has_keys_view_attrs(obj: Any) -> bool:
    return has_attrs(obj, KEYS_VIEW)


def has_values_view_attrs(obj: Any) -> bool:
    return has_attrs(obj, VALUES_VIEW)


def has_awaitable_attrs(obj: Any) -> bool:
    return has_attrs(obj, AWAITABLE)


def has_coroutine_attrs(obj: Any) -> bool:
    return has_attrs(obj, COROUTINE)


def has_async_iterable_attrs(obj: Any) -> bool:
    return has_attrs(obj, ASYNC_ITERABLE)


def has_async_iterator_attrs(obj: Any) -> bool:
    return has_attrs(obj, ASYNC_ITERATOR)


def has_async_generator_attrs(obj: Any) -> bool:
    return has_attrs(obj, ASYNC_GENERATOR)