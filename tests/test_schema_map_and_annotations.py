"""Coverage for schema annotation helpers and Arrow map cell coercion."""

from __future__ import annotations

from typing import Any

import pytest
from pydantable.schema import (
    is_supported_column_annotation,
    is_supported_scalar_column_annotation,
)
from pydantable.schema._impl import _map_arrow_cell_to_dict


def test_scalar_annotation_rejects_any_union_complex_and_nested_generic() -> None:
    assert is_supported_scalar_column_annotation(Any) is False
    assert is_supported_scalar_column_annotation(int | str) is False
    assert is_supported_scalar_column_annotation(list[int] | None) is False

    class _M:
        pass

    assert is_supported_scalar_column_annotation(_M) is False


def test_column_annotation_rejects_bad_collection_shapes() -> None:
    assert is_supported_column_annotation(dict[int, int]) is False
    assert is_supported_column_annotation(list) is False
    assert is_supported_column_annotation(tuple[int, str]) is False


def test_map_arrow_cell_mapping_rejects_bad_keys() -> None:
    with pytest.raises(TypeError, match="null"):
        _map_arrow_cell_to_dict({None: 1})
    with pytest.raises(TypeError, match="string keys"):
        _map_arrow_cell_to_dict({1: "a"})


def test_map_arrow_cell_sequence_rejects_bad_entries() -> None:
    with pytest.raises(TypeError, match="Invalid map entry"):
        _map_arrow_cell_to_dict([42])
    with pytest.raises(TypeError, match="Invalid map entry"):
        _map_arrow_cell_to_dict([(1,)])
    with pytest.raises(TypeError, match="string keys"):
        _map_arrow_cell_to_dict([[1, "v"]])
