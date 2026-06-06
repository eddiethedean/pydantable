"""Consolidated import / registry smoke checks (replaces scattered hasattr tests)."""

from __future__ import annotations

import pydantable.engine._binding as engine_binding
import pytest
from pydantable.plugins import get_reader, get_writer, list_readers, list_writers


def test_engine_binding_symbols_present() -> None:
    assert hasattr(engine_binding, "MISSING_SYMBOL_PREFIX")
    assert callable(engine_binding.load_rust_core)
    assert callable(engine_binding.rust_core_loaded)


def test_pyspark_lazy_sql_exports() -> None:
    pytest.importorskip("moltres_core")
    import pydantable.pyspark as ps

    assert ps.SqlDataFrame is not None
    assert ps.SqlDataFrameModel is not None


def test_pyspark_unknown_attribute_raises() -> None:
    import pydantable.pyspark as ps

    with pytest.raises(AttributeError, match="has no attribute"):
        _ = ps.not_a_real_export_  # type: ignore[attr-defined]


def test_get_reader_resolves_builtin() -> None:
    names = {p.name for p in list_readers()}
    assert "read_parquet" in names
    assert callable(get_reader("read_parquet"))


def test_plugin_registry_lists_and_unknown_errors() -> None:
    writer_names = [p.name for p in list_writers()]
    assert writer_names == sorted(writer_names)
    assert writer_names

    with pytest.raises(ValueError, match="unknown reader"):
        get_reader("not_a_registered_reader")
    with pytest.raises(ValueError, match="unknown writer"):
        get_writer("not_a_registered_writer")
