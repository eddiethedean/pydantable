"""Regression tests for :func:`pydantable.dataframe._scan._extract_missing_scan_column_from_engine_error`.

Polars and pydantable-native error strings evolve across versions; these examples are
the ones we intentionally match for optional-column recovery on scan roots. If a
Polars upgrade changes wording, update patterns in ``dataframe/_scan.py`` and extend
this file.
"""

from __future__ import annotations

import pytest
from pydantable.dataframe._scan import _extract_missing_scan_column_from_engine_error


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        # Rust / pydantable-core style (see materialize scan fallback tests).
        ('not found: "optional_col" not found', "optional_col"),
        # Polars Python exception text (quoted).
        ("ColumnNotFoundError: 'missing_id'", "missing_id"),
        ('ColumnNotFoundError: "other"', "other"),
        ("ColumnNotFoundError: colname", "colname"),
        ("column 'foo' not found", "foo"),
        ('column "bar" not found', "bar"),
    ],
)
def test_extract_missing_column_matches_known_patterns(
    message: str, expected: str
) -> None:
    assert _extract_missing_scan_column_from_engine_error(message) == expected


def test_extract_returns_none_when_unparseable() -> None:
    assert _extract_missing_scan_column_from_engine_error("") is None
    assert _extract_missing_scan_column_from_engine_error("   ") is None
    assert (
        _extract_missing_scan_column_from_engine_error(
            "some other engine failure with no column name"
        )
        is None
    )
