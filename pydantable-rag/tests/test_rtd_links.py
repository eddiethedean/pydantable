"""Read the Docs URL mapping for ingest ``source`` paths."""

from __future__ import annotations

import pytest

from app.rtd_links import RTD_DEFAULT_BASE, source_to_readthedocs_url


@pytest.mark.parametrize(
    ("source", "expected_suffix"),
    [
        ("README.md", "/"),
        ("docs/index.md", "/"),
        ("docs/getting-started/quickstart.md", "/getting-started/quickstart/"),
        ("docs/api/dataframe.md", "/api/dataframe/"),
        ("docs/user-guide/index.md", "/user-guide/"),
    ],
)
def test_rtd_url_known_pages(source: str, expected_suffix: str) -> None:
    url = source_to_readthedocs_url(source)
    assert url is not None
    assert url == f"{RTD_DEFAULT_BASE.rstrip('/')}{expected_suffix}"


def test_skips_build_mirrors() -> None:
    assert source_to_readthedocs_url("docs/_build/html/x.md") is None


def test_skips_non_doc_files() -> None:
    assert (
        source_to_readthedocs_url("docs/examples/foo.py.out.txt") is None
    )


def test_custom_base() -> None:
    u = source_to_readthedocs_url(
        "docs/io/csv.md",
        base="https://example.readthedocs.io/en/stable",
    )
    assert u == "https://example.readthedocs.io/en/stable/io/csv/"
