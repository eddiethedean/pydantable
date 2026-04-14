from __future__ import annotations

import os
import sys

# Make `python/` importable for autodoc.
DOCS_ROOT = os.path.abspath(os.path.dirname(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(DOCS_ROOT, ".."))
PYTHON_SRC = os.path.join(PROJECT_ROOT, "python")
PROTOCOL_SRC = os.path.join(PROJECT_ROOT, "pydantable-protocol", "python")
sys.path.insert(0, PYTHON_SRC)
sys.path.insert(0, PROTOCOL_SRC)

project = "pydantable"

try:
    import pydantable

    release = pydantable.__version__
    version = release
except Exception:
    # If the Rust extension isn't available during docs build, keep going.
    # Autodoc will still render docstrings/signatures from pure-Python code.
    version = release = "unknown"


def setup(app) -> None:
    """Expose optional SQLAlchemy types on modules for autodoc / ``get_type_hints``.

    ``Engine`` / ``Connection`` are imported only under ``TYPE_CHECKING`` in the
    library, so Sphinx would otherwise warn that forward references cannot be
    resolved. Read the Docs installs ``sqlalchemy`` (see ``.readthedocs.yaml``)
    without compiling the Rust extension.
    """
    try:
        from sqlalchemy.engine import Connection, Engine
    except ImportError:
        return
    import pydantable.dataframe_model as dataframe_model
    import pydantable.io.sql as io_sql

    dataframe_model.Engine = Engine  # type: ignore[attr-defined]
    dataframe_model.Connection = Connection  # type: ignore[attr-defined]
    io_sql.Engine = Engine  # type: ignore[attr-defined]
    io_sql.Connection = Connection  # type: ignore[attr-defined]


extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
]

templates_path = ["_templates"]
# Keep internal drafts and build artifacts out of strict (-W) CI builds.
exclude_patterns = ["_build", "async_ideas/*"]

# We commit the API `.rst` pages under `docs/api/` to keep diffs stable and avoid
# autosummary regenerating files during docs builds.
autosummary_generate = False
autodoc_typehints = "description"

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "tasklist",
    "strikethrough",
]

master_doc = "index"

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]

html_theme_options = {
    # Keep navigation discoverable by default (closer to FastAPI docs behavior).
    "collapse_navigation": False,
    # Show an on-page right-hand TOC when headings exist.
    "show_toc_level": 2,
    # Use icon links in the navbar (GitHub + PyPI).
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/eddiethedean/pydantable",
            "icon": "fa-brands fa-github",
        },
        {
            "name": "PyPI",
            "url": "https://pypi.org/project/pydantable/",
            "icon": "fa-solid fa-box",
        },
    ],
    # Improve header and sidebar UX on mobile/desktop.
    "navbar_start": ["navbar-logo"],
    "navbar_center": ["navbar-nav"],
    "navbar_end": ["search-field", "theme-switcher", "navbar-icon-links"],
    "secondary_sidebar_items": ["page-toc", "edit-this-page", "sourcelink"],
    # Prefer section-style navigation in the sidebar.
    "navigation_with_keys": True,
}

html_title = f"{project} {release} documentation"

html_css_files = ["custom.css"]

copybutton_prompt_text = r">>> |\.\.\. |\$ "
copybutton_prompt_is_regexp = True

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pydantic": ("https://docs.pydantic.dev/latest", None),
}

# The API pages are generated via autosummary; some of them may not be
# reachable from the main navigation automatically during generation.
suppress_warnings = ["toc.not_included"]
