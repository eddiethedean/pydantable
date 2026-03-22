from __future__ import annotations

import os
import sys

# Make `python/` importable for autodoc.
DOCS_ROOT = os.path.abspath(os.path.dirname(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(DOCS_ROOT, ".."))
PYTHON_SRC = os.path.join(PROJECT_ROOT, "python")
sys.path.insert(0, PYTHON_SRC)

project = "pydantable"

try:
    import pydantable

    release = pydantable.__version__
    version = release
except Exception:
    # If the Rust extension isn't available during docs build, keep going.
    # Autodoc will still render docstrings/signatures from pure-Python code.
    version = release = "unknown"

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
exclude_patterns = ["_build"]

autosummary_generate = True
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

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

html_theme_options = {
    "collapse_navigation": False,
    "navigation_depth": 4,
    "titles_only": False,
    "prev_next_buttons_location": "bottom",
}

html_title = f"{project} {release} documentation"

copybutton_prompt_text = r">>> |\.\.\. |\$ "
copybutton_prompt_is_regexp = True

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pydantic": ("https://docs.pydantic.dev/latest", None),
}

# The API pages are generated via autosummary; some of them may not be
# reachable from the main navigation automatically during generation.
suppress_warnings = ["toc.not_included"]
