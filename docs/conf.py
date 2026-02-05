# Configuration file for the Sphinx documentation builder.
import os
import sys

# Add project root to path for autodoc
sys.path.insert(0, os.path.abspath('..'))

# -- Project information -----------------------------------------------------
project = 'YT Framework'
copyright = '2026, Gregory Koganovsky, Artem Zavarzin'
author = 'Gregory Koganovsky, Artem Zavarzin'
release = 'v0.1.0'

# -- General configuration ---------------------------------------------------
extensions = [
    'myst_parser',                  # Markdown support
    'sphinx.ext.autodoc',           # Auto-generate API docs from docstrings
    'sphinx.ext.napoleon',          # Google/NumPy docstring styles
    'sphinx.ext.viewcode',          # Add links to source code
    'sphinx.ext.intersphinx',       # Link to other project docs
    'sphinx.ext.githubpages',       # GitHub Pages support (.nojekyll)
    'sphinx_copybutton',            # Copy button for code blocks
    'sphinx_design',                # UI components (cards, tabs, etc.)
]

# MyST Parser configuration for Markdown
myst_enable_extensions = [
    "colon_fence",      # ::: for directives
    "deflist",          # Definition lists
    "html_image",       # HTML images
    "linkify",          # Auto-detect links
    "replacements",     # Smart quotes, arrows, etc.
    "smartquotes",      # Smart quotes
    "tasklist",         # GitHub-style [ ] task lists
]

# Source file suffixes
source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

# The master document
master_doc = 'index'

# List of patterns to exclude
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Options for HTML output -------------------------------------------------
html_theme = 'pydata_sphinx_theme'

html_theme_options = {
    "github_url": "https://github.com/GregoryKogan/yt-framework",
    "show_toc_level": 2,
    "navigation_depth": 4,
    "show_nav_level": 2,
    "header_links_before_dropdown": 5,
    "sidebar_secondary_items": ["page-toc"],
    "use_edit_page_button": False,
    "logo": {
        "text": "YT Framework",
    },
}

html_title = "YT Framework"
html_logo = None  # Add logo path if you create one

# Add any paths that contain custom static files
html_static_path = []

# -- Extension configuration -------------------------------------------------
# Napoleon settings for docstrings
napoleon_google_style = True
napoleon_numpy_style = True
napoleon_include_init_with_doc = True

# Intersphinx mapping (link to other project docs)
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
}

# Autodoc configuration
autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'undoc-members': True,
    'show-inheritance': True,
}
autodoc_typehints = 'description'
