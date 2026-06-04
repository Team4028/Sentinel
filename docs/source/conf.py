# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import json
import os
import sys
from datetime import date


project = 'Sentinel'
copyright = f'{date.today().year}, sponge2'
author = 'sponge2'

build_all_docs = os.environ.get("build_all_docs")
pages_root = os.environ.get("pages_root", "")
if build_all_docs is not None:
    current_version = os.environ.get("current_version")

    with open('../versions.json', 'r') as r:
        versions = json.load(r)

    html_context = {
        "READTHEDOCS": True,
        "current_version": current_version,
        "versions": list(map(lambda x: [x, f'{pages_root}{"" if x == 'latest' else f"{x}/"}'], versions)),
    }

    html_baseurl = pages_root

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosummary",
    "sphinxext.opengraph",
    "myst_parser",
]

templates_path = ['_templates']
exclude_patterns = []

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}

ogp_social_cards = {
    "enable": True, 
}

html_css_files = ['css/rtd.css']

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

autodoc_typehints = "description"

html_favicon = "_static/favicon.ico"
html_logo = "_static/favicon.ico"
html_theme = 'furo'
pygments_style = "friendly"
html_static_path = ['_static']

sys.path.insert(0, os.path.abspath("../.."))