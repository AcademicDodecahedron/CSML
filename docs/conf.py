# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.joinpath("./_extensions")))

project = "csml-cube"
copyright = "2023, snorkysnark"
author = "snorkysnark"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx_click",
    "sphinx_typer_argument_help",
    "sphinx_immaterial",
    "sphinx_immaterial.apidoc.json.domain",
    "sphinx.ext.autosectionlabel",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_immaterial"
html_static_path = ["_static"]
html_title = "CSML Cube Importer"
html_theme_options = {"palette": {"primary": "indigo"}}

json_schemas = ["config_schema.json"]

autosectionlabel_prefix_document = True
