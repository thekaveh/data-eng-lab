from pathlib import Path

from docslib.notebooks import NOTEBOOK_SECTIONS, extract_notebook_doc

FIX = Path(__file__).resolve().parents[1] / "_fixtures" / "nb"


def test_extracts_sections_and_code():
    md = extract_notebook_doc("foo", FIX / "jupyter" / "notebook.ipynb",
                              FIX / "zeppelin" / "notebook.zpln")
    assert "# Notebooks — foo" in md
    assert "## 1. Overview" in md
    assert "## 2. Section map" in md
    assert "## 4. Scala / PySpark parity" in md
    # both languages' code present
    assert "spark = 1" in md
    assert "val s = 1" in md


def test_known_section_list():
    assert NOTEBOOK_SECTIONS[0] == "1. Overview"
    assert len(NOTEBOOK_SECTIONS) == 6
