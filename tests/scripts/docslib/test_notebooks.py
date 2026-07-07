from pathlib import Path

from docslib.notebooks import NOTEBOOK_SECTIONS, extract_notebook_doc

FIX = Path(__file__).resolve().parents[1] / "_fixtures" / "nb"


def test_extracts_scala_and_pyspark_code_under_real_title_convention():
    """Real Zeppelin code paragraphs are titled 'N. Section (code)'. The
    extractor must normalize that so Scala code is NOT dropped, and must not
    emit empty fences, leaked source '## N.' headers, or doc numbering that
    starts at '## 2.'"""
    md = extract_notebook_doc("foo", FIX / "jupyter" / "notebook.ipynb",
                              FIX / "zeppelin" / "notebook.zpln")
    assert "# Notebooks — foo" in md
    # doc sections numbered from 1 (was starting at '## 2.')
    assert "## 1. Section map" in md
    assert "## 2. Walkthrough" in md
    assert "## 3. Scala / PySpark parity" in md
    assert "## 4. How to run" in md
    # walkthrough subsections numbered HIERARCHICALLY under '## 2.' (2.1, 2.2, …),
    # not the notebook's flat 2./3./4. used verbatim (which collided with doc sections)
    assert "### 2.1 Setup" in md
    assert "### 2.2 Read" in md
    assert "### 2. Setup" not in md  # old flat form must be gone
    assert "| 2.1 Setup |" in md     # section map cross-references the subsection
    # BOTH languages' code present — Scala ('val s = 1') was being dropped
    assert "val s = 1" in md
    assert "spark = 1" in md
    assert "val rows = spark.read.parquet" in md
    assert "df = spark.read.parquet" in md
    # NO empty code fences
    assert "```scala\n\n```" not in md
    assert "```python\n\n```" not in md
    # NO leaked source '## N. Section' H2 headers. (The generated subsection is
    # '### N. Section'; check real H2 lines so '### 2. Setup' — which contains
    # '## 2. Setup' as a substring — isn't mistaken for a leak.)
    h2 = [line for line in md.splitlines() if line.startswith("## ") and not line.startswith("### ")]
    assert "## 2. Setup" not in h2
    assert "## 3. Read" not in h2


def test_known_section_list():
    assert NOTEBOOK_SECTIONS[0] == "1. Overview"
    assert len(NOTEBOOK_SECTIONS) == 6
