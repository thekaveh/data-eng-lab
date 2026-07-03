"""Assemble a scenario's notebooks from per-section code maps (Scala + PySpark)."""
from __future__ import annotations

import json
from pathlib import Path

import nbformat

NB_SECTIONS = ["1. Overview", "2. Setup", "3. Read", "4. Transform", "5. Write", "6. Verify"]


def build_zeppelin(name: str, code: dict, interpreter: str = "%spark") -> dict:
    paragraphs = []
    for sec in NB_SECTIONS:
        paragraphs.append({"title": sec, "text": f"%md\n## {sec}",
                           "config": {}, "settings": {"params": {}, "forms": {}}})
        if sec != "1. Overview":
            paragraphs.append({"title": f"{sec} (code)", "text": f"{interpreter}\n{code[sec]}",
                               "config": {}, "settings": {"params": {}, "forms": {}}})
    return {"paragraphs": paragraphs, "name": name, "id": name,
            "noteParams": {}, "config": {}, "info": {}}


def build_jupyter(name: str, py: dict) -> nbformat.NotebookNode:
    nb = nbformat.v4.new_notebook()
    nb.cells.append(nbformat.v4.new_markdown_cell(f"# {name}"))
    for sec in NB_SECTIONS:
        nb.cells.append(nbformat.v4.new_markdown_cell(f"## {sec}"))
        if sec != "1. Overview":
            nb.cells.append(nbformat.v4.new_code_cell(py[sec]))
    nb.metadata["kernelspec"] = {"name": "python3", "display_name": "Python 3"}
    nb.metadata["language_info"] = {"name": "python"}
    return nb


def write_scenario(root: Path, name: str, code: dict, py: dict, dag: str, readme: str,
                   zeppelin_interpreter: str = "%spark") -> Path:
    d = Path(root) / "scenarios" / name
    (d / "zeppelin").mkdir(parents=True, exist_ok=True)
    (d / "jupyter").mkdir(parents=True, exist_ok=True)
    (d / "README.md").write_text(readme, encoding="utf-8")
    (d / "zeppelin" / "notebook.zpln").write_text(
        json.dumps(build_zeppelin(name, code, zeppelin_interpreter), indent=2) + "\n", encoding="utf-8")
    nbformat.write(build_jupyter(name, py), str(d / "jupyter" / "notebook.ipynb"))
    (d / "dag.py").write_text(dag, encoding="utf-8")
    return d
