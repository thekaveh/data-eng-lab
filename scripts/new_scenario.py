#!/usr/bin/env python3
"""Scaffold a conventional scenario folder (README + Zeppelin .zpln + Jupyter .ipynb + optional DAG)."""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import nbformat

NAME_RE = re.compile(r"^[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+$")

README_SECTIONS = [
    "1. Purpose", "2. Data Model", "3. Architecture", "4. Notebooks",
    "5. Orchestration", "6. Usage", "7. Dependencies", "8. Known Issues & Caveats",
]
NB_SECTIONS = ["1. Overview", "2. Setup", "3. Read", "4. Transform", "5. Write", "6. Verify"]


def readme_text(name: str) -> str:
    body = "\n".join(f"## {s}\n\n_TODO (Phase 2b)_\n" for s in README_SECTIONS)
    return f"# {name}\n\n> Scaffolded scenario. Fill the notebook logic in Phase 2b.\n\n{body}"


def _scala_cell(section: str, dataset: str) -> str:
    # Zeppelin `%spark` paragraphs are SCALA — use Scala placeholders.
    return {
        "2. Setup": "// spark is pre-bound by the Atlas Zeppelin interpreter\nspark.version",
        "3. Read": f'// val df = spark.read.parquet("s3a://landing/{dataset}/")',
        "4. Transform": "// TODO (Phase 2b): scenario transform",
        "5. Write": f'// df.writeTo("lakehouse.bronze.{dataset}").using("iceberg").createOrReplace()',
        "6. Verify": f'// spark.table("lakehouse.bronze.{dataset}").count()',
    }.get(section, "// TODO (Phase 2b)")


def _py_cell(section: str, dataset: str) -> str:
    # Jupyter cells are PySpark (Python).
    return {
        "3. Read": f'df = spark.read.parquet("s3a://landing/{dataset}/")',
        "4. Transform": "# TODO (Phase 2b): scenario transform",
        "5. Write": f'# df.writeTo("lakehouse.bronze.{dataset}").using("iceberg").createOrReplace()',
        "6. Verify": f'# spark.table("lakehouse.bronze.{dataset}").count()',
    }.get(section, "# TODO (Phase 2b)")


def zeppelin_notebook(name: str) -> dict:
    dataset = name.split("-")[1]
    paragraphs = []
    for sec in NB_SECTIONS:
        paragraphs.append({"title": sec, "text": f"%md\n## {sec}", "config": {},
                           "settings": {"params": {}, "forms": {}}})
        if sec != "1. Overview":
            paragraphs.append({"title": f"{sec} (code)", "text": f"%spark\n{_scala_cell(sec, dataset)}",
                               "config": {}, "settings": {"params": {}, "forms": {}}})
    return {"paragraphs": paragraphs, "name": name, "id": name, "noteParams": {}, "config": {}, "info": {}}


def jupyter_notebook(name: str) -> nbformat.NotebookNode:
    dataset = name.split("-")[1]
    nb = nbformat.v4.new_notebook()
    nb.cells.append(nbformat.v4.new_markdown_cell(f"# {name}"))
    for sec in NB_SECTIONS:
        nb.cells.append(nbformat.v4.new_markdown_cell(f"## {sec}"))
        if sec == "2. Setup":
            nb.cells.append(nbformat.v4.new_code_cell(
                "from pyspark.sql import SparkSession\n"
                "\n"
                'spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()'))
        elif sec != "1. Overview":
            nb.cells.append(nbformat.v4.new_code_cell(_py_cell(sec, dataset)))
    nb.metadata["language_info"] = {"name": "python"}
    nb.metadata["kernelspec"] = {"name": "python3", "display_name": "Python 3"}
    return nb


def _dag_text(name: str) -> str:
    return (
        '"""Airflow DAG for the ' + name + ' scenario (Phase 2b)."""\n'
        "from __future__ import annotations\n\n"
        "# TODO (Phase 2b): define the DAG that orchestrates this scenario.\n"
    )


def scaffold(root: Path, name: str, with_dag: bool = True) -> Path:
    if not NAME_RE.fullmatch(name):
        raise ValueError(f"scenario name '{name}' must match {NAME_RE.pattern}")
    d = Path(root) / "scenarios" / name
    if d.exists():
        raise ValueError(f"scenario '{name}' already exists at {d}")
    (d / "zeppelin").mkdir(parents=True)
    (d / "jupyter").mkdir(parents=True)
    (d / "README.md").write_text(readme_text(name), encoding="utf-8")
    zpln_text = json.dumps(zeppelin_notebook(name), indent=2) + "\n"
    (d / "zeppelin" / "notebook.zpln").write_text(zpln_text, encoding="utf-8")
    nbformat.write(jupyter_notebook(name), str(d / "jupyter" / "notebook.ipynb"))
    if with_dag:
        (d / "dag.py").write_text(_dag_text(name), encoding="utf-8")
    return d


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Scaffold a new scenario folder.")
    ap.add_argument("name", help="scenario name: <pattern>-<dataset>-<engine>-<format>")
    ap.add_argument("--no-dag", action="store_true")
    ap.add_argument("--root", default=".")
    args = ap.parse_args(argv)
    d = scaffold(Path(args.root), args.name, with_dag=not args.no_dag)
    print(f"scaffolded {d}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
