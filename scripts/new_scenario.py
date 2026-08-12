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
    "1. Purpose",
    "2. Data Model",
    "3. Architecture",
    "4. Notebooks",
    "5. Orchestration",
    "6. Usage",
    "7. Dependencies",
    "8. Known Issues & Caveats",
]
NB_SECTIONS = ["1. Overview", "2. Setup", "3. Read", "4. Transform", "5. Write", "6. Verify"]


def readme_text(name: str) -> str:
    body = "\n".join(f"## {s}\n\n_TODO (Phase 2b)_\n" for s in README_SECTIONS)
    return f"# {name}\n\n> Scaffolded scenario. Fill the notebook logic in Phase 2b.\n\n{body}"


def _scala_cell(section: str, dataset: str) -> str:
    # Zeppelin `%spark` paragraphs are SCALA — use Scala placeholders.
    return {
        "2. Setup": _scala_resolver_bootstrap(dataset),
        "3. Read": "val df = spark.read.parquet(datasetUris: _*)",
        "4. Transform": "// TODO (Phase 2b): scenario transform",
        "5. Write": f'// df.writeTo("lakehouse.bronze.{dataset}").using("iceberg").createOrReplace()',
        "6. Verify": f'// spark.table("lakehouse.bronze.{dataset}").count()',
    }.get(section, "// TODO (Phase 2b)")


def _py_cell(section: str, dataset: str) -> str:
    # Jupyter cells are PySpark (Python).
    return {
        "3. Read": "df = spark.read.parquet(*dataset_uris)",
        "4. Transform": "# TODO (Phase 2b): scenario transform",
        "5. Write": f'# df.writeTo("lakehouse.bronze.{dataset}").using("iceberg").createOrReplace()',
        "6. Verify": f'# spark.table("lakehouse.bronze.{dataset}").count()',
    }.get(section, "# TODO (Phase 2b)")


def _scala_resolver_bootstrap(dataset: str) -> str:
    return f'''import java.net.{{HttpURLConnection, URL}}
import java.nio.charset.StandardCharsets
import com.fasterxml.jackson.databind.ObjectMapper
import scala.collection.JavaConverters._

val datasetScaleOverride = Option(z.input("dataset_scale", "")).filter(_.nonEmpty)
val datasetScale = datasetScaleOverride.orElse(Option(System.getenv("DATASET_SCALE"))).getOrElse("small")
require(Set("tiny", "small", "medium").contains(datasetScale), "invalid DATASET_SCALE")
val resolverUri = Option(System.getenv("DATASET_RESOLVER_URI"))
  .getOrElse(throw new IllegalArgumentException("DATASET_RESOLVER_URI is required"))
val resolverConnection = new URL(resolverUri.stripSuffix("/") + "/v1/resolve")
  .openConnection().asInstanceOf[HttpURLConnection]
resolverConnection.setRequestMethod("POST")
resolverConnection.setRequestProperty("Content-Type", "application/json")
resolverConnection.setDoOutput(true)
val resolverRequest = "{{\\\"dataset\\\":\\\"{dataset}\\\",\\\"expected_scale\\\":\\\"" + datasetScale + "\\\"}}"
resolverConnection.getOutputStream.write(resolverRequest.getBytes(StandardCharsets.UTF_8))
require(resolverConnection.getResponseCode == 200, "dataset resolution failed")
val datasetResolution = new ObjectMapper().readTree(resolverConnection.getInputStream)
val datasetResolutionFields = Set("dataset", "scale", "plan_id", "manifest_sha256", "publication_id", "objects")
require(datasetResolution.fieldNames.asScala.toSet == datasetResolutionFields)
require(datasetResolution.get("dataset").asText == "{dataset}" && datasetResolution.get("scale").asText == datasetScale)
val planId = datasetResolution.get("plan_id").asText
val publicationId = datasetResolution.get("publication_id").asText
val manifestSha256 = datasetResolution.get("manifest_sha256").asText
require(
  planId.matches("[0-9a-f]{{64}}") &&
  publicationId.matches("[0-9a-f]{{32}}") &&
  manifestSha256.matches("[0-9a-f]{{64}}")
)
val datasetObjects = datasetResolution.get("objects").elements.asScala.toVector
val datasetObjectFields = Set("object_name", "uri", "size_bytes", "sha256", "schema_id")
require(datasetObjects.forall {{ item =>
  item.fieldNames.asScala.toSet == datasetObjectFields &&
  item.get("size_bytes").isIntegralNumber && item.get("size_bytes").asLong >= 0 &&
  item.get("sha256").asText.matches("[0-9a-f]{{64}}") && item.get("schema_id").asText.nonEmpty
}})
val datasetObjectNames = datasetObjects.map(_.get("object_name").asText)
val datasetUris = datasetObjects.map(_.get("uri").asText)
require(datasetUris.nonEmpty && datasetObjectNames.distinct.size == datasetObjectNames.size)
val generationPrefix = s"s3://landing/{dataset}/_generations/$planId/$publicationId/"
require(datasetUris.zip(datasetObjectNames).forall {{ case (uri, name) => uri == generationPrefix + name }})
resolverConnection.disconnect()
spark.version'''


def _python_resolver_bootstrap(dataset: str) -> str:
    return f'''import json
import os
import re
import urllib.request
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
dataset_scale_override = globals().get("dataset_scale_override")
dataset_scale = dataset_scale_override or os.environ.get("DATASET_SCALE", "small")
if dataset_scale not in ("tiny", "small", "medium"):
    raise ValueError("invalid DATASET_SCALE")
resolver_uri = os.environ["DATASET_RESOLVER_URI"].rstrip("/")
resolver_request = urllib.request.Request(
    resolver_uri + "/v1/resolve",
    data=json.dumps(
        {{"dataset": "{dataset}", "expected_scale": dataset_scale}},
        separators=(",", ":"),
        sort_keys=True,
    ).encode(),
    headers={{"Content-Type": "application/json"}},
    method="POST",
)
with urllib.request.urlopen(resolver_request, timeout=120) as response:
    dataset_resolution = json.load(response)
if set(dataset_resolution) != {{"dataset", "scale", "plan_id", "manifest_sha256", "publication_id", "objects"}}:
    raise ValueError("dataset resolution failed")
if dataset_resolution.get("dataset") != "{dataset}" or dataset_resolution.get("scale") != dataset_scale:
    raise ValueError("dataset resolution failed")
plan_id = dataset_resolution.get("plan_id")
publication_id = dataset_resolution.get("publication_id")
manifest_sha256 = dataset_resolution.get("manifest_sha256")
if (
    not isinstance(plan_id, str)
    or not re.fullmatch(r"[0-9a-f]{{64}}", plan_id)
    or not isinstance(publication_id, str)
    or not re.fullmatch(r"[0-9a-f]{{32}}", publication_id)
    or not isinstance(manifest_sha256, str)
    or not re.fullmatch(r"[0-9a-f]{{64}}", manifest_sha256)
):
    raise ValueError("dataset resolution failed")
dataset_objects = dataset_resolution.get("objects")
if not isinstance(dataset_objects, list) or not dataset_objects:
    raise ValueError("dataset resolution failed")
dataset_object_fields = {{"object_name", "uri", "size_bytes", "sha256", "schema_id"}}
if any(
    not isinstance(item, dict)
    or set(item) != dataset_object_fields
    or isinstance(item["size_bytes"], bool)
    or not isinstance(item["size_bytes"], int)
    or item["size_bytes"] < 0
    or not re.fullmatch(r"[0-9a-f]{{64}}", item["sha256"])
    or not isinstance(item["schema_id"], str)
    or not item["schema_id"]
    for item in dataset_objects
):
    raise ValueError("dataset resolution failed")
dataset_object_names = [item.get("object_name") for item in dataset_objects]
dataset_uris = tuple(item.get("uri") for item in dataset_objects)
generation_prefix = f"s3://landing/{dataset}/_generations/{{plan_id}}/{{publication_id}}/"
if len(set(dataset_object_names)) != len(dataset_object_names) or any(
    uri != generation_prefix + name for uri, name in zip(dataset_uris, dataset_object_names, strict=True)
):
    raise ValueError("dataset resolution failed")'''


def zeppelin_notebook(name: str) -> dict:
    dataset = name.split("-")[1]
    paragraphs = []
    for sec in NB_SECTIONS:
        paragraphs.append(
            {"title": sec, "text": f"%md\n## {sec}", "config": {}, "settings": {"params": {}, "forms": {}}}
        )
        if sec != "1. Overview":
            paragraphs.append(
                {
                    "title": f"{sec} (code)",
                    "text": f"%spark\n{_scala_cell(sec, dataset)}",
                    "config": {},
                    "settings": {"params": {}, "forms": {}},
                }
            )
    return {"paragraphs": paragraphs, "name": name, "id": name, "noteParams": {}, "config": {}, "info": {}}


def jupyter_notebook(name: str) -> nbformat.NotebookNode:
    dataset = name.split("-")[1]
    nb = nbformat.v4.new_notebook()
    nb.cells.append(nbformat.v4.new_markdown_cell(f"# {name}"))
    for sec in NB_SECTIONS:
        nb.cells.append(nbformat.v4.new_markdown_cell(f"## {sec}"))
        if sec == "2. Setup":
            nb.cells.append(nbformat.v4.new_code_cell(_python_resolver_bootstrap(dataset)))
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
