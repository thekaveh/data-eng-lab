"""Source-backed contracts for public Spark-application documentation."""

from __future__ import annotations

import ast
import html
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
import yaml

from scripts.docs.manifest import iter_leaf_sections, load_manifest

ROOT = Path(__file__).resolve().parents[1]
POM_NAMESPACE = {"m": "http://maven.apache.org/POM/4.0.0"}
PORTABILITY_TOKENS = (
    '<div class="grid cards"',
    ":material-",
    ":octicons-",
    "!!! ",
    "=== ",
)
HERO_H1 = '<h1 align="center">data-eng-lab</h1>'
HERO_TAGLINE_TEXT = "An Iceberg-lakehouse data-engineering lab built on the Atlas platform."
HERO_VALUE_PROPOSITION = (
    "Build, orchestrate, stream, and query production-shaped lakehouse pipelines "
    "from paired notebooks and deployable Spark applications."
)
HERO_BANNER_PATH = "diagrams/img/data-eng-lab-hero.png"
HERO_BANNER_ALT = (
    "Abstract data-eng-lab lakehouse with Iceberg crystal, medallion layers, and flowing data"
)
HERO_BADGE_ROWS = (
    (
        ("Atlas", "https://img.shields.io/badge/Atlas-infrastructure-2563EB?logo=git&logoColor=white"),
        (
            "Docker Compose",
            "https://img.shields.io/badge/"
            "Docker%20Compose-runtime-2496ED?logo=docker&logoColor=white",
        ),
    ),
    (
        (
            "Apache Spark",
            "https://img.shields.io/badge/"
            "Apache%20Spark-compute-E25A1C?logo=apachespark&logoColor=white",
        ),
        (
            "Apache Iceberg",
            "https://img.shields.io/badge/"
            "Apache%20Iceberg-tables-4F46E5?logo=apache&logoColor=white",
        ),
        (
            "MinIO",
            "https://img.shields.io/badge/"
            "MinIO-object%20storage-C72E49?logo=minio&logoColor=white",
        ),
        ("Trino", "https://img.shields.io/badge/Trino-SQL-DD00A1?logo=trino&logoColor=white"),
        (
            "Redpanda",
            "https://img.shields.io/badge/"
            "Redpanda-streaming-FF4D5B?logo=redpanda&logoColor=white",
        ),
    ),
    (
        (
            "Apache Airflow",
            "https://img.shields.io/badge/"
            "Apache%20Airflow-orchestration-017CEE?logo=apacheairflow&logoColor=white",
        ),
        (
            "Jenkins",
            "https://img.shields.io/badge/Jenkins-CI-D24939?logo=jenkins&logoColor=white",
        ),
        (
            "Maven",
            "https://img.shields.io/badge/Maven-builds-C71A36?logo=apachemaven&logoColor=white",
        ),
        (
            "Jupyter",
            "https://img.shields.io/badge/Jupyter-notebooks-F37626?logo=jupyter&logoColor=white",
        ),
        (
            "Zeppelin",
            "https://img.shields.io/badge/"
            "Zeppelin-notebooks-FBBF24?logo=apache&logoColor=white",
        ),
    ),
)
HERO_EXECUTIVE_SUMMARY = (
    "`data-eng-lab` consumes Atlas as its pinned `infra/` git submodule through "
    "`atlas.consumer.yml`, so `make up` launches the default development profile as the "
    "**Data Engineering** workspace. It pairs 19 Zeppelin and Jupyter scenario notebooks—17 "
    "Scala/PySpark implementations plus two Trino client pairs—with Iceberg on MinIO, Airflow, "
    "Jenkins-built Spark apps, Trino, and Redpanda for three broker-backed streams."
)
ARCHITECTURE_LEAD = (
    "## 2. Architecture\n\n"
    "The landing zone and the three Iceberg medallion layers are distinct storage stages:\n\n"
    "```text\n"
    "s3a://landing/  →  bronze  →  silver  →  gold\n"
    "raw source data    clean      enriched    aggregated/modelled\n"
    "```\n\n"
)


def _public_sources() -> tuple[Path, ...]:
    manifest = load_manifest(ROOT / "docs/manifest.yaml", ROOT)
    return tuple(
        ROOT / section.source
        for section in iter_leaf_sections(manifest.sections)
        if section.source is not None
    )


def _opener_parts(path: Path) -> tuple[str, str, str]:
    text = path.read_text(encoding="utf-8")
    h1 = re.search(r'<h1 align="center">[^<]+</h1>', text)
    tagline = re.search(
        r'<p align="center">\s*<strong>(.*?)</strong>\s*</p>',
        text,
        re.DOTALL,
    )
    value = re.search(
        r'<p align="center">\s*'
        r'(Build, orchestrate, stream, and query .*?)\s*</p>',
        text,
        re.DOTALL,
    )
    assert h1 is not None and tagline is not None and value is not None
    plain_tagline = re.sub(r"<[^>]+>", "", html.unescape(tagline.group(1)))
    return h1.group(0), plain_tagline.strip(), " ".join(value.group(1).split())


def _opener_block(path: Path) -> str:
    opener, separator, _ = path.read_text(encoding="utf-8").partition("\n## 1. Quick start")
    assert separator
    return opener


def _badge_row(pairs: tuple[tuple[str, str], ...]) -> str:
    images = "\n".join(f'  <img alt="{name}" src="{url}">' for name, url in pairs)
    return f'<p align="center">\n{images}\n</p>'


def _expected_opener() -> str:
    banner = (
        '<p align="center">\n'
        f'  <img src="{HERO_BANNER_PATH}" alt="{HERO_BANNER_ALT}" width="100%">\n'
        "</p>"
    )
    tagline = (
        '<p align="center">\n'
        '  <strong>An Iceberg-lakehouse data-engineering lab built on the '
        '<a href="https://github.com/thekaveh/atlas">Atlas</a> platform.</strong>\n'
        "</p>"
    )
    value = f'<p align="center">\n  {HERO_VALUE_PROPOSITION}\n</p>'
    return (
        "\n\n".join(
            (
                banner,
                HERO_H1,
                tagline,
                value,
                *(_badge_row(row) for row in HERO_BADGE_ROWS),
                HERO_EXECUTIVE_SUMMARY,
            )
        )
        + "\n"
    )


def _executive_summary(opener: str) -> str:
    match = re.search(r"(?m)^(`data-eng-lab` consumes Atlas .+)$", opener)
    assert match is not None
    return match.group(1)


def _opening_fences(text: str) -> tuple[str, ...]:
    openings: list[str] = []
    marker = ""
    width = 0
    for line in text.splitlines():
        match = re.match(r"^[ \t]*(`{3,}|~{3,})(.*)$", line)
        if match is None:
            continue
        run, info = match.groups()
        if marker:
            if run[0] == marker and len(run) >= width and not info.strip():
                marker = ""
                width = 0
            continue
        marker = run[0]
        width = len(run)
        openings.append(info.strip())
    return tuple(openings)


def _outside_fences(text: str) -> str:
    visible: list[str] = []
    marker = ""
    width = 0
    for line in text.splitlines(keepends=True):
        match = re.match(r"^[ \t]*(`{3,}|~{3,})(.*)$", line.rstrip("\r\n"))
        if match is not None:
            run, info = match.groups()
            if not marker:
                marker = run[0]
                width = len(run)
            elif run[0] == marker and len(run) >= width and not info.strip():
                marker = ""
                width = 0
            visible.append("\n" if line.endswith("\n") else "")
            continue
        visible.append(("\n" if line.endswith("\n") else "") if marker else line)
    return "".join(visible)


def test_opener_is_centered_badged_and_identical_across_canonical_surfaces():
    readme = ROOT / "README.md"
    index = ROOT / "docs/index.md"
    readme_opener = _opener_block(readme)
    index_opener = _opener_block(index)
    readme_parts = _opener_parts(readme)
    index_parts = _opener_parts(index)

    assert readme_opener.count("docs/diagrams/img/data-eng-lab-hero.png") == 1
    assert index_opener.count(HERO_BANNER_PATH) == 1
    normalized_readme_opener = readme_opener.replace(
        "docs/diagrams/img/data-eng-lab-hero.png", HERO_BANNER_PATH
    )
    assert normalized_readme_opener == index_opener == _expected_opener()
    assert readme_parts == index_parts == (
        HERO_H1,
        HERO_TAGLINE_TEXT,
        HERO_VALUE_PROPOSITION,
    )
    manifest = yaml.safe_load((ROOT / "atlas.consumer.yml").read_text(encoding="utf-8"))
    assert manifest["brand"]["tagline"] == HERO_TAGLINE_TEXT

    for path, opener, overview_path in (
        (readme, readme_opener, "docs/diagrams/img/overview.png"),
        (index, index_opener, "diagrams/img/overview.png"),
    ):
        text = path.read_text(encoding="utf-8")
        badge_rows = tuple(
            block
            for block in re.findall(r'<p align="center">.*?</p>', opener, re.DOTALL)
            if "img.shields.io/badge/" in block
        )
        badge_pairs = tuple(
            tuple(re.findall(r'<img alt="([^"]+)" src="([^"]+)">', row))
            for row in badge_rows
        )
        assert badge_pairs == HERO_BADGE_ROWS
        assert all("?logo=" in url for row in badge_pairs for _, url in row)
        assert badge_rows == tuple(_badge_row(row) for row in HERO_BADGE_ROWS)

        ordered_parts = (
            opener.index("data-eng-lab-hero.png"),
            opener.index(HERO_H1),
            opener.index("<strong>"),
            opener.index(HERO_VALUE_PROPOSITION),
            *(opener.index(row) for row in badge_rows),
            opener.index(HERO_EXECUTIVE_SUMMARY),
        )
        assert ordered_parts == tuple(sorted(ordered_parts))
        assert "| Platform |" not in opener
        opener_images = re.findall(r'<img\s+([^>]+)>', opener)
        assert opener_images
        assert all(re.search(r'\balt="[^"]+"', attributes) for attributes in opener_images)
        architecture = text[text.index("## 2. Architecture") :]
        assert architecture.startswith(
            ARCHITECTURE_LEAD + f"![data-eng-lab architecture]({overview_path})\n\n"
        )

    executive_summary = _executive_summary(readme_opener)
    assert executive_summary == _executive_summary(index_opener) == HERO_EXECUTIVE_SUMMARY
    for required in (
        "atlas.consumer.yml",
        "make up",
        "Data Engineering",
        "development profile",
        "Jupyter",
        "Zeppelin",
        "17 Scala/PySpark",
        "two Trino client pairs",
        "Airflow",
        "Jenkins",
        "Trino",
        "Redpanda",
    ):
        assert required in executive_summary


def test_public_markdown_is_portable_and_code_fences_are_labeled():
    for path in (ROOT / "README.md", *_public_sources()):
        text = path.read_text(encoding="utf-8")
        visible = _outside_fences(text)
        for token in PORTABILITY_TOKENS:
            assert token not in visible, f"{path.relative_to(ROOT)} contains {token!r}"
        assert all(_opening_fences(text)), f"{path.relative_to(ROOT)} has an unlabeled fence"


def test_manifest_pages_use_document_local_sequential_h2_numbering():
    for path in _public_sources():
        headings = re.findall(r"^## (\d+)\.\s+", path.read_text(encoding="utf-8"), re.MULTILINE)
        all_h2 = re.findall(r"^##\s+", path.read_text(encoding="utf-8"), re.MULTILINE)
        assert len(headings) == len(all_h2), f"{path.relative_to(ROOT)} has an unnumbered H2"
        assert headings == [str(index) for index in range(1, len(headings) + 1)], (
            f"{path.relative_to(ROOT)} H2 numbering is not document-local and sequential"
        )


def test_getting_started_order_and_runtime_prerequisite_match_execution_contract():
    text = (ROOT / "docs/getting-started.md").read_text(encoding="utf-8")
    assert "Java 17" in text
    assert "Java 11+" not in text
    assert text.index("make up") < text.index("make datasets")


def test_streaming_taxonomy_names_only_real_broker_consumers():
    scenarios = (ROOT / "docs/scenarios/index.md").read_text(encoding="utf-8")
    feedback = (ROOT / "docs/atlas-feedback-a7a9.md").read_text(encoding="utf-8")
    expected = {
        "streaming_ingest-events-spark-iceberg",
        "streaming_windows-events-spark-iceberg",
        "cdc_streaming-online_retail-spark-iceberg",
    }
    for identifier in expected:
        assert identifier in feedback
    assert "Three scenarios require Redpanda" in scenarios
    assert "streaming_ingest-gh_archive-spark-iceberg" in scenarios
    assert "requires no Kafka broker" in scenarios
    assert "file-source + Redpanda" not in feedback
    assert "Three scenarios validate this" in feedback


def test_dataset_inventory_and_scenario_mappings_match_sources():
    text = (ROOT / "docs/datasets.md").read_text(encoding="utf-8")
    assert "MovieLens" in text
    sections = {
        match.group("name"): match.group("body")
        for match in re.finditer(
            r"^### (?P<name>[^\n]+)\n(?P<body>.*?)(?=^### |^## |\Z)",
            text,
            re.MULTILINE | re.DOTALL,
        )
    }
    expected = {
        "NYC Taxi": {
            "batch_ingest-nyc_taxi-spark-iceberg",
            "medallion-nyc_taxi-spark-iceberg",
            "data_quality-nyc_taxi-spark-iceberg",
            "time_travel-nyc_taxi-spark-iceberg",
            "table_maintenance-nyc_taxi-spark-iceberg",
            "federated_query-nyc_taxi-trino-iceberg",
        },
        "TPC-H": {
            "star_schema-tpch-spark-iceberg",
            "join_optimization-tpch-spark-iceberg",
            "bi_query-tpch-trino-iceberg",
        },
        "MovieLens": {"feature_engineering-movielens-spark-iceberg"},
        "Online Retail": {
            "incremental_upsert-online_retail-spark-iceberg",
            "scd2-online_retail-spark-iceberg",
            "cdc_streaming-online_retail-spark-iceberg",
        },
        "GitHub Archive": {
            "streaming_ingest-gh_archive-spark-iceberg",
            "schema_evolution-gh_archive-spark-iceberg",
            "json_flatten-gh_archive-spark-iceberg",
            "sessionization-gh_archive-spark-iceberg",
        },
        "Synthetic Events": {
            "streaming_ingest-events-spark-iceberg",
            "streaming_windows-events-spark-iceberg",
        },
    }
    for prefix, scenario_ids in expected.items():
        body = next(body for name, body in sections.items() if name.startswith(prefix))
        linked = set(re.findall(r"\[([^]]+)]\(scenarios/[^)]+\)", body))
        assert linked == scenario_ids

    primary_table = text[text.index("| Dataset |") : text.index("## 3. Adding a Dataset")]
    for scenario_id in set().union(*expected.values()) - expected["Synthetic Events"]:
        assert f"`{scenario_id}`" in primary_table


def test_atlas_enablement_metadata_matches_current_repository_and_pin():
    text = (ROOT / "docs/atlas-enablement.md").read_text(encoding="utf-8")
    assert "`data-eng-lab` (public)" in text
    assert "Airflow image (3.3.0)" in text
    assert "Airflow image (3.2.2)" not in text

    expectations = (ROOT / "docs/atlas-expectations.md").read_text(encoding="utf-8")
    assert "apache/airflow:3.3.0" in expectations

    for path in _public_sources():
        public_text = path.read_text(encoding="utf-8")
        assert "apache/airflow:3.2.2" not in public_text
        assert "Airflow image (3.2.2)" not in public_text


def test_notebook_docs_distinguish_spark_parity_from_trino_client_pairs():
    index = (ROOT / "docs/notebooks/index.md").read_text(encoding="utf-8")
    assert "Seventeen Spark scenarios" in index
    assert "two Trino scenarios" in index

    for relative in (
        "docs/notebooks/bi_query-tpch-trino-iceberg.md",
        "docs/notebooks/federated_query-nyc_taxi-trino-iceberg.md",
    ):
        text = (ROOT / relative).read_text(encoding="utf-8")
        assert "Scala (Zeppelin)" not in text
        assert "PySpark (Jupyter)" not in text
        assert "Scala / PySpark parity" not in text
        assert "Trino SQL (Zeppelin)" in text
        assert "Python client (Jupyter)" in text


def test_exhaustive_notebook_gate_discloses_destructive_scope_and_stream_safety():
    text = (ROOT / "docs/go-live.md").read_text(encoding="utf-8")
    assert "exclusive, disposable lab stack" in text
    assert "drops each scenario-owned output table" in text
    assert "Do not use the gate on a shared environment" in text
    assert "does not edit Atlas source" in text
    for prefix in ("events/", "gh_events_file/", "event_windows/", "online_retail_cdc/"):
        assert f"`{prefix}`" in text
    assert "stops its own `query` in a `finally` block" in text
    assert "unrelated active queries are not stopped" in text


@pytest.mark.parametrize("relative", ["README.md", "docs/index.md"])
def test_overviews_distinguish_kafka_and_file_source_streaming(relative: str):
    text = (ROOT / relative).read_text(encoding="utf-8")

    assert "drives all streaming scenarios" not in text
    assert (
        "Redpanda (Kafka-compatible) backs the event-ingest, windowing, and CDC "
        "streaming scenarios."
    ) in text
    assert (
        "`streaming_ingest-gh_archive-spark-iceberg` uses an incremental file "
        "source and requires no Kafka broker."
    ) in text


def _pom_contract(app: str) -> dict[str, str]:
    root = ET.parse(ROOT / "spark-apps" / app / "pom.xml").getroot()
    properties = root.find("m:properties", POM_NAMESPACE)
    assert properties is not None
    return {
        "artifact": root.findtext("m:artifactId", namespaces=POM_NAMESPACE) or "",
        "version": root.findtext("m:version", namespaces=POM_NAMESPACE) or "",
        "scala": properties.findtext("m:scala.version", namespaces=POM_NAMESPACE) or "",
        "spark": properties.findtext("m:spark.version", namespaces=POM_NAMESPACE) or "",
    }


def _called_name(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _keyword(call: ast.Call, name: str) -> ast.expr | None:
    return next((item.value for item in call.keywords if item.arg == name), None)


def _literal(node: ast.expr | None):
    return ast.literal_eval(node) if node is not None else None


def _dag_contract(app: str) -> dict[str, object]:
    path = ROOT / "spark-apps" / app / "dag.py"
    module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    hook = next(
        node
        for node in ast.walk(module)
        if isinstance(node, ast.Call) and _called_name(node) == "SparkSubmitHook"
    )
    submit = next(
        node
        for node in ast.walk(module)
        if isinstance(node, ast.Call) and _called_name(node) == "submit_and_confirm_via_rest"
    )
    spark_conf = next(
        node.value
        for node in module.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "spark_conf" for target in node.targets)
    )
    assert isinstance(spark_conf, ast.Dict)
    conf = {
        _literal(key): _literal(value)
        for key, value in zip(spark_conf.keys, spark_conf.values, strict=True)
        if isinstance(key, ast.Constant) and isinstance(value, ast.Constant)
    }
    return {
        "application": _literal(_keyword(submit, "application")),
        "java_class": _literal(_keyword(hook, "java_class")),
        "deploy_mode": _literal(_keyword(hook, "deploy_mode")),
        "application_args": _literal(_keyword(hook, "application_args")),
        "extensions": conf["spark.sql.extensions"],
    }


def _jenkins_contract(app: str) -> dict[str, str]:
    text = (ROOT / "spark-apps" / app / "Jenkinsfile").read_text(encoding="utf-8")
    app_name = re.search(r"APP = '([^']+)'", text)
    version = re.search(r"VERSION = '([^']+)'", text)
    destination = re.search(
        r'MINIO_BUCKET_ICEBERG_JARS}/\$\{APP}/\$\{VERSION}/(app\.jar)', text
    )
    assert app_name and version and destination
    return {
        "application": (
            f"s3a://jars/{app_name.group(1)}/{version.group(1)}/{destination.group(1)}"
        )
    }


@pytest.mark.parametrize("app", ["nyc-taxi-etl", "nyc-taxi-medallion"])
def test_spark_app_docs_match_build_publish_and_dag_contracts(app: str):
    pom = _pom_contract(app)
    dag = _dag_contract(app)
    jenkins = _jenkins_contract(app)
    assert pom["artifact"] == app
    assert dag["application"] == jenkins["application"]

    java_class = str(dag["java_class"])
    entrypoint = f"src/main/scala/{java_class.replace('.', '/')}.scala"
    assert (ROOT / "spark-apps" / app / entrypoint).is_file()

    for relative in (
        f"docs/spark-apps/{app}.md",
        f"spark-apps/{app}/README.md",
    ):
        text = (ROOT / relative).read_text(encoding="utf-8")
        assert f"Scala ({pom['scala']})" in text
        assert f"Spark {pom['spark']}" in text
        assert f"`{entrypoint}`" in text
        assert f"`{java_class}`" in text
        assert f"`{dag['application']}`" in text
        assert "`dag.py`" in text
        assert f"`{dag['extensions']}`" in text
        assert "Spark standalone cluster mode" in text
        assert "src/main/scala/dag.py" not in text
        assert "YARN/K8s" not in text


def test_etl_docs_match_transform_and_positional_argument_contract():
    transform = (
        "src/main/scala/com/thekaveh/dataeng/nyctaxi/transforms/TaxiTransforms.scala"
    )
    source = (ROOT / "spark-apps/nyc-taxi-etl" / transform).read_text(encoding="utf-8")
    assert "def clean(df: DataFrame)" in source
    assert 'F.col("tpep_pickup_datetime")' in source

    for relative in (
        "docs/spark-apps/nyc-taxi-etl.md",
        "spark-apps/nyc-taxi-etl/README.md",
    ):
        text = (ROOT / relative).read_text(encoding="utf-8")
        assert f"`{transform}`" in text
        assert "`TaxiTransforms.clean`" in text
        assert "two positional arguments" in text
        assert "`tpep_pickup_datetime`" in text
        assert "`passenger_count`" in text
        assert "`createOrReplace()`" in text
        assert "TaxiTransforms.sanitize" not in text
        assert "--source" not in text
        assert "partitioned by trip_date" not in text


def test_medallion_docs_match_transform_output_and_fixed_table_contract():
    transform = (
        "src/main/scala/com/thekaveh/dataeng/medallion/transforms/MedallionTransforms.scala"
    )
    source = (ROOT / "spark-apps/nyc-taxi-medallion" / transform).read_text(
        encoding="utf-8"
    )
    assert 'F.count("*").as("trips")' in source

    for relative in (
        "docs/spark-apps/nyc-taxi-medallion.md",
        "spark-apps/nyc-taxi-medallion/README.md",
    ):
        text = (ROOT / relative).read_text(encoding="utf-8")
        assert f"`{transform}`" in text
        assert "one optional positional bronze-table argument" in text
        assert "`trips`" in text
        assert "`createOrReplace()`" in text
        assert "upstream DAG dependency" not in text
        assert "MedallionMedallion" not in text
        assert "trip_count" not in text
        assert "parameterised property tests" not in text


def test_spark_app_overview_matches_publish_and_runtime_ownership_contract():
    text = (ROOT / "docs/spark-apps/index.md").read_text(encoding="utf-8")
    assert "s3a://jars/<app>/0.1.0/app.jar" in text
    assert "Atlas Spark image supplies the Spark, S3A, and Iceberg runtime" in text
    assert "<app>.jar" not in text
    assert "Iceberg bindings bundled" not in text


def _scenario_result_rows(markdown: str) -> list[list[str]]:
    section = re.split(r"^## \d+\. Scenario Execution\s*$", markdown, maxsplit=1, flags=re.MULTILINE)[1]
    section = re.split(r"^## \d+\. Trino Validation\s*$", section, maxsplit=1, flags=re.MULTILINE)[0]
    rows = []
    for line in section.splitlines():
        if not line.startswith("|") or line.startswith("|---") or "| Scenario |" in line:
            continue
        rows.append([cell.strip() for cell in line.strip("|").split("|")])
    return rows


def test_go_live_scenario_matrix_covers_manifest_and_matches_summary_counts():
    manifest = load_manifest(ROOT / "docs/manifest.yaml", ROOT)
    expected = {
        section.source.stem.rsplit("-", 2)[0]
        for section in iter_leaf_sections(manifest.sections)
        if section.source is not None
        and section.source.parent == Path("docs/scenarios")
        and section.source.name != "index.md"
    }
    text = (ROOT / "docs/go-live-results.md").read_text(encoding="utf-8")
    rows = _scenario_result_rows(text)
    by_scenario = {row[0]: row[1:] for row in rows}

    assert len(rows) == len(by_scenario) == len(expected) == 19
    assert set(by_scenario) == expected
    assert sum(columns[0] == "PASS" for columns in by_scenario.values()) == 19
    assert sum(columns[1:] == ["PASS", "MATCH"] for columns in by_scenario.values()) == 17
    assert sum(columns[1:] == ["N/A", "—"] for columns in by_scenario.values()) == 2
    assert by_scenario["incremental_upsert-online_retail"] == ["PASS", "PASS", "MATCH"]
    assert "**Summary: 19/19 scenarios passed. 17/17 dual-language scenarios show parity.**" in text
