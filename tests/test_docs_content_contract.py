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
HERO_BANNER_ALT = "Abstract data-eng-lab lakehouse with Iceberg crystal, medallion layers, and flowing data"
REDPANDA_BADGE_URL = "https://img.shields.io/badge/Redpanda-streaming-FF4D5B?logo=apachekafka&logoColor=white"
HERO_BADGE_ROWS = (
    (
        ("Atlas", "https://img.shields.io/badge/Atlas-infrastructure-2563EB?logo=git&logoColor=white"),
        (
            "Docker Compose",
            "https://img.shields.io/badge/Docker%20Compose-runtime-2496ED?logo=docker&logoColor=white",
        ),
    ),
    (
        (
            "Apache Spark",
            "https://img.shields.io/badge/Apache%20Spark-compute-E25A1C?logo=apachespark&logoColor=white",
        ),
        (
            "Apache Iceberg",
            "https://img.shields.io/badge/Apache%20Iceberg-tables-4F46E5?logo=apache&logoColor=white",
        ),
        (
            "MinIO",
            "https://img.shields.io/badge/MinIO-object%20storage-C72E49?logo=minio&logoColor=white",
        ),
        ("Trino", "https://img.shields.io/badge/Trino-SQL-DD00A1?logo=trino&logoColor=white"),
        (
            "Redpanda",
            REDPANDA_BADGE_URL,
        ),
    ),
    (
        (
            "Apache Airflow",
            "https://img.shields.io/badge/Apache%20Airflow-orchestration-017CEE?logo=apacheairflow&logoColor=white",
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
            "https://img.shields.io/badge/Zeppelin-notebooks-FBBF24?logo=apache&logoColor=white",
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
        ROOT / section.source for section in iter_leaf_sections(manifest.sections) if section.source is not None
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
        r"(Build, orchestrate, stream, and query .*?)\s*</p>",
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
    banner = f'<p align="center">\n  <img src="{HERO_BANNER_PATH}" alt="{HERO_BANNER_ALT}" width="100%">\n</p>'
    tagline = (
        '<p align="center">\n'
        "  <strong>An Iceberg-lakehouse data-engineering lab built on the "
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
    normalized_readme_opener = readme_opener.replace("docs/diagrams/img/data-eng-lab-hero.png", HERO_BANNER_PATH)
    assert normalized_readme_opener == index_opener == _expected_opener()
    assert (
        readme_parts
        == index_parts
        == (
            HERO_H1,
            HERO_TAGLINE_TEXT,
            HERO_VALUE_PROPOSITION,
        )
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
        badge_pairs = tuple(tuple(re.findall(r'<img alt="([^"]+)" src="([^"]+)">', row)) for row in badge_rows)
        assert badge_pairs == HERO_BADGE_ROWS
        assert badge_pairs[1][4] == ("Redpanda", REDPANDA_BADGE_URL)
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
        opener_images = re.findall(r"<img\s+([^>]+)>", opener)
        assert opener_images
        assert all(re.search(r'\balt="[^"]+"', attributes) for attributes in opener_images)
        architecture = text[text.index("## 2. Architecture") :]
        assert architecture.startswith(ARCHITECTURE_LEAD + f"![data-eng-lab architecture]({overview_path})\n\n")

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


def test_unreleased_changelog_does_not_claim_the_architecture_poster_is_the_opener():
    changelog = (ROOT / "docs/CHANGELOG.md").read_text(encoding="utf-8")
    unreleased = changelog.split("## 1. [Unreleased]", 1)[1].split("\n## ", 1)[0]

    assert "now open with a wide lakehouse brand banner" in unreleased
    assert "Documentation now opens with a shared project title, architecture poster" not in unreleased


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


def test_dataset_docs_describe_versioned_fail_closed_provenance_contract():
    text = (ROOT / "docs/datasets.md").read_text(encoding="utf-8")
    for phrase in (
        "registry version 2",
        "strict fail-on-drift",
        "normalized `artifacts` catalog",
        "raw archive",
        "extracted landing object",
        "`scales.<tier>.artifacts`",
        "DuckDB 1.5.4",
        "linux/amd64",
        "C.UTF-8",
        "SHA-256",
        "schema fingerprint",
        "reviewed lock update",
        "immutable generation",
        "active pointer",
    ):
        assert phrase in text

    assert "`fetch.scale_params`" not in text
    assert "runtime enforcement is tracked in issue #81" not in text
    assert "enforcement is pending" not in text


def test_dataset_docs_describe_verified_publication_and_recovery():
    text = (ROOT / "docs/datasets.md").read_text(encoding="utf-8")
    for phrase in (
        "immutable generation",
        "active pointer",
        "--verify-only",
        "--refresh",
        "--force",
        "--rollback-manifest",
        "legacy flat objects",
        "no automatic garbage collection",
        "concurrent publisher",
        "DATASET_SCALE",
        "dataset-resolver",
        "runtime mismatch never updates the registry",
        "issue #80",
    ):
        assert phrase in text

    for command in (
        "make datasets SCALE=small",
        "uv run python scripts/download_datasets.py --scale small --verify-only",
        "uv run python scripts/download_datasets.py --scale small --only movielens --refresh",
        "uv run python scripts/download_datasets.py --scale small --only movielens --rollback-manifest <64-hex-digest>",
    ):
        assert command in text

    assert "explicit parameter, then `DATASET_SCALE`, then `small`" in text
    assert "`make up` does not acquire datasets" in text


def test_entry_and_operations_docs_publish_the_verified_dataset_workflow():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    getting_started = (ROOT / "docs/getting-started.md").read_text(encoding="utf-8")
    go_live = (ROOT / "docs/go-live.md").read_text(encoding="utf-8")
    changelog = (ROOT / "docs/CHANGELOG.md").read_text(encoding="utf-8")

    assert "verified immutable" in readme
    assert "`make up` starts services; it does not acquire datasets" in getting_started
    assert "DATASET_SCALE" in getting_started
    assert "active pointer" in go_live
    assert "runtime mismatch never updates the registry" in go_live
    assert "verified immutable generations" in " ".join(changelog.split())
    assert "runtime enforcement remains explicitly deferred to issue #81" not in changelog


def test_public_consumer_docs_do_not_teach_flat_dataset_reads():
    affected = (
        ROOT / "docs/notebooks/batch_ingest-nyc_taxi-spark-iceberg.md",
        ROOT / "docs/notebooks/feature_engineering-movielens-spark-iceberg.md",
        ROOT / "docs/notebooks/join_optimization-tpch-spark-iceberg.md",
        ROOT / "docs/notebooks/json_flatten-gh_archive-spark-iceberg.md",
        ROOT / "docs/notebooks/sessionization-gh_archive-spark-iceberg.md",
        ROOT / "docs/notebooks/star_schema-tpch-spark-iceberg.md",
        ROOT / "docs/notebooks/streaming_ingest-gh_archive-spark-iceberg.md",
        ROOT / "docs/spark-apps/nyc-taxi-etl.md",
        ROOT / "docs/spark-apps/nyc-taxi-medallion.md",
        ROOT / "spark-apps/nyc-taxi-etl/README.md",
        ROOT / "spark-apps/nyc-taxi-medallion/README.md",
    )
    flat_path = re.compile(r"s3a://landing/(?:nyc_taxi|gh_archive|movielens|online_retail|tpch)(?!/_generations/)")
    offenders = tuple(path.relative_to(ROOT).as_posix() for path in affected if flat_path.search(path.read_text()))
    assert offenders == ()

    for path in affected:
        text = path.read_text(encoding="utf-8")
        assert "immutable" in text


def test_published_dataset_diagram_masters_show_verified_resolution_boundary():
    names = (
        "batch_ingest-nyc_taxi-spark-iceberg",
        "streaming_ingest-gh_archive-spark-iceberg",
        "join_optimization-tpch-spark-iceberg",
        "star_schema-tpch-spark-iceberg",
        "nyc-taxi-etl",
    )
    flat_path = re.compile(r"s3a://landing/(?:nyc_taxi|gh_archive|tpch)(?!/_generations/)")

    for name in names:
        path = ROOT / f"docs/diagrams/{name}.html"
        text = path.read_text(encoding="utf-8")
        assert "expected scale" in text, path
        assert "active pointer + immutable manifest" in text, path
        assert "ordered immutable objects" in text, path
        assert "s3a projection only after verification" in text, path
        assert flat_path.search(text) is None, path
        assert "<script" not in text.casefold(), path

    streaming = (ROOT / "docs/diagrams/streaming_ingest-gh_archive-spark-iceberg.html").read_text()
    assert "file source · one stream per immutable URI" in streaming
    assert "directory scan" not in streaming
    assert "checkpoint key: scale / publication / manifest" in streaming
    assert "checkpoint: s3a://checkpoints/gh_events_file ·" not in streaming

    etl = (ROOT / "docs/diagrams/nyc-taxi-etl.html").read_text()
    assert "Airflow task execution calls dataset-resolver" in etl
    assert "--table" in etl


def test_dataset_design_and_plan_define_effective_stability_and_safe_provenance_text():
    paths = (
        ROOT / "docs/superpowers/specs/2026-08-10-dataset-provenance-lock-design.md",
        ROOT / "docs/superpowers/plans/2026-08-10-dataset-provenance-lock.md",
    )
    for path in paths:
        text = path.read_text(encoding="utf-8")
        assert "must exactly match its effective provenance" in text
        assert "non-global IP addresses" in text
        assert "user-home or temporary paths" in text
        assert "credential key/value forms" in text
        assert "standardized semantic `Label:number` references" in text
        assert "case-insensitive allowlist" in text
        assert "`DOI`" in text
        assert "`GDPR`" in text
        assert "`Article`" in text
        assert "all other endpoint-shaped tokens fail closed" in text
        assert "arbitrary semantic `Label:number` references" not in text

    combined = "\n".join(path.read_text(encoding="utf-8") for path in paths)
    assert "more restrictive than" not in combined
    assert "may be stricter than" not in combined


def _provenance_table_rows(markdown: str) -> tuple[tuple[str, ...], ...]:
    section = markdown.split("## 5. Authoritative Sources, Licenses, and Attribution", 1)[1]
    section = section.split("\n## ", 1)[0]
    lines = tuple(line for line in section.splitlines() if line.startswith("|"))
    assert len(lines) >= 3

    parsed = tuple(
        tuple(cell.replace(r"\|", "|").strip() for cell in re.split(r"(?<!\\)\|", line[1:-1])) for line in lines
    )
    assert parsed[0] == ("Dataset", "Publisher and source", "License or terms", "Attribution")
    assert all(re.fullmatch(r":?-{3,}:?", cell) for cell in parsed[1])
    assert all(len(row) == len(parsed[0]) for row in parsed[2:])
    return parsed[2:]


def test_provenance_table_parser_preserves_links_and_escaped_pipes():
    markdown = (
        "## 5. Authoritative Sources, Licenses, and Attribution\n\n"
        "| Dataset | Publisher and source | License or terms | Attribution |\n"
        "|---|---|---|---|\n"
        r"| Name \| alias | Publisher — [source \| mirror](https://example.org/a%7Cb) "
        r"| [License](https://example.org/license) | Owner \| creator |"
        "\n\n## 6. Next\n"
    )

    assert _provenance_table_rows(markdown) == (
        (
            "Name | alias",
            "Publisher — [source | mirror](https://example.org/a%7Cb)",
            "[License](https://example.org/license)",
            "Owner | creator",
        ),
    )


def test_dataset_docs_link_exact_authoritative_sources_licenses_and_attribution_by_row():
    text = (ROOT / "docs/datasets.md").read_text(encoding="utf-8")
    registry = yaml.safe_load((ROOT / "datasets/registry.yaml").read_text(encoding="utf-8"))
    display_names = {
        "nyc_taxi": "NYC Taxi",
        "gh_archive": "GH Archive",
        "movielens": "MovieLens",
        "online_retail": "Online Retail II",
        "tpch": "TPC-H",
    }
    rows = _provenance_table_rows(text)

    assert tuple(row[0] for row in rows) == tuple(display_names[dataset_id] for dataset_id in registry["datasets"])

    for (dataset_id, dataset), row in zip(registry["datasets"].items(), rows, strict=True):
        assert row[0] == display_names[dataset_id]
        row_text = " | ".join(row)
        provenance = dataset["provenance"]
        for key in ("publisher", "homepage", "license_name", "license_url", "attribution"):
            assert provenance[key] in row_text

    assert "https://files.grouplens.org/datasets/movielens/ml-latest-small-README.html" in text
    assert "https://files.grouplens.org/datasets/movielens/ml-25m-README.html" in text


def test_dataset_docs_record_reviewed_evidence_counts_and_source_realities():
    text = (ROOT / "docs/datasets.md").read_text(encoding="utf-8")
    registry = yaml.safe_load((ROOT / "datasets/registry.yaml").read_text(encoding="utf-8"))
    datasets = registry["datasets"]
    http_datasets = tuple(dataset for dataset in datasets.values() if dataset["fetch"]["kind"] == "http")
    http_artifact_count = sum(len(dataset["artifacts"]) for dataset in http_datasets)
    http_landing_count = sum(
        len(artifact["outputs"]) for dataset in http_datasets for artifact in dataset["artifacts"].values()
    )
    schema_count = sum(len(dataset["schemas"]) for dataset in datasets.values())
    tpch_output_count = sum(len(scale["outputs"]) for scale in datasets["tpch"]["generator"]["scales"].values())
    evidence = text.split("## 4. Reviewed Evidence and Source Realities", 1)[1]
    evidence = evidence.split("\n## ", 1)[0]
    counts = re.search(
        r"The issue #80 review acquired (?P<http>\d+) unique HTTP artifacts, "
        r"recorded (?P<landing>\d+) HTTP landing objects, and derived "
        r"(?P<schemas>\d+) schema contracts\. The canonical TPC-H runs produced "
        r"(?P<tpch>\d+) TPC-H outputs across three tiers and "
        r"(?P<repeats>\d+) corresponding repeat outputs; each matched its "
        r"first-run counterpart in size and SHA-256\.",
        evidence,
    )
    assert counts is not None
    assert tuple(int(counts[name]) for name in ("http", "landing", "schemas", "tpch")) == (
        http_artifact_count,
        http_landing_count,
        schema_count,
        tpch_output_count,
    )
    assert int(counts["repeats"]) == 24
    assert "24 byte-identical repeat outputs" not in evidence

    for phrase in (
        "January 2023 is the physical-schema outlier",
        "`passenger_count` is `float64`",
        "February through June use `int64`",
        "`online_retail_II.xlsx`",
        "`Year 2009-2010`",
        "`Year 2010-2011`",
        "`ml-latest-small.zip` is a mutable alias",
        "`latest-small` is not a source revision",
        "2018-09-26",
        "release-specific terms control",
        "artifact-level provenance governs the selected release",
        "scale-local logical names",
        "separate immutable generation",
        "active pointer changes only after the complete selected release verifies",
    ):
        assert phrase in evidence


def test_dataset_docs_publish_reviewed_update_commands_and_changelog_boundary():
    text = (ROOT / "docs/datasets.md").read_text(encoding="utf-8")
    for command in (
        "uv run python scripts/audit_dataset_lock.py http",
        "docker build --platform linux/amd64 -f datasets/tpch-lock.Dockerfile",
        "docker run --rm --network=none --platform linux/amd64",
        "uv run pytest tests/datasets -q",
        "uv run python scripts/verify_repo.py --root .",
        "make docs-check",
        "make docs-wiki",
    ):
        assert command in text

    changelog = (ROOT / "docs/CHANGELOG.md").read_text(encoding="utf-8")
    unreleased = " ".join(changelog.split("## 1. [Unreleased]", 1)[1].split("\n## ", 1)[0].split())
    for phrase in (
        "registry version 2",
        "source, raw/archive, landing-object, generator-output, and schema locks",
        "verified immutable generations",
    ):
        assert phrase in unreleased
    assert "runtime enforcement remains explicitly deferred to issue #81" not in unreleased
    assert "March's `INT64` schema" not in unreleased


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
    assert ("Redpanda (Kafka-compatible) backs the event-ingest, windowing, and CDC streaming scenarios.") in text
    assert (
        "`streaming_ingest-gh_archive-spark-iceberg` uses an incremental file source and requires no Kafka broker."
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
    operator = next(
        node
        for node in ast.walk(module)
        if isinstance(node, ast.Call) and _called_name(node) == "AtlasSparkSubmitOperator"
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
        "application": _literal(_keyword(operator, "application")),
        "java_class": _literal(_keyword(operator, "java_class")),
        "deploy_mode": _literal(_keyword(operator, "deploy_mode")),
        "application_args": _literal(_keyword(operator, "application_args")),
        "extensions": conf["spark.sql.extensions"],
    }


def _jenkins_contract(app: str) -> dict[str, str]:
    text = (ROOT / "spark-apps" / app / "Jenkinsfile").read_text(encoding="utf-8")
    app_name = re.search(r"APP = '([^']+)'", text)
    version = re.search(r"VERSION = '([^']+)'", text)
    destination = re.search(r"MINIO_BUCKET_ICEBERG_JARS}/\$\{APP}/\$\{VERSION}/(app\.jar)", text)
    assert app_name and version and destination
    return {"application": (f"s3a://jars/{app_name.group(1)}/{version.group(1)}/{destination.group(1)}")}


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
        assert "`AtlasSparkSubmitOperator`" in text
        assert "`SparkSubmitOperator`" in text
        assert "`RestConfirmingSparkHook`" in text
        assert "`driverState=FINISHED` plus `success=true`" in text
        assert "contains one TaskFlow task" not in text
        assert "constructs `SparkSubmitHook`" not in text
        assert "src/main/scala/dag.py" not in text
        assert "YARN/K8s" not in text


def test_etl_docs_match_transform_and_positional_argument_contract():
    transform = "src/main/scala/com/thekaveh/dataeng/nyctaxi/transforms/TaxiTransforms.scala"
    app_root = ROOT / "spark-apps/nyc-taxi-etl"
    source = (app_root / transform).read_text(encoding="utf-8")
    entrypoint = (app_root / "src/main/scala/com/thekaveh/dataeng/nyctaxi/NycTaxiEtl.scala").read_text()
    assert "def clean(df: DataFrame)" in source
    assert 'F.col("tpep_pickup_datetime")' in source
    assert 'args.indexOf("--table")' in entrypoint

    for relative in (
        "docs/spark-apps/nyc-taxi-etl.md",
        "spark-apps/nyc-taxi-etl/README.md",
    ):
        text = (ROOT / relative).read_text(encoding="utf-8")
        assert f"`{transform}`" in text
        assert "`TaxiTransforms.clean`" in text
        assert "one or more ordered" in text
        assert "`--table <target>`" in text
        assert "no flat-path or table default" in text
        assert "`tpep_pickup_datetime`" in text
        assert "`passenger_count`" in text
        assert "`createOrReplace()`" in text
        assert "TaxiTransforms.sanitize" not in text
        assert "--source" not in text
        assert "partitioned by trip_date" not in text


def test_medallion_docs_match_transform_output_and_fixed_table_contract():
    transform = "src/main/scala/com/thekaveh/dataeng/medallion/transforms/MedallionTransforms.scala"
    app_root = ROOT / "spark-apps/nyc-taxi-medallion"
    source = (app_root / transform).read_text(encoding="utf-8")
    entrypoint = (app_root / "src/main/scala/com/thekaveh/dataeng/medallion/NycTaxiMedallion.scala").read_text()
    assert 'F.count("*").as("trips")' in source
    assert 'args.indexOf("--bronze-table")' in entrypoint

    for relative in (
        "docs/spark-apps/nyc-taxi-medallion.md",
        "spark-apps/nyc-taxi-medallion/README.md",
    ):
        text = (ROOT / relative).read_text(encoding="utf-8")
        assert f"`{transform}`" in text
        assert "one or more ordered" in text
        assert "`--bronze-table <table>`" in text
        assert "no flat-path or Bronze-table default" in text
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
    assert "operator-owned `SparkSubmitOperator` subclass" in text
    assert "`RestConfirmingSparkHook`" in text
    assert "Airflow TaskFlow DAG" not in text
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
        and section.source.name not in {"index.md", "execution-modes.md"}
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
