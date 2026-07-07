import pytest
from docslib.links import DocMap, LinkKind, classify, rewrite_for_readme, rewrite_for_wiki


def _map():
    return DocMap(
        readme={"scenarios/batch_ingest-nyc_taxi-spark-iceberg.md":
                "scenarios/batch_ingest-nyc_taxi-spark-iceberg/README.md",
                "spark-apps/nyc-taxi-etl.md": "spark-apps/nyc-taxi-etl/README.md"},
        wiki={"scenarios/batch_ingest-nyc_taxi-spark-iceberg.md": "Scenario-batch_ingest-nyc_taxi-spark-iceberg",
              "spark-apps/nyc-taxi-etl.md": "App-nyc-taxi-etl"},
        concepts_readme_anchor={"datasets.md": "datasets",
                                "lakehouse.md": "lakehouse-architecture"},
    )


@pytest.mark.parametrize("target,kind", [
    ("https://thekaveh.github.io/data-eng-lab/scenarios/foo/", LinkKind.EXTERNAL_SITE),
    ("http://example.com", LinkKind.EXTERNAL_OTHER),
    ("https://github.com/thekaveh/atlas", LinkKind.EXTERNAL_OTHER),
    ("architectures/foo.svg", LinkKind.DIAGRAM),
    ("architectures/foo.html", LinkKind.DIAGRAM),
    ("../bar.md", LinkKind.DOC_RELATIVE),
    ("datasets.md", LinkKind.DOC_RELATIVE),
    ("#anchor", LinkKind.ANCHOR),
    ("mailto:a@b.com", LinkKind.MAILTO),
])
def test_classify(target, kind):
    assert classify(target) == kind


def test_readme_rewrites_internal_and_localizes_diagram():
    m = _map()
    cur = "scenarios/batch_ingest-nyc_taxi-spark-iceberg.md"
    # doc-relative sibling → sibling README (internal)
    assert rewrite_for_readme("medallion-nyc_taxi-spark-iceberg.md", cur, m) == \
        "../medallion-nyc_taxi-spark-iceberg/README.md"
    # concept → root README anchor
    assert rewrite_for_readme("datasets.md", cur, m) == "../../README.md#datasets"
    # diagram → local svg (relative to the scenario README's dir)
    assert rewrite_for_readme("architectures/batch_ingest-nyc_taxi-spark-iceberg.svg", cur, m) == \
        "architectures/batch_ingest-nyc_taxi-spark-iceberg.svg"
    # site URL must NOT appear
    assert "thekaveh.github.io" not in \
        rewrite_for_readme("https://thekaveh.github.io/data-eng-lab/", cur, m)


def test_readme_rejects_site_url():
    m = _map()
    out = rewrite_for_readme("https://thekaveh.github.io/data-eng-lab/foo/", "index.md", m)
    assert out == ""  # cross-surface link dropped


def test_custom_concept_anchor_is_honored():
    # A caller who customizes concepts_readme_anchor must be honored on the README surface
    # (regression: the field was previously a dead field — rewrite_for_readme read the module
    # constant directly).
    m = DocMap(readme={}, wiki={}, concepts_readme_anchor={"datasets.md": "my-custom-datasets"})
    out = rewrite_for_readme("datasets.md", "scenarios/foo-bar-baz-qux.md", m)
    assert out == "../../README.md#my-custom-datasets"


def test_wiki_uses_wiki_links():
    m = _map()
    cur = "scenarios/batch_ingest-nyc_taxi-spark-iceberg.md"
    assert rewrite_for_wiki("medallion-nyc_taxi-spark-iceberg.md", cur, m) == \
        "[[Scenario-medallion-nyc_taxi-spark-iceberg]]"
    assert rewrite_for_wiki("datasets.md", cur, m) == "[[Datasets]]"
    assert rewrite_for_wiki("architectures/batch_ingest-nyc_taxi-spark-iceberg.svg", cur, m) == \
        "batch_ingest-nyc_taxi-spark-iceberg.svg"
