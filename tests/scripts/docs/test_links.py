import pytest

from scripts.docs.links import find_links, is_forbidden


@pytest.mark.parametrize(
    "surface,target,forbidden",
    [
        ("repo", "https://thekaveh.github.io/data-eng-lab/", True),
        ("repo", "https://thekaveh.github.io/data-eng-lab", True),
        ("repo", "https://thekaveh.github.io/data-eng-lab?search=docs", True),
        ("repo", "https://thekaveh.github.io/data-eng-lab#overview", True),
        ("repo", "https://github.com/thekaveh/data-eng-lab/wiki", True),
        ("site", "https://github.com/thekaveh/data-eng-lab/blob/main/docs/index.md", True),
        ("site", "https://github.com/thekaveh/data-eng-lab/wiki", True),
        ("wiki", "https://thekaveh.github.io/data-eng-lab/", True),
        ("wiki", "https://thekaveh.github.io/data-eng-lab", True),
        ("wiki", "https://thekaveh.github.io/data-eng-lab?search=docs", True),
        ("wiki", "https://thekaveh.github.io/data-eng-lab#overview", True),
        ("wiki", "https://github.com/thekaveh/data-eng-lab/blob/main/README.md", True),
        ("repo", "https://airflow.apache.org/", False),
        ("site", "https://iceberg.apache.org/", False),
        ("wiki", "https://spark.apache.org/", False),
    ],
)
def test_surface_link_matrix(surface, target, forbidden):
    assert is_forbidden(target, surface) is forbidden


def test_find_links_reads_markdown_links_and_html_images_in_source_order():
    links = find_links(
        '[Docs](docs/index.md) <img alt="Hero" src="img/hero.png"> '
        "![Flow](architectures/overview.svg)"
    )

    assert [(link.target, link.is_image) for link in links] == [
        ("docs/index.md", False),
        ("img/hero.png", True),
        ("architectures/overview.svg", True),
    ]
