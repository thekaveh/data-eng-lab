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


def test_find_links_reads_real_html_src_and_preserves_source_offsets():
    markdown = (
        '[Docs](docs/index.md) <img data-src="lazy.png" '
        'srcset="small.png 1x" alt="literal src=\'trap.png\'" '
        'src = "img/hero.png"> ![Flow](architectures/overview.svg)'
    )
    links = find_links(markdown)

    assert [(link.target, link.is_image) for link in links] == [
        ("docs/index.md", False),
        ("img/hero.png", True),
        ("architectures/overview.svg", True),
    ]
    assert [markdown[link.start : link.end] for link in links] == [
        "docs/index.md",
        "img/hero.png",
        "architectures/overview.svg",
    ]


@pytest.mark.parametrize(
    "image",
    [
        '<img data-src="lazy.png">',
        '<img srcset="small.png 1x, large.png 2x">',
        '<img alt="literal src=\'trap.png\'">',
    ],
)
def test_find_links_ignores_html_src_attribute_decoys(image):
    assert find_links(image) == ()
