"""Markdown link discovery and cross-surface link classification."""

from __future__ import annotations

import re
from dataclasses import dataclass

REPOSITORY_ORIGIN = "https://github.com/thekaveh/data-eng-lab"
WIKI_ORIGIN = "https://github.com/thekaveh/data-eng-lab/wiki"
PAGES_ORIGIN = "https://thekaveh.github.io/data-eng-lab/"

MARKDOWN_LINK_RE = re.compile(
    r"!?\[(?P<label>[^\]]*)\]\((?P<target>[^)\s]+)(?:\s+[^)]*)?\)"
)
HTML_IMAGE_SRC_RE = re.compile(
    r'(?P<prefix><img\b[^>]*?\bsrc=["\'])(?P<target>[^"\']+)(?P<suffix>["\'])',
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Link:
    """A Markdown link or image target."""

    target: str
    is_image: bool


def find_links(markdown: str) -> tuple[Link, ...]:
    """Return Markdown links and Markdown/HTML images in document order."""
    discovered = [
        (match.start(), Link(match.group("target"), match.group(0).startswith("!")))
        for match in MARKDOWN_LINK_RE.finditer(markdown)
    ]
    discovered.extend(
        (match.start(), Link(match.group("target"), True))
        for match in HTML_IMAGE_SRC_RE.finditer(markdown)
    )
    return tuple(link for _, link in sorted(discovered, key=lambda item: item[0]))


def is_forbidden(target: str, surface: str) -> bool:
    """Return whether *target* points to another documentation surface."""
    if target.startswith(WIKI_ORIGIN):
        return surface != "wiki"
    if target.startswith(REPOSITORY_ORIGIN):
        return surface != "repo"
    if _matches_pages_origin(target):
        return surface != "site"
    return False


def _matches_pages_origin(target: str) -> bool:
    pages_base = PAGES_ORIGIN.rstrip("/")
    return target == pages_base or target.startswith((f"{pages_base}/", f"{pages_base}?", f"{pages_base}#"))
