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


@dataclass(frozen=True)
class Link:
    """A Markdown link target."""

    target: str


def find_links(markdown: str) -> tuple[Link, ...]:
    """Return inline Markdown link and image targets in document order."""
    return tuple(Link(match.group("target")) for match in MARKDOWN_LINK_RE.finditer(markdown))


def is_forbidden(target: str, surface: str) -> bool:
    """Return whether *target* points to another documentation surface."""
    if target.startswith(WIKI_ORIGIN):
        return surface != "wiki"
    if target.startswith(REPOSITORY_ORIGIN):
        return surface != "repo"
    if target.startswith(PAGES_ORIGIN):
        return surface != "site"
    return False
