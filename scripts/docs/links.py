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
_HTML_IMAGE_OPEN_RE = re.compile(r"<img(?=[\s/>])", re.IGNORECASE)


@dataclass(frozen=True)
class Link:
    """A Markdown link or image target."""

    target: str
    is_image: bool
    start: int
    end: int


def find_html_image_sources(markup: str) -> tuple[Link, ...]:
    """Return real ``src`` attribute values from HTML image tags."""
    images: list[Link] = []
    position = 0
    while match := _HTML_IMAGE_OPEN_RE.search(markup, position):
        tag_end = _html_tag_end(markup, match.end())
        if tag_end is None:
            break
        source = _html_src_attribute(markup, match.end(), tag_end)
        if source is not None:
            start, end = source
            images.append(Link(markup[start:end], True, start, end))
        position = tag_end + 1
    return tuple(images)


def find_links(markdown: str) -> tuple[Link, ...]:
    """Return Markdown links and Markdown/HTML images in document order."""
    discovered = [
        Link(
            match.group("target"),
            match.group(0).startswith("!"),
            match.start("target"),
            match.end("target"),
        )
        for match in MARKDOWN_LINK_RE.finditer(markdown)
    ]
    discovered.extend(find_html_image_sources(markdown))
    return tuple(sorted(discovered, key=lambda link: link.start))


def _html_tag_end(markup: str, start: int) -> int | None:
    quote = ""
    for position in range(start, len(markup)):
        character = markup[position]
        if quote:
            if character == quote:
                quote = ""
        elif character in {'"', "'"}:
            quote = character
        elif character == ">":
            return position
    return None


def _html_src_attribute(markup: str, start: int, end: int) -> tuple[int, int] | None:
    position = start
    while position < end:
        while position < end and (markup[position].isspace() or markup[position] == "/"):
            position += 1
        name_start = position
        while (
            position < end
            and not markup[position].isspace()
            and markup[position] not in {"=", "/", ">"}
        ):
            position += 1
        if position == name_start:
            position += 1
            continue
        name = markup[name_start:position].casefold()
        while position < end and markup[position].isspace():
            position += 1
        if position >= end or markup[position] != "=":
            continue
        position += 1
        while position < end and markup[position].isspace():
            position += 1
        if position >= end:
            return None
        quote = markup[position] if markup[position] in {'"', "'"} else ""
        if quote:
            value_start = position + 1
            value_end = markup.find(quote, value_start, end)
            if value_end < 0:
                return None
            position = value_end + 1
        else:
            value_start = position
            while position < end and not markup[position].isspace():
                position += 1
            value_end = position
        if name == "src":
            return value_start, value_end
    return None


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
