"""In-memory model of the docs/ source tree + mkdocs nav."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Section:
    number: str        # "1", "2.1", or "" if unnumbered
    title: str
    level: int         # 1 = H1, 2 = H2, ...
    anchor: str        # url slug


@dataclass
class Page:
    src: Path                  # docs/-relative path, e.g. Path("scenarios/batch_ingest-...md")
    title: str                 # first H1 text
    number: str                # nav number, e.g. "5.3"; "" if unnumbered
    body: str                  # raw markdown
    sections: list[Section]
    see_also: list[str]        # raw link targets from the "See Also" block


@dataclass
class Scenario:
    name: str                  # dir name under scenarios/
    page: Page                 # docs/scenarios/<name>.md
    notebook_paths: dict[str, Path]  # {"jupyter": ..., "zeppelin": ...}


@dataclass
class NavItem:
    number: str
    title: str
    page: Page | None
    children: list["NavItem"] = field(default_factory=list)


@dataclass
class SiteModel:
    pages: dict[str, Page]     # keyed by docs-relative posix path (e.g. "scenarios/foo.md")
    nav: list[NavItem]
    scenarios: list[Scenario]
    apps: list[Page]
