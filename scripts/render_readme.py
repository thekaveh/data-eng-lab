"""Extract key sections from docs/index.md into the root README.md.

Run from repo root:
    uv run --group dev python scripts/render_readme.py

Output:
    README.md - auto-generated from docs/index.md content
"""
from pathlib import Path
import re

REPO_ROOT = Path(__file__).parent.parent


def _extract_section(content: str, heading: str) -> str:
    """Extract a section by its H2 heading (including trailing blank lines)."""
    pattern = rf"^## {heading}\s*\n(.*?)(?=\n## |\Z)"
    m = re.search(pattern, content, re.DOTALL)
    if m:
        return m.group(1).strip() + "\n\n"
    return ""


def _extract_title(content: str) -> str | None:
    """Return the first H1 heading text from *content*, or None."""
    for line in content.splitlines():
        m = re.match(r"^#\s+(.+)", line)
        if m:
            return m.group(1).strip()
    return None


def main() -> None:
    source = REPO_ROOT / "docs" / "index.md"
    readme = REPO_ROOT / "README.md"

    if not source.exists():
        raise FileNotFoundError(f"Source not found: {source}")

    content = source.read_text(encoding="utf-8")

    title = _extract_title(content)
    overview = _extract_section(content, "Overview")
    by_the_numbers = _extract_section(content, "By the numbers")
    quick_nav = _extract_section(content, "Quick navigation")
    architecture = _extract_section(content, "Architecture")

    lines = []
    lines.append(f"# {title}\n")

    if architecture:
        lines.append(f"{architecture}\n")

    lines.append("---\n")

    if quick_nav:
        lines.append(f"## Quick navigation\n\n{quick_nav}\n")

    lines.append("---\n")

    if by_the_numbers:
        lines.append(f"## By the numbers\n\n{by_the_numbers}\n")

    if overview:
        lines.append(f"## Overview\n\n{overview}\n")

    readme.write_text("\n".join(lines), encoding="utf-8")
    print(f"README.md updated from {source}")


if __name__ == "__main__":
    main()
