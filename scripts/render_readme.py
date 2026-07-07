"""Generate root README.md from docs/index.md, stripping MkDocs-specific syntax.

Run from repo root:
    uv run --group dev python scripts/render_readme.py

Output:
    README.md — auto-generated from docs/index.md with MkDocs syntax stripped
"""
from pathlib import Path
import re

REPO_ROOT = Path(__file__).parent.parent


def _strip_mkdocs(content: str) -> str:
    """Strip MkDocs Material-specific syntax for GitHub rendering."""
    lines = content.splitlines()
    out = []
    in_grid = False
    in_admonition = False

    for line in lines:
        stripped = line.strip()

        if stripped.startswith('<div class="grid cards" markdown>'):
            in_grid = True
            continue
        if in_grid and stripped == '</div>':
            in_grid = False
            continue

        if stripped.startswith('!!! ') or stripped.startswith('??? '):
            in_admonition = True
            m = re.match(r'^[!?]+\s+\w+\s+"([^"]+)"', stripped)
            label = f'> **{m.group(1)}**' if m else '> **Note**'
            out.append(label)
            continue
        if in_admonition and stripped and not line.startswith('    '):
            in_admonition = False
        if in_admonition:
            line = line.lstrip()
            line = re.sub(r'\((getting-started|scenarios/index|spark-apps/index|lakehouse|datasets|atlas-[\w-]+|go-live(?:-results)?)\.md\)',
                          r'(docs/\1.md)', line)
            out.append(line)
            continue

        if in_grid:
            line = re.sub(r':material-[\w-]+:\{[^}]*\}\s*\*\*([^*]+)\*\*', r'### \1', line)
            line = re.sub(r':material-[\w-]+:\{[^}]*\}', '', line)
            line = re.sub(r':octicons-[\w-]+:\s*', '', line)
            line = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'- [\1](docs/\2)', line)
            if line.strip() == '---' and out and out[-1].strip():
                continue
            if not line.strip():
                continue

        line = re.sub(r':material-[\w-]+:\s*', '', line)
        line = re.sub(r':octicons-[\w-]+:\s*', '', line)

        out.append(line)

    return '\n'.join(out)


def main() -> None:
    source = REPO_ROOT / "docs" / "index.md"
    readme = REPO_ROOT / "README.md"

    if not source.exists():
        raise FileNotFoundError(f"Source not found: {source}")

    content = source.read_text(encoding="utf-8")
    content = _strip_mkdocs(content)

    footer = """
---

## Quick Start

```bash
git clone https://github.com/thekaveh/data-eng-lab.git
cd data-eng-lab
make setup          # initialize Atlas submodule
make datasets       # download datasets
make up             # launch all services
make preflight      # verify connectivity
```

See [Getting Started](docs/getting-started.md) for the full guide.

## Spark Applications

| Application | Description |
|---|---|
| [nyc-taxi-etl](spark-apps/nyc-taxi-etl/) | Raw Parquet → cleaned Bronze Iceberg |
| [nyc-taxi-medallion](spark-apps/nyc-taxi-medallion/) | Bronze → Silver → Gold medallion pipeline |

Built by Jenkins CI, submitted via Airflow DAG. See [Spark Apps](docs/spark-apps/index.md) for details.

## License

This project uses a proprietary license — see [`LICENSE`](LICENSE) for terms.

---

*Full documentation at [thekaveh.github.io/data-eng-lab](https://thekaveh.github.io/data-eng-lab/). Maintained by `data-eng-lab`. Questions or issues → open a GitHub issue.*
"""

    readme.write_text(content.rstrip() + "\n" + footer, encoding="utf-8")
    lines = len(readme.read_text(encoding="utf-8").splitlines())
    print(f"README.md updated from {source} ({lines} lines)")


if __name__ == "__main__":
    main()
