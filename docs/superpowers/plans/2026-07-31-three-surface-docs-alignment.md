# Three-Surface Documentation Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the repository Markdown, generated MkDocs site, and generated GitHub wiki deterministic, self-contained projections of one manifest-declared public documentation source while correcting all audited Atlas documentation drift.

**Architecture:** Public Markdown remains committed and directly GitHub-renderable. `docs/manifest.yaml` declares the public page set, hierarchy, numbering, and diagrams; an importable `scripts.docs` package renders ignored `generated/site/`, ignored `generated/wiki/`, and ignored root `mkdocs.yml`, while content-hash checks prove determinism. `docs/superpowers/` is an explicit internal archive, and `infra/` remains immutable.

**Tech Stack:** Python 3.11, PyYAML, CairoSVG, MkDocs Material, pytest, Ruff, GNU Make, GitHub Actions, GitHub Pages, GitHub Wiki.

## Global Constraints

- Work only on `codex/three-surface-docs-sync` until the feature PR is merged.
- Do not edit, stage, reset, delete, or move `docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md`; it is user-owned and untracked.
- Do not modify any file inside `infra/` or change the Atlas gitlink `985918ce8c805081947d53b1c48bb80610237a5b`.
- Use `docs/manifest.yaml` as the sole declaration of public page membership, hierarchy, baked numbering, and diagram inventory.
- Treat `docs/superpowers/**` as the sole explicit internal-document exception; never publish it.
- Generate and ignore `/generated/`, `/mkdocs.yml`, and `/site/`.
- Generate MkDocs input under `generated/site/`; generate wiki input under `generated/wiki/`.
- Do not emit `repo_url`, `repo_name`, or `edit_uri` in MkDocs configuration.
- Invoke all documentation tooling as `uv run --group dev python -m scripts.docs.<module>`.
- Derive one site SVG and one committed PNG from each committed diagram HTML master; copy assets physically to each generated surface.
- Keep CI merge gates on both `develop` and `main`; publish Pages/wiki only from `main`.
- Use explicit-path staging for every commit. Never use `git add .` or `git add -A` in the parent repository.

---

## File Structure

### New canonical and generated-system files

- `docs/manifest.yaml` — complete public hierarchy, page numbering, and diagram master inventory.
- `docs/diagrams/*.html` — one fact-checked HTML/SVG master per current architecture diagram.
- `docs/diagrams/img/*.png` — committed repository/wiki raster exports.
- `docs/stylesheets/extra.css` — canonical site stylesheet copied to generated site input.
- `scripts/docs/__init__.py` — importable package marker.
- `scripts/docs/manifest.py` — manifest dataclasses, parsing, structural validation, and path validation.
- `scripts/docs/links.py` — Markdown link discovery and surface-forbidden classification.
- `scripts/docs/transforms.py` — manifest path maps plus site/wiki link and image rewriting.
- `scripts/docs/render_diagrams.py` — SVG extraction/sanitization, PNG rendering, and asset copying.
- `scripts/docs/build_docs.py` — site/wiki/MkDocs rendering and content-hash determinism checks.
- `scripts/docs/check_docs.py` — aggregate public completeness, numbering, self-containment, placeholder, empty-artifact, and determinism gate.
- `scripts/docs/push_wiki.py` — safe synchronization of generated wiki content to wiki `master`.
- `tests/scripts/docs/` — focused unit tests for every new module.

### Modified canonical/content/control files

- `.gitignore`, `Makefile`, `pyproject.toml`, `uv.lock` — generated paths, supported entry points, CairoSVG dependency, and import configuration.
- `README.md`, `docs/*.md`, `docs/scenarios/*.md`, `docs/notebooks/*.md`, `docs/spark-apps/*.md` — baked headings, self-contained links, and corrected Atlas acceptance record.
- `.github/workflows/ci.yml`, `.github/workflows/docs-deploy.yml`, `.github/workflows/docs-sync.yml` — module invocation, Cairo installation, both Gitflow gates, and main-only publication.
- `tests/test_makefile.py`, `tests/test_repo_structure.py`, `tests/test_ci_atlas_contract.py` — repository-level contract updates.

### Retired files after replacement coverage is green

- Tracked root `mkdocs.yml` — removed from the index and regenerated locally/CI.
- `scripts/build_docs.py`, `scripts/check_surfaces.py`, `scripts/check_diagrams.py`, `scripts/diagrams_manifest.yaml`, `scripts/docslib/`, and their superseded tests/fixtures — removed only after their behaviors are represented in `scripts/docs/` tests.
- `docs/architectures/*.svg` and generated architecture copies under scenario/app directories — removed only after HTML masters and PNG/SVG projections are fully referenced.
- `docs/architectures/.gitkeep` and `docs/overrides/.gitkeep` — empty placeholders removed; meaningful override content is either migrated into the generated template or retained in a non-empty canonical asset.

---

### Task 1: Establish the canonical manifest and repository boundary

**Files:**
- Create: `docs/manifest.yaml`
- Create: `scripts/docs/__init__.py`
- Create: `scripts/docs/manifest.py`
- Create: `tests/scripts/docs/__init__.py`
- Create: `tests/scripts/docs/test_manifest.py`
- Modify: `.gitignore`
- Modify: `pyproject.toml`
- Delete from index: `mkdocs.yml`

**Interfaces:**
- Produces: `ManifestError`, `Section`, `DiagramEntry`, `Manifest`, `parse_manifest(text: str) -> Manifest`, `load_manifest(path: Path, repo_root: Path) -> Manifest`, `iter_leaf_sections(sections: tuple[Section, ...]) -> Iterator[Section]`.
- Consumers: Tasks 2–5 import these exact names.

- [ ] **Step 1: Write failing manifest parser and path-validation tests**

```python
from pathlib import Path

import pytest

from scripts.docs.manifest import ManifestError, iter_leaf_sections, load_manifest, parse_manifest


VALID = """\
surfaces: [repo, site, wiki]
numbering: baked
internal_roots: [docs/superpowers]
sections:
  - {id: overview, number: '1', title: Overview, source: docs/index.md}
  - id: scenarios
    number: '5'
    title: Scenarios
    children:
      - {id: catalog, number: '5.1', title: Catalog, source: docs/scenarios/index.md}
diagrams:
  - {id: overview, master: docs/diagrams/overview.html}
"""


def test_parse_manifest_exposes_leaf_order_and_internal_boundary():
    manifest = parse_manifest(VALID)
    assert manifest.surfaces == ("repo", "site", "wiki")
    assert manifest.internal_roots == (Path("docs/superpowers"),)
    assert [(leaf.number, leaf.source) for leaf in iter_leaf_sections(manifest.sections)] == [
        ("1", Path("docs/index.md")),
        ("5.1", Path("docs/scenarios/index.md")),
    ]


@pytest.mark.parametrize(
    "text,message",
    [
        ("surfaces: [repo]\nnumbering: baked\nsections: []\ndiagrams: []\n", "surfaces must be repo, site, wiki"),
        (VALID.replace("numbering: baked", "numbering: generated"), "numbering must be baked"),
        (VALID.replace("source: docs/scenarios/index.md", "source: docs/scenarios/index.md\n    children: []"), "cannot define both source and children"),
        (VALID.replace("number: '5.1'", "number: '1'"), "duplicate section number: 1"),
    ],
)
def test_parse_manifest_rejects_invalid_contract(text, message):
    with pytest.raises(ManifestError, match=message):
        parse_manifest(text)


def test_load_manifest_rejects_missing_source_and_master(tmp_path):
    path = tmp_path / "docs" / "manifest.yaml"
    path.parent.mkdir()
    path.write_text(VALID, encoding="utf-8")
    with pytest.raises(ManifestError, match="missing manifest path: docs/index.md"):
        load_manifest(path, tmp_path)
```

- [ ] **Step 2: Run the focused test and confirm the import failure**

Run: `uv run --group dev pytest tests/scripts/docs/test_manifest.py -q`

Expected: collection fails with `ModuleNotFoundError: No module named 'scripts.docs'`.

- [ ] **Step 3: Implement immutable manifest types and validation**

```python
@dataclass(frozen=True)
class Section:
    id: str
    number: str
    title: str
    source: Path | None = None
    children: tuple["Section", ...] = ()
    diagrams: tuple[str, ...] = ()


@dataclass(frozen=True)
class DiagramEntry:
    id: str
    master: Path


@dataclass(frozen=True)
class Manifest:
    surfaces: tuple[str, ...]
    numbering: str
    internal_roots: tuple[Path, ...]
    sections: tuple[Section, ...]
    diagrams: tuple[DiagramEntry, ...]
```

Implement `parse_manifest` so YAML/parser/key/type errors are wrapped as `ManifestError`, every node is exactly a source leaf or children group, and IDs/numbers/diagram IDs are unique. Implement `load_manifest` so every source/master resolves inside `repo_root` and exists.

- [ ] **Step 4: Add the complete public-page manifest**

Encode the current `mkdocs.yml` navigation exactly as public leaves: overview `1`; getting-started `2`; lakehouse `3`; datasets `4`; scenario catalog `5.1` plus the 19 scenario pages `5.2`–`5.20`; notebook index `6.1` plus the 19 notebook pages `6.2`–`6.20`; Spark app index `7.1` plus two app pages `7.2`–`7.3`; Atlas operation pages `8.1`–`8.7`; changelog `9`.

Set `diagrams: []` in this first valid manifest. Task 3 creates and fact-checks the HTML masters, then atomically replaces that empty list with these exact 23 entries so `load_manifest` never observes missing paths:

```yaml
diagrams:
  - {id: overview, master: docs/diagrams/overview.html}
  - {id: lakehouse, master: docs/diagrams/lakehouse.html}
  - {id: batch_ingest-nyc_taxi-spark-iceberg, master: docs/diagrams/batch_ingest-nyc_taxi-spark-iceberg.html}
  - {id: bi_query-tpch-trino-iceberg, master: docs/diagrams/bi_query-tpch-trino-iceberg.html}
  - {id: cdc_streaming-online_retail-spark-iceberg, master: docs/diagrams/cdc_streaming-online_retail-spark-iceberg.html}
  - {id: data_quality-nyc_taxi-spark-iceberg, master: docs/diagrams/data_quality-nyc_taxi-spark-iceberg.html}
  - {id: feature_engineering-movielens-spark-iceberg, master: docs/diagrams/feature_engineering-movielens-spark-iceberg.html}
  - {id: federated_query-nyc_taxi-trino-iceberg, master: docs/diagrams/federated_query-nyc_taxi-trino-iceberg.html}
  - {id: incremental_upsert-online_retail-spark-iceberg, master: docs/diagrams/incremental_upsert-online_retail-spark-iceberg.html}
  - {id: join_optimization-tpch-spark-iceberg, master: docs/diagrams/join_optimization-tpch-spark-iceberg.html}
  - {id: json_flatten-gh_archive-spark-iceberg, master: docs/diagrams/json_flatten-gh_archive-spark-iceberg.html}
  - {id: medallion-nyc_taxi-spark-iceberg, master: docs/diagrams/medallion-nyc_taxi-spark-iceberg.html}
  - {id: nyc-taxi-etl, master: docs/diagrams/nyc-taxi-etl.html}
  - {id: nyc-taxi-medallion, master: docs/diagrams/nyc-taxi-medallion.html}
  - {id: scd2-online_retail-spark-iceberg, master: docs/diagrams/scd2-online_retail-spark-iceberg.html}
  - {id: schema_evolution-gh_archive-spark-iceberg, master: docs/diagrams/schema_evolution-gh_archive-spark-iceberg.html}
  - {id: sessionization-gh_archive-spark-iceberg, master: docs/diagrams/sessionization-gh_archive-spark-iceberg.html}
  - {id: star_schema-tpch-spark-iceberg, master: docs/diagrams/star_schema-tpch-spark-iceberg.html}
  - {id: streaming_ingest-events-spark-iceberg, master: docs/diagrams/streaming_ingest-events-spark-iceberg.html}
  - {id: streaming_ingest-gh_archive-spark-iceberg, master: docs/diagrams/streaming_ingest-gh_archive-spark-iceberg.html}
  - {id: streaming_windows-events-spark-iceberg, master: docs/diagrams/streaming_windows-events-spark-iceberg.html}
  - {id: table_maintenance-nyc_taxi-spark-iceberg, master: docs/diagrams/table_maintenance-nyc_taxi-spark-iceberg.html}
  - {id: time_travel-nyc_taxi-spark-iceberg, master: docs/diagrams/time_travel-nyc_taxi-spark-iceberg.html}
```

- [ ] **Step 5: Configure imports and generated-path ignores**

Add `pythonpath = ["."]` under `[tool.pytest.ini_options]`. Add root-anchored `/generated/`, `/mkdocs.yml`, and `/site/` entries to `.gitignore`, then run `git rm --cached mkdocs.yml` only after the generator test in Task 4 proves it can reproduce the config.

- [ ] **Step 6: Run tests and commit**

Run: `uv run --group dev pytest tests/scripts/docs/test_manifest.py -q`

Expected: all manifest tests pass.

Commit:

```bash
git add .gitignore pyproject.toml docs/manifest.yaml scripts/docs/__init__.py scripts/docs/manifest.py tests/scripts/docs/__init__.py tests/scripts/docs/test_manifest.py
git commit -m "docs: define canonical documentation manifest"
```

---

### Task 2: Enforce self-contained links and deterministic path transforms

**Files:**
- Create: `scripts/docs/links.py`
- Create: `scripts/docs/transforms.py`
- Create: `tests/scripts/docs/test_links.py`
- Create: `tests/scripts/docs/test_transforms.py`

**Interfaces:**
- Consumes: `Manifest`, `iter_leaf_sections` from Task 1.
- Produces: `Link(target: str)`, `find_links(markdown: str) -> tuple[Link, ...]`, `is_forbidden(target: str, surface: str) -> bool`, `build_source_map(manifest: Manifest, surface: str) -> dict[Path, Path]`, `rewrite_for_surface(markdown: str, surface: str, source: Path, source_map: Mapping[Path, Path]) -> str`.

- [ ] **Step 1: Write the forbidden-link matrix tests**

```python
@pytest.mark.parametrize(
    "surface,target,forbidden",
    [
        ("repo", "https://thekaveh.github.io/data-eng-lab/", True),
        ("repo", "https://github.com/thekaveh/data-eng-lab/wiki", True),
        ("site", "https://github.com/thekaveh/data-eng-lab/blob/main/docs/index.md", True),
        ("site", "https://github.com/thekaveh/data-eng-lab/wiki", True),
        ("wiki", "https://thekaveh.github.io/data-eng-lab/", True),
        ("wiki", "https://github.com/thekaveh/data-eng-lab/blob/main/README.md", True),
        ("repo", "https://airflow.apache.org/", False),
        ("site", "https://iceberg.apache.org/", False),
        ("wiki", "https://spark.apache.org/", False),
    ],
)
def test_surface_link_matrix(surface, target, forbidden):
    assert is_forbidden(target, surface) is forbidden


def test_find_links_reads_markdown_links_and_images():
    links = find_links("[Docs](docs/index.md) ![Flow](architectures/overview.svg)")
    assert [link.target for link in links] == ["docs/index.md", "architectures/overview.svg"]
```

- [ ] **Step 2: Write transform tests for mapped, internal, notebook, and image paths**

```python
def test_build_source_map_uses_home_for_wiki(manifest):
    mapping = build_source_map(manifest, "wiki")
    assert mapping[Path("docs/index.md")] == Path("Home.md")
    assert mapping[Path("docs/scenarios/index.md")] == Path("Scenarios.md")


def test_rewrite_for_site_preserves_subdirectory_image_prefix(manifest):
    mapping = build_source_map(manifest, "site")
    result = rewrite_for_surface(
        "[Catalog](../scenarios/index.md) ![Flow](../architectures/overview.svg)",
        "site",
        Path("docs/notebooks/example.md"),
        mapping,
    )
    assert "[Catalog](../scenarios/index.md)" in result
    assert "![Flow](../assets/img/overview.svg)" in result


def test_rewrite_drops_forbidden_and_non_manifest_targets(manifest):
    mapping = build_source_map(manifest, "wiki")
    result = rewrite_for_surface(
        "[Source](https://github.com/thekaveh/data-eng-lab/blob/main/docs/index.md) "
        "[Draft](superpowers/specs/internal.md) [Notebook](../../scenarios/x/jupyter/notebook.ipynb)",
        "wiki",
        Path("docs/index.md"),
        mapping,
    )
    assert result == "Source Draft Notebook"
```

- [ ] **Step 3: Run tests and confirm missing-module failures**

Run: `uv run --group dev pytest tests/scripts/docs/test_links.py tests/scripts/docs/test_transforms.py -q`

Expected: collection fails for missing `scripts.docs.links` and `scripts.docs.transforms`.

- [ ] **Step 4: Implement the matrix and rewrite pipeline**

Use constants for the exact repository, wiki, and Pages origins. In `is_forbidden`, test the wiki URL before the repository URL because the wiki URL contains the repository URL. Resolve relative Markdown paths against `source.parent`, map only manifest-known pages, convert forbidden/non-manifest/notebook links to their visible label, and rewrite a diagram whose manifest ID is `diagram.id` to `assets/img/{diagram.id}.svg` for site and `img/{diagram.id}.png` for wiki.

- [ ] **Step 5: Run tests and commit**

Run: `uv run --group dev pytest tests/scripts/docs/test_links.py tests/scripts/docs/test_transforms.py -q`

Expected: all tests pass.

Commit:

```bash
git add scripts/docs/links.py scripts/docs/transforms.py tests/scripts/docs/test_links.py tests/scripts/docs/test_transforms.py
git commit -m "docs: add self-contained surface transforms"
```

---

### Task 3: Replace copied SVGs with fact-checked HTML masters and derived assets

**Files:**
- Modify: `docs/manifest.yaml` (replace `diagrams: []` with the 23 entries enumerated in Task 1)
- Create: `scripts/docs/render_diagrams.py`
- Create: `tests/scripts/docs/test_render_diagrams.py`
- Create: `docs/diagrams/*.html` (23 manifest masters)
- Create: `docs/diagrams/img/*.png` (23 committed derived images)
- Modify: scenario, notebook, overview, lakehouse, and Spark-app Markdown diagram references
- Delete after verification: `docs/architectures/*.svg` and redundant scenario/app architecture copies

**Interfaces:**
- Consumes: `Manifest.diagrams` from Task 1.
- Produces: `import_svg_master(svg_text: str, *, title: str, evidence: str) -> str`, `extract_svg(html_text: str) -> str`, `svg_to_png(svg: str, destination: Path, *, width: int = 1600) -> None`, `render_all(manifest: Manifest, repo_root: Path, site_img_dir: Path, png_dir: Path) -> None`, `copy_assets(png_dir: Path, wiki_img_dir: Path) -> None`.

- [ ] **Step 1: Use the architecture-diagram skill to establish the master template**

Each master is a standalone HTML document containing exactly one inline `<svg>` and a short source-evidence comment. Preserve existing diagram semantics unless repository evidence requires a correction. For each master, verify every node/edge against the corresponding scenario DAG/notebook, Spark app source, or top-level Atlas consumer overlay before committing. Implement `import_svg_master` as the one-time, deterministic bulk conversion from each current SVG to its HTML master:

```python
def import_svg_master(svg_text: str, *, title: str, evidence: str) -> str:
    svg = svg_text.strip()
    if not svg.startswith("<svg") or not svg.endswith("</svg>"):
        raise DiagramError("legacy diagram must contain one standalone svg root")
    return (
        "<!doctype html>\n"
        '<html lang="en">\n'
        '<head><meta charset="utf-8">'
        f"<title>{html.escape(title)}</title></head>\n"
        "<body>\n"
        f"  <!-- {html.escape(evidence)} -->\n"
        f"{svg}\n"
        "</body>\n"
        "</html>\n"
    )
```

The required minimal master shape is:

```html
<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>data-eng-lab — overview</title></head>
<body>
  <!-- Verified against atlas.consumer.yml, compose/data-eng-lab.yml, and scenario DAGs on 2026-07-31. -->
  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1600 900" role="img" aria-labelledby="title desc">
    <title id="title">data-eng-lab architecture</title>
    <desc id="desc">Atlas services and repository-owned data-engineering workloads.</desc>
  </svg>
</body>
</html>
```

- [ ] **Step 2: Write failing renderer tests**

```python
def test_extract_svg_sanitizes_html_named_entities():
    html = '<html><svg xmlns="http://www.w3.org/2000/svg"><text>&Sigma; &middot; &amp;</text></svg></html>'
    assert extract_svg(html) == '<svg xmlns="http://www.w3.org/2000/svg"><text>Σ · &amp;</text></svg>'


def test_import_svg_master_preserves_the_complete_svg():
    svg = '<svg xmlns="http://www.w3.org/2000/svg"><path d="M0 0h10v10z"/></svg>'
    master = import_svg_master(svg, title="Overview", evidence="Verified against atlas.consumer.yml on 2026-07-31.")
    assert svg in master
    assert master.count("<svg") == 1
    assert "Verified against atlas.consumer.yml on 2026-07-31." in master


def test_svg_to_png_writes_png_magic(tmp_path):
    pytest.importorskip("cairosvg")
    destination = tmp_path / "diagram.png"
    svg_to_png('<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10"/>', destination, width=100)
    assert destination.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")


def test_render_all_writes_site_svg_and_committed_png(tmp_path, diagram_manifest):
    pytest.importorskip("cairosvg")
    render_all(diagram_manifest, tmp_path, tmp_path / "generated/site/assets/img", tmp_path / "docs/diagrams/img")
    assert (tmp_path / "generated/site/assets/img/overview.svg").is_file()
    assert (tmp_path / "docs/diagrams/img/overview.png").read_bytes().startswith(b"\x89PNG")
```

- [ ] **Step 3: Add CairoSVG and implement rendering**

Add `cairosvg>=2.7,<3.0` to the `dev` dependency group and run `uv lock`. Implement lazy CairoSVG import, entity sanitization that preserves XML entities/numeric references, deterministic UTF-8 SVG writes, and PNG parent-directory creation.

- [ ] **Step 4: Build and inspect all assets**

Run:

```bash
uv run --group dev python -m scripts.docs.render_diagrams --root .
file docs/diagrams/img/*.png
```

Expected: 23 SVG projections exist under `generated/site/assets/img`, 23 committed PNG files exist under `docs/diagrams/img`, and every `file` result reports PNG image data.

- [ ] **Step 5: Replace canonical Markdown image references**

Repository Markdown must reference committed PNGs with correct relative paths: root-level docs use `diagrams/img/{diagram.id}.png`; `docs/scenarios/`, `docs/notebooks/`, and `docs/spark-apps/` use `../diagrams/img/{diagram.id}.png`. The transforms rewrite these to surface-local SVG/PNG locations for generated site/wiki output.

- [ ] **Step 6: Run tests and commit**

Run: `uv run --group dev pytest tests/scripts/docs/test_render_diagrams.py -q`

Expected: all renderer tests pass.

Commit all 23 masters/PNGs, renderer code/tests, dependency lock, canonical Markdown reference changes, and the explicit removal paths with message:

```bash
git commit -m "docs: derive diagram assets from canonical masters"
```

---

### Task 4: Generate the MkDocs and wiki surfaces by content hash

**Files:**
- Create: `scripts/docs/build_docs.py`
- Create: `tests/scripts/docs/test_build_docs.py`
- Move/adapt: `docs/css/custom.css` → `docs/stylesheets/extra.css`
- Retain/adapt: `docs/overrides/main.html` only if the generated MkDocs template references a non-empty custom directory
- Generate/ignore: `generated/site/`, `generated/wiki/`, `mkdocs.yml`
- Delete after parity: `scripts/build_docs.py`, `scripts/docslib/`, superseded `tests/scripts/docslib/`, and old docs fixtures tied to tracked `mkdocs.yml`

**Interfaces:**
- Consumes: manifest loader, transforms, and diagram renderer from Tasks 1–3.
- Produces: `render_site(manifest: Manifest, repo_root: Path, output: Path) -> None`, `render_wiki(manifest: Manifest, repo_root: Path, output: Path) -> None`, `render_mkdocs_yml(manifest: Manifest) -> str`, `hash_tree(root: Path) -> dict[Path, str]`, `assert_dirs_equal(actual: Path, expected: Path) -> None`, `build(repo_root: Path, *, site: bool, wiki: bool, check: bool) -> None`.

- [ ] **Step 1: Write failing builder tests**

```python
def test_render_mkdocs_uses_generated_site_and_has_no_repo_controls(manifest):
    config = render_mkdocs_yml(manifest)
    assert "docs_dir: generated/site" in config
    assert "site_dir: site" in config
    assert "repo_url" not in config
    assert "repo_name" not in config
    assert "edit_uri" not in config
    assert '"1. Overview": index.md' in config


def test_render_site_and_wiki_are_complete(tmp_repo, manifest):
    render_site(manifest, tmp_repo, tmp_repo / "generated/site")
    render_wiki(manifest, tmp_repo, tmp_repo / "generated/wiki")
    assert (tmp_repo / "generated/site/index.md").is_file()
    assert (tmp_repo / "generated/wiki/Home.md").is_file()
    assert (tmp_repo / "generated/wiki/_Sidebar.md").is_file()
    assert (tmp_repo / "generated/wiki/_Footer.md").is_file()


def test_assert_dirs_equal_detects_same_file_set_with_changed_content(tmp_path):
    actual = tmp_path / "actual"
    expected = tmp_path / "expected"
    actual.mkdir()
    expected.mkdir()
    (actual / "page.md").write_text("new", encoding="utf-8")
    (expected / "page.md").write_text("old", encoding="utf-8")
    with pytest.raises(DocumentationDrift, match="page.md"):
        assert_dirs_equal(actual, expected)
```

- [ ] **Step 2: Implement the generated MkDocs template**

Use a static template with `site_name: data-eng-lab`, `site_url: https://thekaveh.github.io/data-eng-lab/`, `docs_dir: generated/site`, `site_dir: site`, current Material dark/light palettes, canonical stylesheet/override copies, existing Markdown extensions that are actually used, search plugin, and a single generated `{nav}` insertion. Do not include repository metadata or the obsolete `exclude_docs` rule.

- [ ] **Step 3: Implement complete site and wiki projection**

For every manifest leaf, copy transformed Markdown to its source-relative site path and to a stable wiki filename (`Home.md` for overview; title/ID-derived names for others). Generate `_Sidebar.md` from the same section tree and `_Footer.md` with project identity but no site/repository links. Copy SVGs to `generated/site/assets/img` and PNGs to `generated/wiki/img`.

- [ ] **Step 4: Implement content-hash determinism**

`hash_tree` returns `{relative_path: sha256(file_bytes).hexdigest()}` for all files in sorted order. `build(..., check=True)` first renders the requested generated output, then rerenders into a temporary directory and calls `assert_dirs_equal` for both trees. It must detect changed bytes even when filenames match.

- [ ] **Step 5: Prove the new generator replaces the legacy pipeline**

Run both builders into temporary directories and compare the public page inventory. Account for deliberate changes: generated site input, self-contained links, manifest numbering, diagram format, and the absence of public `docs/superpowers`. Only then remove legacy implementation/tests and use `git rm --cached mkdocs.yml` so the now-generated config is untracked.

- [ ] **Step 6: Run focused and legacy regression tests, then commit**

Run:

```bash
uv run --group dev pytest tests/scripts/docs/test_build_docs.py tests/scripts/docs/test_manifest.py tests/scripts/docs/test_links.py tests/scripts/docs/test_transforms.py tests/scripts/docs/test_render_diagrams.py -q
uv run --group dev python -m scripts.docs.build_docs --site --wiki --check --root .
```

Expected: all tests pass; both generated trees reproduce with identical hashes.

Commit with message:

```bash
git commit -m "docs: generate site and wiki from one source"
```

---

### Task 5: Add the canonical aggregate gate and Make targets

**Files:**
- Create: `scripts/docs/check_docs.py`
- Create: `scripts/docs/push_wiki.py`
- Create: `tests/scripts/docs/test_check_docs.py`
- Create: `tests/scripts/docs/test_push_wiki.py`
- Modify: `Makefile`
- Modify: `tests/test_makefile.py`
- Modify: `tests/test_repo_structure.py`
- Delete after parity: `scripts/check_surfaces.py`, `scripts/check_diagrams.py`, `scripts/diagrams_manifest.yaml`, `tests/scripts/test_check_surfaces.py`, `tests/scripts/test_check_diagrams.py`

**Interfaces:**
- Produces: `Finding(severity: str, message: str)`, `check_completeness(...)`, `check_numbering(...)`, `check_self_containment(...)`, `check_placeholders(...)`, `check_empty_artifacts(...)`, `check(repo_root: Path) -> tuple[Finding, ...]`, `sync_wiki(source: Path, clone: Path) -> None`, `push_wiki(source: Path, remote: str, key_path: Path | None, *, push: bool) -> None`.

- [ ] **Step 1: Write failing aggregate-gate tests**

```python
def test_completeness_ignores_only_explicit_internal_root(repo_fixture):
    (repo_fixture / "docs/unmanifested.md").write_text("# Unmanifested\n", encoding="utf-8")
    (repo_fixture / "docs/superpowers/internal.md").parent.mkdir(parents=True)
    (repo_fixture / "docs/superpowers/internal.md").write_text("# Internal\n", encoding="utf-8")
    findings = check_completeness(repo_fixture)
    assert [finding.message for finding in findings] == ["public Markdown is absent from manifest: docs/unmanifested.md"]


def test_numbering_matches_manifest_heading(repo_fixture):
    (repo_fixture / "docs/index.md").write_text("# Overview\n", encoding="utf-8")
    findings = check_numbering(repo_fixture)
    assert findings[0].message == "docs/index.md heading must start with '# 1. Overview'"


def test_empty_public_artifacts_are_errors_but_generated_dirs_are_not(repo_fixture):
    (repo_fixture / "docs/empty.md").touch()
    findings = check_empty_artifacts(repo_fixture)
    assert findings[0].message == "empty public documentation file: docs/empty.md"
```

- [ ] **Step 2: Write wiki sync tests including identity and `master`**

```python
def test_sync_wiki_preserves_git_and_removes_stale_files(tmp_path):
    source = tmp_path / "source"
    clone = tmp_path / "clone"
    (source / "Home.md").parent.mkdir()
    (source / "Home.md").write_text("home", encoding="utf-8")
    (clone / ".git").mkdir(parents=True)
    (clone / "stale.md").write_text("stale", encoding="utf-8")
    sync_wiki(source, clone)
    assert (clone / ".git").is_dir()
    assert not (clone / "stale.md").exists()
    assert (clone / "Home.md").read_text(encoding="utf-8") == "home"


def test_push_command_targets_master_and_supplies_default_identity(fake_git, tmp_path):
    push_wiki(tmp_path / "source", "git@github.com:thekaveh/data-eng-lab.wiki.git", tmp_path / "key", push=True)
    assert fake_git.last_push_ref == "master"
    assert fake_git.env["GIT_AUTHOR_NAME"] == "data-eng-lab docs bot"
    assert fake_git.env["GIT_AUTHOR_EMAIL"] == "docs-bot@users.noreply.github.com"


def test_https_remote_does_not_require_an_ssh_key(fake_git, tmp_path):
    push_wiki(
        tmp_path / "source",
        "https://x-access-token:token@github.com/thekaveh/data-eng-lab.wiki.git",
        None,
        push=True,
    )
    assert "GIT_SSH_COMMAND" not in fake_git.env
    assert fake_git.last_push_ref == "master"
```

- [ ] **Step 3: Implement all gate probes**

`check` must render both surfaces with `check=True`, scan canonical repository Markdown plus generated Markdown/config, cross-check every manifest number with the first H1, reject the standard unfinished-work markers outside `docs/superpowers`, reject empty public files/directories, require all HTML-master/PNG/SVG projections, and return findings sorted by message. Define the exact marker tuple in code as `("TO" + "DO", "TB" + "D", "FIX" + "ME", "X" + "XX")` so this plan does not flag itself. The CLI prints each finding and exits `1` if any severity is `error`.

- [ ] **Step 4: Implement safe wiki synchronization**

Clone to a temporary directory for `--push`, preserve `.git`, remove stale working-tree files, copy `generated/wiki`, stage within the clone, skip commit/push when the index is unchanged, default author/committer identity, and push `HEAD:master`. Set `GIT_SSH_COMMAND` only for an SSH remote with a non-null key path; accept an authenticated HTTPS remote with `key_path=None` so the current `GITHUB_TOKEN` publisher remains supported.

- [ ] **Step 5: Add the supported Make interface**

```makefile
docs-build: ## Generate diagrams, site input, and build the strict site
	uv run --group dev python -m scripts.docs.render_diagrams --root .
	uv run --group dev python -m scripts.docs.build_docs --site --root .
	uv run --group dev mkdocs build --strict

docs-check: ## Verify all documentation surfaces and build the strict site
	uv run --group dev python -m scripts.docs.render_diagrams --root .
	uv run --group dev python -m scripts.docs.check_docs --root .
	uv run --group dev mkdocs build --strict

docs-serve: ## Generate site input and serve it locally
	uv run --group dev python -m scripts.docs.render_diagrams --root .
	uv run --group dev python -m scripts.docs.build_docs --site --root .
	uv run --group dev mkdocs serve

docs-wiki: ## Generate and validate the wiki projection without pushing
	uv run --group dev python -m scripts.docs.build_docs --wiki --root .
	uv run --group dev python -m scripts.docs.push_wiki --check --root .
```

Add all four targets to `.PHONY` and assert them in `tests/test_makefile.py`.

- [ ] **Step 6: Run tests and commit**

Run:

```bash
uv run --group dev pytest tests/scripts/docs/test_check_docs.py tests/scripts/docs/test_push_wiki.py tests/test_makefile.py tests/test_repo_structure.py -q
make docs-check
```

Expected: all tests and the aggregate gate pass.

Commit with message:

```bash
git commit -m "docs: enforce three-surface documentation parity"
```

---

### Task 6: Reconcile public headings and Atlas acceptance content

**Files:**
- Modify: all manifest-listed Markdown whose H1 does not begin with its manifest number/title
- Modify: `docs/atlas-enablement.md`
- Modify: `docs/atlas-feedback-go-live.md`
- Modify: `docs/go-live.md`
- Modify: `docs/go-live-results.md`
- Modify: `docs/CHANGELOG.md`
- Modify: other manifest pages only when the self-containment/numbering gate identifies a concrete defect
- Delete: `docs/architectures/.gitkeep`, `docs/overrides/.gitkeep`
- Test: `tests/scripts/docs/test_check_docs.py`

**Interfaces:**
- Consumes: manifest numbering and check gate from Tasks 1 and 5.
- Produces: consistent, clinically written canonical content projected unchanged to site/wiki.

- [ ] **Step 1: Add a content regression test for the completed acceptance record**

```python
def test_atlas_acceptance_record_is_consistent(repo_root):
    required = {
        "docs/atlas-enablement.md": ["2026-07-31", "SparkSubmitOperator", "succeeded"],
        "docs/atlas-feedback-go-live.md": ["2026-07-31", "resolved", "FINISHED"],
        "docs/go-live-results.md": ["8,991,502", "passenger_count", "double", "success=true"],
        "docs/CHANGELOG.md": ["985918ce8c805081947d53b1c48bb80610237a5b", "2026-07-31"],
    }
    for relative, phrases in required.items():
        text = (repo_root / relative).read_text(encoding="utf-8")
        for phrase in phrases:
            assert phrase in text, f"{relative} is missing {phrase!r}"
```

- [ ] **Step 2: Run the regression and confirm stale pages fail**

Run: `uv run --group dev pytest tests/scripts/docs/test_check_docs.py::test_atlas_acceptance_record_is_consistent -q`

Expected: failure identifies the first missing completed-acceptance phrase.

- [ ] **Step 3: Apply baked H1 numbering from the manifest**

For every leaf, make its first heading `# {section.number}. {section.title}` for integer numbers and `# {section.number} {section.title}` when the number already includes a decimal point. Keep child heading hierarchy consistent: H2 has one numeric component below the page context and H3 uses the hierarchical subsection form already required by notebook checks. Do not number code fences or source extracts.

- [ ] **Step 4: Replace pending Atlas language with the verified outcome**

Record these exact facts, with no broader success claim:

- acceptance date: 2026-07-31;
- reviewed Atlas pin: `985918ce8c805081947d53b1c48bb80610237a5b`;
- representative Airflow feature-artifact DAG task: first and only attempt succeeded;
- Spark standalone REST state: `FINISHED` and `success=true`;
- Bronze table row count: `8,991,502`;
- `passenger_count` Iceberg type: `double`;
- Gitflow promotions already completed through PRs #66, #67, and #68 for the Atlas consumer modernization.

Retain reusable warnings and future pin-bump gates as runbook instructions; only historical claims change from pending to complete.

- [ ] **Step 5: Remove empty placeholders and regenerate both outputs**

Run:

```bash
uv run --group dev python -m scripts.docs.render_diagrams --root .
uv run --group dev python -m scripts.docs.build_docs --site --wiki --check --root .
uv run --group dev python -m scripts.docs.check_docs --root .
```

Expected: no numbering, placeholder, completeness, empty-artifact, or cross-surface findings.

- [ ] **Step 6: Run content tests and commit**

Run: `uv run --group dev pytest tests/scripts/docs/test_check_docs.py -q`

Expected: all tests pass.

Commit only manifest-listed Markdown, the two explicit empty-file removals, and the regression test with message:

```bash
git commit -m "docs: reconcile Atlas acceptance across surfaces"
```

---

### Task 7: Replace split CI with module-based Gitflow gates and main publication

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/docs-deploy.yml`
- Modify or retire: `.github/workflows/docs-sync.yml`
- Modify: `tests/test_ci_atlas_contract.py`
- Create: `tests/scripts/docs/test_workflows.py`

**Interfaces:**
- Consumes: Make targets and module CLIs from Tasks 3–5.
- Produces: PR-safe docs checking on `develop`/`main`; Pages and wiki publishing on `main` only.

- [ ] **Step 1: Write failing workflow contract tests**

```python
def test_ci_docs_job_uses_canonical_gate(repo_root):
    text = (repo_root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "branches: [main, develop]" in text
    assert "sudo apt-get install -y libcairo2" in text
    assert "make docs-check" in text
    assert "python scripts/build_docs.py" not in text


def test_publish_workflow_generates_site_and_pushes_wiki(repo_root):
    text = (repo_root / ".github/workflows/docs-deploy.yml").read_text(encoding="utf-8")
    assert "branches: [main]" in text
    assert "python -m scripts.docs.render_diagrams" in text
    assert "python -m scripts.docs.build_docs --site" in text
    assert "python -m scripts.docs.build_docs --wiki" in text
    assert "python -m scripts.docs.push_wiki --push" in text
    assert "path: site" in text
```

- [ ] **Step 2: Run the workflow tests and confirm direct-script failures**

Run: `uv run --group dev pytest tests/scripts/docs/test_workflows.py -q`

Expected: assertions fail on missing Cairo/module invocations and missing wiki publication in `docs-deploy.yml`.

- [ ] **Step 3: Make CI use the canonical gate**

Keep `push` and `pull_request` branch lists as `[main, develop]`. In `docs-build`, install `libcairo2`, then run `make docs-check`, `uv run ruff check scripts/docs/`, and the `tests/scripts/docs/` suite. Remove the four direct legacy script commands.

- [ ] **Step 4: Consolidate main-only publication**

In `docs-deploy.yml`, install Cairo before dependency setup, render diagrams, render site, build strict MkDocs, upload/deploy Pages, then run a wiki job that renders `generated/wiki` and invokes `scripts.docs.push_wiki --push`. Preserve the currently proven GitHub-token wiki authentication if the new pusher accepts an HTTPS remote; otherwise use the existing repository secret path without exposing secret contents. Ensure the pusher targets wiki `master`.

Retire `docs-sync.yml` if all its responsibilities are now in `docs-deploy.yml`; do not keep a workflow that commits generated README changes to protected `main`.

- [ ] **Step 5: Run workflow/repository tests and commit**

Run:

```bash
uv run --group dev pytest tests/scripts/docs/test_workflows.py tests/test_ci_atlas_contract.py tests/test_repo_structure.py -q
uv run ruff check scripts/docs tests/scripts/docs
```

Expected: all tests and lint pass.

Commit with message:

```bash
git commit -m "ci: publish canonical documentation surfaces"
```

---

### Task 8: Run the complete verification matrix and prepare the feature PR

**Files:**
- Modify only files required to fix concrete verification failures.
- Do not touch `infra/` or the preserved untracked plan.

**Interfaces:**
- Consumes: all prior tasks.
- Produces: a review-ready feature branch with reproducible evidence.

- [ ] **Step 1: Verify protected artifacts before tests**

Run:

```bash
git submodule status infra
git -C infra status --short
git status --short
```

Expected: `infra` reports pin `985918ce8c805081947d53b1c48bb80610237a5b`, submodule status is clean, and the only unrelated untracked path is `docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md`.

- [ ] **Step 2: Run documentation verification**

Run:

```bash
make docs-check
uv run --group dev pytest tests/scripts/docs tests/scripts -q
uv run ruff check scripts/docs tests/scripts/docs
```

Expected: all commands exit `0`; strict MkDocs build reports no warnings; 23 diagram PNGs and 23 generated site SVGs exist.

- [ ] **Step 3: Run repository regression verification**

Run:

```bash
uv run pytest -m "not infra and not network" -q
uv run ruff check .
uv run python scripts/verify_repo.py --root .
```

Expected: all commands exit `0` with no new failures.

- [ ] **Step 4: Inspect generated surface parity**

Run a clean regeneration and compare page counts, manifest membership, forbidden-link scan, and SHA-256 determinism through `scripts.docs.check_docs`. Open representative overview, scenario, notebook, Spark app, Atlas result, generated site, and generated wiki pages to confirm correct relative images and links.

- [ ] **Step 5: Commit verification-only fixes**

If verification required changes, stage only the exact affected paths and commit:

```bash
git commit -m "test: close documentation verification gaps"
```

If no tracked changes remain, do not create an empty commit.

- [ ] **Step 6: Request code review**

Use the `requesting-code-review` skill against the full branch diff from `develop...HEAD`. Resolve every correctness or contract finding, rerun the affected focused tests, then rerun Step 2.

---

### Task 9: Promote through Gitflow, verify publication, and clean up

**Files:**
- No source changes unless required by a failed protected check.

**Interfaces:**
- Consumes: verified feature branch from Task 8.
- Produces: merged `develop` and `main`, content-identical protected branches, clean branch/PR inventory, local `develop` checkout.

- [ ] **Step 1: Push feature branch and open feature → develop PR**

Push `codex/three-surface-docs-sync`, create a ready PR targeting `develop`, include the audit remediation map and exact local verification output, and wait for all required checks.

- [ ] **Step 2: Merge only after green checks**

Use the repository's protected merge method. Fetch/prune, switch local `develop`, and fast-forward it to `origin/develop`.

- [ ] **Step 3: Open develop → main promotion PR**

Create the second ready PR from `develop` to `main`. Confirm its diff contains only the reviewed documentation migration and that required checks pass before merge.

- [ ] **Step 4: Verify Pages and wiki publication**

Wait for the main-triggered publishing workflow. Confirm the Pages URL returns HTTP 200, the wiki Home page returns HTTP 200, and both contain the updated 2026-07-31 acceptance record with surface-local diagram assets.

- [ ] **Step 5: Reconcile main back into develop if protected merge commits diverge**

If `main` is not an ancestor of `develop` after promotion, open a protected `main` → `develop` reconciliation PR. Merge only after checks pass. Confirm `git diff origin/main origin/develop --` is empty.

- [ ] **Step 6: Clean merged branches and PRs**

Verify no related PR is open. Delete only the confirmed merged feature branch remotely and locally. Fetch/prune; retain `main`, `develop`, all user branches/worktrees, and the untracked historical plan.

- [ ] **Step 7: Finish on local develop**

Run:

```bash
git switch develop
git pull --ff-only origin develop
git status --short --branch
git branch -a
```

Expected: local `develop` is checked out and synchronized with `origin/develop`; the only unrelated untracked file remains the preserved historical plan; remote branches are `origin/main` and `origin/develop`; main/develop trees are content-identical.

---

## Self-Review Results

- **Spec coverage:** Tasks 1–7 cover every design section: canonical/internal boundary, generated config/output, manifest numbering, self-containment, HTML→SVG/PNG diagrams, content reconciliation, deterministic checks, Make interface, CI, Pages/wiki publication, and immutable Atlas scope. Tasks 8–9 cover verification, review, Gitflow promotion, publication checks, reconciliation, cleanup, and final checkout.
- **Placeholder scan:** The plan contains no unresolved implementation markers, deferred fill-ins, or unspecified error-handling steps. Task 3 defines the exact mechanical SVG-to-HTML conversion and requires all 23 concrete masters listed in Task 1.
- **Type consistency:** `Manifest`, `Section`, `DiagramEntry`, `iter_leaf_sections`, transform signatures, renderer signatures, builder signatures, aggregate checker signatures, and wiki publisher signatures are defined once and consumed under the same names in later tasks.
