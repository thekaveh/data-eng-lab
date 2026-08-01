# Opener Hero Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the unreadable above-the-fold topology with a polished lakehouse brand banner, centered repository identity, and complete icon-bearing stack badges while preserving a synchronized README, MkDocs landing page, and GitHub wiki Home.

**Architecture:** Add the banner as a new manifest-owned deterministic diagram so the existing HTML-master → site SVG → committed/wiki PNG pipeline remains the only asset path. Extend the canonical Markdown transform to rewrite local HTML `<img src>` attributes, then use the same semantic hero markup in `README.md` and `docs/index.md`; retain the existing `overview` diagram under Architecture.

**Tech Stack:** HTML/SVG, Markdown, Shields.io badges, Python 3.12, pytest, CairoSVG, MkDocs Material, Git/GitHub Actions.

## Global Constraints

- Work only on `codex/fix-opener-hero`, created from `develop`.
- Do not edit any file under `infra/`; Atlas remains pinned at `985918ce8c805081947d53b1c48bb80610237a5b`.
- Do not stage or modify `docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md`.
- The banner master is `docs/diagrams/data-eng-lab-hero.html`, with a 1800×560 text-free SVG view box.
- The banner’s committed projection is `docs/diagrams/img/data-eng-lab-hero.png` plus `data-eng-lab-hero.sha256`.
- The opener order is banner → centered H1 → centered tagline → centered value proposition → three centered badge rows → left-aligned executive summary.
- Preserve the exact canonical tagline: `An Iceberg-lakehouse data-engineering lab built on the Atlas platform.`
- Preserve the exact value proposition: `Build, orchestrate, stream, and query production-shaped lakehouse pipelines from paired notebooks and deployable Spark applications.`
- Show exactly 12 technology badges: Atlas, Docker Compose, Apache Spark, Apache Iceberg, MinIO, Trino, Redpanda, Apache Airflow, Jenkins, Maven, Jupyter, and Zeppelin.
- Move the existing `overview` architecture image below `## 2. Architecture`; do not change its master unless fact-checking finds an independent defect.
- README, MkDocs, and wiki must be self-contained and semantically identical; only relative asset/document paths may differ.
- Delivery is feature PR → `develop`, promotion PR → `main`, protection-compliant reconciliation, branch cleanup, and local `develop` checkout.

---

### Task 1: Teach the surface transform to rewrite HTML hero images

**Files:**
- Modify: `scripts/docs/transforms.py`
- Modify: `tests/scripts/docs/test_transforms.py`

**Interfaces:**
- Consumes: `rewrite_for_surface(markdown: str, surface: str, source: Path, source_map: Mapping[Path, Path]) -> str` and the existing diagram entries produced by `build_source_map`.
- Produces: `_HTML_IMAGE_SRC_RE` and HTML `<img src>` rewriting through the same `_rewrite_target` path used by Markdown images.

- [ ] **Step 1: Write failing HTML-image rewrite tests**

Add this fixture entry after `overview` in the manifest string in `tests/scripts/docs/test_transforms.py`:

```yaml
  - {id: data-eng-lab-hero, master: docs/diagrams/data-eng-lab-hero.html}
```

Add these tests:

```python
@pytest.mark.parametrize(
    ("surface", "expected"),
    [
        ("site", '<img src="../assets/img/data-eng-lab-hero.svg" alt="Lakehouse hero">'),
        ("wiki", '<img src="img/data-eng-lab-hero.png" alt="Lakehouse hero">'),
    ],
)
def test_rewrite_maps_local_html_image_sources(manifest, surface, expected):
    source = Path("docs/notebooks/example.md")
    mapping = build_source_map(manifest, surface)
    markdown = (
        '<img src="../diagrams/img/data-eng-lab-hero.png" '
        'alt="Lakehouse hero">'
    )

    assert rewrite_for_surface(markdown, surface, source, mapping) == expected


def test_rewrite_preserves_remote_badge_image_sources(manifest):
    source = Path("docs/index.md")
    mapping = build_source_map(manifest, "site")
    badge = (
        '<img alt="Apache Spark" '
        'src="https://img.shields.io/badge/Apache%20Spark-compute-E25A1C">'
    )

    assert rewrite_for_surface(badge, "site", source, mapping) == badge
```

- [ ] **Step 2: Run the focused tests and confirm RED**

Run:

```bash
uv run pytest tests/scripts/docs/test_transforms.py -q
```

Expected: both `test_rewrite_maps_local_html_image_sources` cases fail because HTML `src` attributes are not currently transformed; the remote-badge case passes.

- [ ] **Step 3: Implement HTML image-source rewriting**

Add this constant beside the other transform constants in `scripts/docs/transforms.py`:

```python
_HTML_IMAGE_SRC_RE = re.compile(
    r'(?P<prefix><img\b[^>]*?\bsrc=["\'])(?P<target>[^"\']+)(?P<suffix>["\'])',
    re.IGNORECASE,
)
```

Replace the final `return MARKDOWN_LINK_RE.sub(replace, markdown)` in `rewrite_for_surface` with:

```python
    rewritten = MARKDOWN_LINK_RE.sub(replace, markdown)

    def replace_html_image(match: re.Match[str]) -> str:
        target = match.group("target")
        replacement = _rewrite_target(
            target,
            surface,
            source,
            source_destination,
            source_map,
        )
        if replacement is None:
            return match.group(0)
        return f'{match.group("prefix")}{replacement}{match.group("suffix")}'

    return _HTML_IMAGE_SRC_RE.sub(replace_html_image, rewritten)
```

This leaves allowed remote Shields.io images unchanged, rewrites manifest-owned local diagram images, and leaves unmapped local HTML images visible to the existing completeness/self-containment gates rather than silently stripping them.

- [ ] **Step 4: Run transform and docs-script tests**

Run:

```bash
uv run pytest tests/scripts/docs/test_transforms.py tests/scripts/docs/test_build_docs.py -q
uv run ruff check scripts/docs/transforms.py tests/scripts/docs/test_transforms.py
```

Expected: all tests pass and Ruff reports `All checks passed!`.

- [ ] **Step 5: Commit the transform contract**

```bash
git add scripts/docs/transforms.py tests/scripts/docs/test_transforms.py
git commit -m "feat(docs): rewrite HTML hero assets per surface"
```

---

### Task 2: Add the deterministic lakehouse brand banner

**Files:**
- Create: `docs/diagrams/data-eng-lab-hero.html`
- Create: `docs/diagrams/img/data-eng-lab-hero.png`
- Create: `docs/diagrams/img/data-eng-lab-hero.sha256`
- Modify: `docs/manifest.yaml`
- Modify: `tests/scripts/docs/test_render_diagrams.py`
- Modify: `tests/scripts/docs/test_build_docs.py`

**Interfaces:**
- Consumes: manifest `DiagramEntry`, `extract_svg`, `render_all`, and the versioned 1600-pixel PNG projection contract.
- Produces: manifest diagram ID `data-eng-lab-hero`, site asset `assets/img/data-eng-lab-hero.svg`, and repository/wiki `data-eng-lab-hero.png`.

- [ ] **Step 1: Write failing banner and inventory tests**

Add to `tests/scripts/docs/test_render_diagrams.py`:

```python
def test_lakehouse_hero_is_wide_text_free_and_accessible():
    root = Path(__file__).resolve().parents[3]
    master = root / "docs/diagrams/data-eng-lab-hero.html"

    svg = extract_svg(master.read_text(encoding="utf-8"))
    element = ET.fromstring(svg)

    assert element.attrib["width"] == "1800"
    assert element.attrib["height"] == "560"
    assert element.attrib["viewBox"] == "0 0 1800 560"
    assert not any(child.tag.endswith("text") for child in element.iter())
    assert "lakehouse" in svg.casefold()
    assert "bronze, silver, and gold" in svg.casefold()
```

Add `import xml.etree.ElementTree as ET` with the standard-library imports in that test module.

Extend `test_repository_manifest_projects_all_public_pages_and_assets` in `tests/scripts/docs/test_build_docs.py`:

```python
    assert "data-eng-lab-hero" in {diagram.id for diagram in manifest.diagrams}
    assert (site / "assets/img/data-eng-lab-hero.svg").is_file()
    assert (wiki / "img/data-eng-lab-hero.png").is_file()
```

- [ ] **Step 2: Run the focused tests and confirm RED**

Run:

```bash
uv run pytest \
  tests/scripts/docs/test_render_diagrams.py::test_lakehouse_hero_is_wide_text_free_and_accessible \
  tests/scripts/docs/test_build_docs.py::test_repository_manifest_projects_all_public_pages_and_assets \
  -q
```

Expected: failures because the master, manifest entry, and projections do not exist.

- [ ] **Step 3: Declare the banner in the documentation manifest**

Add this as the first diagram entry in `docs/manifest.yaml`:

```yaml
  - {id: data-eng-lab-hero, master: docs/diagrams/data-eng-lab-hero.html}
```

- [ ] **Step 4: Create the banner master**

Create `docs/diagrams/data-eng-lab-hero.html` with this complete master:

```html
<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>data-eng-lab lakehouse hero</title></head>
<body>
  <!-- Brand illustration grounded in the repository medallion layers and data-flow architecture. -->
  <svg xmlns="http://www.w3.org/2000/svg" width="1800" height="560" viewBox="0 0 1800 560" role="img" aria-labelledby="diagram-title diagram-description">
    <title id="diagram-title">data-eng-lab lakehouse hero</title>
    <desc id="diagram-description">Abstract lakehouse mark with incoming data streams, an Iceberg crystal, bronze, silver, and gold medallion layers, and outgoing analytical signals.</desc>
    <defs>
      <linearGradient id="background" x1="0" y1="0" x2="1" y2="1">
        <stop offset="0" stop-color="#07111f"/>
        <stop offset="0.52" stop-color="#0b1830"/>
        <stop offset="1" stop-color="#07111f"/>
      </linearGradient>
      <radialGradient id="halo" cx="50%" cy="48%" r="48%">
        <stop offset="0" stop-color="#22d3ee" stop-opacity="0.22"/>
        <stop offset="0.55" stop-color="#2563eb" stop-opacity="0.08"/>
        <stop offset="1" stop-color="#07111f" stop-opacity="0"/>
      </radialGradient>
      <linearGradient id="ice" x1="0" y1="0" x2="1" y2="1">
        <stop offset="0" stop-color="#e0f2fe"/>
        <stop offset="0.42" stop-color="#67e8f9"/>
        <stop offset="1" stop-color="#2563eb"/>
      </linearGradient>
      <linearGradient id="ice-dark" x1="0" y1="0" x2="1" y2="1">
        <stop offset="0" stop-color="#0ea5e9" stop-opacity="0.75"/>
        <stop offset="1" stop-color="#1e3a8a" stop-opacity="0.9"/>
      </linearGradient>
      <linearGradient id="bronze" x1="0" y1="0" x2="1" y2="0">
        <stop offset="0" stop-color="#7c2d12"/>
        <stop offset="0.5" stop-color="#fb923c"/>
        <stop offset="1" stop-color="#7c2d12"/>
      </linearGradient>
      <linearGradient id="silver" x1="0" y1="0" x2="1" y2="0">
        <stop offset="0" stop-color="#475569"/>
        <stop offset="0.5" stop-color="#e2e8f0"/>
        <stop offset="1" stop-color="#475569"/>
      </linearGradient>
      <linearGradient id="gold" x1="0" y1="0" x2="1" y2="0">
        <stop offset="0" stop-color="#92400e"/>
        <stop offset="0.5" stop-color="#fbbf24"/>
        <stop offset="1" stop-color="#92400e"/>
      </linearGradient>
      <pattern id="grid" width="48" height="48" patternUnits="userSpaceOnUse">
        <path d="M48 0H0V48" fill="none" stroke="#38bdf8" stroke-opacity="0.055" stroke-width="1"/>
      </pattern>
    </defs>

    <rect width="1800" height="560" rx="26" fill="url(#background)"/>
    <rect width="1800" height="560" rx="26" fill="url(#grid)"/>
    <ellipse cx="900" cy="278" rx="540" ry="260" fill="url(#halo)"/>
    <rect x="1.5" y="1.5" width="1797" height="557" rx="24.5" fill="none" stroke="#38bdf8" stroke-opacity="0.5" stroke-width="3"/>

    <g fill="none" stroke-linecap="round">
      <path d="M90 148 C330 148 430 210 676 238" stroke="#22d3ee" stroke-opacity="0.5" stroke-width="3"/>
      <path d="M54 236 C320 236 470 258 680 270" stroke="#38bdf8" stroke-opacity="0.8" stroke-width="5"/>
      <path d="M110 332 C344 332 482 316 684 296" stroke="#60a5fa" stroke-opacity="0.6" stroke-width="3"/>
      <path d="M1114 240 C1340 210 1466 152 1716 152" stroke="#a78bfa" stroke-opacity="0.55" stroke-width="3"/>
      <path d="M1118 274 C1360 266 1510 238 1750 238" stroke="#22d3ee" stroke-opacity="0.85" stroke-width="5"/>
      <path d="M1114 304 C1330 326 1470 350 1710 342" stroke="#fbbf24" stroke-opacity="0.6" stroke-width="3"/>
    </g>

    <g fill="#67e8f9">
      <circle cx="90" cy="148" r="7"/><circle cx="54" cy="236" r="9"/><circle cx="110" cy="332" r="7"/>
      <circle cx="250" cy="148" r="4"/><circle cx="304" cy="236" r="5"/><circle cx="286" cy="326" r="4"/>
    </g>
    <g fill="#f8fafc">
      <circle cx="1716" cy="152" r="6"/><circle cx="1750" cy="238" r="8"/><circle cx="1710" cy="342" r="6"/>
    </g>

    <path d="M650 248 C760 232 1038 232 1150 248" fill="none" stroke="#67e8f9" stroke-opacity="0.55" stroke-width="2"/>
    <path d="M650 255 C770 274 1032 274 1150 255" fill="none" stroke="#e0f2fe" stroke-opacity="0.3" stroke-width="1"/>

    <g stroke-linejoin="round">
      <polygon points="900,66 1045,226 998,260 802,260 755,226" fill="url(#ice)" stroke="#e0f2fe" stroke-width="3"/>
      <polygon points="900,66 900,260 802,260 755,226" fill="#0ea5e9" fill-opacity="0.22" stroke="#bae6fd" stroke-opacity="0.65" stroke-width="1.5"/>
      <polygon points="900,66 1045,226 998,260 900,260" fill="#dbeafe" fill-opacity="0.18" stroke="#e0f2fe" stroke-opacity="0.55" stroke-width="1.5"/>
      <path d="M900 66 L838 208 L802 260 M900 66 L962 208 L998 260 M838 208 L962 208" fill="none" stroke="#f0f9ff" stroke-opacity="0.72" stroke-width="2"/>

      <polygon points="802,268 998,268 1036,326 764,326" fill="url(#bronze)" fill-opacity="0.82" stroke="#fb923c" stroke-width="2"/>
      <polygon points="764,336 1036,336 1084,400 716,400" fill="url(#silver)" fill-opacity="0.78" stroke="#cbd5e1" stroke-width="2"/>
      <polygon points="716,410 1084,410 1142,486 658,486" fill="url(#gold)" fill-opacity="0.82" stroke="#fbbf24" stroke-width="2"/>
      <path d="M900 268 V486 M802 268 L840 486 M998 268 L960 486" fill="none" stroke="#f8fafc" stroke-opacity="0.28" stroke-width="1.5"/>
      <polygon points="802,268 900,268 900,486 840,486" fill="url(#ice-dark)" fill-opacity="0.16"/>
      <polygon points="900,268 998,268 960,486 900,486" fill="#e0f2fe" fill-opacity="0.08"/>
    </g>

    <g fill="none" stroke-linecap="round">
      <path d="M680 270 H742" stroke="#22d3ee" stroke-width="5"/>
      <path d="M1058 270 H1120" stroke="#22d3ee" stroke-width="5"/>
      <path d="M742 270 l-18 -10 v20 z" fill="#22d3ee" stroke="none"/>
      <path d="M1120 270 l18 -10 v20 z" fill="#22d3ee" stroke="none"/>
    </g>

    <g fill="#f8fafc" fill-opacity="0.7">
      <circle cx="430" cy="102" r="2"/><circle cx="510" cy="410" r="2"/><circle cx="1240" cy="92" r="2"/>
      <circle cx="1320" cy="420" r="2"/><circle cx="1498" cy="102" r="2"/><circle cx="1540" cy="446" r="2"/>
    </g>
  </svg>
</body>
</html>
```

- [ ] **Step 5: Render the site SVG and committed PNG/fingerprint**

Run:

```bash
uv run --group dev python -m scripts.docs.render_diagrams --root .
```

Expected: new files `docs/diagrams/img/data-eng-lab-hero.png` and `docs/diagrams/img/data-eng-lab-hero.sha256`; generated site SVG exists.

- [ ] **Step 6: Inspect the banner at original and README-scaled widths**

Open `docs/diagrams/img/data-eng-lab-hero.png` with the local image viewer. Confirm:

- the central Iceberg/lakehouse mark is recognizable without zooming;
- bronze, silver, and gold strata are visually distinct;
- no labels or fine topology text exist;
- the mark remains readable when the image is displayed around 1000 CSS pixels wide; and
- the border, background, and data streams work on both light and dark surrounding backgrounds.

If any condition fails, edit only `docs/diagrams/data-eng-lab-hero.html`, rerun the renderer with `--force-png`, and repeat inspection.

- [ ] **Step 7: Run diagram/build tests**

Run:

```bash
uv run pytest tests/scripts/docs/test_render_diagrams.py tests/scripts/docs/test_build_docs.py -q
uv run python -m scripts.docs.check_docs --root .
```

Expected: all tests pass and the docs checker returns without findings.

- [ ] **Step 8: Commit the banner asset**

```bash
git add \
  docs/manifest.yaml \
  docs/diagrams/data-eng-lab-hero.html \
  docs/diagrams/img/data-eng-lab-hero.png \
  docs/diagrams/img/data-eng-lab-hero.sha256 \
  tests/scripts/docs/test_render_diagrams.py \
  tests/scripts/docs/test_build_docs.py
git commit -m "feat(docs): add lakehouse hero banner"
```

---

### Task 3: Recompose the canonical opener and lock its visual contract

**Files:**
- Modify: `README.md`
- Modify: `docs/index.md`
- Modify: `docs/CHANGELOG.md`
- Modify: `tests/test_docs_content_contract.py`

**Interfaces:**
- Consumes: manifest diagram IDs `data-eng-lab-hero` and `overview`; HTML-image rewriting from Task 1.
- Produces: the canonical hero HTML and regression constants for title, tagline, value proposition, badges, ordering, and architecture placement.

- [ ] **Step 1: Replace the old opener assertions with failing hero assertions**

In `tests/test_docs_content_contract.py`, add:

```python
import html
```

Define these constants beneath `PORTABILITY_TOKENS`:

```python
HERO_H1 = '<h1 align="center">data-eng-lab</h1>'
HERO_TAGLINE_TEXT = "An Iceberg-lakehouse data-engineering lab built on the Atlas platform."
HERO_VALUE_PROPOSITION = (
    "Build, orchestrate, stream, and query production-shaped lakehouse pipelines "
    "from paired notebooks and deployable Spark applications."
)
HERO_BADGES = (
    "Atlas",
    "Docker Compose",
    "Apache Spark",
    "Apache Iceberg",
    "MinIO",
    "Trino",
    "Redpanda",
    "Apache Airflow",
    "Jenkins",
    "Maven",
    "Jupyter",
    "Zeppelin",
)
```

Replace `_opener_parts` with:

```python
def _opener_parts(path: Path) -> tuple[str, str, str]:
    text = path.read_text(encoding="utf-8")
    h1 = re.search(r'<h1 align="center">[^<]+</h1>', text)
    tagline = re.search(
        r'<p align="center">\s*<strong>(.*?)</strong>\s*</p>',
        text,
        re.DOTALL,
    )
    value = re.search(
        r'<p align="center">\s*'
        r'(Build, orchestrate, stream, and query .*?)\s*</p>',
        text,
        re.DOTALL,
    )
    assert h1 is not None and tagline is not None and value is not None
    plain_tagline = re.sub(r"<[^>]+>", "", html.unescape(tagline.group(1)))
    return h1.group(0), plain_tagline.strip(), " ".join(value.group(1).split())
```

Replace `test_opener_is_project_first_and_identical_across_canonical_surfaces` with:

```python
def test_opener_is_centered_badged_and_identical_across_canonical_surfaces():
    readme = ROOT / "README.md"
    index = ROOT / "docs/index.md"
    readme_parts = _opener_parts(readme)
    index_parts = _opener_parts(index)

    assert readme_parts == index_parts == (
        HERO_H1,
        HERO_TAGLINE_TEXT,
        HERO_VALUE_PROPOSITION,
    )
    manifest = yaml.safe_load((ROOT / "atlas.consumer.yml").read_text(encoding="utf-8"))
    assert manifest["brand"]["tagline"] == HERO_TAGLINE_TEXT

    for path in (readme, index):
        text = path.read_text(encoding="utf-8")
        first_h2 = text.index("\n## ")
        hero = text.index("data-eng-lab-hero.png")
        title = text.index(HERO_H1)
        value = text.index(HERO_VALUE_PROPOSITION)
        architecture_h2 = text.index("## 2. Architecture")
        architecture = text.index("overview.png")

        assert hero < title < value < first_h2
        assert architecture > architecture_h2
        assert "| Platform |" not in text[:first_h2]
        assert text[:first_h2].count("img.shields.io/badge/") == len(HERO_BADGES)
        for badge in HERO_BADGES:
            assert f'<img alt="{badge}"' in text[:first_h2]

        opener_images = re.findall(r'<img\s+([^>]+)>', text[:first_h2])
        assert opener_images
        assert all(re.search(r'\balt="[^"]+"', attributes) for attributes in opener_images)

    executive_summary = (ROOT / "README.md").read_text(encoding="utf-8").split("</p>", 5)[-1]
    for required in (
        "atlas.consumer.yml",
        "make up",
        "Data Engineering",
        "development profile",
        "Jupyter",
        "Zeppelin",
        "17 Scala/PySpark",
        "two Trino client pairs",
        "Airflow",
        "Jenkins",
        "Trino",
        "Redpanda",
    ):
        assert required in executive_summary
```

- [ ] **Step 2: Run the opener contract and confirm RED**

Run:

```bash
uv run pytest tests/test_docs_content_contract.py::test_opener_is_centered_badged_and_identical_across_canonical_surfaces -q
```

Expected: failure because the canonical sources still use a Markdown H1, the architecture poster is above the title, and there are no Shields.io badges.

- [ ] **Step 3: Replace the README opener**

Replace everything before `## 1. Quick start` in `README.md` with the following, using the exact line breaks and URLs:

```html
<p align="center">
  <img src="docs/diagrams/img/data-eng-lab-hero.png" alt="Abstract data-eng-lab lakehouse with Iceberg crystal, medallion layers, and flowing data" width="100%">
</p>

<h1 align="center">data-eng-lab</h1>

<p align="center">
  <strong>An Iceberg-lakehouse data-engineering lab built on the <a href="https://github.com/thekaveh/atlas">Atlas</a> platform.</strong>
</p>

<p align="center">
  Build, orchestrate, stream, and query production-shaped lakehouse pipelines from paired notebooks and deployable Spark applications.
</p>

<p align="center">
  <img alt="Atlas" src="https://img.shields.io/badge/Atlas-infrastructure-2563EB?logo=git&logoColor=white">
  <img alt="Docker Compose" src="https://img.shields.io/badge/Docker%20Compose-runtime-2496ED?logo=docker&logoColor=white">
</p>

<p align="center">
  <img alt="Apache Spark" src="https://img.shields.io/badge/Apache%20Spark-compute-E25A1C?logo=apachespark&logoColor=white">
  <img alt="Apache Iceberg" src="https://img.shields.io/badge/Apache%20Iceberg-tables-4F46E5?logo=apache&logoColor=white">
  <img alt="MinIO" src="https://img.shields.io/badge/MinIO-object%20storage-C72E49?logo=minio&logoColor=white">
  <img alt="Trino" src="https://img.shields.io/badge/Trino-SQL-DD00A1?logo=trino&logoColor=white">
  <img alt="Redpanda" src="https://img.shields.io/badge/Redpanda-streaming-FF4D5B?logo=apachekafka&logoColor=white">
</p>

<p align="center">
  <img alt="Apache Airflow" src="https://img.shields.io/badge/Apache%20Airflow-orchestration-017CEE?logo=apacheairflow&logoColor=white">
  <img alt="Jenkins" src="https://img.shields.io/badge/Jenkins-CI-D24939?logo=jenkins&logoColor=white">
  <img alt="Maven" src="https://img.shields.io/badge/Maven-builds-C71A36?logo=apachemaven&logoColor=white">
  <img alt="Jupyter" src="https://img.shields.io/badge/Jupyter-notebooks-F37626?logo=jupyter&logoColor=white">
  <img alt="Zeppelin" src="https://img.shields.io/badge/Zeppelin-notebooks-FBBF24?logo=apache&logoColor=white">
</p>

`data-eng-lab` consumes Atlas as its pinned `infra/` git submodule through `atlas.consumer.yml`, so `make up` launches the default development profile as the **Data Engineering** workspace. It pairs 19 Zeppelin and Jupyter scenario notebooks—17 Scala/PySpark implementations plus two Trino client pairs—with Iceberg on MinIO, Airflow, Jenkins-built Spark apps, Trino, and Redpanda for three broker-backed streams.
```

- [ ] **Step 4: Apply the equivalent landing-page opener**

Replace everything before `## 1. Quick start` in `docs/index.md` with the same block, changing only the banner source to:

```html
  <img src="diagrams/img/data-eng-lab-hero.png" alt="Abstract data-eng-lab lakehouse with Iceberg crystal, medallion layers, and flowing data" width="100%">
```

All other hero markup and prose must be byte-identical.

- [ ] **Step 5: Move the detailed architecture image into Architecture**

In both canonical sources, keep the medallion text block attached to the sentence that introduces it, then place the surface-appropriate overview image immediately after that block.

README:

````markdown
## 2. Architecture

The landing zone and the three Iceberg medallion layers are distinct storage stages:

```text
s3a://landing/  →  bronze  →  silver  →  gold
raw source data    clean      enriched    aggregated/modelled
```

![data-eng-lab architecture](docs/diagrams/img/overview.png)
````

`docs/index.md`:

````markdown
## 2. Architecture

The landing zone and the three Iceberg medallion layers are distinct storage stages:

```text
s3a://landing/  →  bronze  →  silver  →  gold
raw source data    clean      enriched    aggregated/modelled
```

![data-eng-lab architecture](diagrams/img/overview.png)
````

Keep the existing architecture prose after the image.

- [ ] **Step 6: Record the opener correction in the changelog**

Add this first bullet under `## 1. [Unreleased]` → `### Changed` in `docs/CHANGELOG.md`:

```markdown
- The repository, site, and wiki now open with a wide lakehouse brand banner,
  centered project identity, and twelve icon-bearing stack badges. The detailed
  topology remains available under Architecture instead of occupying the first
  viewport.
```

- [ ] **Step 7: Run the content contract and confirm GREEN**

Run:

```bash
uv run pytest tests/test_docs_content_contract.py -q
uv run ruff check tests/test_docs_content_contract.py
```

Expected: every content contract passes and Ruff reports clean.

- [ ] **Step 8: Commit the opener recomposition**

```bash
git add README.md docs/index.md docs/CHANGELOG.md tests/test_docs_content_contract.py
git commit -m "feat(docs): redesign repository opener"
```

---

### Task 4: Generate and inspect all three surfaces

**Files:**
- Generated/ignored: `generated/site/index.md`
- Generated/ignored: `generated/wiki/Home.md`
- Generated/ignored: `generated/site/assets/img/data-eng-lab-hero.svg`
- Generated/ignored: `generated/wiki/img/data-eng-lab-hero.png`
- Generated/ignored: `site/index.html`

**Interfaces:**
- Consumes: canonical sources and manifest/render/transform behavior from Tasks 1–3.
- Produces: synchronized MkDocs and wiki projections plus a rendered local site for visual acceptance.

- [ ] **Step 1: Regenerate diagrams and both documentation surfaces**

Run:

```bash
uv run --group dev python -m scripts.docs.render_diagrams --root .
uv run --group dev python -m scripts.docs.build_docs --site --wiki --root .
uv run --group dev mkdocs build --strict
```

Expected: all commands exit zero and strict MkDocs emits no warnings.

- [ ] **Step 2: Assert generated opener parity and asset projection**

Run:

```bash
rg -n "data-eng-lab-hero|<h1 align=|img.shields.io/badge" \
  generated/site/index.md generated/wiki/Home.md
```

Expected: both pages contain the centered H1 and 12 badges; the site image target is `assets/img/data-eng-lab-hero.svg` and the wiki target is `img/data-eng-lab-hero.png`.

Run:

```bash
cmp docs/diagrams/img/data-eng-lab-hero.png generated/wiki/img/data-eng-lab-hero.png
```

Expected: exit zero with no output.

- [ ] **Step 3: Inspect the built landing page**

Serve or open `site/index.html` and inspect at desktop and approximately 768-pixel content widths. Confirm:

- the banner fills the content width without dominating the entire first viewport;
- the H1, tagline, value proposition, and all badge rows are centered;
- badge wrapping remains orderly at the narrower width;
- the executive summary begins below the badge block;
- the detailed architecture diagram appears only under Architecture; and
- no broken image icons or raw HTML/Markdown syntax appear.

- [ ] **Step 4: Run all documentation checks**

Run:

```bash
make docs-check
make docs-wiki
uv run pytest tests/scripts/docs/ tests/test_docs_content_contract.py -q
```

Expected: strict MkDocs, deterministic site/wiki checks, diagram checks, link checks, and content contracts all pass.

- [ ] **Step 5: Commit any visual corrections**

If visual inspection required edits, stage only the exact corrected canonical/master/test files and commit:

```bash
git commit -m "fix(docs): polish opener visual balance"
```

If inspection required no edits, do not create an empty commit.

---

### Task 5: Full verification, review, and Gitflow promotion

**Files:**
- Verify: all changed files
- Preserve: `infra/`
- Preserve: `docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md`

**Interfaces:**
- Consumes: completed feature branch.
- Produces: reviewed and promoted content-identical `main`/`develop` trees, published Pages/wiki, no dangling feature refs, and local `develop` checkout.

- [ ] **Step 1: Run the complete local verification matrix**

Run:

```bash
make test
make verify
make lint
make docs-check
make docs-wiki
git diff --check
```

Expected: offline tests pass, verifier reports `0 finding(s), 0 error(s)`, Ruff is clean, both docs gates pass, and diff check has no output.

- [ ] **Step 2: Recheck protected invariants**

Run:

```bash
git submodule status infra
git diff --submodule=short -- infra
shasum -a 256 docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md
```

Expected:

- Atlas pin begins `985918ce8c805081947d53b1c48bb80610237a5b`;
- the `infra` diff is empty; and
- protected-plan SHA-256 remains `f7eb55036e3020f419d9acb42bbc502bfe0528219a980a6973ed083591d2ba66`.

- [ ] **Step 3: Request independent final review**

Use the requesting-code-review workflow. The reviewer must inspect:

- GitHub/MkDocs/wiki opener ordering and parity;
- banner readability, aspect ratio, and accessibility metadata;
- exact 12-badge breadth and alt text;
- HTML image-source rewriting safety;
- architecture placement;
- deterministic PNG/SVG publication; and
- Atlas/protected-plan invariants.

Resolve every Critical, Important, and Minor finding and rerun affected gates.

- [ ] **Step 4: Push the feature branch and open the develop PR**

```bash
git push -u origin codex/fix-opener-hero
gh pr create \
  --base develop \
  --head codex/fix-opener-hero \
  --title "feat(docs): redesign repository opener" \
  --body "Replaces the dense above-the-fold topology with a responsive lakehouse hero, centered identity, and complete technology badges across README, MkDocs, and wiki. The detailed topology moves under Architecture."
```

Wait for every required check, then merge with a merge commit and delete the remote feature branch.

- [ ] **Step 5: Promote develop to main**

Refresh local `develop`, then:

```bash
gh pr create \
  --base main \
  --head develop \
  --title "release: promote opener hero redesign" \
  --body "Promotes the fully reviewed three-surface opener redesign from develop to main."
```

Wait for every required check, merge without deleting `develop`, and wait for the Pages and wiki jobs to succeed.

- [ ] **Step 6: Reconcile and clean up**

If branch protection rejects a direct `develop` fast-forward, create and merge a `main` → `develop` reconciliation PR. Then verify:

```bash
git fetch origin --prune
git rev-parse origin/main^{tree} origin/develop^{tree}
git ls-remote --heads origin
gh pr list --state open
```

Expected: protected-branch tree hashes are identical, only `main` and `develop` remain remotely, and no PR is open.

Update local `main`, check out local `develop`, and confirm only the protected user plan remains untracked.

- [ ] **Step 7: Verify published surfaces**

Check that `https://thekaveh.github.io/data-eng-lab/` returns HTTP 200. Clone the wiki into a fresh temporary directory and compare it recursively with `generated/wiki`, excluding `.git`; expected result is no diff.
