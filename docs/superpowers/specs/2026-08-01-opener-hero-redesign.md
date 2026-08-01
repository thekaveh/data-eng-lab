# data-eng-lab Opener and Hero Redesign

**Date:** 2026-08-01  
**Status:** Approved design  
**References:** Atlas repository opener; Blaizzy/Nativ repository opener

## 1. Problem

The current opener presents the detailed 1200×700 architecture diagram as a poster. At GitHub README width, its component labels and arrows become too small to read, while the image consumes most of the first viewport. The Markdown H1 is left-aligned, and the stack appears as a plain table rather than the icon-bearing badges readers expect from a polished repository landing page.

The architecture diagram remains useful, but it is an explanatory technical artifact rather than a brand hero. The opener must separate those two roles.

## 2. Reference Lessons

Atlas and Nativ use the same effective repository-opening sequence:

1. a wide, visually simple banner;
2. a centered H1;
3. a centered bold tagline;
4. a short centered value proposition;
5. centered Shields.io technology badges; and
6. left-aligned explanatory content below the hero.

Nativ is the stronger layout reference because its banner contains one recognizable mark and no small explanatory text. Atlas is the stronger stack-breadth reference because its badges represent the platform in multiple rows. `data-eng-lab` will combine those strengths without copying either project’s artwork.

## 3. Approved Direction

Use a brand-first hero above the fold and move the existing detailed architecture diagram to the Architecture section.

The new banner is a repository-brand asset, not a topology diagram. It must communicate lakehouse/data-engineering identity at a glance and remain legible when scaled to a narrow README column.

Rejected alternatives:

- **Badge-only opener:** clean but visually anonymous.
- **Simplified topology banner:** still invites labels and arrows that become unreadable at README scale.
- **Atlas artwork derivative:** visually strong but would make the consumer repository look like a reskin of its infrastructure dependency.

## 4. Hero Composition

Both canonical opener sources—`README.md` and `docs/index.md`—will use this order:

1. Full-width banner image.
2. `<h1 align="center">data-eng-lab</h1>`.
3. Centered bold canonical tagline.
4. Centered one-sentence value proposition.
5. Centered technology badges in three deliberate rows.
6. The existing concise executive-summary paragraph, left-aligned.
7. `## 1. Quick start`.

GitHub strips arbitrary CSS from README content, so the design uses supported HTML alignment attributes rather than inline styles. An H1 is the largest dependable title treatment on GitHub, the generated site, and the native wiki.

### 4.1 Canonical copy

The existing tagline remains unchanged:

> An Iceberg-lakehouse data-engineering lab built on the Atlas platform.

The centered value proposition will state the differentiator rather than repeat a component list:

> Build, orchestrate, stream, and query production-shaped lakehouse pipelines from paired notebooks and deployable Spark applications.

The executive summary remains the source of exact inventory and operating-model claims: pinned Atlas submodule, `atlas.consumer.yml`, `make up`, the Data Engineering workspace, 19 paired notebooks, the 17 Spark/two Trino split, and the integrated Airflow/Jenkins/Trino/Redpanda path.

## 5. Banner Art Direction

Create a new deterministic SVG/HTML master and committed PNG projection under the existing diagram pipeline.

### 5.1 Format

- Wide aspect ratio: approximately 3.2:1, targeting a 1800×560 SVG view box.
- Full-width embedding on all three surfaces.
- Text-free except for accessible SVG `<title>` and `<desc>` metadata.
- No tiny labels, legends, tables, topology boxes, or embedded wordmark.
- Readable in GitHub light and dark themes through a self-contained dark background and visible edge treatment.

### 5.2 Visual language

- Deep navy background consistent with the repository’s existing diagram family.
- A central crystalline lakehouse/iceberg mark with three clearly separated submerged strata.
- Bronze, silver, and gold accents used sparingly to evoke the medallion architecture.
- Cyan/ice-blue data streams entering from the left and clean query/output lines leaving to the right.
- Subtle grid or particle texture for depth, with generous negative space.
- Crisp geometric construction rather than photorealistic or mythological imagery.

The banner conveys identity; the existing `overview` architecture diagram continues to convey system relationships.

### 5.3 Asset contract

- Master: `docs/diagrams/data-eng-lab-hero.html`.
- Committed repository/wiki projection: `docs/diagrams/img/data-eng-lab-hero.png` plus its render fingerprint.
- Generated site projection: `generated/site/assets/img/data-eng-lab-hero.svg`.
- Generated wiki projection: `generated/wiki/img/data-eng-lab-hero.png`.
- Canonical source references use surface-appropriate relative paths and the documentation transforms preserve self-containment.

## 6. Technology Badge System

Replace the plain opener stack table with centered Shields.io badges. The rows communicate breadth without becoming an unstructured logo wall.

### 6.1 Row 1: platform

- Atlas — infrastructure
- Docker Compose — runtime

### 6.2 Row 2: lakehouse and data plane

- Apache Spark — compute
- Apache Iceberg — tables
- MinIO — object storage
- Trino — SQL
- Redpanda — streaming

### 6.3 Row 3: orchestration and development surfaces

- Apache Airflow — orchestration
- Jenkins — CI
- Maven — builds
- Jupyter — notebooks
- Zeppelin — notebooks

Each badge must have accurate alternative text, a deliberate color, and a technology icon where Shields/Simple Icons provides one. A missing upstream icon must not block the badge itself; the text label remains authoritative.

The table may remain elsewhere if it adds information, but it will not remain in the hero because it duplicates the badge inventory and weakens the first viewport.

## 7. Architecture Placement

The current `overview` diagram will move from the opener into `## 2. Architecture`, immediately after the short architecture lead-in. Its existing master and committed PNG remain unchanged unless fact-checking uncovers an independent content defect.

This placement gives readers a clear progression:

1. understand the project identity and value;
2. start the stack; and
3. inspect the detailed topology when architecture context is useful.

## 8. Three-Surface Behavior

The same semantic hero must appear in:

- the root GitHub README;
- the generated MkDocs landing page; and
- the native GitHub wiki Home page.

The canonical README and `docs/index.md` use equivalent markup with only path differences. The existing generation pipeline projects `docs/index.md` to the site and wiki. No surface links to another surface, and no surface depends on a remote copy of the banner.

Remote Shields.io badges are allowed as badge resources; the project banner and architecture image remain repository-owned local assets.

## 9. Regression and Visual Contracts

Automated tests will require:

- the banner to precede the title on both canonical opener sources;
- an exact centered HTML H1 on both sources;
- exact canonical tagline and value-proposition parity;
- all 12 declared technology badges on both sources;
- non-empty, accurate `alt` text for every opener image;
- the plain stack table to be absent from the hero;
- the detailed `overview` diagram to appear only after the Architecture H2;
- the banner master, PNG, fingerprint, site SVG, and wiki PNG to exist in their expected projections;
- deterministic render checks and byte-identical wiki PNG publication; and
- strict MkDocs, repository-link, portability, and three-surface documentation checks to remain green.

Visual verification must inspect the rendered GitHub-equivalent README width, the built MkDocs landing page, and the generated wiki Home. Acceptance requires that the banner’s primary mark and medallion layers remain recognizable without zooming.

## 10. Safety and Delivery

- Work occurs on `codex/fix-opener-hero`, created from `develop`.
- Atlas remains a pinned infrastructure submodule; no file inside `infra/` is edited.
- The user-owned untracked Atlas modernization plan remains untouched and unstaged.
- Delivery follows feature PR → `develop`, promotion PR → `main`, and a protection-compliant reconciliation PR if needed to restore tree equality.
- The feature branch is removed locally and remotely after promotion, and local `develop` is checked out at handoff.

## 11. Acceptance Criteria

1. The repository opener has a wide, polished, readable brand banner rather than a dense topology diagram.
2. `data-eng-lab`, the tagline, and the value proposition are centered above the fold.
3. The complete material technology stack is represented by icon-bearing badges.
4. The detailed architecture diagram remains available in the Architecture section.
5. README, MkDocs landing, and wiki Home remain semantically and visually synchronized.
6. All documentation, rendering, link, unit, verifier, and CI checks pass.
7. Atlas internals and the protected user plan remain unchanged.
