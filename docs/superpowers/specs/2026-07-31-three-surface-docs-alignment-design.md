# Three-Surface Documentation Alignment Design

**Status:** approved for implementation
**Date:** 2026-07-31
**Branch:** `codex/three-surface-docs-sync`
**Scope:** top-level `data-eng-lab` documentation and documentation tooling only; the `infra/` Atlas submodule remains immutable

## 1. Purpose

Bring the repository's documentation into the canonical three-surface model:

1. GitHub-rendered in-repository Markdown is the authored source.
2. The MkDocs site is generated from that source.
3. The GitHub wiki is generated from the same source.

The three surfaces must contain the same public documentation, remain self-contained, and be reproducible from one manifest-driven build. This migration also corrects the content drift discovered after the completed Atlas consumer modernization and records the successful live acceptance evidence.

## 2. Current defects

The 2026-07-31 audit found the following material defects:

- The tracked root `mkdocs.yml` builds directly from `docs/` and injects repository/edit links into every site page.
- The site, wiki, and repository Markdown are produced through different paths rather than one manifest-driven projection.
- `docs/superpowers/` is excluded from the public site without an explicit canonical/internal boundary.
- Atlas enablement, go-live, changelog, and results pages retain pre-acceptance language after successful production-like validation and Gitflow promotion.
- No `docs/manifest.yaml` defines the complete page set, hierarchy, or numbering, and existing navigation labels can drift from headings.
- `scripts/build_docs.py --check` does not compare every generated surface and asset by content hash.
- Diagram SVGs are copied directly; no committed HTML masters or PNG exports exist.
- Empty placeholder files remain under `docs/`.
- CI invokes documentation scripts by filesystem path rather than as Python modules.
- No `make docs-check` target provides the canonical local/CI gate.

## 3. Locked decisions

| Concern | Decision |
| --- | --- |
| Public source of truth | Public canonical Markdown plus `docs/manifest.yaml`; GitHub renders these files directly as the repository surface. |
| Internal engineering archive | `docs/superpowers/` remains an explicitly internal, non-manifest archive. It is excluded from generated public surfaces and from placeholder/content checks that target published documentation. The user-owned untracked modernization plan remains untouched. |
| Generated outputs | `generated/site/`, `generated/wiki/`, root `mkdocs.yml`, and `site/` are generated and gitignored. The tracked root `mkdocs.yml` is removed from the index. |
| Site configuration | A deterministic template in the documentation builder generates `mkdocs.yml` with `docs_dir: generated/site` and no `repo_url`, `repo_name`, or `edit_uri`. |
| Navigation and numbering | `docs/manifest.yaml` is the only declaration of public page membership and nav/sidebar hierarchy. Page numbers remain baked into portable Markdown headings and are cross-checked against the manifest. |
| Diagrams | Each public diagram has one committed HTML master. The renderer extracts a sanitized SVG for the site and emits a committed PNG for repository/wiki use. Assets are physically copied to every generated surface. |
| Runtime content | Atlas status pages are updated to state the completed 2026-07-31 acceptance and promotion, with exact evidence grounded in the repository and captured run results. |
| Atlas ownership | No file inside `infra/` is modified. The existing reviewed submodule pin remains unchanged. |
| Gitflow | Feature branch → `develop` PR → `main` PR. Publishing runs from `main`; merge gates cover both protected branches. |

## 4. Target architecture

```text
CANONICAL (committed)                 GENERATED (ignored)          PUBLISHED
README.md                       ─┐
docs/public Markdown             ├─ build/check ─ generated/site ─ MkDocs Pages
docs/manifest.yaml               │               generated/wiki ─ GitHub Wiki
docs/diagrams/*.html             │               mkdocs.yml
docs/diagrams/img/*.png         ─┘               site/

docs/superpowers/  ─ internal engineering archive; not published
infra/             ─ immutable Atlas submodule; not modified
```

The in-repository surface needs no generated mirror: GitHub renders the canonical Markdown. Existing committed README projections outside the canonical public page set are either converted to intentional repository entry points or retired when their content is already represented canonically. No generated public page may link to another surface.

## 5. Canonical manifest contract

`docs/manifest.yaml` declares:

- all three surfaces (`repo`, `site`, and `wiki`);
- baked numbering;
- every public section and leaf page in display order;
- notebook documentation and its source specifications where applicable;
- diagram IDs and HTML master paths.

Validation fails for malformed YAML, duplicate IDs or numbers, missing referenced files, a section that mixes `source` and `children`, unmanifested public Markdown, or disagreement between a manifest number and the page's baked heading.

`docs/superpowers/**` is the single explicit internal-tree exception. It is not silently treated as public content and is never copied into generated outputs.

## 6. Build and checking package

Documentation tooling moves to the importable `scripts/docs/` package:

- `manifest.py` parses and validates the manifest.
- `links.py` classifies cross-surface links.
- `transforms.py` maps canonical paths and rewrites links per surface.
- `render_diagrams.py` extracts/sanitizes SVG and renders PNG assets.
- `build_docs.py` renders site, wiki, sidebar/footer, assets, and root `mkdocs.yml` deterministically.
- `check_docs.py` runs completeness, numbering, placeholder, self-containment, empty-artifact, and content-hash determinism checks.
- `push_wiki.py` synchronizes `generated/wiki/` to the wiki's `master` branch with a safe no-op guard and default CI identity.

All local and CI calls use module invocation (`python -m scripts.docs...`). The Makefile exposes `docs-build`, `docs-check`, `docs-serve`, and `docs-wiki` as the supported interface.

The migration may adapt tested logic from the current `scripts/build_docs.py` and `scripts/docslib/`, but the final tree has one active implementation and no compatibility shims that preserve the split pipeline.

## 7. Surface self-containment

The checker enforces the full matrix:

- repository Markdown has no site or wiki links;
- site Markdown/config has no repository source/edit or wiki links;
- wiki Markdown has no repository source or site links;
- relative Markdown links are rewritten only to manifest-known targets;
- links to notebooks or non-public internal Markdown become unlinked text on generated surfaces;
- every referenced diagram exists locally on the surface that embeds it.

The site configuration deliberately omits repository navigation and edit controls. User-facing README prose describes the project and its operation, not the documentation build system.

## 8. Diagram migration

Each existing architecture diagram is reviewed against current DAGs, notebooks, Compose overlays, Spark applications, and Atlas consumer configuration. The migration then creates or consolidates one HTML master per diagram under `docs/diagrams/`, derives site SVG and committed repository/wiki PNG assets, and removes obsolete copied SVG trees only after all references have been migrated.

Diagram rendering tests cover SVG extraction, HTML-entity sanitization, PNG validity, deterministic filenames, subdirectory-relative paths, and physical copying into both generated surfaces. Cairo is installed explicitly in CI.

## 9. Content reconciliation

The Atlas-related pages are updated as one factual set:

- `docs/atlas-enablement.md` records that the representative Airflow `SparkSubmitOperator` task completed successfully.
- `docs/atlas-feedback-go-live.md` closes the prior pending gate and distinguishes resolved upstream issues from consumer-side configuration.
- `docs/go-live.md` changes future-tense promotion guidance into a completed-gate record without weakening the reusable runbook.
- `docs/go-live-results.md` records the 2026-07-31 run: first and only Airflow attempt succeeded, Spark REST reported `FINISHED` with `success=true`, the Bronze table contained 8,991,502 rows, and `passenger_count` was `double`.
- `docs/CHANGELOG.md` records the final reviewed Atlas pin, live acceptance, and completed feature→develop→main promotion.

Generated wiki/site output receives these corrections solely through regeneration. Claims are verified against the current repository tree and the captured acceptance evidence before publication.

## 10. CI and publishing

The documentation gate runs for pull requests and pushes targeting both `develop` and `main`. It installs Cairo and documentation dependencies, then runs:

1. diagram rendering;
2. manifest/surface checks;
3. strict MkDocs build;
4. documentation-tool lint;
5. documentation-tool unit tests.

The main-branch publishing workflow regenerates the site and wiki, deploys `site/` to GitHub Pages, then pushes `generated/wiki/` to the wiki `master` branch. Existing working wiki authentication is preserved unless repository evidence shows it violates the canonical publisher contract; secret material is never printed or committed.

## 11. Verification and acceptance criteria

The work is complete only when all of the following are true:

- `make docs-check` passes and includes content-hash determinism.
- `mkdocs build --strict` reports no documentation warnings or broken internal links.
- Documentation unit tests and the repository's existing relevant test suite pass.
- Every manifest page exists in repository, site, and wiki form.
- Every public Markdown page is represented in the manifest; only `docs/superpowers/**` is excluded by policy.
- No generated or repository surface contains a forbidden cross-surface link.
- Root `mkdocs.yml`, `generated/`, and `site/` are ignored and reproducible.
- Every diagram has an HTML master, generated site SVG, committed PNG, and surface-local references.
- `find docs -type f -empty -o -type d -empty` reports no empty public artifact or directory.
- Atlas acceptance and promotion claims agree across all relevant canonical pages.
- `infra/` remains at the reviewed pin with no submodule worktree modifications.
- The preserved untracked historical plan is byte-for-byte untouched and never staged.
- Feature→`develop` and `develop`→`main` PR checks pass before merge; final `main` and `develop` trees are content-identical and local `develop` is checked out.

## 12. Risks and controls

| Risk | Control |
| --- | --- |
| Public/internal boundary accidentally publishes engineering plans | Manifest allow-list plus an explicit `docs/superpowers/**` exclusion test. |
| Migration silently drops a current page | Inventory current nav, wiki map, README projections, and all public Markdown before finalizing the manifest. |
| Generated trees appear clean while content differs | Compare SHA-256 hashes, not filenames alone. |
| Diagram conversion changes semantics | Fact-check every master against current code/config and review generated SVG/PNG output. |
| Wiki publication breaks | Preserve `master`, CI git identity, no-op behavior, and the already functional repository authentication path. |
| Atlas submodule is modified accidentally | Check `git submodule status` and `git -C infra status` before each commit and stage explicit parent-repository paths only. |
| User-owned plan is captured by broad staging | Never use broad staging; verify the path remains untracked before each commit. |

## 13. Supersession

This design supersedes the documentation-pipeline architecture in the 2026-07-05, 2026-07-06, and 2026-07-07 documentation overhaul designs. Those files remain historical engineering records under the internal archive. Their delivered public content remains valid only where it agrees with the current manifest, source tree, and verified Atlas acceptance state.
