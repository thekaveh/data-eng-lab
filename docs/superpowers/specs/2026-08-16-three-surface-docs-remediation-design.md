# Three-Surface Documentation Remediation Design

**Date:** 2026-08-16
**Status:** Implemented
**Scope:** Resolve the 2026-08-16 three-surface documentation audit findings and any additional evidence-backed findings discovered during the implementation audit.

## 1. Objective

Restore complete agreement between the committed documentation, generated MkDocs site, and generated GitHub wiki while preserving the repository's single-source, self-contained publication model. The change corrects stale public metadata, brings the full-stack architecture diagram up to date with shipped observability, removes authoring metadata from public artwork, strengthens the shared opener, and records the repository's approved wiki authentication model.

The implementation must not modify Atlas source, scenario behavior, datasets, application runtime behavior, release state, or notebook execution semantics.

## 2. Source and projection boundaries

Humans continue to edit only committed canonical sources:

- `README.md` and `docs/index.md` for the shared opening;
- `docs/manifest.yaml` for public hierarchy;
- `docs/diagrams/*.html` for diagram masters;
- `docs/diagrams/img/*.png` for committed raster projections;
- `scripts/docs/build_docs.py` for the generated MkDocs configuration;
- `.github/workflows/docs-deploy.yml` for publication behavior.

`generated/site/`, `generated/wiki/`, root `mkdocs.yml`, and `site/` remain ignored build products. The implementation regenerates them for verification but commits only canonical sources and required PNG projections.

## 3. Public metadata contract

The MkDocs `site_description` must report six CI-built Maven Spark applications, matching the six repository-owned `spark-apps/*/pom.xml` roots and the public inventory. A regression test must derive the expected count from the repository tree or otherwise bind the rendered description to the six-app public contract; a free-standing unchecked numeral is not sufficient.

## 4. Shared opener contract

The README and landing source retain identical title, poster, tagline, value proposition, badge rows, and executive summary after normalizing only the different local poster path. The executive summary expands from 53 words to approximately 100–150 words and must:

- remain one canonical exact string asserted on both surfaces;
- name the pinned Atlas consumer boundary, `make up`, the default development profile, and the **Data Engineering** display name;
- explain the integrated value of the lab rather than only enumerate tools;
- state the 19 paired notebooks, 17 Scala/PySpark pairs, two Trino client pairs, six Maven applications, Airflow orchestration, MinIO/Iceberg storage, Trino analytics, Redpanda streaming, and Prometheus/Grafana observability;
- stay free of MkDocs, wiki synchronization, Pages, and publication implementation details.

Tests must assert exact parity, the approved word-count range, required grounded terms, and the six-app count.

## 5. Architecture diagram contract

`docs/diagrams/overview.html` remains the canonical full-stack master. It must be revised using the architecture-diagram workflow so the rendered system view includes the shipped Iceberg REST observability path:

1. `iceberg-rest-probe` performs the bounded catalog probe;
2. Prometheus scrapes the probe metrics and evaluates the repository-owned alert rules;
3. Grafana reads the fixed Prometheus datasource and presents the provisioned dashboard/alerts.

The monitoring flow must be visually distinct without obscuring the data, metadata, artifact, and retention flows already shown. The diagram must continue to represent the components and relationships grounded in `atlas.consumer.yml`, `compose/data-eng-lab.yml`, `docs/iceberg-rest-observability.md`, and the Prometheus/Grafana configuration. Visible strings describing the artwork's theme, orientation, or production process are removed.

After changing the master, the implementation regenerates and commits `docs/diagrams/img/overview.png`; the site SVG and wiki PNG must be rebuilt and verified as physical surface-local copies. Diagram tests must require the monitoring components and reject the removed styling metadata.

## 6. Wiki publisher decision

The repository retains its current isolated `GITHUB_TOKEN` publisher rather than introducing a long-lived deploy key. This is the approved repository-specific equivalent to the generic deploy-key example because:

- the privileged `wiki` job is isolated from the untrusted build job;
- it receives only `contents: write` permission;
- the checked-out privileged script is SHA-256 verified before execution;
- the token is ephemeral and repository-scoped;
- successful live publication and exact generated/live wiki parity are already proven.

The workflow continues to push `HEAD:master` with the existing default bot identity. A repository contract test and an internal design note must make this authentication choice explicit so future audits do not interpret the intentional absence of deploy keys or Actions secrets as an accidental omission. The workflow must not support two simultaneous authentication mechanisms.

## 7. Additional audit and fail-closed behavior

After the named corrections are green, rerun the complete A–L three-surface audit against an exact worktree build. Any newly discovered issue is fixed only when it is reproducible, in scope, and evidence-backed. Each behavior change begins with a failing regression test where practical.

The implementation must preserve:

- ignored generated roots and untracked root `mkdocs.yml`;
- content-hash determinism;
- manifest/H1 and heading-number parity;
- cross-surface self-containment;
- local diagram assets on all three surfaces;
- wiki `master` publication and default git identity;
- `main`-only publication with merge gates covering `develop` and `main`;
- zero empty public artifacts, unfinished markers, leaked notebook headings, orphan sections, or empty code fences.

## 8. Verification and acceptance

Acceptance requires all of the following on the final worktree state:

1. Focused RED-to-GREEN tests for metadata, opener, diagram, and publisher contracts.
2. `make docs-check` and a separate `mkdocs build --strict` with zero warnings.
3. `make docs-wiki`.
4. The complete docs tooling/content test suites.
5. `make verify`, Ruff check, Ruff format check, and `git diff --check`.
6. Fresh generation proving 24 diagram masters have corresponding committed PNG, site SVG, and wiki PNG projections.
7. Self-containment, placeholder, empty-artifact, notebook-numbering, duplicate-heading, and empty-fence scans.
8. A post-change read-only audit report with every remaining finding resolved or explicitly reported.

The separate 19-scenario notebook reproducibility suite remains a live-stack gate and is not conflated with documentation health. It is not required for this documentation-only remediation unless the implementation changes notebook execution or generated notebook content.

## 9. Non-goals

- No new documentation surface or publishing service.
- No persistent deploy key or additional authentication fallback.
- No edits to the pinned Atlas submodule.
- No scenario, DAG, Spark application, dataset, checkpoint-retention, observability-runtime, or release-policy behavior changes.
- No publication or GitHub repository-setting mutation during local implementation.

## 10. Implementation result

The named metadata, opener, diagram, authoring-label, and publisher-contract findings are closed. The post-change audit also found and corrected three additional evidence-backed defects: Ruff formatting drift in the documentation tooling, an incorrect Iceberg probe port in the first diagram revision, and a pre-existing release-contract parser omission for raw `iframe` governance evidence. The intentional workflow comment changed a privileged workflow digest, so the release-policy allowlist was updated to bind the reviewed bytes.

Final local evidence includes 209 documentation/content tests, 24 complete diagram projections on each generated surface, strict MkDocs and wiki generation, zero repository-verifier findings, and the full offline repository suite with 3,635 passing tests, two expected opt-in MinIO skips, and 72 live/infra deselections. The separate 19-scenario live notebook reproducibility suite was not run because this remediation does not change notebook execution or generated notebook content.
