# Atlas Consumer Modernization Design

**Status:** approved design; implementation planning pending review
**Date:** 2026-07-25
**Owner:** data-eng-lab
**Target Atlas pin:** `881df596907dce15daaed92e405f92b2830fd7d1` (`origin/main` observed 2026-07-25)

## 1. Purpose and scope

Modernize `data-eng-lab`'s vendored Atlas infrastructure from `2d006cae` to the immutable target above. The consumer will follow Atlas's current submodule adoption runbook:

1. Keep Atlas as a pinned, never locally edited `infra/` submodule.
2. Keep committed consumer intent in parent-owned `atlas.consumer.yml` and the parent-owned Compose overlay.
3. Validate merged configuration before startup; export and assert the supported endpoint contract after startup.
4. Use service DNS for in-network code and dynamically exported endpoints for host-side code—never fixed host ports.

The work covers all material Atlas consumers: launcher, manifest, Compose overlay, Spark app DAGs, 19 scenario DAGs, Jupyter and Zeppelin notebooks, scenario docs, live probes, test helpers, platform/go-live/runbook docs, diagrams, and generated README/wiki surfaces.

It does not add Atlas services, edit `infra/`, alter scenario semantics, replace in-network service DNS with host URLs, or require full live execution of every scenario.

## 2. Fixed decisions

| Decision | Chosen design |
| --- | --- |
| Atlas version | Refresh, verify ancestry, then pin the exact target SHA. Never track the branch. |
| Configuration ownership | `atlas.consumer.yml` owns portable identity, `BASE_PORT: auto`, sources, branding, storage, and overlays. Machine-local scalars and secrets remain untracked. |
| Ollama | Retain the profile-specific `dev` override selecting `ollama-localhost`; no flat per-launch source flag. |
| Endpoint model | Generate and assert the supported endpoint export for MinIO. Use a tested parent resolver (explicit override, then `infra/.env`) for data-eng host ports that Atlas does not yet export: Iceberg REST, Trino, Redpanda, Zeppelin, and Airflow. In-container code keeps DNS names such as `iceberg-rest`, `spark-master`, `trino`, and `redpanda`. |
| Validation | Static coverage spans the full catalog; focused live smokes prove runtime compatibility. |
| Integration | Protected Gitflow promotion: feature branch → `develop` PR → `main` PR, with required checks passing at both gates. |

## 3. Consumer architecture

`atlas.consumer.yml` remains the parent repository's declarative registration point. It supplies the `data-eng-lab` Docker namespace, durable auto-assigned port block, data-eng service sources, `lakehouse-test` bucket, `dev` Ollama override, and `compose/data-eng-lab.yml`.

The Compose overlay remains parent-owned. It mounts this repo's DAGs, Spark apps, datasets, and probes into Atlas-owned services; it must be checked against target service names and mounts, but must not create `services/_user` symlinks or mutate submodule files.

`scripts/start-all.sh` becomes the lifecycle boundary:

```text
manifest + local Atlas env
  → env backfill
  → consumer-aware Compose validation
  → consumer doctor
  → detached data-eng startup
  → endpoint export + required-field assertions
  → Iceberg namespace registration
  → Layer 1 and Layer 2 preflight
```

The endpoint file is a generated, ignored runtime artifact. The target Atlas contract currently exports `ATLAS_MINIO_HOST_ENDPOINT`, which host-side object-storage helpers consume and assert. It does not export data-eng host endpoints for Iceberg REST, Trino, Redpanda, Zeppelin, or Airflow. A small parent resolver therefore uses an explicit test/runtime override first and the matching value from `infra/.env` second for those services; it never derives ports from a historical `BASE_PORT`. Atlas's `.env` remains Atlas-owned configuration, not a stable public contract.

## 4. Material-impact treatment

### 4.1 Runtime configuration and startup

- Reconcile the manifest with the target schema and retain only intentional portable scalars. Add manifest tests for its source, profile, overlay, and storage contract.
- Update the launcher to run consumer-aware `compose validate` before `doctor`, export endpoints only after a successful detached start, and assert the supported `ATLAS_MINIO_HOST_ENDPOINT` field. Do not assert non-existent data-eng endpoint fields.
- Update lifecycle docs: target Atlas detects a changed source commit and rebuilds stale local images automatically. Preserve explicit cold start only for destructive reset or uncommitted Dockerfile changes.

### 4.2 Airflow, Spark apps, and scenario DAGs

- Validate every DAG module against the upgraded Airflow/Spark contract, retaining in-network Iceberg REST and Spark catalog configuration.
- Replace documentation and expectations that call DAG execution blocked by Atlas #791. The target routes scheduler and DAG-processor execution API traffic to `airflow-webserver`.
- Prove that repair with one representative Spark-submit DAG run; retain unit coverage for catalog values, task shape, and importability.
- Verify the overlay mounts DAGs into both scheduler and DAG processor. Do not duplicate Atlas-owned Airflow environment overrides.

### 4.3 Notebooks, scenarios, and datasets

- Statically inventory every Zeppelin `.zpln` and Jupyter `.ipynb` artifact for catalog, topic, service-DNS, and host-port assumptions. Preserve valid in-network endpoints; reject fixed host ports.
- Verify generated `notebooks.md`, scenario `README.md`, and canonical artifacts stay in parity after docs regeneration.
- Exercise representative Zeppelin/Spark and Jupyter/PyIceberg notebooks live; retain static catalog coverage for the rest.
- Retain current Redpanda topic behavior: consumer topics are produced or explicitly seeded, while Atlas's default demo topic remains Atlas-owned.

### 4.4 Host-side code, tests, docs, and diagrams

- Move host-side MinIO URL resolution to the endpoint export, while retaining local Atlas-env parsing for credentials. Give Iceberg, Trino, Redpanda, Zeppelin, and Airflow one tested parent resolver with explicit override precedence because the target Atlas export does not publish those data-eng endpoints. Keep unit fixtures literal only when testing parsing.
- Assert `ATLAS_MINIO_HOST_ENDPOINT`, the supported exported field this repository consumes; cover the unexported data-eng service ports with resolver tests instead of unsupported endpoint assertions.
- Update platform expectations, go-live findings, pin-bump runbook, getting-started instructions, changelog, and Atlas-specific diagram annotations for the new pin and resolved Airflow condition.
- Regenerate README and wiki surfaces from `docs/`, then run existing surface, link, and diagram checks.

## 5. Verification design

### Static gates

1. Submodule-pointer and ancestry checks against the captured SHA.
2. Atlas `env backfill`, consumer-aware `compose validate`, `doctor`, and `endpoints assert --require ATLAS_MINIO_HOST_ENDPOINT`.
3. Offline tests and lint, including manifest, DAG catalog, notebook/document parsing, and repository-structure checks.
4. Static checks that distinguish permitted in-network addresses from invalid fixed host endpoints.
5. Documentation regeneration plus surface, link, and diagram checks.

### Focused live smoke matrix

| Capability | Proof |
| --- | --- |
| Consumer startup | Controlled upgrade start, generated endpoint export, `ATLAS_MINIO_HOST_ENDPOINT` assertion, and healthy service summary. |
| Infrastructure wiring | Existing Layer 1/2 preflight passes for Spark/MinIO/Iceberg, Airflow/Spark, Zeppelin/Spark, Trino/Iceberg, and Spark/Redpanda. |
| Airflow repair | One representative Spark-submit DAG succeeds without task-side `localhost:8080/execution/` failure. |
| Notebooks | One Zeppelin `%spark` path and one Jupyter/PyIceberg path run against target Atlas. |
| SQL and streaming | One Trino query and one Redpanda producer/consumer path succeed. |
| Isolation | Resolved port block and `${PROJECT_NAME}` containers are correct; no duplicate Ollama container under the localhost profile. |

Capture commands, target SHA, endpoint artifact, and outcomes in the feature PR. A failed gate blocks promotion until fixed or deferred through a separate linked issue.

## 6. Branch, PR, and cleanup protocol

Before implementation, fetch/prune remotes, confirm `develop` is current, and inspect the worktree. The existing untracked historical plan `docs/superpowers/plans/2026-07-21-atlas-submodule-modernization.md` is user-owned context: preserve it and do not stage or overwrite it.

1. Work on `codex/atlas-consumer-modernization` from `develop`.
2. Capture refreshed `infra` `origin/main` SHA, verify it is an ancestor, checkout the SHA detached in the submodule, and commit the pointer with parent integration changes.
3. Push the feature branch and merge its passing PR into `develop`.
4. Refresh `develop`, then create and merge the promotion PR from `develop` into `main`, again only after required checks pass.
5. Fetch/prune; confirm no migration PR remains open; delete the merged feature branch remotely and locally; inspect before deleting only confirmed stale merged branches. Never delete `main`, `develop`, user worktrees, or untracked plans.

The final handoff reports the exact Atlas SHA, parent commit, validations, intentional follow-up, and branch/PR cleanup.

## 7. Risks and handling

| Risk | Handling |
| --- | --- |
| Atlas main advances | Capture and commit one SHA at implementation start; a newer SHA is a separate reviewed bump. |
| Historical plan is accidentally included | Check status before every stage and stage explicit paths only. |
| Image rebuild behavior is misunderstood | Validate target's `.atlas-build-state` behavior on upgrade; document cold rebuild only for its defined cases. |
| Dynamic ports cause stale/cross-stack traffic | Use `BASE_PORT: auto`, exported endpoints, assertions, and static host-port guards. |
| Airflow fix is present but parent integration breaks it | Smoke one real Spark-submit DAG and retain Layer 2 Airflow coverage. |
| Broad edits cause doc drift | Treat `docs/` as source of truth and regenerate/check derived surfaces in the same PR. |

## 8. Supersession

The untracked 2026-07-21 modernization draft documents the already-completed `85ff46b` → `2d006cae` migration. It is preserved as history, not edited. This design supersedes its version-specific assumptions for the next `2d006cae` → `881df596` upgrade.
