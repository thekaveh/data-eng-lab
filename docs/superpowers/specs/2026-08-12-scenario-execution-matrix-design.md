# Scenario Execution-Mode Matrix Design

**Date:** 2026-08-12

**Issue:** [#82](https://github.com/thekaveh/data-eng-lab/issues/82)

**Dependencies:** [#78](https://github.com/thekaveh/data-eng-lab/issues/78), [#81](https://github.com/thekaveh/data-eng-lab/issues/81)

**Status:** Approved design

## 1. Objective

Give every one of the repository's 19 scenario directories one truthful,
tested execution contract. The contract separates notebook demonstrations,
continuous streaming sessions, approved future production work, and the two
production Spark applications that exist today. It removes Airflow DAGs that
contain only `EmptyOperator` so neither Airflow discovery nor public guidance
can imply that a no-op performs scenario work.

This issue defines and publishes contracts. It does not implement any new
production DAG.

## 2. Decision and rejected alternatives

### 2.1 Selected: structured matrix with generated public projection

`scenarios/execution-modes.yaml` is the only editable execution-mode inventory.
A typed Python loader validates its closed schema against the live scenario
directories and renders a committed, manifest-owned Markdown matrix. The
repository verifier and documentation gate compare the projection byte for
byte with the canonical YAML.

The exact classification vocabulary is:

- `existing production DAG`
- `approved new production DAG`
- `intentionally notebook-only`
- `intentionally unscheduled long-running streaming`
- `deprecated or superseded`

Every row records the exact scenario identifier, classification,
justification, owner, runtime, schedule policy, execution entrypoint or
`null`, dependencies, an acceptance contract, and a child issue or `null`.

### 2.2 Rejected: retain placeholder DAGs with stronger warnings

Airflow would still discover and successfully run tasks that do no work.
Warnings in comments or documentation cannot make a successful no-op run a
truthful execution surface.

### 2.3 Rejected: infer modes from prose or filenames

README text, notebook code, DAG schedules, and diagrams have already drifted.
Inference would preserve that ambiguity and provide no reviewable owner,
dependency, or acceptance boundary.

## 3. Classification

| Classification | Scenarios | Decision |
|---|---:|---|
| existing production DAG | 2 | NYC Taxi batch ingest uses `spark-apps/nyc-taxi-etl/dag.py`; NYC Taxi medallion uses `spark-apps/nyc-taxi-medallion/dag.py`. |
| approved new production DAG | 7 | Data quality, TPCH star schema, MovieLens feature engineering, GH Archive JSON flatten, GH Archive sessionization, and both Trino queries require child implementation. |
| intentionally notebook-only | 7 | Schema evolution, time travel, table maintenance, incremental upsert, SCD2, join optimization, and GH Archive file streaming remain bounded teaching/experiment surfaces. |
| intentionally unscheduled long-running streaming | 3 | Event ingest, event windows, and online-retail CDC remain operator-started notebook streams with durable checkpoints and no batch schedule. |
| deprecated or superseded | 0 | The obsolete scenario-local no-op DAG artifacts are deleted; supersession is explained on the two production rows rather than represented as a second row. |

The seven approved rows map to five child issues. Existing issue #91 owns data
quality and #83 owns both Trino rows. Three new issues own, respectively, the
TPCH star-schema data product, the MovieLens feature data product, and the
coupled GH Archive flatten-to-sessionization pipeline. The GH pair shares one
child because sessionization consumes the flattened event contract; its child
acceptance criteria must still prove and review each stage independently.

## 4. Validation contract

The loader rejects malformed YAML, unknown top-level or row fields, duplicate
scenario identifiers, unknown classifications, empty required strings/lists,
invalid child issue values, and unsafe or nonexistent existing-production
entrypoints. It compares the 19 matrix identifiers exactly with the 19
directories containing paired Jupyter and Zeppelin notebooks.

Additional semantic validation requires:

- exactly two existing-production rows, whose entrypoints are the two mounted
  production Spark-app DAG files;
- every approved-new-production row to have one open child issue and no current
  execution entrypoint;
- all other rows to have no child issue;
- every intentionally unscheduled row to have no Airflow entrypoint and an
  explicit unscheduled policy;
- no `dag.py` below `scenarios/`, no `EmptyOperator` in runtime DAG files, and
  exactly two production DAGs below `spark-apps/`;
- the consumer overlay to keep the production `spark-apps/` DAG mount;
- new-scenario scaffolding to omit DAG files rather than create placeholders.

## 5. Documentation projection

The renderer emits `docs/scenarios/execution-modes.md` from the YAML and the
manifest projects that page to the MkDocs site and GitHub wiki. The scenario
catalog and notebook index link to the matrix. Each scenario README and its
canonical scenario page state the same classification, entrypoint, schedule
policy, and child issue. Scenario diagram masters replace obsolete Airflow DAG
labels with the same execution contract, then the existing diagram pipeline
regenerates committed PNGs and site SVGs.

Current public instructions may trigger only the two real production DAGs.
Notebook-only and continuous-stream rows direct readers to the paired
notebooks. Approved rows link their child issue and state that no production
DAG exists yet. Historical plans remain unchanged and excluded from current
public-claim checks.

## 6. Test strategy

TDD begins with contracts that fail on the current tree: matrix absence,
19 scheduled/discoverable no-op DAGs, false trigger instructions, stale Trino
blocker text, missing child links, and missing documentation projection. GREEN
adds the strict loader/renderer, the reviewed 19-row YAML, child issues, DAG
removal/scaffolding behavior, public prose corrections, diagram projections,
and verifier/docs integration.

Focused tests cover schema failures and every cross-tree invariant. Completion
requires the full offline suite, Ruff, repository verification, deterministic
site/wiki builds, strict MkDocs, DAG import/static checks, notebook/document
inventory checks, and protected-file/submodule invariants.
