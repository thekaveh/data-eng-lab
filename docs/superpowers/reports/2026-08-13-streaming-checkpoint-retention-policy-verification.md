# Issue #85 streaming checkpoint retention policy verification

**Date:** 2026-08-13
**Branch:** `codex/85-streaming-checkpoint-policy`
**Base:** `origin/develop`
**Live mutation:** none; issue #85 is intentionally non-networked and non-destructive

## Delivered contract

- `checkpoints/retention-policy.yaml` owns exactly five checkpoint identities and
  freezes the approved owners, sources, sinks, durability classes, terminal states,
  recovery consequences, retention durations, lease clocks, and operation bounds.
- `scripts/checkpoints/policy.py` uses a duplicate-aware strict YAML loader,
  deeply immutable valid generation facts, exact path/template matching, and a pure fail-closed
  evaluator. It emits deterministic compact local plan JSON and SHA-256 without
  contacting a service or changing caller state.
- All four streaming README/Jupyter/Zeppelin triples and their public scenario and
  notebook projections state the checkpoint identity, class, recovery consequence,
  disabled scheduling boundary, and issue #86 enforcement dependency.
- `docs/checkpoint-retention.md` is canonical in the documentation manifest and
  projects deterministically to MkDocs and the GitHub wiki.
- The existing notebook reproducibility reset is explicitly exclusive-test-only.
  Its `gh_events_file/` family-root reset is not a policy eligibility decision and
  remains assigned for replacement by issue #86.

## TDD evidence

| Slice | RED | GREEN |
|---|---:|---:|
| Registry/parser | 33 expected missing-module failures | 33 passed |
| Pure evaluator | 25 expected missing-API failures | 58 combined checkpoint tests passed |
| Repository ownership | 1 missing reset-policy failure, 3 existing mappings passed | 44 focused tests passed |
| Notebook warnings | 8 missing-warning failures | 21 focused tests passed |
| Public projections | 8 missing scenario/notebook projection failures | included in 254 focused docs tests |
| Canonical docs | 5 missing runbook/manifest/matrix/go-live failures | 254 focused docs tests passed |

No production behavior was written before its named RED observation. Two fixture
corrections preserved the approved policy: control-prefix rejection uses its specific
code, and an anchor test uses a final heartbeat rather than an object newer than the
terminal record.

Independent review then established a combined adversarial RED of 77 failures and 17
passes. The named gaps were durable retirement transition evidence, class-specific
lease/terminal states, exact fact types and clock/TTL consistency, safe overflow and
invalid hash/ETag refusal, bounded YAML construction, retained active-durable audit
reasons, deep generation immutability, exhaustive executable checkpoint discovery,
and deterministic runbook examples. The corrected focused parser/evaluator/ownership
suite passes these cases without adding a network or mutation path.

The first re-review narrowed the residual boundary to an unhashable lease-state raw
exception, a malformed-YAML cause-chain payload leak, and a direct-string allocation
before the byte bound. A microscopic RED reproduced all four adversarial cases (list
and mapping states, traceback payload, and pre-encode allocation). The final boundary
uses a sanitized local state, suppresses PyYAML causes, performs a cheap character
guard before the authoritative UTF-8 byte guard, and retains multibyte enforcement.

## Fresh verification

| Gate | Result |
|---|---|
| `uv run ruff check . --exclude graphify-out` | pass |
| `uv run pytest -m 'not infra and not network' -q --junitxml=/tmp/issue85-review-fix-pytest.xml` | 3,062 passed, 71 deselected, 51.56 s |
| `uv run pytest -q tests/checkpoints` | 115 passed, 0 failed, 0 skipped, 0.53 s |
| focused checkpoint and documentation projections | 356 passed, 0 failed, 0 skipped, 4.58 s |
| `make verify` | 0 findings, 0 errors |
| `make docs-check` | pass; strict MkDocs build |
| `make docs-wiki` | pass; deterministic wiki check |
| `uv run python -m scripts.docs.build_docs --site --wiki --check --root .` | pass |
| `./start.sh --consumer ../atlas.consumer.yml compose validate` from `infra/` | `Compose config is valid.` |
| `make build-apps` | all six Maven applications packaged; all six shaded JARs present |
| `git diff --check origin/develop...HEAD` | pass |

Raw `make lint` also inspects the protected untracked `graphify-out/` directory and
reports one pre-existing import-order finding in its stale copied #83 corpus. The
scope-approved equivalent excludes that unrelated directory and passes. Neither the
graph nor its copied file was modified.

## Non-networked and destructive-scope proof

Importing `scripts.checkpoints.policy` loads none of `boto3`, `requests`, `airflow`,
`pyspark`, `minio`, `socket`, or `subprocess`. The issue #85 implementation has no S3
client, delete/put operation, Airflow DAG, credential, schedule, or import-time
network path. It reads only the caller-selected local policy file; the evaluator
accepts supplied immutable facts.

A destructive live gate would violate this issue's boundary. Issue #86 alone must
live-prove conditional lease/tombstone writes, scoped RBAC, bounded inventory and
deletion, changed-state refusal, partial retry, audit/metrics persistence, and
volume-preserving cleanup using disposable fixtures.

## Repository and external-state invariants

- Issue #85: Open / Project In Progress.
- Issue #84: Open / Project Todo.
- Issue #86: Open / Project Todo.
- Project containers: zero all-state containers.
- Project volumes: all 13 named `data-eng-lab-*` volumes remain.
- Protected unrelated plan SHA-256:
  `f7eb55036e3020f419d9acb42bbc502bfe0528219a980a6973ed083591d2ba66`.
- `uv.lock` SHA-256:
  `a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1`.
- Dataset registry SHA-256:
  `093de54a5c7288087e40f679a886cc0b558e750efa00ca24d0f0d888f7f76119`.
- Atlas gitlink: `c6cf73d7168db1a7840fc45c9ed3e385071996d8`; nested worktree clean.
- `graphify-out/` remains untracked and unchanged by issue #85.

## Commits before review

1. `fb45cbf` — design specification
2. `3aa2a90` — implementation plan
3. `ea5dd34` — strict canonical registry and parser
4. `6626bab` — pure fail-closed evaluator
5. `13aeee7` — executable ownership/reset boundary
6. `3a88a6d` — source notebook and README warnings
7. `f958e7c` — canonical runbook and all public documentation surfaces
8. `a79b2c5` — initial verification report and immutable review handoff
9. `db5b17b` — close fail-closed evaluator, parser, ownership, and runbook review gaps
10. `b3b2257` — record the first review-fix verification evidence

The branch has not been pushed and no pull request exists. Independent specification
and quality/security reviews must use the exact immutable diff package produced after
the report commit.
