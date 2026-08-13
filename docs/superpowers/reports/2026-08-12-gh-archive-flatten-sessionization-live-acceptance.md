# GH Archive Flatten and Sessionization Live Acceptance

**Issue:** #109

**Date:** 2026-08-12

**Result:** PASS after contract corrections for canonical duplicate IDs, Iceberg timestamp labels,
and the session-boundary measure

## Accepted artifact and immutable input

- Local and published JAR: `gh-archive-pipeline/0.1.0/app.jar`
- JAR SHA-256: `ee683d4e25edce245146a6dd43ff7170681fc92cbded256b1dfcdf58361ccf47`
- Dataset/scale: `gh_archive` / `tiny`
- Plan ID: `8ab812c3621cc3dae68989d9f24134351ea9683453133b31feaff579d0fa3e7f`
- Publication ID: `e53a481df5d54c6eabc645838fb2f2ba`
- Manifest SHA-256: `998ec39bc61dca1b460e4b851d718a5347b8c7e575b96dd1e3ec62fd0b791678`
- Active-pointer ETag: `8e10b023584e4d737587845a38b0ce4d`
- Source object: `2023-01-01-0.json.gz`, schema `gh_archive_consumed_fields`,
  59,785,519 bytes, SHA-256
  `2b0c0cc3b067f61c0f39d7623517904d95d22ef9d5c998953050a0b78adb6258`

The first acceptance attempt failed closed because the tiny pointer did not exist. With explicit
operator approval, the prerequisite publication was created outside the harness with:

```bash
uv run python scripts/download_datasets.py --scale tiny --only gh_archive --refresh
uv run python scripts/download_datasets.py --scale tiny --only gh_archive --verify-only
```

Before publication the active pointer was absent. The supported bounded refresh created the pointer
with `If-None-Match: *`, retained legacy flat objects and publication history, deleted nothing, and
left the registry at SHA-256
`093de54a5c7288087e40f679a886cc0b558e750efa00ca24d0f0d888f7f76119`.
The live harness itself never refreshes, infers absence from a generic error, or mutates a dataset
pointer. It resolved the verified pointer before and after `--verify-only`, required equal resolver
documents, and required the exact pointer body and ETag to remain unchanged across both runs.

## Real orchestration evidence

The project had zero running, stopped, exited, or created containers before the controlled replay.
The harness started an exclusively owned stack, kept `gh_archive_flatten_sessionization` paused,
and restored its initial pause state. It used complete bounded Airflow-v2 DagRun pagination and
`airflow dags test --use-executor` with two unique whole-second logical dates:

| Run | Logical date | Airflow | Spark drivers | REST terminal |
|---|---|---|---|---|
| first | `2026-08-13T00:23:10Z` | API-visible terminal `success` | `driver-20260813002508-0000`, `driver-20260813002546-0001` | both `FINISHED`, `success=true` |
| second | `2026-08-13T00:26:19Z` | API-visible terminal `success` | `driver-20260813002653-0002`, `driver-20260813002731-0003` | both `FINISHED`, `success=true` |

The bounded API inventory gained exactly one unique owned DagRun after each command, exactly two in
the acceptance window, and no unexpected queued/running run. The second run started only after the
first completed. Four distinct drivers prove the flatten and session entrypoints both ran twice.

## Source, table, session, and provenance evidence

The harness independently downloaded the exact resolver-identified gzip object, verified its size
and SHA-256, decoded each bounded JSON line with duplicate-key rejection, required the strict nested
schema and exact whole-second UTC timestamps, and counted:

- 101,917 source records
- 101,916 distinct nonblank event IDs
- one repeated event whose five required flattened fields were exactly identical
- zero conflicting records that reused an ID with different required fields

Both accepted runs produced exact source-to-events-to-sessions multiset conservation:

| Table | Exact ordered schema | Rows |
|---|---|---:|
| `lakehouse.silver.gh_events` | `id:string`, `type:string`, `actor_login:string`, `repo_name:string`, `created_at:timestamptz` | 101917 |
| `lakehouse.silver.gh_sessions` | the five event columns, `previous_created_at:timestamptz`, `new_session:int`, `session_id:long` | 101917 |

Meaningful session measures were:

- 16,331 distinct actors and exactly 16,331 null predecessors
- 16,767 session starts
- zero rows with a gap greater than 1,800 seconds that were not marked as a new session
- zero rows with a non-null gap at most 1,800 seconds that were marked as a new session

Both `$properties` tables contained the same exact values:

```text
data_eng_lab.dataset=gh_archive
data_eng_lab.dataset.scale=tiny
data_eng_lab.dataset.plan_id=8ab812c3621cc3dae68989d9f24134351ea9683453133b31feaff579d0fa3e7f
data_eng_lab.dataset.publication_id=e53a481df5d54c6eabc645838fb2f2ba
data_eng_lab.dataset.manifest_sha256=998ec39bc61dca1b460e4b851d718a5347b8c7e575b96dd1e3ec62fd0b791678
```

The second run reproduced both complete logical table snapshots and deterministic checksums while
advancing each Iceberg snapshot ID, proving convergent same-generation recovery and idempotent
replacement. The harness also proved sessionization reread and matched all five `gh_events`
properties before its source-table read.

## Diagnostic corrections and safe cleanup

The first published-source replay discovered the canonical exact duplicate. The implementation and
contract were corrected under RED tests to preserve identical duplicate rows, reject conflicting
duplicate IDs before writing, and compare deterministic multiplicity-aware event/session multisets.
Subsequent replays exposed two live-test assumptions rather than production defects: PyIceberg
reports Iceberg timestamps with timezone as `timestamptz`, and null predecessors equal distinct
actors while total session starts can be greater. Both assertions were narrowed under focused RED
tests before the unchanged canonical replay passed:

```bash
RUN_INFRA=1 uv run --group live pytest \
  tests/scenarios/test_gh_archive_pipeline_live.py -v -s
# 1 passed in 425.50s
```

The final `scripts/stop-all.sh` cleanup preserved every volume, restored the DAG pause state, and
left zero project containers in `docker ps --all`. Direct concurrent JAR invocation remains
unsupported; serialized Airflow execution is the production boundary and same-generation rerun is
the supported recovery path.
