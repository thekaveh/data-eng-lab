# MovieLens Feature-Pipeline Live Acceptance

**Issue:** #108

**Date:** 2026-08-12

**Result:** PASS after one test-first production readback correction

## Accepted artifact and immutable input

- Local and published JAR: `movielens-feature-pipeline/0.1.0/app.jar`
- JAR SHA-256: `6acd7ff2c00b806217bb225a31435fd6f56eec7ac2e117e9a9108b05f6b94568`
- Dataset/scale: `movielens` / `tiny`
- Plan ID: `a2942463086277225d704e94f2dbad83b96cdb921bf43cfa8e18268dec393ef8`
- Publication ID: `12bfd87494fa4c3785732e3d48a18085`
- Manifest SHA-256: `e348c4b3c0a74d4923fc060b5cf202f0f5f1edea03a5f92709baadb61ae4ffc1`

The acceptance harness resolved the already verified pointer, ran `--verify-only`, and resolved it
again. It never called `--refresh`, inferred absence from a generic resolver failure, or mutated the
MovieLens pointer. The exact accepted registry inventory was:

| Object | Schema ID | Bytes | SHA-256 |
|---|---|---:|---|
| `links.csv` | `movielens_latest_small_links` | 197979 | `97ad18e4e56a09363c65676b6cb3482ce3e2cea2372a24620c1599c843325f31` |
| `tags.csv` | `movielens_latest_small_tags` | 118660 | `92a9f8bb7916dceef6151209845788c3643f794dfa79d1feaec7121b5960399d` |
| `ratings.csv` | `movielens_latest_small_ratings` | 2483723 | `aa289ca83157595d0df6aea1be6a4ded676ddc4385472e8313a8ed9805352646` |
| `README.txt` | `movielens_latest_small_readme` | 8342 | `63d22ac138e80fae37021797ed8e1b9424b5239dc576a137422adf783ae3f404` |
| `movies.csv` | `movielens_latest_small_movies` | 494431 | `5a5f32dd9bb3797b8e728a1b98958789d2b13f294a69fdfbc5727f8a9611aa07` |

## Real orchestration evidence

The project had zero running, stopped, exited, or created containers before each controlled run.
The harness started an exclusively owned stack, kept `movielens_feature_pipeline` paused throughout,
and restored its initial paused state (`true`). It used complete bounded Airflow-v2 DagRun
pagination and `airflow dags test --use-executor` with unique whole-second logical dates.

| Run | Logical/start time | End | Airflow | Spark driver | REST terminal |
|---|---|---|---|---|---|
| `manual__2026-08-12T21:59:40.741979+00:00` | `2026-08-12T21:59:08Z` | `2026-08-12T22:00:32.608186Z` | `success` | `driver-20260812215946-0000` | `FINISHED`, `success=true` |
| `manual__2026-08-12T22:00:38.976246+00:00` | `2026-08-12T22:00:36Z` | `2026-08-12T22:01:28.698005Z` | `success` | `driver-20260812220042-0001` | `FINISHED`, `success=true` |

The API inventory gained exactly one unique run after each command. There was no third acceptance
run and no unexpected queued/running run before teardown. The second run started after the first
ended, exercising the production serialization contract.

## Table, measure, provenance, and rerun evidence

Both runs produced the same logical snapshots:

| Table | Exact columns/types | Rows | Deterministic checksum |
|---|---|---:|---|
| `lakehouse.gold.ml_user_features` | `userId:long`, `avg_rating:double`, `num_ratings:long` | 610 | `377574ef54523af2` |
| `lakehouse.gold.ml_movie_features` | `movieId:long`, `movie_avg:double`, `popularity:long` | 9724 | `4c87d628b90fe38e` |

Meaningful measures:

- `sum(num_ratings) = 100836`
- `sum(popularity) = 100836`
- user average range: `1.275` to `5.0`
- movie average range: `0.5` to `5.0`

Both `$properties` tables contained the exact same five values:

```text
data_eng_lab.dataset=movielens
data_eng_lab.dataset.scale=tiny
data_eng_lab.dataset.plan_id=a2942463086277225d704e94f2dbad83b96cdb921bf43cfa8e18268dec393ef8
data_eng_lab.dataset.publication_id=12bfd87494fa4c3785732e3d48a18085
data_eng_lab.dataset.manifest_sha256=e348c4b3c0a74d4923fc060b5cf202f0f5f1edea03a5f92709baadb61ae4ffc1
```

The first successful run also recovered the preserved partial state from the preceding diagnostic
attempt by replacing both tables with the same generation and passing schema, row-key, row-count,
row-equality, and provenance readback. The accepted rerun then proved unchanged logical results.

## Diagnostic RED and correction

The initial canonical attempt submitted `driver-20260812215239-0000` and correctly failed Airflow
after writing both tables because Iceberg readback relaxed Spark aggregate-column nullability
metadata. The application had compared complete `StructType` objects, even though the contract's
null behavior is enforced against actual rows. Driver stderr identified:

```text
IllegalStateException: lakehouse.gold.ml_user_features readback validation failed
Caused by: IllegalArgumentException: userId features have the wrong schema
```

A focused Scala regression reproduced this catalog-nullability boundary (RED: 1 of 10 tests), then
the implementation was narrowed to compare exact ordered column names/types while retaining explicit
non-null row, unique-key, finite-average, positive-count, and count-equality checks (GREEN: 10 of 10).
The corrected canonical replay is the two-run evidence above.

## Replay commands

These commands are replayable after the explicit verified-tiny prerequisite is satisfied. Secrets
remain in the ignored environment files and are not printed.

```bash
# Optional intentional provisioning, separate from acceptance:
uv run python scripts/download_datasets.py --scale tiny --only movielens --refresh
uv run python scripts/download_datasets.py --scale tiny --only movielens --verify-only

# Normal acceptance requires that verified pointer already to exist:
uv run python scripts/resolve_dataset.py movielens --scale tiny
mvn -q -B -f spark-apps/movielens-feature-pipeline/pom.xml test
mvn -q -B -f spark-apps/movielens-feature-pipeline/pom.xml package
RUN_INFRA=1 uv run pytest tests/scenarios/test_movielens_feature_pipeline_live.py -vv -s
```

The harness packages and SHA-verifies the published JAR, starts/stops only its owned stack, performs
the two paused-DAG executions, checks Spark REST and Trino, restores pause state, and uses
`./scripts/stop-all.sh`. Final state: zero project containers; volumes and the verified MovieLens
pointer are preserved.
