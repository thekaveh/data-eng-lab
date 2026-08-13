# NYC Taxi Data Quality Live Acceptance

Status: pending canonical `RUN_INFRA=1` replay.

The tracked executable source of truth is
`tests/scenarios/test_nyc_taxi_data_quality_live.py`. It requires an existing verified tiny NYC
Taxi publication and fails closed without refreshing or mutating the dataset pointer. It requires
exclusive ownership of a stopped project stack, keeps both daily DAGs paused during controlled
manual acceptance, restores their initial pause states, and stops only its owned stack without
removing volumes.

Prerequisite and acceptance commands:

```bash
uv run python scripts/download_datasets.py --scale tiny --only nyc_taxi --verify-only
uv run python scripts/resolve_dataset.py nyc_taxi --scale tiny
RUN_INFRA=1 uv run pytest tests/scenarios/test_nyc_taxi_data_quality_live.py -vv -s
```

If the verified publication is intentionally absent, an operator may provision it separately with
the supported bounded command below and must then run verify-only before acceptance. The harness
never performs this operation itself.

```bash
uv run python scripts/download_datasets.py --scale tiny --only nyc_taxi --refresh
uv run python scripts/download_datasets.py --scale tiny --only nyc_taxi --verify-only
```

## Prerequisite recovery evidence

The pointer was confirmed absent (`NoSuchKey`) before the authorized bounded refresh. The original
refresh failed closed on the Arrow Parquet metadata verifier; the separately reviewed recovery is
recorded in `2026-08-13-parquet-schema-normalization-blocker.md`. After both independent reviews
returned Critical 0 / Important 0 / Minor 0 and Ready Yes, the identical supported command
published and verify-only accepted:

- plan `66929ee59188f5a2deb8e29e8593fbe9bad1ad6dc1c4daadd7aeb45b51916189`;
- publication `16e280e900a84d1b9d617743472b8ada`;
- manifest SHA-256 `3b678261e704aeb6dee3ae981d699bf81db5696fa9da64abfbce6fe2bd7f6c12`;
- one 47,673,370-byte object with SHA-256
  `32df6f67578fa86c484a6b5ef23a5281992ff085521082340b0f9e5889e9a572`; and
- canonical `s3://landing/nyc_taxi/_generations/<plan>/<publication>/yellow_tripdata_2023-01.parquet`.

Registry and lock hashes were unchanged. No legacy key or volume was deleted, and the provisioning
stack stopped with zero project containers.

## First acceptance replay and corrected scale binding

The first replay failed before any Spark write or quality DagRun. The matching ETL task made two
resolver requests for `small` and received the resolver's redacted HTTP 500
`{"error":"dataset resolution failed"}`. Direct health and tiny resolution succeeded from the
resolver, Airflow scheduler, and Jupyter containers, while a direct small request reproduced the
same 500. Host and image hashes for publication, registry, resolver, S3, schema, verification, and
registry YAML matched exactly; the resolver image was the freshly rebuilt reviewed image.

Root cause: the acceptance harness omitted Airflow's `--conf` argument, so the production ETL
correctly fell back to the scheduler's `DATASET_SCALE=small` instead of the verified tiny
prerequisite. The harness now passes exact bounded canonical JSON
`--conf '{"dataset_scale":"tiny"}'` to every ETL and quality test invocation, proves the real ETL
`_effective_scale` path selects it over the environment, and rejects any created DagRun whose
stored conf is not exactly that mapping. Production DAG code is unchanged. RED was two command
contract failures; GREEN is eleven offline harness tests with one expected live skip.

Exact artifact, run, driver, snapshot, fact, query, pointer, and teardown evidence will be appended
only after the canonical replay succeeds.
