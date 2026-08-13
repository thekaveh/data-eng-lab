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

Exact artifact, run, driver, snapshot, fact, query, pointer, and teardown evidence will be appended
only after the canonical replay succeeds.
