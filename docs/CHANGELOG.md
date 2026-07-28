# Changelog

All notable changes to this project are documented here (Keep a Changelog format).

## [Unreleased]
### Added
- Phase 0: repository foundation, Atlas submodule, launch harness, base tooling,
  verifier skeleton, and infra-preflight Layer 1.

### Changed
- Atlas consumer operations now pin `af7713ee` (superseding the prior
  `881df596` reviewed/live-gate baseline); changed committed Atlas source
  automatically rebuilds local images using ignored `.atlas-build-state`, and
  the launcher exports/asserts only `ATLAS_MINIO_HOST_ENDPOINT`. Atlas #791's
  in-network Airflow Execution API configuration and Atlas #850's shared JWT
  repair are present. The focused DAG retest remains pending; the separate #792
  SparkSubmitHook status-poll caveat remains documented.
- Batch NYC Taxi notebook ingestion now normalizes `passenger_count` to `double`
  per declared Parquet object before unioning, so the January–June 2023 input
  set retains Scala/PySpark parity despite March's `INT64` schema.
- Atlas consumption modernized: pin bumped `85ff46b2` → `2d006cae` (v0.1.0-587);
  adopted the `atlas.consumer.yml` consumer manifest (replaces the `_user/`
  symlink, `.env` injection, wrapper source flags, and `create_buckets.sh`);
  unwound the #309–#311 go-live workarounds fixed upstream (#308 remains caveated —
  see docs/atlas-feedback-go-live.md).
