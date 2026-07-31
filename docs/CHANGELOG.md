# Changelog

All notable changes to this project are documented here (Keep a Changelog format).

## [Unreleased]
### Added
- Phase 0: repository foundation, Atlas submodule, launch harness, base tooling,
  verifier skeleton, and infra-preflight Layer 1.

### Changed
- Atlas consumer operations now pin `985918ce` (superseding the #880-predecessor
  `0644a8f3`, the #850-corrected
  `882877a4` pin and the failed-gate
  `af7713ee` pin and retaining the prior `881df596` reviewed/live-gate baseline);
  changed committed Atlas source
  automatically rebuilds local images using ignored `.atlas-build-state`, and
  the launcher exports/asserts only `ATLAS_MINIO_HOST_ENDPOINT`. Atlas #791's
  in-network Airflow Execution API configuration is live and validated. The
  prior `af7713ee` focused retest proved its attempted #850 patch set
  `AIRFLOW__API__JWT_SECRET` while Airflow 3.3 reads `[api_auth] jwt_secret`.
  The corrected reviewed pin closes #850 with
  `AIRFLOW__API_AUTH__JWT_SECRET`; no Airflow DAG success or promotion is
  claimed until its live retest. Atlas #880 corrects the remaining #792 wrapper
  defect for the
  shipped provider: both production cluster-mode DAGs construct
  `SparkSubmitHook` without an application and call
  `submit_and_confirm_via_rest()` to submit on `:7077`, extract the driver ID
  from the spark-submit log, and verify it on `spark-master:6066` without
  masking real driver failures.
- Batch NYC Taxi notebook ingestion now normalizes `passenger_count` to `double`
  per declared Parquet object before unioning, so the January–June 2023 input
  set retains Scala/PySpark parity despite March's `INT64` schema.
- Atlas consumption modernized: pin bumped `85ff46b2` → `2d006cae` (v0.1.0-587);
  adopted the `atlas.consumer.yml` consumer manifest (replaces the `_user/`
  symlink, `.env` injection, wrapper source flags, and `create_buckets.sh`);
  unwound the #309–#311 go-live workarounds fixed upstream (#308 remains caveated —
  see docs/atlas-feedback-go-live.md).
