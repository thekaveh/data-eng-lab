# 9. Changelog

All notable changes to this project are documented here (Keep a Changelog format).

## 1. [Unreleased]
### Added
- NYC Taxi data-quality now runs as the serialized `nyc_taxi_data_quality` production DAG after a successful matching ETL logical date. The snapshot-bound Spark application conserves every Bronze row across clean and quarantine, idempotently records eight governed Gold facts, and exposes three fixed Trino dashboard queries. Canonical live acceptance proved same-date recovery, terminal Spark drivers, deterministic dashboards, unchanged source pointer, and volume-preserving cleanup.
- The two Trino scenarios now run as staggered, serialized, read-only production DAGs through a
  bounded same-origin statement-protocol client. TPC-H fails closed on exact five-key provenance;
  NYC is explicitly snapshot-bound. Both return canonical metadata-DB XCom artifacts, preserve
  Iceberg/pointer state, and produced deterministic results across two live reruns with no Spark
  driver delta.
- The GH Archive flatten and sessionization scenarios now share a serialized daily production DAG
  and Jenkins-published Scala application. Two live tiny-scale Airflow runs preserved all 101,917
  source rows through `gh_events` and `gh_sessions`, including one exact duplicate, with four
  REST-confirmed Spark drivers, stable logical checksums, advancing snapshots, and equal five-key
  provenance on both tables. The paired notebooks remain educational writers without production
  provenance or serialization.
- Phase 0: repository foundation, Atlas submodule, launch harness, base tooling,
  verifier skeleton, and infra-preflight Layer 1.

### Changed
- The MovieLens feature-engineering scenario now has a daily serialized production DAG and
  Jenkins-published Scala application. Two live tiny-scale Airflow runs ended in success with Spark
  `FINISHED` and `success=true`; `ml_user_features` and `ml_movie_features` reproduced 610 and 9,724
  keyed rows, equal rating-count sums of 100,836, deterministic logical checksums, and matching
  dataset, scale, plan, publication, and manifest properties. The paired notebooks remain
  educational only because they can overwrite those tables without production provenance.
- TPC-H star-schema follow-up now serializes non-atomic replacements, checks positive resolver object
  sizes, gives downstream #83 an exact five-property fail-closed preflight, warns that notebooks are
  not production writers, and replaces report-string acceptance with an executable live lifecycle.
- The TPC-H star-schema scenario now has a daily operator-owned production DAG and Jenkins-published Scala application. Two live tiny-scale Airflow runs ended in success with Spark `FINISHED` and `success=true`; `dim_customer` and `fct_orders` reproduced identical logical checksums and carry matching scale, plan, publication, and manifest properties for downstream Trino generation checks.
- All 19 scenarios now have one tested execution-mode contract. Eight current production DAGs
  cover nine scenarios: `nyc_taxi_etl`, `nyc_taxi_medallion`, `nyc_taxi_data_quality`, `tpch_star_schema`,
  `movielens_feature_pipeline`, `gh_archive_flatten_sessionization`, `tpch_bi_query`, and
  `nyc_taxi_trino_daily`; seven remain intentionally
  notebook-only, and three remain unscheduled continuous streams. The 19
  scenario-local no-op DAGs were removed so Airflow and public
  documentation cannot report successful no-op work.
- Scenario documentation now distinguishes educational notebook behavior from
  production trust boundaries. Data quality documents its exact Bronze snapshot, null-safe
  Silver split, governed Gold facts, and dashboard queries; maintenance and time-travel pages describe
  only their executable cells. All 19 scenario diagram masters use portable
  ASCII visible text so committed PNG and wiki projections do not depend on
  CairoSVG finding optional Unicode glyphs.
- The dataset catalog now uses registry version 2 with normalized source,
  raw/archive, landing-object, generator-output, and schema locks for every
  supported tier. The reviewed contract records authoritative provenance,
  per-object SHA-256 and size identities, canonical generator inputs, and schema
  fingerprints. MovieLens now carries exact artifact-level provenance for each
  mutually exclusive release while retaining intentional scale-local flattened
  names. Dataset acquisition now verifies raw, extracted, generated, uploaded,
  reused, and consumed bytes plus physical schemas. Publications use verified
  immutable generations, content-addressed manifest history, and one conditional
  active-pointer switch; runtime consumers require an expected scale and retain
  one resolver result per run.
- Atlas is now pinned at `c6cf73d7168db1a7840fc45c9ed3e385071996d8`.
  The NYC Taxi production Spark DAGs again use `SparkSubmitOperator` ownership: their
  `AtlasSparkSubmitOperator` subclass preserves provider execution and
  OpenLineage injection while wrapping `super()._get_hook()` with Atlas's
  `RestConfirmingSparkHook`. Submission remains cluster-mode through
  `spark_default` on `:7077`, and successful completion still requires the
  standalone REST record on `spark-master:6066` to report `FINISHED` and
  `success=true`.
  Current-pin acceptance promoted through PRs #95, #96, and #97 after Airflow
  runs `issue78_nyc_taxi_etl_20260810T233212Z` and
  `issue78_nyc_taxi_medallion_20260810T233242Z` succeeded. Spark REST drivers
  `driver-20260810233215-0003` and `driver-20260810233245-0004` both reached
  `FINISHED` with `success=true`; Jenkins ETL build #5 and medallion build #1
  succeeded; and preflight passed Layer 1 at 13/13 and Layer 2 at 6/6. No false
  driver-status polling failure or exception was present.
- The repository, site, and wiki now open with a wide lakehouse brand banner,
  centered project identity, and twelve icon-bearing stack badges. The detailed
  topology remains available under Architecture instead of occupying the first
  viewport.
- Public pages on all three documentation surfaces use neutral Markdown,
  document-local H2 numbering, labeled fences, and source-backed
  scenario/dataset/Atlas facts.
- Documentation publication validates every repository-local file and Markdown
  fragment, rejects manifest path aliases and projection collisions, and tracks
  each committed diagram PNG with a source/render-contract fingerprint. The site
  generates SVG projections from the HTML masters; the wiki publishes the
  reviewed committed PNG bytes without host-dependent rerendering. Maintainers
  refresh committed PNG projections explicitly when a master changes.
- Historically, on 2026-07-31, Atlas consumer operations accepted pin
  `985918ce8c805081947d53b1c48bb80610237a5b` after the representative Airflow
  feature-artifact task succeeded on its first and only attempt. Spark standalone
  REST reported `FINISHED` with `success=true`; the Bronze table contained
  `8,991,502` rows and its Iceberg `passenger_count` type was `double`. The Atlas
  consumer modernization had already completed Gitflow promotion through PRs
  #66, #67, and #68. This pin supersedes the #880 predecessor `0644a8f3`, the
  #850-corrected `882877a4` pin, and the failed-gate `af7713ee` pin while
  retaining the prior `881df596` reviewed/live-gate baseline. Committed Atlas
  source changes automatically rebuild local images using ignored `.atlas-build-state`, and
  the launcher exports/asserts only `ATLAS_MINIO_HOST_ENDPOINT`. Atlas #791's
  in-network Airflow Execution API configuration is live and validated. The
  prior `af7713ee` focused retest proved its attempted #850 patch set
  `AIRFLOW__API__JWT_SECRET` while Airflow 3.3 reads `[api_auth] jwt_secret`.
  The corrected reviewed pin closes #850 with
  `AIRFLOW__API_AUTH__JWT_SECRET`. Atlas #880 corrects the remaining #792 wrapper
  defect for the
  shipped provider: at that historical pin, both production cluster-mode DAGs constructed
  `SparkSubmitHook` without an application and called
  `submit_and_confirm_via_rest()` to submit on `:7077`, extract the driver ID
  from the spark-submit log, and verify it on `spark-master:6066` without
  masking real driver failures.
- Batch NYC Taxi notebook ingestion now normalizes `passenger_count` to `double`
  per declared Parquet object before unioning, so the January–June 2023 input
  set retains Scala/PySpark parity despite January's distinct physical schema.
- Atlas consumption modernized: pin bumped `85ff46b2` → `2d006cae` (v0.1.0-587);
  adopted the `atlas.consumer.yml` consumer manifest (replaces the `_user/`
  symlink, `.env` injection, wrapper source flags, and `create_buckets.sh`);
  unwound the #309–#311 go-live workarounds fixed upstream. #308 remained
  caveated at that historical pin and was resolved by the later #880 wrapper.
