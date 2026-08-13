# Changelog

All notable changes to this project are documented here (Keep a Changelog format).

## [Unreleased]
### Added
- Production GH Archive flatten and sessionization Spark stages, one serialized daily Airflow DAG,
  Jenkins publication, strict immutable resolver handoff, equal five-key table provenance, and a
  repeatable live gate proving source-to-events-to-sessions multiset conservation.
- Production MovieLens feature Spark application, serialized daily Airflow DAG, Jenkins publication,
  equal five-key table provenance, deterministic two-table recovery, and repeatable live acceptance
  for `ml_user_features` and `ml_movie_features`.
- Production TPC-H star-schema Spark application, daily Airflow DAG, Jenkins publication, table-level source provenance, and repeatable live acceptance for `dim_customer` and `fct_orders`.
- Review hardening for that path: serialized Airflow runs, positive resolver object sizes, an exact
  downstream provenance preflight, a real opt-in live lifecycle, and an explicit notebook trust boundary.
- Phase 0: repository foundation, Atlas submodule, launch harness, base tooling,
  verifier skeleton, and infra-preflight Layer 1.
