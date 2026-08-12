# Changelog

All notable changes to this project are documented here (Keep a Changelog format).

## [Unreleased]
### Added
- Production TPC-H star-schema Spark application, daily Airflow DAG, Jenkins publication, table-level source provenance, and repeatable live acceptance for `dim_customer` and `fct_orders`.
- Review hardening for that path: serialized Airflow runs, positive resolver object sizes, an exact
  downstream provenance preflight, a real opt-in live lifecycle, and an explicit notebook trust boundary.
- Phase 0: repository foundation, Atlas submodule, launch harness, base tooling,
  verifier skeleton, and infra-preflight Layer 1.
