# Changelog

All notable changes to this project are documented here (Keep a Changelog format).

## [Unreleased]
### Added
- Phase 0: repository foundation, Atlas submodule, launch harness, base tooling,
  verifier skeleton, and infra-preflight Layer 1.

### Changed
- Atlas consumption modernized: pin bumped `85ff46b2` → `2d006cae` (v0.1.0-587);
  adopted the `atlas.consumer.yml` consumer manifest (replaces the `_user/`
  symlink, `.env` injection, wrapper source flags, and `create_buckets.sh`);
  unwound the #308–#311 go-live workarounds fixed upstream.
