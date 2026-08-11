# 4. Datasets

`data-eng-lab` lands five curated datasets in MinIO's `landing` bucket. The registry version 2 contract turns their catalog into a reviewable provenance lock: it identifies authoritative sources or generator inputs, the accepted bytes and schemas, and the `tiny`, `small`, and `medium` tier membership from one declarative file.

## 1. Registry

`datasets/registry.yaml` is the single source of truth for the following contract:

| Property | Description |
|---|---|
| `version` and `lock` | Registry version 2 plus SHA-256 source/object drift, schema-fingerprint, and reviewed-update policy |
| `format`, `license`, `landing_prefix`, `fetch` | Dataset identity, landing location, and either `http` or `tpch` acquisition |
| `provenance` | Authoritative publisher, homepage, license, attribution, source stability, and update policy |
| `schemas` | Ordered physical field contracts in `exact` or `minimum` mode; each schema fingerprint is SHA-256 over canonical JSON |
| `artifacts` | The normalized `artifacts` catalog for HTTP source versions, raw bytes, and landing outputs |
| `scales` | HTTP `scales.<tier>.artifacts` references; tiers reuse normalized artifacts instead of duplicating their lock metadata |
| `generator` | TPC-H engine, extension, environment, command, export settings, tier scale factors, and output locks |

For an HTTP source, every artifact records an authoritative URL and revision or publication identity. Its raw archive or direct file has an exact byte size and SHA-256 digest. Every extracted landing object has its own object name, size, SHA-256, and schema reference; direct downloads explicitly record that the landing bytes equal the raw bytes.

TPC-H is locked as a generator rather than an HTTP artifact. The canonical environment uses DuckDB 1.5.4 on `linux/amd64`, an immutable base-image digest, wheel and extension digests, the repository `uv.lock` digest, `C.UTF-8`, UTC, `threads=1`, and `preserve_insertion_order=true`. Each tier selects a scale factor and eight outputs with stable table ordering, Zstandard Parquet compression, 100,000-row groups, exact sizes, SHA-256 digests, and schema references.

The strict fail-on-drift policy accepts one size, digest, and schema identity for each locked item. Mutable sources are not exempt: changed upstream bytes require a reviewed lock update after provenance, licensing, schema, and downstream compatibility are reconsidered.

Issue #80 defines the contract boundary. The lock is defined, validated, and parsed, but it does not verify downloaded, extracted, generated, uploaded, or reused bytes. Contract runtime enforcement is tracked in issue #81; until that work lands, neither a successful download nor an existing MinIO object is a runtime verification claim.

## 2. Current Datasets

| Dataset | Shape | Format | Fetch | Scenarios |
|---|---|---|---|---|
| `nyc_taxi` | Columnar analytical | Parquet | HTTP direct | `batch_ingest-nyc_taxi-spark-iceberg`, `data_quality-nyc_taxi-spark-iceberg`, `medallion-nyc_taxi-spark-iceberg`, `federated_query-nyc_taxi-trino-iceberg`, `table_maintenance-nyc_taxi-spark-iceberg`, `time_travel-nyc_taxi-spark-iceberg` |
| `gh_archive` | Semi-structured events | Gzipped JSON Lines | HTTP direct | `json_flatten-gh_archive-spark-iceberg`, `schema_evolution-gh_archive-spark-iceberg`, `sessionization-gh_archive-spark-iceberg`, `streaming_ingest-gh_archive-spark-iceberg` (file source) |
| `movielens` | Rating and join data | CSV members | HTTP ZIP | `feature_engineering-movielens-spark-iceberg` |
| `online_retail` | Transactional retail invoices | XLSX workbook | HTTP ZIP | `incremental_upsert-online_retail-spark-iceberg`, `scd2-online_retail-spark-iceberg`, `cdc_streaming-online_retail-spark-iceberg` |
| `tpch` | Benchmark star schema | Parquet | DuckDB generator | `star_schema-tpch-spark-iceberg`, `join_optimization-tpch-spark-iceberg`, `bi_query-tpch-trino-iceberg` |

## 3. Adding a Dataset

1. Establish the authoritative publisher, source identity, license, attribution, and source-stability classification.
2. Add `provenance` and complete `schemas` entries to `datasets/registry.yaml`.
3. For HTTP data, add each source once under `artifacts`, lock its raw bytes and landing outputs, then reference artifact identifiers from each `scales` tier. For generated data, add the complete `generator` environment and per-tier output locks.
4. Add a fetch implementation only when the source is neither the existing `http` nor `tpch` kind.
5. Audit the source or generator with the reviewed procedure below, validate the registry, and run all dataset and documentation gates.
6. Add the scenario folder and paired notebooks that consume the landing objects.

## 4. Reviewed Evidence and Source Realities

The issue #80 review acquired 15 unique HTTP artifacts and recorded 25 HTTP landing objects. The canonical TPC-H runs produced 24 TPC-H outputs across three tiers plus 24 byte-identical repeat outputs with networking disabled. Temporary source and generated bytes remained outside the repository.

- **NYC Taxi:** January 2023 is the physical-schema outlier. Its `VendorID`, pickup-location ID, and drop-off-location ID fields are 64-bit; `passenger_count` is `float64`; and `airport_fee` is lowercase. February through June use `int64` for `passenger_count`, use the other recorded widths, and capitalize `Airport_fee`. The registry therefore assigns January and February-through-June separate exact schemas.
- **GH Archive:** the minimum JSON contract locks the fields consumed by the scenarios while permitting additional event fields. Exact artifact digests still detect any byte change.
- **Online Retail II:** the archive contains one `online_retail_II.xlsx` workbook, not CSV. Its two exact sheets are `Year 2009-2010` and `Year 2010-2011`, both with the locked header and nullability contract.
- **MovieLens:** `ml-latest-small.zip` is a mutable alias and `latest-small` is not a source revision. Its bundled README supplies publication identity `2018-09-26`. Latest-small and 25M have distinct bundled usage terms, so release-specific terms control rather than a shared label being treated as permission for every archive.
- **TPC-H:** all three scales use the same eight complete Parquet schemas and the same locked generator environment. Reference runs load the preverified extension offline; they do not install it at runtime.

## 5. Authoritative Sources, Licenses, and Attribution

| Dataset | Publisher and source | License or terms | Attribution |
|---|---|---|---|
| NYC Taxi | New York City Taxi and Limousine Commission — [trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | [NYC Open Data Terms of Use (unrestricted open-data use, no warranty)](https://opendata.cityofnewyork.us/overview/) | NYC Taxi and Limousine Commission; source data supplied by authorized technology providers |
| GH Archive | GH Archive (Ilya Grigorik), archiving GitHub Events API public events — [GH Archive](https://www.gharchive.org/) | [GitHub Terms of Service and licenses attached to underlying public content](https://docs.github.com/en/site-policy/github-terms/github-terms-of-service) | GH Archive and GitHub event authors/repositories |
| MovieLens | GroupLens Research, University of Minnesota — [MovieLens datasets](https://grouplens.org/datasets/movielens/) | [MovieLens usage license (research use; attribution; no redistribution without permission; no commercial use without permission)](https://files.grouplens.org/datasets/movielens/ml-25m-README.html) | F. Maxwell Harper and Joseph A. Konstan, The MovieLens Datasets: History and Context (2015), plus GroupLens Research |
| Online Retail II | UCI Machine Learning Repository; creator Daqing Chen — [dataset 502](https://archive.ics.uci.edu/dataset/502/online%2Bretail%2Bii) | [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/) | Chen, D. (2012). Online Retail II [Dataset]. UCI Machine Learning Repository. DOI 10.24432/C5CG6D |
| TPC-H | Transaction Processing Performance Council — [TPC-H](https://www.tpc.org/tpch/) | [TPC-H specification](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) | Transaction Processing Performance Council |

MovieLens terms must be read per release. The [latest-small README](https://files.grouplens.org/datasets/movielens/ml-latest-small-README.html) permits redistribution only under its stated conditions. The [25M README](https://files.grouplens.org/datasets/movielens/ml-25m-README.html) states that redistribution requires separate permission. The registry uses the conservative 25M terms at dataset level without extending those terms into a broader permission claim.

## 6. Reviewed Lock Update

A reviewed lock update is deliberate and never an automatic rewrite of `datasets/registry.yaml`:

1. Identify the upstream revision or publication date and review the authoritative license and attribution.
2. Acquire the bytes from the authoritative HTTPS source, or build the canonical generator image from its immutable inputs.
3. Calculate and inspect raw, extracted, and generated sizes and SHA-256 digests in an owned temporary directory.
4. Derive canonical schema contracts and recalculate each schema fingerprint.
5. Review downstream compatibility, then edit the registry in a dedicated change.
6. Run focused validation, the repository verifier, and all documentation gates before review.

Audit a direct HTTP source into a candidate file outside the repository:

```bash
uv run python scripts/audit_dataset_lock.py http --url https://example.org/path/artifact.parquet --output /private/tmp/dataset-lock-candidate.yaml
```

Add `--archive` for a ZIP source. The command emits candidate metadata only: it neither changes the registry nor uploads to MinIO.

Build and run the canonical TPC-H exporter with networking disabled during generation:

```bash
docker build --platform linux/amd64 -f datasets/tpch-lock.Dockerfile -t data-eng-lab-tpch-lock:1.5.4 .
docker run --rm --network=none --platform linux/amd64 -v /private/tmp/data-eng-lab-dataset-lock-review:/out data-eng-lab-tpch-lock:1.5.4 --scale 0.01 --output-dir /out/tiny --metadata /out/tiny.yaml
```

Generate each scale again into separate destinations and compare every size and digest before proposing registry edits. Then run the offline and documentation gates:

```bash
uv run pytest tests/datasets -q
uv run python scripts/verify_repo.py --root .
make docs-check
make docs-wiki
```

## 7. Usage

```bash
make up                    # boot the Atlas data-eng track; MinIO must be running
make datasets              # land the small tier
make datasets SCALE=tiny   # CI-sized subset
make datasets SCALE=medium # more data; heavier queries
uv run python scripts/download_datasets.py --scale medium --only nyc_taxi
uv run python scripts/download_datasets.py --dry-run
```

The downloader reads MinIO credentials and the published S3 port from `infra/.env`. It currently skips an existing object unless `--force` is specified, but that existence check is not digest, size, schema, or provenance verification; issue #81 owns fail-before-use enforcement.

## 8. Related Scenarios by Dataset

### NYC Taxi (`nyc_taxi`)
- [batch_ingest-nyc_taxi-spark-iceberg](scenarios/batch_ingest-nyc_taxi-spark-iceberg.md)
- [medallion-nyc_taxi-spark-iceberg](scenarios/medallion-nyc_taxi-spark-iceberg.md)
- [data_quality-nyc_taxi-spark-iceberg](scenarios/data_quality-nyc_taxi-spark-iceberg.md)
- [time_travel-nyc_taxi-spark-iceberg](scenarios/time_travel-nyc_taxi-spark-iceberg.md)
- [table_maintenance-nyc_taxi-spark-iceberg](scenarios/table_maintenance-nyc_taxi-spark-iceberg.md)
- [federated_query-nyc_taxi-trino-iceberg](scenarios/federated_query-nyc_taxi-trino-iceberg.md)

### TPC-H (`tpch`)
- [star_schema-tpch-spark-iceberg](scenarios/star_schema-tpch-spark-iceberg.md)
- [join_optimization-tpch-spark-iceberg](scenarios/join_optimization-tpch-spark-iceberg.md)
- [bi_query-tpch-trino-iceberg](scenarios/bi_query-tpch-trino-iceberg.md)

### MovieLens (`movielens`)
- [feature_engineering-movielens-spark-iceberg](scenarios/feature_engineering-movielens-spark-iceberg.md)

### Online Retail (`online_retail`)
- [incremental_upsert-online_retail-spark-iceberg](scenarios/incremental_upsert-online_retail-spark-iceberg.md)
- [scd2-online_retail-spark-iceberg](scenarios/scd2-online_retail-spark-iceberg.md)
- [cdc_streaming-online_retail-spark-iceberg](scenarios/cdc_streaming-online_retail-spark-iceberg.md)

### GitHub Archive (`gh_archive`)
- [streaming_ingest-gh_archive-spark-iceberg](scenarios/streaming_ingest-gh_archive-spark-iceberg.md)
- [schema_evolution-gh_archive-spark-iceberg](scenarios/schema_evolution-gh_archive-spark-iceberg.md)
- [json_flatten-gh_archive-spark-iceberg](scenarios/json_flatten-gh_archive-spark-iceberg.md)
- [sessionization-gh_archive-spark-iceberg](scenarios/sessionization-gh_archive-spark-iceberg.md)

### Synthetic Events (producer-generated)
- [streaming_ingest-events-spark-iceberg](scenarios/streaming_ingest-events-spark-iceberg.md)
- [streaming_windows-events-spark-iceberg](scenarios/streaming_windows-events-spark-iceberg.md)

## 9. See Also

- [Scenario Catalog](scenarios/index.md)
- [Lakehouse Architecture](lakehouse.md)
