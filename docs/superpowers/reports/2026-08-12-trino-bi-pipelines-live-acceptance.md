# Trino BI Pipelines Live Acceptance

- **Date:** 2026-08-12 America/New_York / 2026-08-13 UTC
- **Branch:** `codex/83-trino-bi-pipelines`
- **Result:** `1 passed in 168.85s`
- **Harness:** `tests/scenarios/test_trino_bi_pipelines_live.py`

## Replay contract

The acceptance gate requires zero `data-eng-lab` project containers in every Docker state, an
already verified tiny TPC-H publication, and the existing NYC Bronze Iceberg table. It starts and
stops only its exclusively owned Atlas stack with standard volume-preserving cleanup. It never
publishes or refreshes a dataset and never writes an Iceberg table.

```bash
RUN_INFRA=1 uv run pytest tests/scenarios/test_trino_bi_pipelines_live.py -vv -s
```

Internally, the gate used the repository lifecycle commands below. Airflow/MinIO credentials came
from `infra/.env`; the harness redacts and bounds task diagnostics and this report contains no
credential values.

```bash
./scripts/start-all.sh
uv run python scripts/resolve_dataset.py tpch --scale tiny
uv run python scripts/download_datasets.py --scale tiny --only tpch --verify-only
# four unique whole-second paused runs, two per DAG:
docker exec data-eng-lab-airflow-scheduler bash -o pipefail -c \
  'airflow dags test "$@" 2>&1 | tail -n 200' airflow-dags-test \
  <tpch_bi_query-or-nyc_taxi_trino_daily> <logical-date> --use-executor
./scripts/stop-all.sh
```

The NYC task is deliberately snapshot-bound. The harness does not call the resolver or acquisition
workflow for `nyc_taxi`. It reads `_data-eng-locks/current/nyc_taxi.json` directly as an optional
negative control: the key was explicitly absent before and after acceptance. Only `NoSuchKey` is
accepted as absence; ambiguous transport/authorization failures fail closed.

## Immutable source evidence

The TPC-H resolver and verify-only results were byte-equivalent across the acceptance boundary:

| Identity | Exact value |
|---|---|
| scale | `tiny` |
| plan | `9feb94629d99d3a813e920dc63d5df2fe87a00eac990a09f70256ea76193d8e5` |
| publication | `1cd69dac4c4444e6b346c542318e7cd4` |
| manifest | `cd11af757ca2ad9c9baa26d6058cde536eecc0a66ac01b5e73d5d3993df4539f` |

Both TPC-H `$properties` tables exposed exactly those five equal keys. The task also reconciled
fact order, line, and revenue totals and required zero unmatched customer joins. NYC was bound to
Iceberg snapshot `6090932775096319165`; its schema, source count, daily row counts, and average fares
validated before the same snapshot was reread.

## Airflow, Trino, and artifacts

Both `0 1 * * *` and `0 2 * * *` DAGs stayed paused throughout controlled execution and their
initial pause states were restored. Complete bounded `/api/v2` inventories showed exactly two new
terminal-success DagRuns per DAG and no third or unexpected active run.

| DAG | First run | Second run | Result checksum |
|---|---|---|---|
| `tpch_bi_query` | `manual__2026-08-13T03:57:06.273601+00:00` | `manual__2026-08-13T03:57:21.708990+00:00` | `8eda339e07012ba2a7bea164d2152d845d5abd3440725b749e8a4c6110fb97d9` |
| `nyc_taxi_trino_daily` | `manual__2026-08-13T03:57:16.164227+00:00` | `manual__2026-08-13T03:57:27.184079+00:00` | `d500d6e3954bddbc6febb109004b4889de60dfbbb4c2009e884c2d0e49610969` |

The exact Trino query IDs were:

- TPC-H first: `20260813_035709_00003_dd37g`, `20260813_035709_00004_dd37g`,
  `20260813_035709_00005_dd37g`, `20260813_035709_00006_dd37g`,
  `20260813_035710_00007_dd37g`, `20260813_035710_00008_dd37g`,
  `20260813_035710_00009_dd37g`.
- TPC-H second: `20260813_035722_00015_dd37g`, `20260813_035722_00016_dd37g`,
  `20260813_035722_00017_dd37g`, `20260813_035722_00018_dd37g`,
  `20260813_035722_00019_dd37g`, `20260813_035722_00020_dd37g`,
  `20260813_035722_00021_dd37g`.
- NYC first: `20260813_035716_00010_dd37g`, `20260813_035716_00011_dd37g`,
  `20260813_035716_00012_dd37g`, `20260813_035716_00013_dd37g`,
  `20260813_035717_00014_dd37g`.
- NYC second: `20260813_035727_00022_dd37g`, `20260813_035727_00023_dd37g`,
  `20260813_035727_00024_dd37g`, `20260813_035727_00025_dd37g`,
  `20260813_035727_00026_dd37g`.

Each task returned a typed, bounded metadata-DB XCom only after all pre/post checks. The reruns had
different query IDs but byte-identical canonical `columns`/`rows` payloads and equal checksums.
Decimals were canonical strings and dates ISO strings; no endpoint, user, header, SQL, credential,
or response body was present in an artifact.

## Read-only and teardown evidence

Before and after state was exactly equal:

| Table | Iceberg snapshot ID |
|---|---:|
| `lakehouse.gold.dim_customer` | `4472638688582451133` |
| `lakehouse.gold.fct_orders` | `3291191197297397256` |
| `lakehouse.bronze.nyc_taxi_trips` | `6090932775096319165` |

TPC-H table properties and both raw pointer controls were unchanged. The Spark master driver set had
an exact empty delta, proving the Airflow tasks used Trino rather than Spark. Standard
`scripts/stop-all.sh` cleanup preserved volumes, and the final all-state Docker query returned zero
`data-eng-lab` project containers.

## Recovery observations

Earlier isolated attempts failed closed before accepted XCom: first on the mounted package namespace,
then on the absent NYC raw pointer precondition that was corrected to the approved snapshot-bound
contract, and finally on Trino 482's exact declared type spellings. Every attempt used standard
volume-preserving teardown and left zero project containers. No failed attempt wrote an Iceberg
table, changed an input snapshot/property, or mutated a pointer.
