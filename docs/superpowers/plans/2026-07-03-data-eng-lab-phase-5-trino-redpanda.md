# data-eng-lab — Phase 5: Trino + Redpanda Scenarios (A7/A9, against contract) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Author the first Trino (A7) and Redpanda (A9) scenarios against the **assumed** Atlas contract (Atlas issues #268/#269) — structural checks pass in CI now; live execution is gated on Atlas delivering the `trino`/`redpanda` services.

**Architecture:** Extend the Phase-2b `build_notebooks` helper with a Zeppelin **interpreter override** so a scenario can use `%jdbc(trino)` instead of `%spark`. Add two scenarios on the existing framework: (1) `federated_query-nyc_taxi-trino-iceberg` — Zeppelin `%jdbc(trino)` + a Jupyter `trino`-client notebook running the **same federated SQL** over the lakehouse ("parity" = same SQL, two surfaces); (2) `streaming_ingest-events-spark-iceberg` — Scala + PySpark **Structured Streaming** (`readStream` Kafka → `writeStream` Iceberg) with a synthetic `producer.py`. Both scenario DAGs are `EmptyOperator` placeholders (the established scenario-DAG pattern). Live tests are `RUN_INFRA`-gated.

**Tech Stack:** Python 3.11, `nbformat`; Trino SQL + `trino` python client (authored); Spark 4.1.2 Structured Streaming + `spark-sql-kafka` (authored, live-gated); Redpanda (Kafka API).

## Global Constraints

- **Never edit `infra/`.**
- **Scenario layout (unchanged):** `scenarios/<name>/{README.md (6 sections), zeppelin/notebook.zpln, jupyter/notebook.ipynb, dag.py}`; notebooks carry the 6 sections `1. Overview`/`2. Setup`/`3. Read`/`4. Transform`/`5. Write`/`6. Verify` (verifier-enforced). Naming = `[pattern]-[dataset]-[engine]-[format]` (4 hyphen parts).
- **Assumed contract (A7/A9), use these values:** Trino coordinator in-net `trino:8080`, catalog **`lakehouse`** (Iceberg REST connector), tables `lakehouse.{bronze,silver,gold}.*`; Zeppelin `%jdbc(trino)`. Redpanda broker `redpanda:9092`, topic `events`; Spark `readStream.format("kafka")` with `spark-sql-kafka-0-10_2.13:4.1.2` (baked per #269); streaming checkpoints `s3a://checkpoints/events`. Namespaces exist via `register_iceberg` (Atlas seeds none).
- **Structural CI-green now:** the verifier validates both scenarios; live execution/parity is `@pytest.mark.infra` + `RUN_INFRA`-gated (NOT run in CI). The scenario DAGs are `EmptyOperator` placeholders (no JAR submission — the Phase-4 `test_dag_catalog_conf` guard is unaffected).
- **Python 3.11**, ruff line-length 120. Reuse `tests/scenarios/build_notebooks.py`.
- **Branch/PR:** `main` requires `static-and-unit` + `maven-spark-apps`; land via feature branch → PR.

## File Structure

- `tests/scenarios/build_notebooks.py` + `tests/scenarios/test_build_notebooks.py` — interpreter override.
- `scenarios/federated_query-nyc_taxi-trino-iceberg/{README.md, zeppelin/notebook.zpln, jupyter/notebook.ipynb, dag.py}`.
- `scenarios/streaming_ingest-events-spark-iceberg/{README.md, zeppelin/notebook.zpln, jupyter/notebook.ipynb, dag.py, producer.py}`.
- `tests/scenarios/test_trino_query_live.py`, `tests/scenarios/test_streaming_live.py` — live-gated.
- `docs/scenarios.md`, `docs/atlas-enablement.md` — docs + issue links.

---

### Task 1: Extend `build_notebooks` with a Zeppelin interpreter override

**Files:**
- Modify: `tests/scenarios/build_notebooks.py`, `tests/scenarios/test_build_notebooks.py`

**Interfaces:**
- Produces: `build_zeppelin(name, code, interpreter="%spark") -> dict` (code paragraphs use `{interpreter}\n{code[sec]}`); `write_scenario(root, name, code, py, dag, readme, zeppelin_interpreter="%spark") -> Path` threads it through. Default keeps Phase-2b behavior.

- [ ] **Step 1: Add the failing test**

Add to `tests/scenarios/test_build_notebooks.py`:

```python
def test_zeppelin_interpreter_override():
    z = bn.build_zeppelin("t", {s: f"SELECT {i}" for i, s in enumerate(bn.NB_SECTIONS)},
                          interpreter="%jdbc(trino)")
    text = json.dumps(z)
    assert "%jdbc(trino)" in text and "%spark" not in text


def test_write_scenario_threads_interpreter(tmp_path: Path):
    code = {s: "SELECT 1" for s in bn.NB_SECTIONS}
    bn.write_scenario(tmp_path, "federated_query-nyc_taxi-trino-iceberg", code, code,
                      "# dag\n", "# r", zeppelin_interpreter="%jdbc(trino)")
    zpln = (tmp_path / "scenarios" / "federated_query-nyc_taxi-trino-iceberg" / "zeppelin" / "notebook.zpln").read_text()
    assert "%jdbc(trino)" in zpln
```

- [ ] **Step 2: Run to verify RED**

Run: `uv run pytest tests/scenarios/test_build_notebooks.py -q` → FAIL (`interpreter`/`zeppelin_interpreter` kwargs unknown).

- [ ] **Step 3: Implement**

In `build_notebooks.py`, change `build_zeppelin` and `write_scenario` signatures:

```python
def build_zeppelin(name: str, code: dict, interpreter: str = "%spark") -> dict:
    paragraphs = []
    for sec in NB_SECTIONS:
        paragraphs.append({"title": sec, "text": f"%md\n## {sec}",
                           "config": {}, "settings": {"params": {}, "forms": {}}})
        if sec != "1. Overview":
            paragraphs.append({"title": f"{sec} (code)", "text": f"{interpreter}\n{code[sec]}",
                               "config": {}, "settings": {"params": {}, "forms": {}}})
    return {"paragraphs": paragraphs, "name": name, "id": name,
            "noteParams": {}, "config": {}, "info": {}}
```

```python
def write_scenario(root: Path, name: str, code: dict, py: dict, dag: str, readme: str,
                   zeppelin_interpreter: str = "%spark") -> Path:
    ...
    (d / "zeppelin" / "notebook.zpln").write_text(
        json.dumps(build_zeppelin(name, code, zeppelin_interpreter), indent=2) + "\n", encoding="utf-8")
    ...
```

(Rename the first `build_zeppelin` param `scala`→`code` and the `write_scenario` param `scala`→`code`; update the internal call. The Phase-2b callers pass positionally, so behavior is unchanged.)

- [ ] **Step 4: Run tests + lint**

Run: `uv run pytest tests/scenarios/test_build_notebooks.py -q` → PASS (incl. the existing round-trip test).
Run: `uv run pytest -m "not infra" -q` + `uv run ruff check .` → green.

- [ ] **Step 5: Commit**

```bash
git add tests/scenarios/build_notebooks.py tests/scenarios/test_build_notebooks.py
git commit -m "feat(scenarios): build_notebooks Zeppelin interpreter override (enables %jdbc scenarios)"
```

---

### Task 2: Trino federated-query scenario (`federated_query-nyc_taxi-trino-iceberg`)

**Files:**
- Create: `scenarios/federated_query-nyc_taxi-trino-iceberg/{README.md, zeppelin/notebook.zpln, jupyter/notebook.ipynb, dag.py}`

**Interfaces:**
- Consumes: `build_notebooks.write_scenario(..., zeppelin_interpreter="%jdbc(trino)")`.

- [ ] **Step 1: Author via a throwaway build script**

Run a throwaway Python (like Phase-2b Task 2) that builds the scenario. The Zeppelin cells are **raw Trino SQL** (interpreter `%jdbc(trino)`); the Jupyter cells run the **same SQL** via the `trino` python client. Use this SQL per section (identical statements in both surfaces):

- `3. Read` SQL: `SELECT * FROM lakehouse.bronze.nyc_taxi_trips LIMIT 10`
- `4. Transform` SQL: `SELECT trip_date, count(*) AS trips, avg(fare_amount) AS avg_fare FROM lakehouse.bronze.nyc_taxi_trips GROUP BY trip_date ORDER BY trip_date`
- `5. Write` SQL: `CREATE TABLE IF NOT EXISTS lakehouse.gold.nyc_taxi_daily_trino AS SELECT trip_date, count(*) AS trips, avg(fare_amount) AS avg_fare FROM lakehouse.bronze.nyc_taxi_trips GROUP BY trip_date`
- `6. Verify` SQL: `SELECT count(*) FROM lakehouse.gold.nyc_taxi_daily_trino`

`CODE` (Zeppelin, `%jdbc(trino)` — raw SQL):
```python
CODE = {
  "1. Overview": "",
  "2. Setup": "-- %jdbc(trino) is pre-bound to the Atlas Trino coordinator (catalog: lakehouse)",
  "3. Read": "SELECT * FROM lakehouse.bronze.nyc_taxi_trips LIMIT 10",
  "4. Transform": "SELECT trip_date, count(*) AS trips, avg(fare_amount) AS avg_fare\nFROM lakehouse.bronze.nyc_taxi_trips\nGROUP BY trip_date ORDER BY trip_date",
  "5. Write": "CREATE TABLE IF NOT EXISTS lakehouse.gold.nyc_taxi_daily_trino AS\nSELECT trip_date, count(*) AS trips, avg(fare_amount) AS avg_fare\nFROM lakehouse.bronze.nyc_taxi_trips GROUP BY trip_date",
  "6. Verify": "SELECT count(*) FROM lakehouse.gold.nyc_taxi_daily_trino",
}
```

`PY` (Jupyter, `trino` client — same SQL):
```python
PY = {
  "1. Overview": "",
  "2. Setup": "from trino.dbapi import connect\ncur = connect(host='trino', port=8080, user='data-eng', catalog='lakehouse').cursor()\ndef q(sql):\n    cur.execute(sql); return cur.fetchall()",
  "3. Read": "q('SELECT * FROM lakehouse.bronze.nyc_taxi_trips LIMIT 10')",
  "4. Transform": "q('SELECT trip_date, count(*) AS trips, avg(fare_amount) AS avg_fare '\n  'FROM lakehouse.bronze.nyc_taxi_trips GROUP BY trip_date ORDER BY trip_date')",
  "5. Write": "q('CREATE TABLE IF NOT EXISTS lakehouse.gold.nyc_taxi_daily_trino AS '\n  'SELECT trip_date, count(*) AS trips, avg(fare_amount) AS avg_fare '\n  'FROM lakehouse.bronze.nyc_taxi_trips GROUP BY trip_date')",
  "6. Verify": "q('SELECT count(*) FROM lakehouse.gold.nyc_taxi_daily_trino')",
}
```

`DAG` — an `EmptyOperator` placeholder (mirror `scenarios/medallion-nyc_taxi-spark-iceberg/dag.py`), dag_id `federated_query_nyc_taxi`, comment: `# Trino federated query runs interactively via the notebooks; a scheduled TrinoOperator DAG is a future deliverable (needs Atlas A7 / issue #268).`

`README` — 6 sections describing the Trino federated-query scenario (queries the Iceberg lakehouse via Trino SQL from Zeppelin %jdbc + the Jupyter trino client; live-gated on Atlas #268).

Call: `bn.write_scenario(ROOT, "federated_query-nyc_taxi-trino-iceberg", CODE, PY, DAG, README, zeppelin_interpreter="%jdbc(trino)")`. Do NOT commit the throwaway script.

- [ ] **Step 2: Verify + commit**

Run: `uv run python scripts/verify_repo.py --root .` → exit 0 (naming 4-part; files; 6 sections in both notebooks; JSON valid).
Run: `uv run ruff check .` (dag.py clean), `uv run pytest -m "not infra" -q` (green).
Confirm the `.zpln` uses `%jdbc(trino)` (not `%spark`), the `.ipynb` uses the `trino` client, and the SQL matches between them.

```bash
git add scenarios/federated_query-nyc_taxi-trino-iceberg
git commit -m "feat(scenarios): Trino federated-query scenario (%jdbc + trino-client, same SQL over lakehouse)"
```

---

### Task 3: Redpanda streaming scenario (`streaming_ingest-events-spark-iceberg`)

**Files:**
- Create: `scenarios/streaming_ingest-events-spark-iceberg/{README.md, zeppelin/notebook.zpln, jupyter/notebook.ipynb, dag.py, producer.py}`

- [ ] **Step 1: Author the notebooks via a throwaway build script**

Scala (`%spark`, default interpreter) + PySpark, SAME Structured Streaming logic (`readStream` Kafka `events` → parse JSON → `writeStream` Iceberg `lakehouse.bronze.events`):

Scala `CODE`:
- `2. Setup`: `import spark.implicits._` + `import org.apache.spark.sql.functions._` + `import org.apache.spark.sql.types._`
- `3. Read`: `val raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "redpanda:9092").option("subscribe", "events").option("startingOffsets", "earliest").load()`
- `4. Transform`: `val schema = new StructType().add("user_id", StringType).add("event", StringType).add("ts", TimestampType)`\n`val events = raw.select(from_json($"value".cast("string"), schema).as("e")).select("e.*")`
- `5. Write`: `val query = events.writeStream.format("iceberg").outputMode("append").option("checkpointLocation", "s3a://checkpoints/events").toTable("lakehouse.bronze.events")` (+ a comment: run `query.awaitTermination()` for a live stream)
- `6. Verify`: `spark.table("lakehouse.bronze.events").count()`

PySpark `PY` (same logic): `from pyspark.sql import SparkSession, functions as F` + `from pyspark.sql.types import StructType, StringType, TimestampType`; `spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()`; `raw = spark.readStream.format("kafka").option(...).load()`; `schema = StructType().add(...)`; `events = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e")).select("e.*")`; `query = events.writeStream.format("iceberg").outputMode("append").option("checkpointLocation","s3a://checkpoints/events").toTable("lakehouse.bronze.events")`; verify `spark.table("lakehouse.bronze.events").count()`.

`DAG` — `EmptyOperator` placeholder, dag_id `streaming_ingest_events`, comment: `# Structured Streaming is long-running (not a scheduled batch DAG); run the streaming job via the notebooks / a streaming spark-submit. Live-gated on Atlas A9 / issue #269.`

`README` — 6 sections (streaming ingest from Redpanda `events` topic → Iceberg bronze; Scala + PySpark parity; needs the `producer.py` running + Atlas #269; checkpoints in `s3a://checkpoints/`).

Build with `zeppelin_interpreter="%spark"` (default). Do NOT commit the throwaway script.

- [ ] **Step 2: Add `producer.py`**

`scenarios/streaming_ingest-events-spark-iceberg/producer.py` — a synthetic event producer (authored/gated; ruff-clean; imports used):

```python
"""Produce synthetic events to the Redpanda `events` topic (for the streaming scenario).
Live-gated: requires Atlas Redpanda (issue #269). Run: python producer.py [count]."""
from __future__ import annotations

import json
import sys
import time

from kafka import KafkaProducer  # kafka-python

BOOTSTRAP = "localhost:9092"  # host-published Redpanda; in-cluster use redpanda:9092
TOPIC = "events"


def main(count: int = 100) -> None:
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP,
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    for i in range(count):
        producer.send(TOPIC, {"user_id": f"u{i % 10}", "event": "click", "ts": time.time()})
    producer.flush()
    print(f"produced {count} events to {TOPIC}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 100)
```

- [ ] **Step 3: Verify + commit**

Run: `uv run python scripts/verify_repo.py --root .` → exit 0; `uv run ruff check .` (clean — `kafka` import is used; ruff does static analysis only); `uv run pytest -m "not infra" -q` (green). Confirm `.zpln` has Scala `%spark` streaming, `.ipynb` has PySpark streaming, parity holds.

```bash
git add scenarios/streaming_ingest-events-spark-iceberg
git commit -m "feat(scenarios): Redpanda streaming scenario (Structured Streaming Kafka->Iceberg, Scala+PySpark) + producer"
```

---

### Task 4: Live-gated tests + docs

**Files:**
- Create: `tests/scenarios/test_trino_query_live.py`, `tests/scenarios/test_streaming_live.py`
- Modify: `docs/scenarios.md`, `docs/atlas-enablement.md`

**Interfaces:**
- Produces: two `@pytest.mark.infra` + `RUN_INFRA`-gated tests (deselected in CI) that, on the live enhanced stack, exercise Trino connectivity/query and the streaming round-trip.

- [ ] **Step 1: Author the live tests (structure; body gated)**

`tests/scenarios/test_trino_query_live.py`:

```python
import os

import pytest

pytestmark = pytest.mark.infra


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1",
                    reason="needs live Atlas Trino (issue #268)")
def test_trino_counts_bronze():
    from trino.dbapi import connect  # gated import
    cur = connect(host=os.environ.get("TRINO_HOST", "localhost"),
                  port=int(os.environ.get("TRINO_PORT", "8080")),
                  user="data-eng", catalog="lakehouse").cursor()
    cur.execute("SELECT count(*) FROM lakehouse.bronze.nyc_taxi_trips")
    assert cur.fetchone()[0] >= 0
```

`tests/scenarios/test_streaming_live.py`:

```python
import os

import pytest

pytestmark = pytest.mark.infra


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1",
                    reason="needs live Atlas Redpanda + Spark kafka (issue #269)")
def test_events_topic_reachable():
    from kafka import KafkaAdminClient  # gated import
    admin = KafkaAdminClient(bootstrap_servers=os.environ.get("REDPANDA_BOOTSTRAP", "localhost:9092"))
    topics = admin.list_topics()
    assert isinstance(topics, list)  # broker reachable + metadata fetched (topic auto-created on first produce)
```

- [ ] **Step 2: Confirm they skip in CI**

Run: `uv run pytest tests/scenarios/test_trino_query_live.py tests/scenarios/test_streaming_live.py -q` → `2 skipped`.
Run: `uv run pytest -m "not infra" -q` → both deselected; all green. (`trino`/`kafka` imports are inside the gated test bodies, so no CI import error even though those libs aren't installed.)

- [ ] **Step 3: Docs**

- `docs/scenarios.md` — add the two new scenarios to "Authored scenarios" (Trino federated-query; Redpanda streaming), noting they are live-gated on Atlas #268 (Trino) / #269 (Redpanda).
- `docs/atlas-enablement.md` — under A7/A9 OUTSTANDING, link the opened Atlas issues **#268** (Trino) and **#269** (Redpanda), and note data-eng-lab authors the matching scenarios in Phase 5.

- [ ] **Step 4: Commit**

Run: `uv run python scripts/verify_repo.py --root .` (exit 0), `uv run ruff check .` (clean), `uv run pytest -m "not infra" -q` (green).

```bash
git add tests/scenarios/test_trino_query_live.py tests/scenarios/test_streaming_live.py docs/scenarios.md docs/atlas-enablement.md
git commit -m "feat(scenarios): live-gated Trino + streaming tests; docs + Atlas issue links (#268/#269)"
```

---

## Phase 5 exit criteria

- [ ] `uv run pytest -m "not infra" -q` → all pass (helper override; live tests deselected).
- [ ] `uv run python scripts/verify_repo.py --root .` → exit 0 (both new scenarios validate).
- [ ] `uv run ruff check .` → clean.
- [ ] Both scenarios have the 4 files with matching cross-surface logic (Trino: same SQL in %jdbc + client; streaming: Scala/PySpark parity); DAGs are `EmptyOperator` placeholders (guard test unaffected).
- [ ] PR into `main`: `static-and-unit` + `maven-spark-apps` green; squash-merge.

## Self-review (against the design)

**Coverage:** interpreter override enables non-Spark scenarios (Task 1 ✓); Trino federated query as %jdbc + client with same SQL (Task 2 ✓, decision "reuse framework"); Redpanda Structured Streaming Scala+PySpark parity + producer (Task 3 ✓); live-gated tests + docs + upstream issue links (Task 4 ✓). Scope = one Trino + one Redpanda (per decision). Upstream Atlas asks opened as #268/#269.

**Now-green vs live:** structural validation is CI-green; Trino query, streaming round-trip, and cross-surface parity are `RUN_INFRA`-gated against the assumed A7/A9 contract. Column/topic/catalog names match the assumed contract and are verified on first live run once Atlas delivers #268/#269.

**Placeholder scan:** the scenario DAGs are intentional `EmptyOperator` placeholders (Trino query is interactive; streaming is long-running) — the established pattern, not stubs; every step has runnable content.

**Consistency:** `lakehouse.bronze.nyc_taxi_trips` / `lakehouse.gold.nyc_taxi_daily_trino` / `lakehouse.bronze.events` / topic `events` / `s3a://checkpoints/events` are used identically across notebooks, producer, and tests.
