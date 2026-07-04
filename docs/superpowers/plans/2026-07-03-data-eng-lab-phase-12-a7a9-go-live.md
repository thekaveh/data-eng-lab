# data-eng-lab — Phase 12: A7 (Trino) / A9 (Redpanda) Go-Live Reconciliation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Atlas delivered Trino (A7) and Redpanda (A9) at atlas `72e30d1`. Reconcile data-eng-lab's Trino + streaming scenarios/tests with the delivered reality, and mark A7/A9 delivered in the docs. Reconcile-only (no live launch); CI-green.

**Architecture:** Re-pin `infra/` to `72e30d1`; enable `--trino-source`/`--redpanda-source`; fix the 5 mismatches the investigation found (M1 `%jdbc(trino)`→`%trino`; M2 Trino user `data-eng`→`atlas`; M3 host ports `9092`/`8080`→slot-allocated `REDPANDA_KAFKA_PORT`/`TRINO_PORT`; M4 topic-seed note; M5 test env var). Update the A1–A9 ledger.

**Tech Stack:** git submodule; bash; JSON string edits on `.zpln`/`.ipynb`; Python (producer + live tests); docs.

## Global Constraints

- **Never edit files UNDER `infra/`** — but re-pin its commit and READ its files.
- **Delivered A7/A9 facts (atlas `72e30d1`), use these EXACT values:**
  - Trino: in-net `trino:8080`, host port env **`TRINO_PORT`** (default `63029`), catalog **`lakehouse`** (iceberg REST connector, `iceberg.rest-catalog.uri=http://iceberg-rest:8181`, warehouse `s3://lakehouse/`, iceberg-scoped MinIO creds); **no auth** (any user works; Atlas convention is user **`atlas`**); **read-write** (CTAS supported); Zeppelin interpreter is the named **`%trino`** (URL `jdbc:trino://trino:8080/lakehouse`, user `atlas`) — NOT `%jdbc(trino)`.
  - Redpanda: in-net broker **`redpanda:9092`**; host-published on **`REDPANDA_KAFKA_PORT`** (default `63010`, → container `19092`); demo topics seeded from `REDPANDA_DEMO_TOPICS` (default only `atlas_stream_events`); console on `REDPANDA_CONSOLE_PORT`. `spark-sql-kafka-0-10_2.13:4.1.2` baked into the Spark image (readStream works in-cluster with no `--packages`).
  - Both services default `--*-source disabled`; both are in the `data-eng` track.
- **Live-gated:** the Trino/streaming execution is `@pytest.mark.infra`/`RUN_INFRA` (not run in CI). The host-side clients (producer, live tests) connect from the HOST → use `localhost:<host-port>`; in-cluster notebooks use `redpanda:9092`/`trino:8080`.
- **CI must stay green** (`static-and-unit` + `maven-spark-apps`). Re-pinning the submodule is inert for CI (`submodules: false`). Python 3.11, ruff 120.
- **Branch/PR:** land via feature branch → PR; **push before merge.**

## File Structure

- `infra` (gitlink) → `72e30d1`; `scripts/start-all.sh` — sources.
- `scenarios/{federated_query,bi_query}-*-trino-iceberg/{zeppelin/notebook.zpln, README.md, jupyter/notebook.ipynb}` — `%trino` + user.
- `scenarios/streaming_ingest-events-spark-iceberg/producer.py`; `tests/scenarios/test_trino_query_live.py`; `tests/scenarios/test_streaming_live.py` — host ports.
- streaming READMEs — topic note.
- `docs/atlas-expectations.md`, `docs/atlas-enablement.md`, `docs/go-live.md` — A7/A9 delivered.

---

### Task 1: Re-pin submodule to `72e30d1` + enable Trino/Redpanda sources

**Files:** Modify (gitlink): `infra`; Modify: `scripts/start-all.sh`

- [ ] **Step 1:** Ensure the submodule is at `72e30d1` and stage it:
```bash
git -C infra fetch origin --quiet
git -C infra checkout 72e30d1
git add infra
git -C infra rev-parse --short HEAD   # expect 72e30d1
```
- [ ] **Step 2:** In `scripts/start-all.sh`, append `--trino-source container --redpanda-source container` to the Atlas start invocation (which already has `--iceberg-rest-source container --jenkins-source container`). Keep everything else.
- [ ] **Step 3:** Verify: `bash -n scripts/start-all.sh` + `shellcheck scripts/start-all.sh` (no new warnings); `uv run pytest -m "not infra" -q` + `uv run ruff check .` green; confirm `git status` shows `infra` staged at `72e30d1`.
- [ ] **Step 4:** Commit:
```bash
git add infra scripts/start-all.sh
git commit -m "feat(go-live): re-pin atlas to 72e30d1 (A7 Trino + A9 Redpanda delivered); enable trino+redpanda sources"
```

---

### Task 2: Fix the Trino Zeppelin interpreter (M1) + user convention (M2)

**Files:** Modify: `scenarios/federated_query-nyc_taxi-trino-iceberg/{zeppelin/notebook.zpln, README.md, jupyter/notebook.ipynb}`, `scenarios/bi_query-tpch-trino-iceberg/{zeppelin/notebook.zpln, README.md, jupyter/notebook.ipynb}`

**M1:** Atlas seeds the named `%trino` interpreter (Zeppelin uses the interpreter name as the paragraph prefix); `%jdbc(trino)` won't bind.

- [ ] **Step 1 (M1):** In BOTH scenarios, replace every `%jdbc(trino)` with `%trino` in the `.zpln` (the paragraph `text` fields) and the `README.md`. This is a clean substring replace (no JSON escaping issue). E.g.:
```bash
for f in scenarios/federated_query-nyc_taxi-trino-iceberg/zeppelin/notebook.zpln scenarios/federated_query-nyc_taxi-trino-iceberg/README.md scenarios/bi_query-tpch-trino-iceberg/zeppelin/notebook.zpln scenarios/bi_query-tpch-trino-iceberg/README.md; do
  python3 - "$f" <<'PY'
import sys, pathlib
p = pathlib.Path(sys.argv[1]); p.write_text(p.read_text().replace("%jdbc(trino)", "%trino"))
PY
done
```
Confirm: `grep -rl "%jdbc(trino)" scenarios/` returns nothing; `grep -rl "%trino" scenarios/federated_query-nyc_taxi-trino-iceberg scenarios/bi_query-tpch-trino-iceberg` returns the .zpln + README for each.
Also update, in each README, any sentence describing the interpreter as `%jdbc(trino)` to note **`%trino`** (Atlas seeds a named `trino` JDBC interpreter; Zeppelin 0.12.1 uses the interpreter name as the prefix).

- [ ] **Step 2 (M2, cosmetic — align to Atlas's convention):** In BOTH `jupyter/notebook.ipynb`, change the trino client `user='data-eng'` → `user='atlas'` (Trino has no auth, but `atlas` is Atlas's convention). Clean substring replace `user='data-eng'` → `user='atlas'` in the two `.ipynb`.

- [ ] **Step 3:** Verify: `uv run python scripts/verify_repo.py --root .` exit 0 (the `.zpln`/`.ipynb` are still valid JSON with the 6 sections); `uv run ruff check .` clean; `uv run pytest -m "not infra" -q` green; `uv run pytest tests/test_dag_catalog_conf.py -q` 1 passed. Confirm the `.zpln` code paragraphs now begin `%trino` and the `.ipynb` client uses `user='atlas'`.

- [ ] **Step 4:** Commit:
```bash
git add scenarios/federated_query-nyc_taxi-trino-iceberg scenarios/bi_query-tpch-trino-iceberg
git commit -m "fix(scenarios): Trino Zeppelin interpreter %jdbc(trino) -> %trino (atlas convention); client user atlas"
```

---

### Task 3: Fix host ports (M3/M5) + topic-seed note (M4)

**Files:** Modify: `scenarios/streaming_ingest-events-spark-iceberg/producer.py`, `tests/scenarios/test_trino_query_live.py`, `tests/scenarios/test_streaming_live.py`, and the §6 of `scenarios/{streaming_ingest-events,streaming_windows-events,cdc_streaming-online_retail}-*/README.md`.

**Host access:** the producer + live tests run on the HOST → must use `localhost:<slot-allocated host port>`, not the container ports.

- [ ] **Step 1 (M3, producer):** In `scenarios/streaming_ingest-events-spark-iceberg/producer.py`, replace the `BOOTSTRAP = "localhost:9092"  # …` line with (add `import os` if missing):
```python
# Host-published Redpanda: localhost:$REDPANDA_KAFKA_PORT (default 63010). In-cluster use redpanda:9092.
BOOTSTRAP = os.environ.get("REDPANDA_BOOTSTRAP") or f"localhost:{os.environ.get('REDPANDA_KAFKA_PORT', '63010')}"
```

- [ ] **Step 2 (M3, trino test):** In `tests/scenarios/test_trino_query_live.py`, change the port default from the container `8080` to the host default and align the user:
```python
    cur = connect(host=os.environ.get("TRINO_HOST", "localhost"),
                  port=int(os.environ.get("TRINO_PORT", "63029")),
                  user="atlas", catalog="lakehouse").cursor()
```
(Add a comment: `TRINO_PORT` is the host-published port, default 63029; source `infra/.env` before `RUN_INFRA=1`.)

- [ ] **Step 3 (M3/M5, streaming test):** In `tests/scenarios/test_streaming_live.py`, change the bootstrap to the host-published port (Atlas exposes `redpanda:9092` only in-net; a host-run test needs `localhost:$REDPANDA_KAFKA_PORT`):
```python
    bootstrap = os.environ.get("REDPANDA_BOOTSTRAP") or f"localhost:{os.environ.get('REDPANDA_KAFKA_PORT', '63010')}"
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
```

- [ ] **Step 4 (M4, topic note):** In the §6 (Known issues) of the three streaming scenario READMEs (`streaming_ingest-events`, `streaming_windows-events`, `cdc_streaming-online_retail`), add one line: Atlas seeds only the `atlas_stream_events` demo topic; this scenario's topic (`events` / `online_retail_cdc`) is auto-created on first produce — run `producer.py` before the streaming notebook, or add the topic to `REDPANDA_DEMO_TOPICS` in `infra/.env`.

- [ ] **Step 5:** Verify: `uv run ruff check .` (producer + tests clean — `os` import used); `uv run pytest -m "not infra" -q` green; `uv run pytest tests/scenarios/test_trino_query_live.py tests/scenarios/test_streaming_live.py -q` → `2 skipped` (RUN_INFRA unset; imports still inside gated bodies); `uv run python scripts/verify_repo.py --root .` exit 0. Confirm `grep -rn "localhost:9092\|\"8080\"" scenarios/streaming_ingest-events-spark-iceberg/producer.py tests/scenarios/test_*_live.py` finds none.

- [ ] **Step 6:** Commit:
```bash
git add scenarios/streaming_ingest-events-spark-iceberg tests/scenarios/test_trino_query_live.py tests/scenarios/test_streaming_live.py scenarios/streaming_windows-events-spark-iceberg scenarios/cdc_streaming-online_retail-spark-iceberg
git commit -m "fix(streaming): host-published Redpanda/Trino ports for producer+live tests; topic-seed note"
```

---

### Task 4: Mark A7/A9 delivered in the ledger

**Files:** Modify: `docs/atlas-expectations.md`, `docs/atlas-enablement.md`, `docs/go-live.md`

- [ ] **Step 1: `docs/atlas-expectations.md`** — update to the delivered reality:
  - Header: submodule now atlas **`72e30d1`**.
  - §0 TL;DR: **A7 → ✅ delivered (#270 + Trino JDBC interpreter seed)**, **A9 → ✅ delivered (Redpanda + kafka)**; update the "Key takeaway" (Trino + Kafka streaming are now live).
  - Move the A7/A9 build specs from §2 OUTSTANDING into "delivered" with the actual shapes (Trino `trinodb/trino:482`, `trino:8080`/`TRINO_PORT`, catalog `lakehouse` iceberg-REST connector, no-auth, CTAS, **`%trino`** interpreter; Redpanda `v26.1.12`, `redpanda:9092`/`REDPANDA_KAFKA_PORT`, `REDPANDA_DEMO_TOPICS` seeds `atlas_stream_events`, console; `spark-sql-kafka-0-10_2.13:4.1.2` baked).
  - §A8 track: now includes `trino`, `redpanda`; flags `--trino-source`/`--redpanda-source` (default disabled).
  - Add a "Delivered deviations from our A7/A9 asks" note: Zeppelin interpreter is **`%trino`** (Atlas declined `%jdbc(trino)` — Zeppelin 0.12.1 name-as-prefix); Trino user convention `atlas`; only `atlas_stream_events` topic seeded.
- [ ] **Step 2: `docs/atlas-enablement.md`** — mark A7 + A9 DELIVERED (was OUTSTANDING); note the `%trino` deviation.
- [ ] **Step 3: `docs/go-live.md`** — add a short "Trino + streaming" validation section: enable `--trino-source`/`--redpanda-source` (Task 1 already does), run `federated_query`/`bi_query` via Zeppelin `%trino` or the Jupyter trino client at `localhost:$TRINO_PORT`, run `producer.py` then the streaming notebooks, `RUN_INFRA=1 uv run pytest tests/scenarios/test_trino_query_live.py tests/scenarios/test_streaming_live.py`.
- [ ] **Step 4:** Verify + commit:
```bash
uv run python scripts/verify_repo.py --root .   # exit 0
uv run ruff check .   # clean
uv run pytest -m "not infra" -q   # green
git add docs/atlas-expectations.md docs/atlas-enablement.md docs/go-live.md
git commit -m "docs: A7 Trino + A9 Redpanda DELIVERED (atlas 72e30d1); record %trino deviation + topic seed"
```

---

## Phase 12 exit criteria

- [ ] `infra` pinned at `72e30d1`; `git status` clean after commits.
- [ ] `grep -rn "%jdbc(trino)" scenarios/` → nothing; both trino scenarios use `%trino`.
- [ ] `grep -rn "localhost:9092\|\"8080\"" scenarios/streaming_ingest-events-spark-iceberg/producer.py tests/scenarios/test_*_live.py` → nothing (host ports via env).
- [ ] `uv run pytest -m "not infra" -q`, `uv run ruff check .`, `uv run python scripts/verify_repo.py --root .` → all green; live tests `2 skipped`.
- [ ] PR into `main`: both required checks green; **push the branch, confirm CI, then squash-merge.**

## Self-review (against the investigation)

**Reconciliation coverage:** M1 `%trino` (Task 2) ✓; M2 user `atlas` (Task 2) ✓; M3 host ports (Task 3) ✓; M4 topic note (Task 3) ✓; M5 test env var (Task 3) ✓; re-pin + sources (Task 1) ✓; ledger (Task 4) ✓. In-net addresses (`redpanda:9092`/`trino:8080`), catalog shape, CTAS, and the baked Spark Kafka jars matched already — unchanged.

**No infra edits:** only the gitlink moves. **Live-gated:** Trino/streaming execution stays `RUN_INFRA`-gated; the changes are host-connectivity + interpreter-prefix correctness verified on the first live run.

**Consistency:** the `%trino` prefix, `localhost:$TRINO_PORT`/`localhost:$REDPANDA_KAFKA_PORT` host access, and `user='atlas'` now match the delivered Atlas; in-cluster notebooks keep `redpanda:9092`/`trino:8080`.
