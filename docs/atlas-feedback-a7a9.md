# Atlas A7 (Trino) / A9 (Redpanda) Delivery — Feedback from `data-eng-lab`

**From:** `data-eng-lab` (private consumer of Atlas)
**Re:** the Trino + Redpanda services delivered on Atlas `main` @ `72e30d1` (PRs "Add Trino lakehouse query service #270", "Add Redpanda streaming service", "Seed Trino JDBC interpreter for Zeppelin")
**Against:** our requested contract — Atlas issues [#268 (Trino)](https://github.com/thekaveh/atlas/issues/268) / [#269 (Redpanda)](https://github.com/thekaveh/atlas/issues/269) and `docs/atlas-expectations.md` §2.

**TL;DR:** The delivery is **very close to the requested contract** — in-net addresses, catalog shape, CTAS, the baked Spark Kafka jars, track membership, and launch flags all match. We found **5 divergences** while reconciling; **none block the delivery**, and `data-eng-lab` has already adapted its side for all of them. This report is a heads-up + a couple of small suggestions, categorized by who (if anyone) should act.

---

## A. Confirmed intentional deviations — no Atlas action needed (we adapted)

### A1 — Zeppelin Trino interpreter is `%trino`, not `%jdbc(trino)`
- **We asked** (#268 / expectations §2 A7): "Seed the Zeppelin `%jdbc(trino)` interpreter."
- **Delivered:** a **named `trino` interpreter** (group `jdbc`), so the paragraph prefix is **`%trino`**. `services/zeppelin/init/scripts/seed-spark-interpreter.py` seeds it from `TRINO_JDBC_URL=jdbc:trino://trino:8080/lakehouse`, `TRINO_JDBC_USER=atlas`, driver `io.trino:trino-jdbc:482`. `services/zeppelin/README.md` explicitly documents *why*: Zeppelin 0.12.1 uses the interpreter **name** as the paragraph prefix, so `%jdbc(trino)` is not the documented happy path.
- **Assessment:** **This is a reasonable, well-documented Atlas decision — we agree with it.** Our original `%jdbc(trino)` ask was based on older Zeppelin semantics. **We changed our notebooks to `%trino`.** No Atlas action required. (Optional nicety, only if trivial: also expose a `jdbc`-grouped connection so both prefixes work — but not worth any effort.)

### A2 — Host ports are slot-allocated (this was our bug, not Atlas's)
- Atlas publishes Trino/Redpanda on slot-allocated host ports (`TRINO_PORT`=63029, `REDPANDA_KAFKA_PORT`=63010 → container 19092), consistent with every other Atlas service. The container ports (`8080`, `9092`) are in-net only.
- Our host-side clients (`producer.py`, the `RUN_INFRA` live tests) had hardcoded the container ports — **our mistake.** **We fixed them to use `localhost:$TRINO_PORT` / `localhost:$REDPANDA_KAFKA_PORT`.** No Atlas action. (Listed only for completeness.)

---

## B. Small suggestions for Atlas (nice-to-have, low priority)

### B1 — Redpanda demo-topic default only seeds `atlas_stream_events`
- **We asked** (#269): "A topic-creation seam … for demo topics (e.g. `events`)."
- **Delivered:** the seam exists and works — `services/redpanda/init/scripts/init-redpanda.sh` runs `rpk topic create <t> --if-not-exists` for each topic in `REDPANDA_DEMO_TOPICS` (default `atlas_stream_events`). Our scenario topics (`events`, `online_retail_cdc`) are **not** in that default, so they rely on Redpanda `dev-container` auto-create (which appears enabled) or a `REDPANDA_DEMO_TOPICS` override.
- **Suggestion (optional):** In `services/redpanda/README.md`, add one line telling downstream projects to set `REDPANDA_DEMO_TOPICS=<their,topics>` in `.env` to pre-seed their own topics — and/or explicitly state whether `auto_create_topics_enabled` is guaranteed on in `dev-container` mode (our streaming demos depend on a topic existing before a `readStream.subscribe`). **We adapted** by documenting "run `producer.py` first (auto-creates) or set `REDPANDA_DEMO_TOPICS`" in our scenario READMEs.

---

## C. Documentation clarifications we'd have liked (informational)

These cost us investigation time; a line each in the service READMEs would help the next consumer:

- **C1 — Redpanda broker env var name.** When enabled, Atlas injects `REDPANDA_BROKERS` and `SPARK_KAFKA_BOOTSTRAP_SERVERS` (both `redpanda:9092`, in-net). We had guessed `REDPANDA_BOOTSTRAP`. Worth naming the exact vars in `services/redpanda/README.md`.
- **C2 — Trino has no auth; convention user is `atlas`.** `catalog/lakehouse.properties` sets no authenticator; any `user` string is accepted, and Atlas's examples use `user='atlas'`. Fine — just worth stating "no auth; pass any user (we use `atlas`)."
- **C3 — Namespaces still aren't seeded (affects Trino CTAS).** Consistent with the rest of the lakehouse (apps own namespaces), but Trino `CREATE TABLE lakehouse.gold.… AS …` fails if `lakehouse.gold` doesn't exist. `services/trino/README.md` §4 does note this — good; just reinforcing it's the #1 first-run gotcha for CTAS.
- **C4 — Trino catalog uses `fs.s3.enabled=true` (Trino-482 native S3), not `fs.native-s3.enabled`.** Correct for Trino 482; noting only because the property name changed across Trino versions.

---

## D. What matched the requested contract (delivery validation)

For a fair picture — the large majority landed exactly as requested:

**Trino (A7):**
- `trinodb/trino:482`; in-net `trino:8080`; host `TRINO_PORT`; `--trino-source` flag (default `disabled`); member of the `data-eng` track. ✅
- Catalog **`lakehouse`** via the Iceberg **REST** connector: `iceberg.catalog.type=rest`, `iceberg.rest-catalog.uri=http://iceberg-rest:8181`, `iceberg.rest-catalog.warehouse=s3://lakehouse/`, MinIO endpoint `http://minio:9000` path-style, **iceberg-scoped** MinIO creds (same account Spark uses). Tables addressed `lakehouse.<schema>.<table>`. ✅
- **Read-write** connector — `CREATE TABLE … AS SELECT` (CTAS) works (needed by our `bi_query`/`federated_query`). ✅
- Zeppelin interpreter auto-seeded (no manual UI step) — just under `%trino` (see A1). ✅
- `depends_on` iceberg-rest healthy + minio-init; bootstrapper hard-fails if those sources are disabled. ✅

**Redpanda (A9):**
- `redpandadata/redpanda:v26.1.12`; in-net broker **`redpanda:9092`**; host-published on `REDPANDA_KAFKA_PORT`; console (`REDPANDA_CONSOLE_PORT`); `--redpanda-source` flag (default `disabled`); member of the `data-eng` track. ✅
- Topic-creation seam via `REDPANDA_DEMO_TOPICS` + `rpk topic create` (see B1). ✅

**Spark Kafka connector:**
- `spark-sql-kafka-0-10_2.13:4.1.2` + `spark-token-provider-kafka-0-10_2.13:4.1.2` + `kafka-clients:3.9.1` + `commons-pool2:2.12.1` baked (SHA-512-verified) into the single Spark image used by spark-master, spark-worker, spark-connect, and spark-history → `readStream.format("kafka")` works with **no `--packages`**, in both Spark Connect and standalone. ✅

**Track / launch:** `data-eng` = `[spark, airflow, jupyterhub, zeppelin, jenkins, supavisor, minio, iceberg-rest, trino, redpanda, …]`; both new services default `disabled`. ✅

---

## E. Net

A7 + A9 complete the full A1–A9 contract. The delivery is high-fidelity; the only *substantive* divergence (A1, `%trino`) is a deliberate, documented Atlas choice that we've adopted. Items in **B** and **C** are optional documentation polish. **No Atlas fixes are required for `data-eng-lab` to go live** once the stack is launched.

*Thanks — issues #268 and #269 can be closed as delivered. Questions → comment on those issues or ping `data-eng-lab`.*
