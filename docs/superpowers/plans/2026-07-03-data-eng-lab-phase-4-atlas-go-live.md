# data-eng-lab — Phase 4: Atlas Go-Live Reconciliation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reconcile data-eng-lab's assumed-contract code with the **now-delivered** Atlas lakehouse (A1–A6 + A8, at atlas commit `62eb6df`), so the live-gated surface is actually correct against the real stack — without launching it yet (reconcile-only; live validation deferred to a runbook).

**Architecture:** Re-pin the `infra/` submodule to `62eb6df`; enable the new `iceberg-rest`/`jenkins` sources in bootstrap; slim bucket creation to what Atlas doesn't provide; bring the Maven app to Spark **4.1.2** and fix its real-schema drift (issue #7); rebuild the Airflow DAG's Spark conf and the Jenkins publish step from Atlas's own reference; refresh the enablement ledger + add a go-live runbook. Most changes are CI-verifiable now (the `maven-spark-apps` job compiles/tests against 4.1.2; Python + verifier stay green); the DAG/Jenkins path stays authored + live-gated.

**Tech Stack:** git submodule; bash; Maven/Scala 2.13/Spark 4.1.2; Airflow `SparkSubmitOperator` (cluster mode); Jenkins JCasC + REST seed; Python verifier/tests.

## Global Constraints

- **Never edit files under `infra/`** (it's the Atlas submodule) — but DO re-pin its commit and READ its reference files.
- **Atlas reality (from investigation of `62eb6df`), use these EXACT values:**
  - Iceberg REST catalog: name **`lakehouse`**, in-net `http://iceberg-rest:8181`, host port env `ICEBERG_REST_PORT` (default `63020`), warehouse bucket **`lakehouse`** (Spark clients use `s3a://lakehouse/`).
  - Spark: **4.1.2**, Scala 2.13, Iceberg runtime **1.11.0**; Spark Connect `sc://spark-connect:15002`.
  - Buckets Atlas already creates: `landing`, `lakehouse`, `jars`, `checkpoints` (+ `spark-history`). We only need our own **`lakehouse-test`**.
  - **Namespaces `bronze`/`silver`/`gold` are NOT pre-created by Atlas** — the app/bootstrap must create them.
  - Airflow: conn `spark_default` = `spark://spark-master:7077` (extra `deploy-mode: cluster`), conn `minio_default` (aws, endpoint `http://minio:9000`); default `deploy_mode=cluster`. Reference DAG: `infra/services/airflow/dags/lakehouse_spark_submit_smoke.py`.
  - Jenkins: JDK 21 + Maven + `mc`; **no** alias/seed-job shipped; publish contract = `mc alias set atlas "$MINIO_ENDPOINT" "$MINIO_ICEBERG_ACCESS_KEY" "$MINIO_ICEBERG_SECRET_KEY"` then `mc cp target/*.jar "atlas/$MINIO_BUCKET_ICEBERG_JARS/<app>/<version>/app.jar"` (bucket `jars`). Host port env `JENKINS_PORT` (default `63080`); admin `JENKINS_ADMIN_USER`/`JENKINS_ADMIN_PASSWORD`.
  - New services default to `disabled`; enable via `--iceberg-rest-source container` / `--jenkins-source container`.
- **Real NYC-TLC yellow schema** uses `tpep_pickup_datetime` / `tpep_dropoff_datetime` / `passenger_count` / `fare_amount` (NOT `pickup_datetime`).
- **CI must stay green:** `static-and-unit` + `maven-spark-apps` (the latter now compiles/tests the app against Spark 4.1.2). Re-pinning the submodule is inert for CI (`submodules: false`).
- **Branch/PR:** land via feature branch → PR with both required checks green.

## File Structure

- `infra` (gitlink) — re-pinned to `62eb6df`.
- `scripts/start-all.sh`, `scripts/create_buckets.sh` — bootstrap reconciliation.
- `spark-apps/nyc-taxi-etl/pom.xml` — Spark 4.1.2.
- `spark-apps/nyc-taxi-etl/src/main/scala/.../transforms/TaxiTransforms.scala` + `NycTaxiEtl.scala` + `src/test/.../TaxiTransformsSpec.scala` — real schema, namespace.
- `spark-apps/nyc-taxi-etl/dag.py`, `spark-apps/nyc-taxi-etl/Jenkinsfile` — reconciled to Atlas.
- `jenkins/nyc-taxi-etl-job.xml`, `jenkins/seed-job.sh` — Jenkins seed (new).
- `docs/atlas-enablement.md`, `docs/go-live.md` — ledger + runbook.

---

### Task 1: Re-pin submodule + enable new sources + slim buckets

**Files:**
- Modify (gitlink): `infra`
- Modify: `scripts/start-all.sh`, `scripts/create_buckets.sh`

**Interfaces:**
- Produces: `infra` at `62eb6df` (Atlas reference files available to later tasks); bootstrap that enables `iceberg-rest` + `jenkins` and creates only `lakehouse-test`.

- [ ] **Step 1: Re-pin the submodule**

```bash
git -C infra fetch origin --quiet
git -C infra checkout 62eb6df
git add infra
git -C infra rev-parse --short HEAD   # expect 62eb6df
```

- [ ] **Step 2: Enable the new sources in `scripts/start-all.sh`**

Read `scripts/start-all.sh`. Find the Atlas start invocation (currently `--spark-source container --zeppelin-source container --airflow-source container --minio-source container --jupyterhub-source container`) and append `--iceberg-rest-source container --jenkins-source container`. (Keep everything else, including the existing `export ICEBERG_REST_ENABLED=true`.)

- [ ] **Step 3: Slim `scripts/create_buckets.sh`**

Read `scripts/create_buckets.sh`. Atlas now creates `landing`, `lakehouse`, `jars`, `checkpoints` via its own `minio-init`. Change the `BUCKETS=(...)` array to only our test bucket:

```bash
# Atlas's minio-init creates landing/lakehouse/jars/checkpoints (iceberg service account).
# We only add our integration-test bucket here.
BUCKETS=(lakehouse-test)
```

Add a one-line comment above documenting that the lakehouse buckets come from Atlas. Keep the `mb --ignore-existing` loop and `MINIO_USER`/`MINIO_PASS` usage unchanged.

- [ ] **Step 4: Verify (static — no live launch)**

Run: `bash -n scripts/start-all.sh && bash -n scripts/create_buckets.sh` → no syntax errors.
Run: `shellcheck scripts/start-all.sh scripts/create_buckets.sh` → no new warnings (match the repo's existing shellcheck cleanliness).
Run: `uv run pytest -m "not infra" -q` and `uv run ruff check .` → green (unchanged).
Confirm `git -C infra rev-parse --short HEAD` = `62eb6df` and `git status` shows `infra` staged as modified.

- [ ] **Step 5: Commit**

```bash
git add infra scripts/start-all.sh scripts/create_buckets.sh
git commit -m "feat(go-live): re-pin atlas to 62eb6df; enable iceberg-rest+jenkins sources; slim buckets to atlas reality"
```

---

### Task 2: Spark app go-live — Spark 4.1.2 + real TLC schema + self-created namespace (closes #7)

**Files:**
- Modify: `spark-apps/nyc-taxi-etl/pom.xml`
- Modify: `spark-apps/nyc-taxi-etl/src/main/scala/com/thekaveh/dataeng/nyctaxi/transforms/TaxiTransforms.scala`
- Modify: `spark-apps/nyc-taxi-etl/src/main/scala/com/thekaveh/dataeng/nyctaxi/NycTaxiEtl.scala`
- Modify: `spark-apps/nyc-taxi-etl/src/test/scala/com/thekaveh/dataeng/nyctaxi/TaxiTransformsSpec.scala`

**Interfaces:**
- Produces: `TaxiTransforms.clean` filtering on the real TLC columns, **no bronze-level dedup** (dedup lives at silver in the medallion notebook — this reconciles the "3 delivery modes"); `NycTaxiEtl.main` creating its namespace before writing. `mvn -B package` passes on Spark 4.1.2 (CI-verified by `maven-spark-apps`).

- [ ] **Step 1: Bump Spark to 4.1.2**

In `pom.xml`, change `<spark.version>4.0.0</spark.version>` → `<spark.version>4.1.2</spark.version>`. Leave scala 2.13.14, `release` 17, the `--add-opens` argLine, and the shade/scalatest plugins unchanged.

> **If `spark-sql_2.13:4.1.2` does not resolve from Maven Central** (check `https://repo1.maven.org/maven2/org/apache/spark/spark-sql_2.13/`), use the nearest available `4.1.x` on Central and note it in the report — Atlas's runtime is 4.1.2 and `spark-sql` is `provided`, so binary compat holds within 4.1.x. Do NOT silently revert to 4.0.0.

- [ ] **Step 2: Update the scalatest for the real schema + no-dedup (RED first)**

Rewrite `TaxiTransformsSpec.scala` so the test data uses the real columns and there is no duplicate-collapsing assertion. Replace the DataFrame + assertions with:

```scala
    val raw = Seq(
      (ts("2023-01-01 10:00:00"), 2, 5.0),
      (null.asInstanceOf[Timestamp], 1, 3.0),  // null pickup -> dropped
      (ts("2023-01-02 11:00:00"), 0, 4.0)      // passenger_count 0 -> dropped
    ).toDF("tpep_pickup_datetime", "passenger_count", "fare_amount")

    val out = TaxiTransforms.clean(raw)
    assert(out.count() == 1)
    val row = out.select("trip_date").as[java.sql.Date].collect().head
    assert(row.toString == "2023-01-01")
```

Update the `test("...")` name to describe "drops null pickup + non-positive passengers, adds trip_date" (no dedup claim). Keep the local `SparkSession` setup; add `.config("spark.sql.session.timeZone", "UTC")` to the builder for determinism.

Run: `cd spark-apps/nyc-taxi-etl && mvn -q -B test ; cd -` → FAIL (clean still references old column / still dedups).

- [ ] **Step 3: Update `TaxiTransforms.clean`**

```scala
  /** Clean raw taxi trips: drop null pickups + non-positive passenger counts, add trip_date.
    * No dedup at bronze — deduplication lives at the silver layer (see the medallion scenario). */
  def clean(df: DataFrame): DataFrame =
    df.where(F.col("tpep_pickup_datetime").isNotNull && (F.col("passenger_count") > 0))
      .withColumn("trip_date", F.to_date(F.col("tpep_pickup_datetime")))
```

Run: `cd spark-apps/nyc-taxi-etl && mvn -q -B test ; cd -` → PASS.

- [ ] **Step 4: Make `NycTaxiEtl` self-create its namespace**

In `NycTaxiEtl.scala`, after building `spark` and before the write, add (mirrors Atlas's `LakehouseSmoke`):

```scala
      val ns = table.substring(0, table.lastIndexOf('.'))  // e.g. lakehouse.bronze
      spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $ns")
```

(Keep the arg-defaulted `landing`/`table`, the `TaxiTransforms.clean` call, `writeTo(...).using("iceberg").createOrReplace()`, and the `try/finally spark.stop()`.)

Run: `cd spark-apps/nyc-taxi-etl && mvn -q -B package ; cd -` → BUILD SUCCESS (test + shade); confirm `target/nyc-taxi-etl-0.1.0.jar` is produced.

- [ ] **Step 5: Python gate unaffected + commit**

Run: `uv run pytest -m "not infra" -q`, `uv run ruff check .`, `uv run python scripts/verify_repo.py --root .` → all green.

```bash
git add spark-apps/nyc-taxi-etl/pom.xml spark-apps/nyc-taxi-etl/src
git commit -m "fix(spark-apps): Spark 4.1.2 + real TLC columns (tpep_*), drop bronze dedup, self-create namespace (closes #7)"
```

---

### Task 3: Reconcile the Airflow DAG + Jenkins publish to Atlas's reference

**Files:**
- Modify: `spark-apps/nyc-taxi-etl/dag.py`, `spark-apps/nyc-taxi-etl/Jenkinsfile`
- Create: `jenkins/nyc-taxi-etl-job.xml`, `jenkins/seed-job.sh`

**Interfaces:**
- Consumes: Atlas's reference `infra/services/airflow/dags/lakehouse_spark_submit_smoke.py` (available now that Task 1 re-pinned) and `infra/services/jenkins/README.md`.
- Produces: a DAG whose `SparkSubmitOperator` carries the full `spark.sql.catalog.lakehouse.*` conf + `deploy_mode="cluster"`; a Jenkinsfile that sets the `atlas` mc alias before publishing; a Jenkins seed mechanism.

- [ ] **Step 1: Rebuild `dag.py` from Atlas's reference**

Read `infra/services/airflow/dags/lakehouse_spark_submit_smoke.py`. Rewrite `spark-apps/nyc-taxi-etl/dag.py` to mirror its `SparkSubmitOperator` **`conf` dict verbatim** (the whole `spark.master`, `spark.sql.extensions`, `spark.sql.catalog.lakehouse.*`, s3a Hadoop keys, `spark.eventLog.dir`, and driver/executor `AWS_*` env) and `deploy_mode="cluster"`, changing only:
- `dag_id="nyc_taxi_etl"`, `schedule="@daily"`, `start_date` pendulum 2023-01-01 UTC, `catchup=False`, tags `["data-eng-lab","scenario"]`.
- `application="s3a://jars/nyc-taxi-etl/0.1.0/app.jar"`, `java_class="com.thekaveh.dataeng.nyctaxi.NycTaxiEtl"`, `conn_id="spark_default"`.
- `application_args=["s3a://landing/nyc_taxi/", "lakehouse.bronze.nyc_taxi_trips"]`.

Drop Atlas's `prepare_s3a_assets` upload step (our JAR is published by Jenkins, not the DAG). Keep imports minimal + ruff-clean.

- [ ] **Step 2: Fix the `Jenkinsfile` publish step**

Read the current `Jenkinsfile`. In the Publish stage, before `mc cp`, add the alias set, and use Atlas's env + bucket var:

```groovy
    stage('Publish JAR') {
      steps {
        dir("spark-apps/${APP}") {
          sh '''
            mc alias set atlas "$MINIO_ENDPOINT" "$MINIO_ICEBERG_ACCESS_KEY" "$MINIO_ICEBERG_SECRET_KEY"
            mc cp target/${APP}-${VERSION}.jar "atlas/${MINIO_BUCKET_ICEBERG_JARS}/${APP}/${VERSION}/app.jar"
          '''
        }
      }
    }
```

(Keep the Build & Test / Package stages; keep `APP`/`VERSION` env.)

- [ ] **Step 3: Add the Jenkins seed job (authored/gated)**

`jenkins/nyc-taxi-etl-job.xml` — a minimal Jenkins **Pipeline-from-SCM** job `config.xml` pointing at this repo, script path `spark-apps/nyc-taxi-etl/Jenkinsfile`:

```xml
<?xml version='1.1' encoding='UTF-8'?>
<flow-definition plugin="workflow-job">
  <description>Build + test + publish nyc-taxi-etl (data-eng-lab). Seeded via jenkins/seed-job.sh.</description>
  <keepDependencies>false</keepDependencies>
  <definition class="org.jenkinsci.plugins.workflow.cps.CpsScmFlowDefinition" plugin="workflow-cps">
    <scm class="hudson.plugins.git.GitSCM" plugin="git">
      <configVersion>2</configVersion>
      <userRemoteConfigs>
        <hudson.plugins.git.UserRemoteConfig>
          <url>https://github.com/thekaveh/data-eng-lab.git</url>
        </hudson.plugins.git.UserRemoteConfig>
      </userRemoteConfigs>
      <branches>
        <hudson.plugins.git.BranchSpec><name>*/main</name></hudson.plugins.git.BranchSpec>
      </branches>
    </scm>
    <scriptPath>spark-apps/nyc-taxi-etl/Jenkinsfile</scriptPath>
    <lightweight>true</lightweight>
  </definition>
</flow-definition>
```

`jenkins/seed-job.sh` — POST it to Jenkins via REST (uses the Atlas-provided admin creds + host port; idempotent create-or-update):

```bash
#!/usr/bin/env bash
# Seed the nyc-taxi-etl pipeline job into Atlas's Jenkins (which ships JCasC but no jobs).
# Requires: JENKINS_URL (default http://localhost:${JENKINS_PORT:-63080}), JENKINS_ADMIN_USER, JENKINS_ADMIN_PASSWORD.
set -euo pipefail
JENKINS_URL="${JENKINS_URL:-http://localhost:${JENKINS_PORT:-63080}}"
JOB="nyc-taxi-etl"
XML="$(dirname "$0")/nyc-taxi-etl-job.xml"
auth=(-u "${JENKINS_ADMIN_USER:?}:${JENKINS_ADMIN_PASSWORD:?}")
crumb=$(curl -fsS "${auth[@]}" "$JENKINS_URL/crumbIssuer/api/json" | sed -n 's/.*"crumb":"\([^"]*\)".*/\1/p' || true)
hdr=(); [ -n "$crumb" ] && hdr=(-H "Jenkins-Crumb: $crumb")
if curl -fsS "${auth[@]}" "$JENKINS_URL/job/$JOB/api/json" >/dev/null 2>&1; then
  curl -fsS "${auth[@]}" "${hdr[@]}" -H "Content-Type: application/xml" --data-binary "@$XML" "$JENKINS_URL/job/$JOB/config.xml"
else
  curl -fsS "${auth[@]}" "${hdr[@]}" -H "Content-Type: application/xml" --data-binary "@$XML" "$JENKINS_URL/createItem?name=$JOB"
fi
echo "seeded job: $JOB"
```

- [ ] **Step 4: Verify (static)**

Run: `uv run ruff check .` (dag.py clean), `bash -n jenkins/seed-job.sh` + `shellcheck jenkins/seed-job.sh` (no warnings), `uv run python scripts/verify_repo.py --root .` (exit 0), `uv run pytest -m "not infra" -q` (green). The DAG/Jenkins path is live-gated — do NOT execute it.

- [ ] **Step 5: Commit**

```bash
git add spark-apps/nyc-taxi-etl/dag.py spark-apps/nyc-taxi-etl/Jenkinsfile jenkins/
git commit -m "fix(go-live): DAG carries full lakehouse catalog conf (cluster); Jenkins mc-alias publish + seed job"
```

---

### Task 4: Enablement ledger refresh + go-live runbook

**Files:**
- Modify: `docs/atlas-enablement.md`
- Create: `docs/go-live.md`

**Interfaces:**
- Produces: an accurate A1–A9 ledger (A1–A6/A8 delivered with real shapes + key deviations; A7/A9 outstanding) and a runbook for the deferred live validation.

- [ ] **Step 1: Refresh `docs/atlas-enablement.md`**

Read it, then update each item's status:
- **A1 Iceberg REST — DELIVERED** (`apache/iceberg-rest-fixture:1.10.1` + Postgres-JDBC layer → Supabase `iceberg` DB; catalog `lakehouse`; warehouse `s3://lakehouse/`; host port `ICEBERG_REST_PORT`=63020; `iceberg-rest-init` seeds the DB, **no namespaces**).
- **A2 Spark Iceberg runtime — DELIVERED** (Spark **4.1.2**, iceberg-spark-runtime **1.11.0** + iceberg-aws-bundle + hadoop-aws-3.4.2 baked; Spark Connect config injects the full `spark.sql.catalog.lakehouse.*`).
- **A3 Zeppelin interpreter — DELIVERED, with a deviation:** Zeppelin uses **standalone `spark.master=spark://spark-master:7077` (client mode), NOT Spark Connect** (`spark.remote` is explicitly removed — Spark 4 rejects mixing). Record this; our Zeppelin `.zpln` notebooks rely on the seeded interpreter, which is fine.
- **A4 Jupyter clients — DELIVERED** (boto3/s3fs/pyiceberg[s3fs]/duckdb/pyarrow; `SPARK_REMOTE=sc://spark-connect:15002`; PyIceberg auto-config; note Jupyter uses the `jupyter`-scoped MinIO account).
- **A5 Jenkins — DELIVERED** (JDK21 + Maven + `mc`, JCasC, **no jobs/alias shipped** — downstream provides; our `jenkins/seed-job.sh` fills this).
- **A6 Airflow spark-submit — DELIVERED** (`hadoop-aws` baked; `spark_default` cluster + `minio_default`; reference smoke DAG).
- **A8 Track — DELIVERED** (`data-eng` = spark, airflow, jupyterhub, zeppelin, jenkins, supavisor, minio, iceberg-rest, weaviate, neo4j; new `--iceberg-rest-source`/`--jenkins-source` flags, default `disabled`).
- **A7 Trino — OUTSTANDING** and **A9 Redpanda — OUTSTANDING** (keep as the asks for the Atlas worker; data-eng-lab authors these scenarios in Phase 5 against the assumed contract).
Add a short "Key deviations from our original assumptions" list: namespaces not pre-created; Zeppelin standalone (not Connect); warehouse scheme `s3` server-side vs `s3a` client-side; Jenkins ships no jobs.

- [ ] **Step 2: Write `docs/go-live.md`**

A runbook for the deferred live validation:
- **Launch:** `./scripts/start-all.sh` (now includes `--iceberg-rest-source`/`--jenkins-source`); note it brings up ~20+ containers.
- **Bootstrap:** buckets (Atlas + our `lakehouse-test`), `scripts/register_iceberg.py` (creates `bronze`/`silver`/`gold` namespaces host-side — required, Atlas creates none), preflight Layer 1 + Layer 2.
- **Validate live:** `RUN_INFRA=1 uv run pytest -m infra -q` (probes + Layer 2); run a scenario notebook in Zeppelin + JupyterHub; publish the JAR via Jenkins (`jenkins/seed-job.sh` then trigger the job) and confirm `s3a://jars/nyc-taxi-etl/0.1.0/app.jar`; trigger the `nyc_taxi_etl` Airflow DAG and confirm `lakehouse.bronze.nyc_taxi_trips`.
- **Manual steps:** Jenkins job seeding (`seed-job.sh`), Zeppelin JDBC interpreter (one-time UI), enabling sources.
- Cross-link `docs/atlas-enablement.md`.

- [ ] **Step 3: Verify + commit**

Run: `uv run python scripts/verify_repo.py --root .` (exit 0), `uv run ruff check .` (clean), `uv run pytest -m "not infra" -q` (green).

```bash
git add docs/atlas-enablement.md docs/go-live.md
git commit -m "docs(go-live): refresh A1-A9 ledger to delivered reality + add live-validation runbook"
```

---

## Phase 4 exit criteria

- [ ] `infra` pinned at `62eb6df`; `git status` clean after commits.
- [ ] `mvn -q -B -f spark-apps/nyc-taxi-etl/pom.xml package` → BUILD SUCCESS on Spark 4.1.2 (scalatest green, jar produced).
- [ ] `uv run pytest -m "not infra" -q`, `uv run ruff check .`, `uv run python scripts/verify_repo.py --root .` → all green.
- [ ] `shellcheck` clean on touched scripts; DAG/Jenkins/seed authored (live-gated, not executed).
- [ ] PR into `main`: `static-and-unit` + `maven-spark-apps` green; squash-merge. Close issue #7.

## Self-review (against the investigation)

**Reconciliation coverage:** namespaces (Task 2 app self-creates + Task 4 runbook notes register_iceberg) ✓; Spark 4.1.2 (Task 2) ✓; DAG catalog conf (Task 3, from Atlas reference) ✓; Jenkins alias + seed (Task 3) ✓; issue #7 column names + dedup (Task 2) ✓; sources + buckets (Task 1) ✓; ledger + runbook (Task 4) ✓. Deferred (per decision): live launch (runbook only). Not needed: catalog.py warehouse scheme (`s3a://lakehouse/` already matches Spark clients) and host-side root creds (functional) — noted in the ledger, not changed.

**Placeholder scan:** every step has concrete content; the DAG conf is sourced verbatim from Atlas's reference (Task 3 Step 1) rather than invented.

**Consistency:** `nyc-taxi-etl`/`NycTaxiEtl`/`lakehouse.bronze.nyc_taxi_trips`/`s3a://jars/nyc-taxi-etl/0.1.0/app.jar` are identical across the app, DAG, Jenkinsfile, and seed job.
