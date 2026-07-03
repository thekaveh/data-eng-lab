# data-eng-lab — Phase 11: Productionized Medallion Maven App — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a second Maven Scala Spark app — `spark-apps/nyc-taxi-medallion` — that productionizes the medallion transform (bronze→silver→gold), with **scalatest run in CI** (like `nyc-taxi-etl`). This closes the gap where the medallion scenario's DAG referenced a non-existent JAR. Also generalize the DAG-conf guard, build both apps in CI, and apply the deferred doc nits.

**Architecture:** The new app mirrors `spark-apps/nyc-taxi-etl` (same pom/Jenkinsfile/dag pattern) with medallion logic: pure `MedallionTransforms.silver`/`.gold` (scalatest-covered, CI-verified) + a `NycTaxiMedallion` entrypoint (reads `bronze.nyc_taxi_trips` → `silver.nyc_taxi_trips` → `gold.nyc_taxi_daily`, matching the `medallion-nyc_taxi` notebook). The CI `maven-spark-apps` job builds ALL `spark-apps/*/pom.xml`; the `test_dag_catalog_conf` guard is generalized to any `s3a://jars` submitter.

**Tech Stack:** Java 17, Maven, Scala 2.13, Spark 4.1.2 (`provided`), scalatest; Python guard/CI.

## Global Constraints

- **Never edit `infra/`.** Java 17, Scala 2.13, Spark 4.1.2 (mirror `nyc-taxi-etl/pom.xml` exactly — same versions, `--add-opens` argLine, shade/scalatest plugins — change only `artifactId` → `nyc-taxi-medallion`).
- **Medallion logic matches the `medallion-nyc_taxi-spark-iceberg` scenario:** silver = `dropDuplicates("tpep_pickup_datetime","tpep_dropoff_datetime")`; gold = `groupBy(trip_date).agg(count("*") as trips, avg(fare_amount) as avg_fare)`; targets `lakehouse.silver.nyc_taxi_trips` + `lakehouse.gold.nyc_taxi_daily`; source `lakehouse.bronze.nyc_taxi_trips`.
- **Package** `com.thekaveh.dataeng.medallion`; entrypoint `com.thekaveh.dataeng.medallion.NycTaxiMedallion`; jar `s3a://jars/nyc-taxi-medallion/0.1.0/app.jar`.
- **CI must stay green** (`static-and-unit` + `maven-spark-apps`); the maven job now builds both apps. Java 17 locally (Maven + JDK 17 installed).
- **DAGs that submit a JAR carry the full `spark.sql.catalog.lakehouse.*` conf** (cluster mode) — the guard test enforces this generally.
- **Branch/PR:** both required checks green; **push before merge.**

## File Structure

- `spark-apps/nyc-taxi-medallion/{pom.xml, README.md, Jenkinsfile, dag.py, src/main/resources/log4j2.properties, src/main/scala/com/thekaveh/dataeng/medallion/{NycTaxiMedallion.scala, transforms/MedallionTransforms.scala}, src/test/scala/com/thekaveh/dataeng/medallion/MedallionTransformsSpec.scala}`.
- `tests/test_dag_catalog_conf.py` — generalized guard.
- `.github/workflows/ci.yml`, `Makefile`, `docs/spark-apps.md` — build both + docs.
- `scenarios/medallion-nyc_taxi-spark-iceberg/dag.py`, `scenarios/{incremental_upsert,scd2}-online_retail-spark-iceberg/README.md` — comment/doc nits.

---

### Task 1: Medallion app skeleton + transforms + scalatest (CI-verified core)

**Files:**
- Create: `spark-apps/nyc-taxi-medallion/pom.xml` (copy `spark-apps/nyc-taxi-etl/pom.xml`, change `<artifactId>` to `nyc-taxi-medallion`), `spark-apps/nyc-taxi-medallion/src/main/resources/log4j2.properties` (copy), `spark-apps/nyc-taxi-medallion/README.md`.
- Create: `spark-apps/nyc-taxi-medallion/src/main/scala/com/thekaveh/dataeng/medallion/transforms/MedallionTransforms.scala`
- Create: `spark-apps/nyc-taxi-medallion/src/test/scala/com/thekaveh/dataeng/medallion/MedallionTransformsSpec.scala`

**Interfaces:**
- Produces: `object MedallionTransforms { def silver(df: DataFrame): DataFrame; def gold(df: DataFrame): DataFrame }`.

- [ ] **Step 1: Skeleton** — copy `nyc-taxi-etl/pom.xml` → `nyc-taxi-medallion/pom.xml`, change ONLY `<artifactId>nyc-taxi-etl</artifactId>` → `<artifactId>nyc-taxi-medallion</artifactId>`. Copy `log4j2.properties`. Write a 6-section README (mirrors `nyc-taxi-etl/README.md`: this app productionizes the medallion transform; built by Jenkins, run by Airflow).

- [ ] **Step 2: scalatest (RED)** — `MedallionTransformsSpec.scala`:

```scala
package com.thekaveh.dataeng.medallion

import java.sql.{Date, Timestamp}

import com.thekaveh.dataeng.medallion.transforms.MedallionTransforms
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class MedallionTransformsSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession.builder().appName("test").master("local[2]")
      .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").getOrCreate()

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def ts(s: String): Timestamp = Timestamp.valueOf(s)

  test("silver dedupes on pickup+dropoff; gold aggregates trips + avg_fare by trip_date") {
    val s = spark
    import s.implicits._
    val bronze = Seq(
      (ts("2023-01-01 10:00:00"), ts("2023-01-01 10:30:00"), 5.0, Date.valueOf("2023-01-01")),
      (ts("2023-01-01 10:00:00"), ts("2023-01-01 10:30:00"), 5.0, Date.valueOf("2023-01-01")), // dup
      (ts("2023-01-01 11:00:00"), ts("2023-01-01 11:20:00"), 7.0, Date.valueOf("2023-01-01"))
    ).toDF("tpep_pickup_datetime", "tpep_dropoff_datetime", "fare_amount", "trip_date")

    val silver = MedallionTransforms.silver(bronze)
    assert(silver.count() == 2) // one duplicate collapsed

    val gold = MedallionTransforms.gold(silver)
    val row = gold.where($"trip_date" === Date.valueOf("2023-01-01")).collect().head
    assert(row.getAs[Long]("trips") == 2)
    assert(math.abs(row.getAs[Double]("avg_fare") - 6.0) < 1e-9)
  }
}
```

Run: `cd spark-apps/nyc-taxi-medallion && mvn -q -B test ; cd -` → FAIL (`MedallionTransforms` missing). (First `mvn` reuses cached Spark 4.1.2 jars.)

- [ ] **Step 3: transforms (GREEN)** — `MedallionTransforms.scala`:

```scala
package com.thekaveh.dataeng.medallion.transforms

import org.apache.spark.sql.{DataFrame, functions => F}

object MedallionTransforms {

  /** Silver: dedupe bronze on the natural trip key. */
  def silver(df: DataFrame): DataFrame =
    df.dropDuplicates("tpep_pickup_datetime", "tpep_dropoff_datetime")

  /** Gold: daily trip counts + average fare. */
  def gold(df: DataFrame): DataFrame =
    df.groupBy(F.col("trip_date"))
      .agg(F.count("*").as("trips"), F.avg(F.col("fare_amount")).as("avg_fare"))
}
```

Run: `cd spark-apps/nyc-taxi-medallion && mvn -q -B test ; cd -` → PASS.

- [ ] **Step 4: Python gate + commit** — `uv run pytest -m "not infra" -q`, `uv run ruff check .`, `uv run python scripts/verify_repo.py --root .` all green (the verifier `spark_app.files` check requires `pom.xml`+`src/main/scala`+`Jenkinsfile`+`dag.py` — Jenkinsfile/dag.py come in Task 2; to keep the tree green, add STUB `Jenkinsfile` (`// placeholder — Task 2`) and `dag.py` (`"""placeholder — Task 2."""\nfrom __future__ import annotations`) now). Then commit:

```bash
git add spark-apps/nyc-taxi-medallion
git commit -m "feat(spark-apps): nyc-taxi-medallion app skeleton + MedallionTransforms + scalatest"
```

---

### Task 2: Entrypoint + DAG + Jenkinsfile

**Files:**
- Create: `spark-apps/nyc-taxi-medallion/src/main/scala/com/thekaveh/dataeng/medallion/NycTaxiMedallion.scala`
- Modify: `spark-apps/nyc-taxi-medallion/Jenkinsfile`, `spark-apps/nyc-taxi-medallion/dag.py` (replace the Task-1 stubs)

- [ ] **Step 1: Entrypoint** — `NycTaxiMedallion.scala`:

```scala
package com.thekaveh.dataeng.medallion

import com.thekaveh.dataeng.medallion.transforms.MedallionTransforms
import org.apache.spark.sql.SparkSession

object NycTaxiMedallion {
  def main(args: Array[String]): Unit = {
    val bronzeTable = if (args.length > 0) args(0) else "lakehouse.bronze.nyc_taxi_trips"
    val spark = SparkSession.builder().appName("nyc-taxi-medallion").getOrCreate()
    try {
      val bronze = spark.table(bronzeTable)
      val silver = MedallionTransforms.silver(bronze)
      spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
      silver.writeTo("lakehouse.silver.nyc_taxi_trips").using("iceberg").createOrReplace()
      val gold = MedallionTransforms.gold(silver)
      spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
      gold.writeTo("lakehouse.gold.nyc_taxi_daily").using("iceberg").createOrReplace()
    } finally spark.stop()
  }
}
```

Compile-check: `cd spark-apps/nyc-taxi-medallion && mvn -q -B package ; cd -` → BUILD SUCCESS; `target/nyc-taxi-medallion-0.1.0.jar` produced.

- [ ] **Step 2: Jenkinsfile** — copy `spark-apps/nyc-taxi-etl/Jenkinsfile`, change `APP = 'nyc-taxi-etl'` → `APP = 'nyc-taxi-medallion'` (everything else — the `mc alias set atlas` + publish to `$MINIO_BUCKET_ICEBERG_JARS/${APP}/${VERSION}/app.jar` — is identical).

- [ ] **Step 3: dag.py** — copy `spark-apps/nyc-taxi-etl/dag.py`, change ONLY: `dag_id="nyc_taxi_medallion"`; `application="s3a://jars/nyc-taxi-medallion/0.1.0/app.jar"`; `java_class="com.thekaveh.dataeng.medallion.NycTaxiMedallion"`; `application_args=["lakehouse.bronze.nyc_taxi_trips"]`; `spark.app.name` (in conf) → `"nyc-taxi-medallion"`; `task_id` → `"submit_nyc_taxi_medallion"`. Keep the ENTIRE `spark.sql.catalog.lakehouse.*` + s3a + deploy_mode conf identical (this is what the guard checks).

- [ ] **Step 4: verify + commit** — `uv run ruff check .` (dag.py clean), `uv run python scripts/verify_repo.py --root .` (exit 0 — all 4 spark-app files present now), `uv run pytest -m "not infra" -q` + `uv run pytest tests/test_dag_catalog_conf.py -q` green (the new dag.py submits a jar AND carries the catalog conf).

```bash
git add spark-apps/nyc-taxi-medallion
git commit -m "feat(spark-apps): NycTaxiMedallion entrypoint + DAG (catalog conf) + Jenkinsfile"
```

---

### Task 3: Build both apps in CI + generalize the guard + Makefile

**Files:**
- Modify: `.github/workflows/ci.yml`, `tests/test_dag_catalog_conf.py`, `Makefile`

- [ ] **Step 1: Generalize the guard (RED-safe)** — in `tests/test_dag_catalog_conf.py`, change the condition from the hardcoded `nyc-taxi-etl/0.1.0/app.jar` to any published jar:

```python
        if "SparkSubmitOperator" in text and "s3a://jars/" in text and "app.jar" in text \
                and "spark.sql.catalog.lakehouse" not in text:
```

Update the module docstring to say "any dag.py that spark-submits a `s3a://jars/…app.jar`". Run `uv run pytest tests/test_dag_catalog_conf.py -q` → PASS (both nyc-taxi-etl and nyc-taxi-medallion DAGs carry the conf).

- [ ] **Step 2: CI builds all apps** — in `.github/workflows/ci.yml`, replace the `maven-spark-apps` run step `mvn -q -B -f spark-apps/nyc-taxi-etl/pom.xml package` with a loop over every app pom:

```yaml
      - name: Build + test all Maven Spark apps (scalatest + shade)
        run: |
          for pom in spark-apps/*/pom.xml; do
            echo "::group::$pom"; mvn -q -B -f "$pom" package; echo "::endgroup::"
          done
```

- [ ] **Step 3: Makefile** — update the `build-apps` target to loop over all app poms (mirror the CI loop). Keep `tests/test_makefile.py` passing (it greps for `mvn` + `spark-apps`).

- [ ] **Step 4: verify + commit** — `uv run pytest -m "not infra" -q` (incl. makefile + guard) green; `uv run ruff check .` clean; `shellcheck` not needed (yaml). Locally: `for p in spark-apps/*/pom.xml; do mvn -q -B -f "$p" package; done` → both BUILD SUCCESS.

```bash
git add .github/workflows/ci.yml tests/test_dag_catalog_conf.py Makefile
git commit -m "ci(spark-apps): build+test all apps; generalize DAG-conf guard to any s3a://jars submitter"
```

---

### Task 4: Docs + deferred nits

**Files:**
- Modify: `docs/spark-apps.md`; `scenarios/medallion-nyc_taxi-spark-iceberg/dag.py`; `scenarios/incremental_upsert-online_retail-spark-iceberg/README.md`; `scenarios/scd2-online_retail-spark-iceberg/README.md`

- [ ] **Step 1: docs/spark-apps.md** — add `nyc-taxi-medallion` alongside `nyc-taxi-etl` (productionizes the medallion transform; same Jenkins→JAR→Airflow loop; scalatest CI-verified).

- [ ] **Step 2: medallion scenario DAG comment** — the `scenarios/medallion-nyc_taxi-spark-iceberg/dag.py` is an `EmptyOperator` placeholder whose comment says the productionized JAR is "a future deliverable". Update that comment to point at the now-real app: `# Productionized as spark-apps/nyc-taxi-medallion (dag_id nyc_taxi_medallion). This scenario placeholder demonstrates the transform via the notebooks.` (Keep it an `EmptyOperator` — do not add a SparkSubmitOperator here; the productionized DAG lives in spark-apps.)

- [ ] **Step 3: deferred re-run doc nits** — in the `incremental_upsert` and `scd2` scenario READMEs (§6 Known issues), add one line each: the notebook's seed `INSERT` is not guarded, so re-running the full notebook accumulates seed rows — run on a fresh table (drop the target first) for a clean demo.

- [ ] **Step 4: verify + commit** — `uv run python scripts/verify_repo.py --root .` exit 0; `uv run ruff check .` clean; `uv run pytest -m "not infra" -q` green; `uv run pytest tests/test_dag_catalog_conf.py -q` (medallion scenario dag still EmptyOperator → not flagged).

```bash
git add docs/spark-apps.md scenarios/medallion-nyc_taxi-spark-iceberg/dag.py scenarios/incremental_upsert-online_retail-spark-iceberg/README.md scenarios/scd2-online_retail-spark-iceberg/README.md
git commit -m "docs: document nyc-taxi-medallion app; point medallion scenario at it; upsert/scd2 re-run notes"
```

---

## Phase 11 exit criteria

- [ ] `mvn -q -B -f spark-apps/nyc-taxi-medallion/pom.xml package` → BUILD SUCCESS (scalatest green, jar produced).
- [ ] `uv run pytest -m "not infra" -q`, `uv run ruff check .`, `uv run python scripts/verify_repo.py --root .` → all green; `test_dag_catalog_conf` guard passes (generalized).
- [ ] CI `maven-spark-apps` builds BOTH apps green.
- [ ] PR into `main`: both required checks green; **push the branch, confirm CI, then squash-merge.**

## Self-review

**Coverage:** a second CI-verified Maven app (`nyc-taxi-medallion`) productionizing the medallion transform — the last CI-verifiable artifact; closes the medallion-DAG placeholder gap. Generalized guard + build-all CI keep future apps covered. Deferred Phase-8/10 doc nits applied.

**Consistency:** the app's silver/gold logic + targets (`silver.nyc_taxi_trips`, `gold.nyc_taxi_daily`) match the `medallion-nyc_taxi` notebook — the medallion "3 delivery modes" (notebook / DAG / JAR) now agree. The dag.py carries the full catalog conf (guard-enforced).

**Placeholder scan:** the Task-1 `Jenkinsfile`/`dag.py` stubs are fleshed out in Task 2 (sequenced, not leftover). The medallion SCENARIO dag stays an `EmptyOperator` by design (its productionized form is the spark-apps DAG).
