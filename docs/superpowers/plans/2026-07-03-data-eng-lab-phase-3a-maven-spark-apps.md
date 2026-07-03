# data-eng-lab — Phase 3a: Maven Scala Spark Apps — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish the production **Maven Scala Spark app** pattern (requirement #6): a standard-convention `spark-apps/nyc-taxi-etl` project with **pure, scalatest-covered transforms that run in CI** (JVM + Spark local — genuinely verified, not just authored), a config-driven entrypoint, a `Jenkinsfile` (build→test→shade→publish JAR to MinIO), and an Airflow `dag.py` — plus a verifier check that enforces the spark-app layout.

**Architecture:** The Scala **transforms** are pure `DataFrame -> DataFrame` functions, unit-tested with a local `SparkSession` via scalatest — a new **CI job** runs `mvn test` (real validation). The **entrypoint**, **Jenkinsfile**, and **dag.py** are authored against the assumed Atlas contract (A5 Jenkins, A6 Airflow-spark-submit) and are not executed in CI (they need the live stack); they compile/lint. The Python verifier gains a `_check_spark_apps` structural check.

**Tech Stack:** Java 21, Maven, Scala 2.13, Spark 4.0.0 (compile `provided`; runtime is Atlas's 4.1.x — binary-compatible within 4.x), scalatest; Python verifier; GitHub Actions.

## Global Constraints

- **Never edit `infra/`.**
- **Maven** (not sbt), **Scala 2.13**, **Spark 4.0.0** as the compile/test version via a `spark.version` pom property (Atlas runtime is 4.1.x; Spark deps are `provided` scope). Java 21 (Spark 4 needs JDK 17+).
- **Spark-on-JDK-21 requires `--add-opens`** JVM flags for the test runner — the pom sets them in the scalatest plugin `argLine`. Without them, a local `SparkSession` throws `InaccessibleObjectException`.
- **App layout:** `spark-apps/<app>/` with `pom.xml`, `src/main/scala/...`, `src/test/scala/...`, `Jenkinsfile`, `dag.py`, `README.md`. Transforms live in a `transforms/` package (pure, no `main`).
- **Published JAR:** `s3a://jars/<app>/<version>/app.jar` (Jenkins step — authored/gated).
- **CI:** a new `maven-spark-apps` job runs `mvn -q -B test` (scalatest, Spark local). The Python `static-and-unit` job is unchanged. Both become required checks on `main`.
- **Python verifier** additions are ruff/pytest CI-green now.
- **Branch/PR:** `main` is protected; land via feature branch → PR with the required checks green.

## File Structure

- `scripts/verify_repo.py` + `scripts/verify_repo_config.yaml` — `_check_spark_apps` + config.
- `spark-apps/nyc-taxi-etl/pom.xml` — Maven build.
- `spark-apps/nyc-taxi-etl/src/main/scala/com/thekaveh/dataeng/nyctaxi/transforms/TaxiTransforms.scala` — pure transforms.
- `spark-apps/nyc-taxi-etl/src/main/scala/com/thekaveh/dataeng/nyctaxi/NycTaxiEtl.scala` — entrypoint.
- `spark-apps/nyc-taxi-etl/src/test/scala/com/thekaveh/dataeng/nyctaxi/TaxiTransformsSpec.scala` — scalatest.
- `spark-apps/nyc-taxi-etl/src/main/resources/{application.conf,log4j2.properties}`.
- `spark-apps/nyc-taxi-etl/{Jenkinsfile,dag.py,README.md}`.
- `.github/workflows/ci.yml` (maven job), `Makefile` (`build-apps`), `docs/spark-apps.md`.

---

### Task 1: Verifier `_check_spark_apps` structural check

**Files:**
- Modify: `scripts/verify_repo.py`, `scripts/verify_repo_config.yaml`
- Modify: `tests/test_verify_repo.py`

**Interfaces:**
- Produces: `_discover_spark_apps(root) -> list[str]` (dir names under `spark-apps/`, excluding `.`/`_`) and `_check_spark_apps(root, cfg) -> list[Finding]` — for each app, emits `spark_app.files` errors for missing entries in `cfg["spark_app_required_files"]` (`pom.xml`, `src/main/scala`, `Jenkinsfile`, `dag.py`).

- [ ] **Step 1: Write the failing test**

Add to `tests/test_verify_repo.py`:

```python
def _make_valid_spark_app(root: Path, name: str = "nyc-taxi-etl"):
    d = root / "spark-apps" / name
    (d / "src" / "main" / "scala").mkdir(parents=True)
    (d / "pom.xml").write_text("<project/>")
    (d / "Jenkinsfile").write_text("pipeline {}")
    (d / "dag.py").write_text("# dag\n")
    return d


SPARK_CFG = dict(CFG, spark_app_required_files=["pom.xml", "src/main/scala", "Jenkinsfile", "dag.py"])


def test_valid_spark_app_passes(tmp_path: Path):
    _make_valid_spark_app(tmp_path)
    errors = [f for f in verify_repo.run_checks(tmp_path, SPARK_CFG) if f.severity == "error"]
    assert errors == [], errors


def test_spark_app_missing_pom_flags_error(tmp_path: Path):
    d = _make_valid_spark_app(tmp_path)
    (d / "pom.xml").unlink()
    findings = verify_repo.run_checks(tmp_path, SPARK_CFG)
    assert any(f.check == "spark_app.files" and f.severity == "error" for f in findings), findings


def test_no_spark_apps_dir_is_ok(tmp_path: Path):
    errors = [f for f in verify_repo.run_checks(tmp_path, SPARK_CFG) if f.severity == "error"]
    assert errors == []
```

- [ ] **Step 2: Run to verify RED**

Run: `uv run pytest tests/test_verify_repo.py -q` → FAIL (`_check_spark_apps` missing / not registered).

- [ ] **Step 3: Implement the check**

In `scripts/verify_repo.py` add:

```python
def _discover_spark_apps(root: Path) -> list[str]:
    base = root / "spark-apps"
    if not base.is_dir():
        return []
    return sorted(p.name for p in base.iterdir()
                  if p.is_dir() and not p.name.startswith((".", "_")))


def _check_spark_apps(root: Path, cfg: dict) -> list[Finding]:
    findings: list[Finding] = []
    required = cfg.get("spark_app_required_files", [])
    for name in _discover_spark_apps(root):
        adir = root / "spark-apps" / name
        for rel in required:
            if not (adir / rel).exists():
                findings.append(Finding("spark_app.files", "error",
                                        f"spark-app '{name}' is missing '{rel}'"))
    return findings
```

Update `CHECKS = [_check_scenarios, _check_spark_apps, _check_dataset_registry]`.

Add to `scripts/verify_repo_config.yaml`:

```yaml
spark_app_required_files:
  - pom.xml
  - src/main/scala
  - Jenkinsfile
  - dag.py
```

- [ ] **Step 4: Run tests + verifier + lint**

Run: `uv run pytest tests/test_verify_repo.py -q` → PASS.
Run: `uv run python scripts/verify_repo.py --root .` → exit 0.
Run: `uv run pytest -m "not infra" -q` and `uv run ruff check .` → green.

- [ ] **Step 5: Commit**

```bash
git add scripts/verify_repo.py scripts/verify_repo_config.yaml tests/test_verify_repo.py
git commit -m "feat(verify): enforce spark-app structure (pom/src/Jenkinsfile/dag)"
```

---

### Task 2: Maven project skeleton (`pom.xml` + resources + README)

**Files:**
- Create: `spark-apps/nyc-taxi-etl/pom.xml`, `spark-apps/nyc-taxi-etl/src/main/resources/application.conf`, `spark-apps/nyc-taxi-etl/src/main/resources/log4j2.properties`, `spark-apps/nyc-taxi-etl/README.md`, and an empty `spark-apps/nyc-taxi-etl/src/main/scala/.gitkeep`

**Interfaces:**
- Produces: a buildable Maven project (`mvn -q -B validate` succeeds) with Spark 4.0.0 `provided`, scalatest, shade, and the JDK-21 `--add-opens` test argLine.

- [ ] **Step 1: Create `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.thekaveh.dataeng</groupId>
  <artifactId>nyc-taxi-etl</artifactId>
  <version>0.1.0</version>
  <packaging>jar</packaging>

  <properties>
    <maven.compiler.release>21</maven.compiler.release>
    <scala.binary.version>2.13</scala.binary.version>
    <scala.version>2.13.14</scala.version>
    <spark.version>4.0.0</spark.version>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <sparkAddOpens>--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED</sparkAddOpens>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-sql_${scala.binary.version}</artifactId>
      <version>${spark.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.scalatest</groupId>
      <artifactId>scalatest_${scala.binary.version}</artifactId>
      <version>3.2.19</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>net.alchim31.maven</groupId>
        <artifactId>scala-maven-plugin</artifactId>
        <version>4.9.2</version>
        <executions>
          <execution>
            <goals><goal>compile</goal><goal>testCompile</goal></goals>
          </execution>
        </executions>
        <configuration>
          <scalaVersion>${scala.version}</scalaVersion>
        </configuration>
      </plugin>

      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>3.2.5</version>
        <configuration><skipTests>true</skipTests></configuration>
      </plugin>

      <plugin>
        <groupId>org.scalatest</groupId>
        <artifactId>scalatest-maven-plugin</artifactId>
        <version>2.2.0</version>
        <configuration>
          <reportsDirectory>${project.build.directory}/surefire-reports</reportsDirectory>
          <argLine>${sparkAddOpens}</argLine>
        </configuration>
        <executions>
          <execution><id>test</id><goals><goal>test</goal></goals></execution>
        </executions>
      </plugin>

      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.6.0</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <createDependencyReducedPom>false</createDependencyReducedPom>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

`src/main/resources/application.conf`:

```hocon
nyc-taxi-etl {
  landing-uri = "s3a://landing/nyc_taxi/"
  bronze-table = "lakehouse.bronze.nyc_taxi_trips"
}
```

`src/main/resources/log4j2.properties`:

```properties
rootLogger.level = warn
rootLogger.appenderRef.stdout.ref = console
appender.console.type = Console
appender.console.name = console
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{HH:mm:ss} %-5p %c{1} - %m%n
```

`README.md` (6-section, mirrors the scenario convention):

```markdown
# nyc-taxi-etl

A Maven Scala Spark app: raw NYC-taxi Parquet in `s3a://landing/nyc_taxi/` → cleaned Iceberg
`lakehouse.bronze.nyc_taxi_trips`. Built by Jenkins, run by Airflow (`spark-submit`).

## 1. Scenario summary
## 2. Why this exists
## 3. What's in the app
Pure transforms (`transforms/TaxiTransforms`), a config-driven entrypoint (`NycTaxiEtl`), scalatest,
a `Jenkinsfile`, and an Airflow `dag.py`.
## 4. How to run
`mvn -q -B test` (unit); `mvn -q -B package` (shaded JAR); Jenkins publishes it, Airflow submits it.
## 5. Data & dependencies
Spark 4 (`provided`), scalatest. Runtime: Atlas Spark + Iceberg.
## 6. Known issues & caveats
The Jenkins/Airflow path is live-gated on Atlas A5/A6.
```

Create the empty scala dir marker: `src/main/scala/.gitkeep`.

- [ ] **Step 2: Validate the build resolves**

Run: `cd spark-apps/nyc-taxi-etl && mvn -q -B validate && cd -`
Expected: BUILD SUCCESS (POM is well-formed; plugins/deps resolvable). If Maven isn't installed locally, install it (`brew install maven`) or note it — CI will validate regardless.

- [ ] **Step 3: Verifier still passes**

Run: `uv run python scripts/verify_repo.py --root .` — note: the app currently LACKS `Jenkinsfile`/`dag.py` (added in Task 4), so `_check_spark_apps` will report `spark_app.files` errors. That is expected mid-phase; to keep the tree green, this task adds placeholder `Jenkinsfile` and `dag.py` stubs now (they're fleshed out in Task 4):

`Jenkinsfile` (stub): `// placeholder — implemented in Task 4`
`dag.py` (stub):
```python
"""Airflow DAG for nyc-taxi-etl (implemented in Task 4)."""
from __future__ import annotations
```
Then `uv run python scripts/verify_repo.py --root .` → exit 0, and `uv run ruff check .` → clean.

- [ ] **Step 4: Commit**

```bash
git add spark-apps/nyc-taxi-etl
git commit -m "feat(spark-apps): nyc-taxi-etl Maven skeleton (pom + resources + stubs)"
```

---

### Task 3: Pure transforms + scalatest (the CI-verified core)

**Files:**
- Create: `spark-apps/nyc-taxi-etl/src/main/scala/com/thekaveh/dataeng/nyctaxi/transforms/TaxiTransforms.scala`
- Create: `spark-apps/nyc-taxi-etl/src/test/scala/com/thekaveh/dataeng/nyctaxi/TaxiTransformsSpec.scala`

**Interfaces:**
- Produces: `object TaxiTransforms` with `def clean(df: DataFrame): DataFrame` — drops rows with null `pickup_datetime`, filters `passenger_count > 0`, dedupes, and adds a `trip_date` column (date of `pickup_datetime`). Pure; no I/O.

- [ ] **Step 1: Write the failing scalatest**

`src/test/scala/com/thekaveh/dataeng/nyctaxi/TaxiTransformsSpec.scala`:

```scala
package com.thekaveh.dataeng.nyctaxi

import java.sql.Timestamp

import com.thekaveh.dataeng.nyctaxi.transforms.TaxiTransforms
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TaxiTransformsSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession.builder().appName("test").master("local[2]")
      .config("spark.ui.enabled", "false").getOrCreate()

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def ts(s: String): Timestamp = Timestamp.valueOf(s)

  test("clean drops null pickup, non-positive passengers, and dupes; adds trip_date") {
    val s = spark
    import s.implicits._
    val raw = Seq(
      (ts("2023-01-01 10:00:00"), 2, 5.0),
      (ts("2023-01-01 10:00:00"), 2, 5.0),   // duplicate
      (null.asInstanceOf[Timestamp], 1, 3.0), // null pickup -> dropped
      (ts("2023-01-02 11:00:00"), 0, 4.0)     // passenger_count 0 -> dropped
    ).toDF("pickup_datetime", "passenger_count", "fare_amount")

    val out = TaxiTransforms.clean(raw)
    assert(out.count() == 1)
    val row = out.select("trip_date").as[java.sql.Date].collect().head
    assert(row.toString == "2023-01-01")
  }
}
```

- [ ] **Step 2: Run to verify RED**

Run: `cd spark-apps/nyc-taxi-etl && mvn -q -B test ; cd -`
Expected: FAIL — `TaxiTransforms` does not exist (compile error).

- [ ] **Step 3: Implement the transform**

`src/main/scala/com/thekaveh/dataeng/nyctaxi/transforms/TaxiTransforms.scala`:

```scala
package com.thekaveh.dataeng.nyctaxi.transforms

import org.apache.spark.sql.{DataFrame, functions => F}

object TaxiTransforms {

  /** Clean raw taxi trips: drop null pickups + non-positive passenger counts, dedupe, add trip_date. */
  def clean(df: DataFrame): DataFrame =
    df.where(F.col("pickup_datetime").isNotNull && (F.col("passenger_count") > 0))
      .dropDuplicates()
      .withColumn("trip_date", F.to_date(F.col("pickup_datetime")))
}
```

- [ ] **Step 4: Run to verify GREEN**

Run: `cd spark-apps/nyc-taxi-etl && mvn -q -B test ; cd -`
Expected: BUILD SUCCESS, `TaxiTransformsSpec` passes (1 test). If it fails with `InaccessibleObjectException`, confirm the `--add-opens` argLine in the pom is applied (Task 2).

- [ ] **Step 5: Commit**

```bash
git add spark-apps/nyc-taxi-etl/src
git commit -m "feat(spark-apps): TaxiTransforms.clean + scalatest (Spark local)"
```

---

### Task 4: Entrypoint + Jenkinsfile + DAG (authored against A5/A6)

**Files:**
- Create: `spark-apps/nyc-taxi-etl/src/main/scala/com/thekaveh/dataeng/nyctaxi/NycTaxiEtl.scala`
- Modify: `spark-apps/nyc-taxi-etl/Jenkinsfile`, `spark-apps/nyc-taxi-etl/dag.py` (flesh out the Task-2 stubs)

**Interfaces:**
- Produces: `object NycTaxiEtl { def main(args: Array[String]): Unit }` — reads the landing Parquet, applies `TaxiTransforms.clean`, writes the Iceberg bronze table via `writeTo(...).using("iceberg").createOrReplace()`. Compiles in CI; runs only on the live stack.

- [ ] **Step 1: Implement the entrypoint**

`NycTaxiEtl.scala`:

```scala
package com.thekaveh.dataeng.nyctaxi

import com.thekaveh.dataeng.nyctaxi.transforms.TaxiTransforms
import org.apache.spark.sql.SparkSession

object NycTaxiEtl {
  def main(args: Array[String]): Unit = {
    val landing = if (args.length > 0) args(0) else "s3a://landing/nyc_taxi/"
    val table = if (args.length > 1) args(1) else "lakehouse.bronze.nyc_taxi_trips"

    val spark = SparkSession.builder().appName("nyc-taxi-etl").getOrCreate()
    try {
      val cleaned = TaxiTransforms.clean(spark.read.parquet(landing))
      cleaned.writeTo(table).using("iceberg").createOrReplace()
      // scalastyle:off println
      println(s"wrote $table: ${spark.table(table).count()} rows")
      // scalastyle:on println
    } finally spark.stop()
  }
}
```

Compile-check: `cd spark-apps/nyc-taxi-etl && mvn -q -B test-compile && cd -` → BUILD SUCCESS.

- [ ] **Step 2: Flesh out the `Jenkinsfile`** (authored against A5 — Jenkins server; not run in CI)

```groovy
pipeline {
  agent any
  environment {
    APP = 'nyc-taxi-etl'
    VERSION = '0.1.0'
  }
  stages {
    stage('Build & Test') { steps { dir("spark-apps/${APP}") { sh 'mvn -q -B test' } } }
    stage('Package')      { steps { dir("spark-apps/${APP}") { sh 'mvn -q -B package -DskipTests' } } }
    stage('Publish JAR')  {
      steps {
        dir("spark-apps/${APP}") {
          sh 'mc cp target/${APP}-${VERSION}.jar minio/jars/${APP}/${VERSION}/app.jar'
        }
      }
    }
  }
}
```

- [ ] **Step 3: Flesh out `dag.py`** (authored against A6 — Airflow spark-submit; live-gated)

```python
"""Airflow DAG: run the nyc-taxi-etl JAR (published to s3a://jars) on the Spark cluster."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="nyc_taxi_etl",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-eng-lab", "spark-app"],
) as dag:
    SparkSubmitOperator(
        task_id="submit_nyc_taxi_etl",
        conn_id="spark_default",
        application="s3a://jars/nyc-taxi-etl/0.1.0/app.jar",
        java_class="com.thekaveh.dataeng.nyctaxi.NycTaxiEtl",
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
    )
```

- [ ] **Step 4: Verify structure + lint**

Run: `uv run python scripts/verify_repo.py --root .` → exit 0 (all four required files present).
Run: `uv run ruff check .` → clean (`dag.py` imports are used; ruff does not import airflow).
Run: `uv run pytest -m "not infra" -q` → green (Python suite unaffected).

- [ ] **Step 5: Commit**

```bash
git add spark-apps/nyc-taxi-etl
git commit -m "feat(spark-apps): NycTaxiEtl entrypoint + Jenkinsfile + Airflow dag (A5/A6)"
```

---

### Task 5: CI Maven job + `make build-apps` + docs

**Files:**
- Modify: `.github/workflows/ci.yml`, `Makefile`, `tests/test_makefile.py`
- Create: `docs/spark-apps.md`; modify `README.md`

**Interfaces:**
- Produces: a `maven-spark-apps` CI job that runs `mvn -q -B test`; `make build-apps` (mvn package). After merge, `maven-spark-apps` is added to the branch's required checks (controller action).

- [ ] **Step 1: Add the CI job**

In `.github/workflows/ci.yml`, add a job alongside `static-and-unit`:

```yaml
  maven-spark-apps:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5  # v4
        with:
          submodules: false
          persist-credentials: false
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: '21'
          cache: maven
      - name: Unit tests (scalatest, Spark local)
        run: mvn -q -B -f spark-apps/nyc-taxi-etl/pom.xml test
```

- [ ] **Step 2: `make build-apps` + its test**

Add to `tests/test_makefile.py`:

```python
def test_build_apps_target_runs_maven():
    text = subprocess.run(["make", "-npq"], cwd=ROOT, capture_output=True, text=True).stdout
    assert "mvn" in text and "spark-apps" in text
```

Add `build-apps` to `TARGETS`. In the `Makefile`, add to `.PHONY` and:

```makefile
build-apps: ## Build (test + shade) the Maven Spark apps
	mvn -q -B -f spark-apps/nyc-taxi-etl/pom.xml package
```

- [ ] **Step 3: Docs**

Create `docs/spark-apps.md`:

```markdown
# Spark apps (Maven / Scala)

Production Spark jobs live in `spark-apps/<app>/` as standard Maven Scala projects: pure transforms
(scalatest-covered, run in CI via `mvn test`), a config-driven entrypoint, a `Jenkinsfile`, and an
Airflow `dag.py`.

## Loop (requirement #6)
Jenkins builds + tests the app, shades a JAR, and publishes it to `s3a://jars/<app>/<version>/app.jar`;
an Airflow DAG `spark-submit`s that JAR on the cluster (reads `landing`, writes Iceberg). The Jenkins +
Airflow steps are authored against Atlas A5/A6 and run once those land; the **transforms are CI-verified now**.

## Build locally
```bash
make build-apps        # mvn package (test + shaded jar)
mvn -q -B -f spark-apps/nyc-taxi-etl/pom.xml test
```
```

Add a README §2 link to `docs/spark-apps.md`.

- [ ] **Step 4: Run Python checks, then push and confirm BOTH CI jobs**

Run: `uv run pytest -m "not infra" -q`, `uv run ruff check .`, `uv run python scripts/verify_repo.py --root .` → all green.
Commit, push the branch, and confirm on the PR that BOTH `static-and-unit` AND `maven-spark-apps` pass. **The `maven-spark-apps` job is the risk point** — if it fails (dependency resolution, `--add-opens`, Scala/Spark version), read the logs and iterate on the pom/workflow until green (this is expected; Spark-on-CI often needs a tweak). Do NOT leave the Maven job red.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml Makefile tests/test_makefile.py docs/spark-apps.md README.md
git commit -m "ci(spark-apps): mvn test job + make build-apps + docs"
```

---

## Phase 3a exit criteria

- [ ] `uv run pytest -m "not infra" -q` → all pass (verifier spark-app check + makefile).
- [ ] `mvn -q -B -f spark-apps/nyc-taxi-etl/pom.xml test` → `TaxiTransformsSpec` passes (Spark local).
- [ ] `uv run python scripts/verify_repo.py --root .` → exit 0; `uv run ruff check .` → clean.
- [ ] PR into `main`: **both** `static-and-unit` and `maven-spark-apps` green; squash-merge; add `maven-spark-apps` to the required checks.

## Self-review (against the spec §5)

**Spec coverage:** Maven Scala Spark app with standard conventions (Tasks 2–4 ✓); pure transforms unit-tested (Task 3 ✓, CI-verified); config-driven entrypoint (Task 4 ✓); Jenkinsfile publishing the JAR to a MinIO bucket (Task 4 ✓, gated); Airflow DAG running it via spark-submit (Task 4 ✓, gated); verifier enforcement (Task 1 ✓); CI + docs (Task 5 ✓). The full build→publish→run loop is live-gated on A5/A6; the **transforms are genuinely CI-verified now**.

**Risks:** the `maven-spark-apps` CI job (Spark-on-JDK21 needs `--add-opens`; Spark 4.0.0 must resolve from Central) is the one iteration point — Task 5 explicitly iterates it to green. Spark compile version (4.0.0) vs Atlas runtime (4.1.x) is binary-compatible within 4.x (`provided` scope).

**Placeholder scan:** the Task-2 `Jenkinsfile`/`dag.py` stubs are explicitly fleshed out in Task 4 (a sequenced build-up, not a leftover placeholder); every step has complete, runnable content.

**Type/name consistency:** `_discover_spark_apps`/`_check_spark_apps`, `TaxiTransforms.clean`, `NycTaxiEtl.main` are used identically across defining and consuming steps.
