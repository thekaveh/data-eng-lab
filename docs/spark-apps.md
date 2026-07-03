# Spark apps (Maven / Scala)

Production Spark jobs live in `spark-apps/<app>/` as standard Maven Scala projects: pure transforms
(scalatest-covered, run in CI via `mvn test`), an argument-driven entrypoint, a `Jenkinsfile`, and an
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
