from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / "spark-apps" / "gh-archive-pipeline"


def test_jenkins_tests_packages_then_publishes_exact_jar():
    text = (APP / "Jenkinsfile").read_text()
    assert text.index("mvn -q -B test") < text.index("mvn -q -B package -DskipTests") < text.index("mc cp")
    assert "target/${APP}-${VERSION}.jar" in text
    assert 'atlas/${MINIO_BUCKET_ICEBERG_JARS}/${APP}/${VERSION}/app.jar' in text


def test_runbook_freezes_coupled_contract_and_recovery():
    text = (APP / "README.md").read_text()
    for value in (
        "lakehouse.silver.gh_events", "lakehouse.silver.gh_sessions",
        "com.thekaveh.dataeng.gharchive.GhArchiveFlatten",
        "com.thekaveh.dataeng.gharchive.GhArchiveSessionization",
        "s3a://jars/gh-archive-pipeline/0.1.0/app.jar",
        "yyyy-MM-dd'T'HH:mm:ss'Z'", "(created_at, id)", "max_active_runs=1",
        'data_eng_lab.dataset=gh_archive', '"gh_events$properties"', '"gh_sessions$properties"',
        "Concurrent direct", "same immutable generation", "not a production write path",
    ):
        assert value in text
    assert ".readStream(" not in text


def test_flatten_main_runs_bounded_raw_preflight_before_spark_json_inference():
    flatten = (APP / "src/main/scala/com/thekaveh/dataeng/gharchive/GhArchiveFlatten.scala").read_text()
    preflight = (APP / "src/main/scala/com/thekaveh/dataeng/gharchive/GhArchiveRawPreflight.scala").read_text()
    assert flatten.index("GhArchiveRawPreflight.validate(spark, sources)") < flatten.index(
        'spark.read.option("mode", "FAILFAST").json'
    )
    for phrase in (
        "MaxLineBytes",
        "MaxDepth",
        "MaxRecords",
        "MaxExpandedBytes",
        "STRICT_DUPLICATE_DETECTION",
        "ExpectedLocks",
        "MessageDigest",
        "GZIPInputStream",
    ):
        assert phrase in preflight
