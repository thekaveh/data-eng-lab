from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / "spark-apps/movielens-feature-pipeline"


def test_jenkins_tests_packages_and_publishes_exact_reviewed_jar():
    text = (APP / "Jenkinsfile").read_text(encoding="utf-8")
    assert text.index("mvn -q -B test") < text.index("mvn -q -B package -DskipTests") < text.index("mc cp")
    assert "target/${APP}-${VERSION}.jar" in text
    assert "${MINIO_BUCKET_ICEBERG_JARS}/${APP}/${VERSION}/app.jar" in text
    assert "MINIO_ICEBERG_ACCESS_KEY" in text and "MINIO_ICEBERG_SECRET_KEY" in text


def test_runbook_freezes_source_features_duplicates_provenance_and_recovery():
    text = (APP / "README.md").read_text(encoding="utf-8")
    for value in (
        "links.csv",
        "genome-scores.csv",
        "movielens_latest_small_ratings",
        "movielens_25m_ratings",
        "userId long",
        "avg_rating double",
        "num_ratings long",
        "movieId long",
        "movie_avg double",
        "popularity long",
        "Duplicate rating rows",
        "data_eng_lab.dataset.plan_id",
        "data_eng_lab.dataset.publication_id",
        "data_eng_lab.dataset.manifest_sha256",
        '"ml_user_features$properties"',
        '"ml_movie_features$properties"',
        "same immutable generation",
        "@daily",
        "max_active_runs=1",
        "Concurrent direct application invocations are unsupported",
        "FINISHED",
        "success=true",
        "s3a://jars/movielens-feature-pipeline/0.1.0/app.jar",
    ):
        assert value in text


def test_downstream_guard_is_executable_and_checks_all_five_keys_before_sql():
    text = (APP / "README.md").read_text(encoding="utf-8")
    for value in (
        '"ml_user_features$properties"',
        '"ml_movie_features$properties"',
        "data_eng_lab.dataset",
        "data_eng_lab.dataset.scale",
        "data_eng_lab.dataset.plan_id",
        "data_eng_lab.dataset.publication_id",
        "data_eng_lab.dataset.manifest_sha256",
        "FULL OUTER JOIN",
        "property_key IS NULL",
        "user_value <> movie_value",
        "fail closed before downstream SQL",
    ):
        assert value in text


def test_runbook_marks_notebooks_outside_production_write_trust_boundary():
    text = (APP / "README.md").read_text(encoding="utf-8")
    assert "not a production write path" in text
    assert "can invalidate downstream provenance" in text
    assert "movielens_feature_pipeline" in text
