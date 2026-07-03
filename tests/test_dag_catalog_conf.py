"""Guard: any dag.py that spark-submits a `s3a://jars/…app.jar` must pass the lakehouse catalog conf.
Standalone cluster-mode drivers do NOT inherit spark-connect's catalog defaults, so the DAG must
carry spark.sql.catalog.lakehouse.* itself (see Phase 4 reconciliation)."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _dag_files():
    return sorted((ROOT / "scenarios").rglob("dag.py")) + sorted((ROOT / "spark-apps").rglob("dag.py"))


def test_jar_submitting_dags_carry_lakehouse_catalog_conf():
    offenders = []
    for f in _dag_files():
        text = f.read_text()
        if "SparkSubmitOperator" in text and "s3a://jars/" in text and "app.jar" in text \
                and "spark.sql.catalog.lakehouse" not in text:
            offenders.append(str(f.relative_to(ROOT)))
    assert not offenders, f"DAGs submit the JAR without lakehouse catalog conf: {offenders}"
