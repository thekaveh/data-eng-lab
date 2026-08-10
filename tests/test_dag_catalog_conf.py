"""Guards for production JAR DAG Spark submission and Atlas REST confirmation.

Standalone cluster-mode drivers do NOT inherit spark-connect's catalog defaults, so every JAR
submission must carry its own lakehouse configuration. Atlas keeps ``SparkSubmitOperator`` in
charge of normal execution and OpenLineage injection while an operator-owned hook adapter confirms
the completed driver through the master's :6066 REST API.
"""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _dag_files():
    return sorted((ROOT / "scenarios").rglob("dag.py")) + sorted((ROOT / "spark-apps").rglob("dag.py"))


def _called_name(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _keyword(call: ast.Call, name: str) -> ast.expr | None:
    return next((keyword.value for keyword in call.keywords if keyword.arg == name), None)


def _string_literal(node: ast.expr | None) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _literal_dict_assignment(module: ast.Module, name: str) -> ast.Dict:
    for node in ast.walk(module):
        if not isinstance(node, ast.Assign):
            continue
        if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
            assert isinstance(node.value, ast.Dict), f"{name} must be a literal dict"
            return node.value
    raise AssertionError(f"missing {name} assignment")


def _literal_dict_keys(value: ast.Dict) -> set[str]:
    return {
        key.value
        for key in value.keys
        if isinstance(key, ast.Constant) and isinstance(key.value, str)
    }


def _literal_dict_values(value: ast.Dict) -> dict[str, ast.expr]:
    return {
        key.value: item
        for key, item in zip(value.keys, value.values, strict=True)
        if isinstance(key, ast.Constant) and isinstance(key.value, str)
    }


def _string_or_environment_default(node: ast.expr) -> str | None:
    literal = _string_literal(node)
    if literal is not None:
        return literal
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "get"
        and isinstance(node.func.value, ast.Attribute)
        and isinstance(node.func.value.value, ast.Name)
        and node.func.value.value.id == "os"
        and node.func.value.attr == "environ"
        and len(node.args) >= 2
    ):
        return _string_literal(node.args[1])
    return None


def _imports_name(module: ast.Module, module_name: str, name: str) -> bool:
    return any(
        isinstance(node, ast.ImportFrom)
        and node.module == module_name
        and any(alias.name == name for alias in node.names)
        for node in ast.walk(module)
    )


def _class_definition(module: ast.Module, name: str) -> ast.ClassDef:
    classes = [
        node for node in module.body if isinstance(node, ast.ClassDef) and node.name == name
    ]
    assert len(classes) == 1, f"must define exactly one {name}"
    return classes[0]


def _method_definition(class_node: ast.ClassDef, name: str) -> ast.FunctionDef:
    methods = [
        node for node in class_node.body if isinstance(node, ast.FunctionDef) and node.name == name
    ]
    assert len(methods) == 1, f"{class_node.name} must define exactly one {name} method"
    return methods[0]


def _super_get_hook_call(node: ast.expr) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "_get_hook"
        and isinstance(node.func.value, ast.Call)
        and isinstance(node.func.value.func, ast.Name)
        and node.func.value.func.id == "super"
    )


def test_jar_submitting_dags_carry_lakehouse_catalog_conf():
    offenders = []
    for f in _dag_files():
        text = f.read_text()
        if ("SparkSubmitOperator" in text or "SparkSubmitHook" in text) \
                and "s3a://jars/" in text and "app.jar" in text \
                and "spark.sql.catalog.lakehouse" not in text:
            offenders.append(str(f.relative_to(ROOT)))
    assert not offenders, f"DAGs submit the JAR without lakehouse catalog conf: {offenders}"


def test_cluster_jar_dags_use_operator_owned_rest_confirmation():
    required_catalog_config = (
        "spark.sql.extensions",
        "spark.sql.catalog.lakehouse",
        "spark.sql.catalog.lakehouse.type",
        "spark.sql.catalog.lakehouse.uri",
        "spark.sql.catalog.lakehouse.warehouse",
        "spark.sql.catalog.lakehouse.io-impl",
        "spark.sql.catalog.lakehouse.s3.endpoint",
        "spark.sql.catalog.lakehouse.s3.path-style-access",
        "spark.sql.catalog.lakehouse.s3.access-key-id",
        "spark.sql.catalog.lakehouse.s3.secret-access-key",
        "spark.sql.catalog.lakehouse.client.region",
    )
    required_submission_config = (
        "spark.master",
        "spark.standalone.submit.waitAppCompletion",
        "spark.hadoop.fs.s3a.endpoint",
        "spark.hadoop.fs.s3a.endpoint.region",
        "spark.hadoop.fs.s3a.access.key",
        "spark.hadoop.fs.s3a.secret.key",
        "spark.hadoop.fs.s3a.path.style.access",
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "spark.driverEnv.AWS_ACCESS_KEY_ID",
        "spark.driverEnv.AWS_SECRET_ACCESS_KEY",
        "spark.driverEnv.AWS_REGION",
        "spark.driverEnv.AWS_ENDPOINT_URL_S3",
        "spark.executorEnv.AWS_ACCESS_KEY_ID",
        "spark.executorEnv.AWS_SECRET_ACCESS_KEY",
        "spark.executorEnv.AWS_REGION",
        "spark.executorEnv.AWS_ENDPOINT_URL_S3",
        "spark.eventLog.enabled",
        "spark.eventLog.dir",
    )
    expected_apps = {
        "nyc-taxi-etl": {
            "task_id": "submit_nyc_taxi_etl",
            "application": "s3a://jars/nyc-taxi-etl/0.1.0/app.jar",
            "java_class": "com.thekaveh.dataeng.nyctaxi.NycTaxiEtl",
            "application_args": [
                "s3a://landing/nyc_taxi/",
                "lakehouse.bronze.nyc_taxi_trips",
            ],
        },
        "nyc-taxi-medallion": {
            "task_id": "submit_nyc_taxi_medallion",
            "application": "s3a://jars/nyc-taxi-medallion/0.1.0/app.jar",
            "java_class": "com.thekaveh.dataeng.medallion.NycTaxiMedallion",
            "application_args": ["lakehouse.bronze.nyc_taxi_trips"],
        },
    }
    for path in sorted((ROOT / "spark-apps").rglob("dag.py")):
        module = ast.parse(path.read_text(), filename=str(path))
        expected = expected_apps[path.parent.name]
        assert _imports_name(
            module,
            "airflow.providers.apache.spark.operators.spark_submit",
            "SparkSubmitOperator",
        ), f"{path} must import SparkSubmitOperator from its provider module"
        assert _imports_name(module, "atlas_spark_utils", "RestConfirmingSparkHook"), (
            f"{path} must import Atlas's operator-compatible REST-confirming hook adapter"
        )
        assert not _imports_name(
            module, "airflow.providers.apache.spark.hooks.spark_submit", "SparkSubmitHook"
        ), f"{path} must not restore direct-hook ownership"
        assert not _imports_name(module, "airflow.decorators", "task"), (
            f"{path} must not restore TaskFlow submission ownership"
        )

        operator_class = _class_definition(module, "AtlasSparkSubmitOperator")
        assert len(operator_class.bases) == 1
        assert isinstance(operator_class.bases[0], ast.Name)
        assert operator_class.bases[0].id == "SparkSubmitOperator"
        get_hook = _method_definition(operator_class, "_get_hook")
        returns = [node for node in ast.walk(get_hook) if isinstance(node, ast.Return)]
        assert len(returns) == 1
        adapter_call = returns[0].value
        assert isinstance(adapter_call, ast.Call)
        assert _called_name(adapter_call) == "RestConfirmingSparkHook"
        assert len(adapter_call.args) == 1 and _super_get_hook_call(adapter_call.args[0]), (
            f"{path} must wrap super()._get_hook() with RestConfirmingSparkHook"
        )
        rest_host = _keyword(adapter_call, "rest_host")
        assert (
            isinstance(rest_host, ast.Attribute)
            and isinstance(rest_host.value, ast.Name)
            and rest_host.value.id == "self"
            and rest_host.attr == "rest_host"
        ), f"{path} must pass the operator's mandatory REST host to the adapter"

        operator_calls = [
            node
            for node in ast.walk(module)
            if isinstance(node, ast.Call) and _called_name(node) == "AtlasSparkSubmitOperator"
        ]
        assert len(operator_calls) == 1, (
            f"{path} must instantiate exactly one AtlasSparkSubmitOperator"
        )
        operator_call = operator_calls[0]
        assert _string_literal(_keyword(operator_call, "task_id")) == expected["task_id"]
        assert _string_literal(_keyword(operator_call, "conn_id")) == "spark_default"
        assert _string_literal(_keyword(operator_call, "application")) == expected["application"]
        assert _string_literal(_keyword(operator_call, "java_class")) == expected["java_class"]
        assert _string_literal(_keyword(operator_call, "deploy_mode")) == "cluster"
        assert ast.literal_eval(_keyword(operator_call, "application_args")) == expected[
            "application_args"
        ]
        assert _string_literal(_keyword(operator_call, "rest_host")) == "spark-master"
        conf = _keyword(operator_call, "conf")
        assert isinstance(conf, ast.Name) and conf.id == "spark_conf", (
            f"{path} must pass its spark_conf to AtlasSparkSubmitOperator"
        )

        spark_conf = _literal_dict_assignment(module, "spark_conf")
        spark_conf_keys = _literal_dict_keys(spark_conf)
        spark_conf_values = _literal_dict_values(spark_conf)
        missing_catalog = [key for key in required_catalog_config if key not in spark_conf_keys]
        assert not missing_catalog, (
            f"{path} is missing lakehouse Spark configuration: {missing_catalog}"
        )
        missing_submission = [key for key in required_submission_config if key not in spark_conf_keys]
        assert not missing_submission, (
            f"{path} is missing Spark submission configuration: {missing_submission}"
        )
        assert _string_or_environment_default(spark_conf_values["spark.master"]) == (
            "spark://spark-master:7077"
        )
        assert _string_literal(spark_conf_values["spark.standalone.submit.waitAppCompletion"]) == "true"
        assert _string_literal(spark_conf_values["spark.hadoop.fs.s3a.path.style.access"]) == "true"
        assert _string_literal(spark_conf_values["spark.hadoop.fs.s3a.connection.ssl.enabled"]) == "false"
        assert _string_literal(spark_conf_values["spark.eventLog.enabled"]) == "true"
        assert _string_literal(spark_conf_values["spark.eventLog.dir"]) == "s3a://spark-history/"

def test_parent_dags_can_import_atlas_rest_adapter_from_the_shared_dags_root():
    """The consumer overlay nests parent DAGs below Atlas's `/opt/airflow/dags` mount.

    Atlas supplies ``atlas_spark_utils.py`` at that mount root. Keeping the consumer
    DAGs in a child directory therefore preserves the canonical import documented by
    Atlas, without copying an upstream helper into the parent.
    """
    overlay = (ROOT / "compose" / "data-eng-lab.yml").read_text()
    assert "/opt/airflow/dags/data_eng_lab_spark_apps:ro" in overlay
    for path in sorted((ROOT / "spark-apps").rglob("dag.py")):
        module = ast.parse(path.read_text(), filename=str(path))
        assert _imports_name(module, "atlas_spark_utils", "RestConfirmingSparkHook")
