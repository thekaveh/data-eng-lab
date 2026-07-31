"""Guards for production JAR DAG Spark submission and Atlas #792/#880 confirmation.

Standalone cluster-mode drivers do NOT inherit spark-connect's catalog defaults, so every JAR
submission must carry its own lakehouse configuration. Atlas #880 provides the provider-compatible
#792 path: construct ``SparkSubmitHook`` without an application, then submit, extract the driver
ID from the submission log, and confirm through the master's :6066 REST API with Atlas's helper.
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


def _is_taskflow_decorator(decorator: ast.expr) -> bool:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    return isinstance(target, ast.Name) and target.id == "task"


def _taskflow_function(module: ast.Module) -> ast.FunctionDef:
    tasks = [
        node for node in ast.walk(module)
        if isinstance(node, ast.FunctionDef)
        and any(_is_taskflow_decorator(decorator) for decorator in node.decorator_list)
    ]
    assert len(tasks) == 1, "production JAR DAG must define exactly one TaskFlow task"
    return tasks[0]


def _assignment_to(node: ast.stmt, name: str) -> ast.Assign | None:
    if isinstance(node, ast.Assign) and any(
        isinstance(target, ast.Name) and target.id == name for target in node.targets
    ):
        return node
    return None


def _imports_or_calls_operator(module: ast.Module) -> bool:
    for node in ast.walk(module):
        if isinstance(node, ast.ImportFrom) and any(
            alias.name == "SparkSubmitOperator" for alias in node.names
        ):
            return True
        if isinstance(node, ast.Call) and _called_name(node) == "SparkSubmitOperator":
            return True
    return False


def test_jar_submitting_dags_carry_lakehouse_catalog_conf():
    offenders = []
    for f in _dag_files():
        text = f.read_text()
        if ("SparkSubmitOperator" in text or "SparkSubmitHook" in text) \
                and "s3a://jars/" in text and "app.jar" in text \
                and "spark.sql.catalog.lakehouse" not in text:
            offenders.append(str(f.relative_to(ROOT)))
    assert not offenders, f"DAGs submit the JAR without lakehouse catalog conf: {offenders}"


def test_cluster_jar_dags_use_atlas_792_hook_and_rest_confirmation():
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
    for path in sorted((ROOT / "spark-apps").rglob("dag.py")):
        module = ast.parse(path.read_text(), filename=str(path))
        assert not _imports_or_calls_operator(module), (
            f"{path} must not import or instantiate SparkSubmitOperator"
        )
        assert _imports_name(
            module, "airflow.providers.apache.spark.hooks.spark_submit", "SparkSubmitHook"
        ), f"{path} must import SparkSubmitHook from its provider module"

        task_function = _taskflow_function(module)
        task_body = task_function.body
        hook_assignments = [
            (index, assignment)
            for index, statement in enumerate(task_body)
            if (assignment := _assignment_to(statement, "hook")) is not None
            and isinstance(assignment.value, ast.Call)
            and _called_name(assignment.value) == "SparkSubmitHook"
        ]
        assert len(hook_assignments) == 1, (
            f"{path} must assign exactly one SparkSubmitHook constructor to hook"
        )
        hook_index, hook_assignment = hook_assignments[0]
        hook_call = hook_assignment.value
        assert isinstance(hook_call, ast.Call)
        assert _string_literal(_keyword(hook_call, "conn_id")) == "spark_default"
        assert _string_literal(_keyword(hook_call, "deploy_mode")) == "cluster"
        assert _keyword(hook_call, "application") is None, (
            f"{path} must pass the application to Atlas's helper, not SparkSubmitHook"
        )
        conf = _keyword(hook_call, "conf")
        assert isinstance(conf, ast.Name) and conf.id == "spark_conf", (
            f"{path} must pass its spark_conf to SparkSubmitHook"
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

        assert _imports_name(module, "atlas_spark_utils", "submit_and_confirm_via_rest"), (
            f"{path} must import Atlas's provider-compatible submission helper"
        )
        submissions = [
            statement.value
            for statement in task_body[hook_index + 1:]
            if isinstance(statement, ast.Expr)
            and isinstance(statement.value, ast.Call)
            and _called_name(statement.value) == "submit_and_confirm_via_rest"
            and statement.value.args
            and isinstance(statement.value.args[0], ast.Name)
            and statement.value.args[0].id == "hook"
            and _string_literal(_keyword(statement.value, "application")) is not None
            and _string_literal(_keyword(statement.value, "rest_host")) == "spark-master"
        ]
        assert len(submissions) == 1, (
            f"{path} must use Atlas's canonical Spark submit-and-confirm helper"
        )


def test_parent_dags_can_import_atlas_880_helper_from_the_shared_dags_root():
    """The consumer overlay nests parent DAGs below Atlas's `/opt/airflow/dags` mount.

    Atlas #883 supplies ``atlas_spark_utils.py`` at that mount root. Keeping the
    consumer DAGs in a child directory therefore preserves the canonical direct
    import documented by Atlas, without copying an upstream helper into the parent.
    """
    overlay = (ROOT / "compose" / "data-eng-lab.yml").read_text()
    assert "/opt/airflow/dags/data_eng_lab_spark_apps:ro" in overlay
    for path in sorted((ROOT / "spark-apps").rglob("dag.py")):
        module = ast.parse(path.read_text(), filename=str(path))
        assert _imports_name(module, "atlas_spark_utils", "submit_and_confirm_via_rest")
