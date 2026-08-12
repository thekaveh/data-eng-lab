from __future__ import annotations

from pathlib import Path

import pytest

from datasets.locking import canonical_json
from datasets.publication import ResolvedDataset, ResolvedObject

RESULT = ResolvedDataset(
    dataset="tpch",
    scale="tiny",
    plan_id="1" * 64,
    manifest_sha256="2" * 64,
    publication_id="0123456789ab4def8123456789abcdef",
    objects=(ResolvedObject("orders.parquet", "s3://landing/g/orders.parquet", 3, "3" * 64, "orders"),),
)


@pytest.fixture
def cli_module():
    import scripts.resolve_dataset as module

    return module


def test_run_uses_host_client_and_returns_exact_canonical_bytes(cli_module, monkeypatch, tmp_path):
    registry = {"tpch": object()}
    client = object()
    captured = {}
    monkeypatch.setattr(cli_module, "load_registry", lambda path: registry)
    monkeypatch.setattr(cli_module, "s3_client_from_env", lambda path: client)

    def resolve(actual_client, actual_registry, dataset, scale):
        captured.update(client=actual_client, registry=actual_registry, dataset=dataset, scale=scale)
        return RESULT

    monkeypatch.setattr(cli_module, "resolve_active_dataset", resolve)
    body = cli_module.run("tpch", "tiny", tmp_path / "registry.yaml", tmp_path / "infra")
    assert body == canonical_json(
        {
            "dataset": "tpch",
            "manifest_sha256": "2" * 64,
            "objects": [
                {
                    "object_name": "orders.parquet",
                    "schema_id": "orders",
                    "sha256": "3" * 64,
                    "size_bytes": 3,
                    "uri": "s3://landing/g/orders.parquet",
                }
            ],
            "plan_id": "1" * 64,
            "publication_id": "0123456789ab4def8123456789abcdef",
            "scale": "tiny",
        }
    )
    assert captured == {"client": client, "registry": registry, "dataset": "tpch", "scale": "tiny"}


@pytest.mark.parametrize(
    ("argv", "environment", "expected"),
    [
        (["tpch", "--scale", "tiny"], "medium", "tiny"),
        (["tpch"], "medium", "medium"),
        (["tpch"], None, "small"),
    ],
)
def test_scale_precedence_is_explicit_then_environment_then_small(
    cli_module, monkeypatch, capsysbinary, argv, environment, expected
):
    if environment is None:
        monkeypatch.delenv("DATASET_SCALE", raising=False)
    else:
        monkeypatch.setenv("DATASET_SCALE", environment)
    calls = []
    monkeypatch.setattr(
        cli_module,
        "run",
        lambda dataset, scale, *_args: calls.append((dataset, scale)) or b'{"ok":true}',
    )
    assert cli_module.main(argv) == 0
    assert calls == [("tpch", expected)]
    assert capsysbinary.readouterr().out == b'{"ok":true}'


def test_cli_requires_dataset(cli_module):
    with pytest.raises(SystemExit) as caught:
        cli_module.main([])
    assert caught.value.code == 2


def test_cli_rejects_invalid_environment_scale(cli_module, monkeypatch, capsys):
    monkeypatch.setenv("DATASET_SCALE", "large")
    with pytest.raises(SystemExit) as caught:
        cli_module.main(["tpch"])
    assert caught.value.code == 2
    assert "DATASET_SCALE must be one of: tiny, small, medium" in capsys.readouterr().err


def test_explicit_scale_does_not_inspect_lower_priority_environment(cli_module, monkeypatch, capsysbinary):
    monkeypatch.setenv("DATASET_SCALE", "intentionally-invalid")
    calls = []
    monkeypatch.setattr(
        cli_module,
        "run",
        lambda dataset, scale, *_args: calls.append((dataset, scale)) or b'{"ok":true}',
    )
    assert cli_module.main(["tpch", "--scale", "tiny"]) == 0
    assert calls == [("tpch", "tiny")]
    assert capsysbinary.readouterr().out == b'{"ok":true}'


def test_cli_failure_is_nonzero_redacted_and_stdout_empty(cli_module, monkeypatch, capsysbinary):
    secret = "http://localhost:61234/?secret=value"
    monkeypatch.setattr(cli_module, "run", lambda *_args: (_ for _ in ()).throw(RuntimeError(secret)))
    assert cli_module.main(["tpch"]) == 1
    captured = capsysbinary.readouterr()
    assert captured.out == b""
    assert captured.err == b"dataset resolution failed\n"
    assert secret.encode() not in captured.err


@pytest.mark.parametrize("error", [BrokenPipeError("closed"), OSError("secret-output-path")])
def test_cli_stdout_write_failure_exits_cleanly_without_traceback(cli_module, monkeypatch, capsysbinary, error):
    monkeypatch.setattr(cli_module, "run", lambda *_args: b'{"ok":true}')
    monkeypatch.setattr(cli_module.sys.stdout.buffer, "write", lambda _body: (_ for _ in ()).throw(error))
    assert cli_module.main(["tpch"]) == 1
    captured = capsysbinary.readouterr()
    assert captured.out == b""
    assert b"Traceback" not in captured.err
    assert b"secret-output-path" not in captured.err


def test_cli_defaults_are_repository_registry_and_infra(cli_module):
    parser = cli_module._parser()
    args = parser.parse_args(["tpch"])
    assert Path(args.registry) == cli_module.ROOT / "datasets" / "registry.yaml"
    assert Path(args.infra_dir) == cli_module.ROOT / "infra"
