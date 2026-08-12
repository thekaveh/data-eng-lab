from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from datasets.registry import GeneratorScale, load_registry, resolve_scale
from datasets.sources import tpch
from datasets.verification import LockMismatch, VerifiedFile

ROOT = Path(__file__).resolve().parents[2]
TABLES = (
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
)


def _bytes(table: str) -> bytes:
    return f"locked-{table}\n".encode()


@pytest.fixture
def tpch_plan():
    plan = resolve_scale(load_registry(ROOT / "datasets" / "registry.yaml")["tpch"], "tiny")
    assert plan.generator_scale is not None
    outputs = tuple(
        replace(
            output,
            size_bytes=len(_bytes(output.table)),
            sha256=hashlib.sha256(_bytes(output.table)).hexdigest(),
        )
        for output in plan.generator_scale.outputs
    )
    return replace(plan, generator_scale=replace(plan.generator_scale, outputs=outputs))


class FakeRunner:
    def __init__(self, plan):
        contract = plan.dataset.generator
        assert contract is not None
        self.evidence = tpch.ImageEvidence(
            image_id="sha256:" + "1" * 64,
            base_image=contract.environment.image,
            base_image_digest=contract.environment.image_digest,
            platform=contract.environment.platform,
            entrypoint=tpch.CANONICAL_ENTRYPOINT,
            environment=tpch.canonical_environment(contract),
            labels=tpch.canonical_labels(contract),
            uv_lock_sha256=contract.environment.uv_lock_sha256,
            duckdb_version=contract.engine_version,
            duckdb_wheel_sha256=contract.engine_wheel_sha256,
            tpch_extension_sha256=contract.extension_sha256,
            requirements_sha256=hashlib.sha256(
                (ROOT / "datasets" / "tpch-lock-requirements.txt").read_bytes()
            ).hexdigest(),
            exporter_sha256=hashlib.sha256((ROOT / "datasets" / "tpch_lock_export.py").read_bytes()).hexdigest(),
            base_rootfs_match=True,
            uses_hashed_requirements=True,
        )
        self.metadata_environment = tpch.expected_metadata_environment(contract)
        self.ensure_calls = []
        self.run_args = None
        self.fail_after = None
        self.raw_metadata = None

    def ensure_image(self, contract):
        self.ensure_calls.append(contract)
        return self.evidence

    def run(self, contract, scale, output_root, metadata_path):
        self.run_args = tpch.ContainerRunArgs(
            platform=contract.environment.platform,
            network="none",
            scale=tpch.canonical_scale(scale.scale_factor),
            output_root=output_root,
            metadata_path=metadata_path,
        )
        outputs = {}
        for output in scale.outputs:
            if output.table == self.fail_after:
                raise RuntimeError("simulated container failure")
            path = output_root / output.object_name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(_bytes(output.table))
            outputs[output.table] = {
                "object_name": output.object_name,
                "size_bytes": output.size_bytes,
                "sha256": output.sha256,
            }
        metadata_path.write_text(
            self.raw_metadata
            or yaml.safe_dump(
                {
                    "scale_factor": scale.scale_factor,
                    "environment": self.metadata_environment,
                    "outputs": outputs,
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )


@pytest.fixture
def fake_runner(tpch_plan):
    return FakeRunner(tpch_plan)


@pytest.fixture(autouse=True)
def accept_test_parquet_schemas(monkeypatch):
    calls = []
    monkeypatch.setattr(tpch, "verify_physical_schema", lambda *args: calls.append(args), raising=False)
    return calls


def test_generate_tpch_uses_network_none_and_exact_platform(tmp_path, fake_runner, tpch_plan):
    tpch.generate_tpch(tpch_plan, tmp_path, runner=fake_runner)

    assert fake_runner.run_args.platform == "linux/amd64"
    assert fake_runner.run_args.network == "none"


def test_generate_tpch_returns_eight_verified_files_in_registry_order(
    tmp_path, fake_runner, tpch_plan, accept_test_parquet_schemas
):
    files = tpch.generate_tpch(tpch_plan, tmp_path, runner=fake_runner)

    assert isinstance(files, tuple)
    assert all(isinstance(item, VerifiedFile) for item in files)
    assert tuple(item.path.name for item in files) == tuple(
        output.object_name for output in tpch_plan.generator_scale.outputs
    )
    assert tuple(item.expected.schema_id for item in files) == TABLES
    assert len(accept_test_parquet_schemas) == 16
    assert not list(tmp_path.glob(".dataset-tpch-*"))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("base_image", "python:drifted"),
        ("image_id", "not-a-digest"),
        ("image_id", True),
        ("base_image_digest", "sha256:" + "0" * 64),
        ("platform", "linux/arm64"),
        ("entrypoint", ("python", "bad.py")),
        ("uv_lock_sha256", "0" * 64),
        ("duckdb_version", "1.5.3"),
        ("duckdb_wheel_sha256", "0" * 64),
        ("tpch_extension_sha256", "0" * 64),
        ("requirements_sha256", "0" * 64),
        ("exporter_sha256", "0" * 64),
        ("base_rootfs_match", False),
        ("uses_hashed_requirements", False),
    ],
)
def test_generate_tpch_rejects_image_evidence_drift(tmp_path, fake_runner, tpch_plan, field, value):
    fake_runner.evidence = replace(fake_runner.evidence, **{field: value})

    with pytest.raises(LockMismatch, match=field):
        tpch.generate_tpch(tpch_plan, tmp_path, runner=fake_runner)

    assert not list(tmp_path.glob("*.parquet"))


def test_generate_tpch_rejects_image_environment_drift(tmp_path, fake_runner, tpch_plan):
    fake_runner.evidence.environment["TZ"] = "Europe/London"
    with pytest.raises(LockMismatch, match="environment"):
        tpch.generate_tpch(tpch_plan, tmp_path, runner=fake_runner)


def test_generate_tpch_rejects_image_label_drift(tmp_path, fake_runner, tpch_plan):
    fake_runner.evidence.labels["io.data-eng-lab.tpch.contract"] = "drifted"
    with pytest.raises(LockMismatch, match="labels"):
        tpch.generate_tpch(tpch_plan, tmp_path, runner=fake_runner)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("duckdb_version", "1.5.3"),
        ("duckdb_wheel_sha256", "0" * 64),
        ("uv_lock_sha256", "0" * 64),
        ("tpch_extension_sha256", "0" * 64),
        ("locale", "en_US.UTF-8"),
        ("timezone", "Europe/London"),
        ("threads", 4),
        ("preserve_insertion_order", False),
        ("format", "csv"),
        ("compression", "snappy"),
        ("row_group_size", 10),
    ],
)
def test_generate_tpch_rejects_metadata_environment_drift(tmp_path, fake_runner, tpch_plan, field, value):
    fake_runner.metadata_environment[field] = value

    with pytest.raises(LockMismatch, match=field):
        tpch.generate_tpch(tpch_plan, tmp_path, runner=fake_runner)

    assert not list(tmp_path.glob("*.parquet"))


def test_generate_tpch_rejects_scale_factor_drift(tmp_path, fake_runner, tpch_plan):
    fake_runner.run = lambda contract, scale, output_root, metadata_path: metadata_path.write_text(
        yaml.safe_dump(
            {
                "scale_factor": 1.0,
                "environment": fake_runner.metadata_environment,
                "outputs": {},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    with pytest.raises(LockMismatch, match="scale_factor"):
        tpch.generate_tpch(tpch_plan, tmp_path / "scale", runner=fake_runner)


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("scale_factor", "true"),
        ("scale_factor", "1"),
        ("threads", "true"),
        ("threads", "1.0"),
        ("preserve_insertion_order", "1"),
    ],
)
def test_generate_tpch_rejects_metadata_type_confusion(tmp_path, fake_runner, tpch_plan, field, replacement):
    original_run = FakeRunner.run

    def drift_type(contract, scale, output_root, metadata_path):
        original_run(fake_runner, contract, scale, output_root, metadata_path)
        text = metadata_path.read_text(encoding="utf-8")
        if field == "scale_factor":
            text = text.replace("scale_factor: 0.01", f"scale_factor: {replacement}")
        else:
            text = re.sub(rf"^  {field}: .*$", f"  {field}: {replacement}", text, flags=re.MULTILINE)
        metadata_path.write_text(text, encoding="utf-8")

    fake_runner.run = drift_type
    with pytest.raises(LockMismatch, match=field):
        tpch.generate_tpch(tpch_plan, tmp_path, runner=fake_runner)


@pytest.mark.parametrize(
    "raw_metadata",
    [
        "scale_factor: 0.01\nscale_factor: 0.01\nenvironment: {}\noutputs: {}\n",
        "scale_factor: 0.01\nenvironment:\n  threads: 1\n  threads: 1\noutputs: {}\n",
        "scale_factor: 0.01\nenvironment: {}\noutputs:\n  customer: {}\n  customer: {}\n",
    ],
)
def test_generate_tpch_rejects_duplicate_yaml_mapping_keys(tmp_path, fake_runner, tpch_plan, raw_metadata):
    fake_runner.raw_metadata = raw_metadata
    with pytest.raises(LockMismatch, match="duplicate metadata key"):
        tpch.generate_tpch(tpch_plan, tmp_path, runner=fake_runner)


def test_generate_tpch_rejects_unexpected_output_name(tmp_path, fake_runner, tpch_plan):
    original_run = FakeRunner.run
    fake_runner.run = lambda contract, scale, output_root, metadata_path: (
        original_run(fake_runner, contract, scale, output_root, metadata_path),
        (output_root / "unexpected.parquet").write_bytes(b"unexpected"),
    )
    with pytest.raises(LockMismatch, match="object_names"):
        tpch.generate_tpch(tpch_plan, tmp_path / "names", runner=fake_runner)


def test_generate_tpch_rejects_output_byte_drift(tmp_path, fake_runner, tpch_plan):
    original_run = FakeRunner.run

    def corrupt_run(contract, scale, output_root, metadata_path):
        original_run(fake_runner, contract, scale, output_root, metadata_path)
        (output_root / scale.outputs[0].object_name).write_bytes(b"corrupt")

    fake_runner.run = corrupt_run
    with pytest.raises(LockMismatch, match="size_bytes|sha256"):
        tpch.generate_tpch(tpch_plan, tmp_path / "bytes", runner=fake_runner)


def test_generate_tpch_rejects_physical_schema_drift(tmp_path, fake_runner, tpch_plan, monkeypatch):
    original_run = FakeRunner.run
    fake_runner.run = lambda contract, scale, output_root, metadata_path: original_run(
        fake_runner, contract, scale, output_root, metadata_path
    )
    monkeypatch.setattr(
        tpch,
        "verify_physical_schema",
        lambda *args: (_ for _ in ()).throw(LockMismatch(args[2], "schema fingerprint", "locked", "drifted")),
    )
    with pytest.raises(LockMismatch, match="schema fingerprint"):
        tpch.generate_tpch(tpch_plan, tmp_path / "schema", runner=fake_runner)


def test_generate_tpch_is_all_or_nothing_on_runner_or_publication_failure(
    tmp_path, fake_runner, tpch_plan, monkeypatch
):
    destination = tmp_path / "runner"
    fake_runner.fail_after = "part"
    with pytest.raises(RuntimeError, match="container failure"):
        tpch.generate_tpch(tpch_plan, destination, runner=fake_runner)
    assert list(destination.iterdir()) == []

    fake_runner.fail_after = None
    destination = tmp_path / "publication"
    real_link = tpch.os.link
    links = 0

    def fail_second_link(source, target, **kwargs):
        nonlocal links
        links += 1
        if links == 2:
            raise OSError("simulated publication failure")
        real_link(source, target, **kwargs)

    monkeypatch.setattr(tpch.os, "link", fail_second_link)
    with pytest.raises(OSError, match="publication failure"):
        tpch.generate_tpch(tpch_plan, destination, runner=fake_runner)
    assert list(destination.iterdir()) == []


def test_publication_rollback_preserves_concurrent_replacement(tmp_path, fake_runner, tpch_plan, monkeypatch):
    destination = tmp_path / "publication-race"
    real_link = tpch.os.link
    links = 0

    def replace_then_fail(source, target, **kwargs):
        nonlocal links
        links += 1
        if links == 2:
            first = destination / tpch_plan.generator_scale.outputs[0].object_name
            first.unlink()
            first.write_bytes(b"foreign replacement")
            raise OSError("simulated second-link failure")
        return real_link(source, target, **kwargs)

    monkeypatch.setattr(tpch.os, "link", replace_then_fail)
    with pytest.raises(OSError, match="second-link failure"):
        tpch.generate_tpch(tpch_plan, destination, runner=fake_runner)

    first = destination / tpch_plan.generator_scale.outputs[0].object_name
    assert first.read_bytes() == b"foreign replacement"
    assert not list(destination.glob(".dataset-cleanup-*"))


def test_publication_rollback_tracks_actual_link_after_source_swap(tmp_path, fake_runner, tpch_plan, monkeypatch):
    destination = tmp_path / "source-swap"
    real_link = tpch.os.link
    links = 0

    def swap_source_before_first_link(source, target, **kwargs):
        nonlocal links
        links += 1
        if links == 1:
            source_descriptor = kwargs["src_dir_fd"]
            os.unlink(source, dir_fd=source_descriptor)
            descriptor = os.open(source, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=source_descriptor)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(b"swapped after verification")
        return real_link(source, target, **kwargs)

    monkeypatch.setattr(tpch.os, "link", swap_source_before_first_link)
    with pytest.raises(ValueError, match="size_bytes|sha256"):
        tpch.generate_tpch(tpch_plan, destination, runner=fake_runner)

    assert list(destination.iterdir()) == []


def test_publication_rejects_source_symlink_swap_without_residue(tmp_path, fake_runner, tpch_plan, monkeypatch):
    destination = tmp_path / "source-symlink-swap"
    foreign = tmp_path / "foreign.parquet"
    foreign.write_bytes(b"foreign")
    real_link = tpch.os.link
    swapped = False

    def swap_source_to_symlink(source, target, **kwargs):
        nonlocal swapped
        if not swapped:
            swapped = True
            source_descriptor = kwargs["src_dir_fd"]
            os.unlink(source, dir_fd=source_descriptor)
            os.symlink(foreign, source, dir_fd=source_descriptor)
        return real_link(source, target, **kwargs)

    monkeypatch.setattr(tpch.os, "link", swap_source_to_symlink)
    with pytest.raises(ValueError, match="regular|identity"):
        tpch.generate_tpch(tpch_plan, destination, runner=fake_runner)

    assert list(destination.iterdir()) == []
    assert foreign.read_bytes() == b"foreign"


def test_publication_rollback_uses_bound_directory_after_destination_replacement(
    tmp_path, fake_runner, tpch_plan, monkeypatch
):
    destination = tmp_path / "destination"
    displaced = tmp_path / "displaced"
    real_link = tpch.os.link
    links = 0

    def replace_directory_then_fail(source, target, **kwargs):
        nonlocal links
        links += 1
        if links == 2:
            raise OSError("simulated failure after destination replacement")
        result = real_link(source, target, **kwargs)
        if links == 1:
            destination.rename(displaced)
            destination.mkdir()
        return result

    monkeypatch.setattr(tpch.os, "link", replace_directory_then_fail)
    with pytest.raises((OSError, ValueError)):
        tpch.generate_tpch(tpch_plan, destination, runner=fake_runner)

    assert list(displaced.iterdir()) == []
    assert list(destination.iterdir()) == []


def test_transaction_staging_is_sibling_and_rejects_destination_rebind_during_creation(
    tmp_path, fake_runner, tpch_plan, monkeypatch
):
    destination = tmp_path / "destination"
    displaced = tmp_path / "displaced"
    replacement_marker = destination / "foreign"
    real_mkdir = tpch.os.mkdir
    replaced = False

    def rebind_before_staging(path, mode=0o777, *, dir_fd=None):
        nonlocal replaced
        name = os.fsdecode(path)
        if Path(name).name.startswith(".dataset-tpch-") and not replaced:
            replaced = True
            destination.rename(displaced)
            destination.mkdir()
            replacement_marker.write_bytes(b"foreign")
        return real_mkdir(path, mode, dir_fd=dir_fd)

    monkeypatch.setattr(tpch.os, "mkdir", rebind_before_staging)

    with pytest.raises(ValueError, match="destination directory changed"):
        tpch.generate_tpch(tpch_plan, destination, runner=fake_runner)

    assert replacement_marker.read_bytes() == b"foreign"
    assert list(displaced.iterdir()) == []
    assert not list(tmp_path.glob(".dataset-tpch-*"))


def test_destination_rebind_after_final_rename_rolls_back_bound_outputs(tmp_path, fake_runner, tpch_plan, monkeypatch):
    destination = tmp_path / "destination"
    displaced = tmp_path / "displaced"
    replacement_marker = destination / "foreign"
    real_rename = tpch._renameat_noreplace
    rebound = False

    def rebind_after_final(directory_descriptor, source, target):
        nonlocal rebound
        real_rename(directory_descriptor, source, target)
        if target == "supplier.parquet" and not rebound:
            rebound = True
            destination.rename(displaced)
            destination.mkdir()
            replacement_marker.write_bytes(b"foreign")

    monkeypatch.setattr(tpch, "_renameat_noreplace", rebind_after_final)

    with pytest.raises(ValueError, match="destination directory changed"):
        tpch.generate_tpch(tpch_plan, destination, runner=fake_runner)

    assert replacement_marker.read_bytes() == b"foreign"
    assert list(displaced.iterdir()) == []
    assert not list(tmp_path.glob(".dataset-tpch-*"))


def test_publication_reverifies_staged_bytes_and_schema(tmp_path, fake_runner, tpch_plan, accept_test_parquet_schemas):
    tpch.generate_tpch(tpch_plan, tmp_path, runner=fake_runner)

    assert len(accept_test_parquet_schemas) == 16
    staged = accept_test_parquet_schemas[8:]
    assert all(str(call[0].path).startswith("/dev/fd/") for call in staged)


def test_publication_actual_primary_close_failure_rolls_back_outputs(tmp_path, fake_runner, tpch_plan, monkeypatch):
    destination = tmp_path / "commit-close"
    real_dup = tpch.os.dup
    real_close = tpch.os.close
    duplicated = []
    target_close_calls = 0
    failed = False

    def track_dup(descriptor):
        duplicated_descriptor = real_dup(descriptor)
        duplicated.append(duplicated_descriptor)
        return duplicated_descriptor

    def close_then_fail(descriptor):
        nonlocal failed, target_close_calls
        if len(duplicated) >= 2 and descriptor == duplicated[0]:
            target_close_calls += 1
        if len(duplicated) >= 2 and descriptor == duplicated[0] and not failed:
            failed = True
            real_close(descriptor)
            raise OSError("simulated close failure")
        return real_close(descriptor)

    monkeypatch.setattr(tpch.os, "dup", track_dup)
    monkeypatch.setattr(tpch.os, "close", close_then_fail)
    with pytest.raises(OSError, match="close failure"):
        tpch.generate_tpch(tpch_plan, destination, runner=fake_runner)

    assert list(destination.iterdir()) == []
    assert target_close_calls == 1


def test_publication_partial_dup_failure_closes_acquired_descriptor(tmp_path, fake_runner, tpch_plan, monkeypatch):
    real_dup = tpch.os.dup
    duplicated = []

    def fail_second_dup(descriptor):
        if duplicated:
            raise OSError("simulated rollback capability failure")
        result = real_dup(descriptor)
        duplicated.append(result)
        return result

    monkeypatch.setattr(tpch.os, "dup", fail_second_dup)
    with pytest.raises(OSError, match="rollback capability failure"):
        tpch.generate_tpch(tpch_plan, tmp_path, runner=fake_runner)

    assert duplicated
    with pytest.raises(OSError):
        os.fstat(duplicated[0])
    assert list(tmp_path.iterdir()) == []


def test_publication_rollback_attempts_all_cleanup_and_notes_failures(tmp_path, fake_runner, tpch_plan, monkeypatch):
    destination = tmp_path / "cleanup-failures"
    real_link = tpch.os.link
    links = 0
    cleanup_calls = []

    def fail_third_link(source, target, **kwargs):
        nonlocal links
        links += 1
        if links == 3:
            raise OSError("publication failed")
        return real_link(source, target, **kwargs)

    def fail_cleanup(directory_descriptor, name, identity):
        if len(cleanup_calls) < 2:
            cleanup_calls.append((name, identity))
            raise OSError(f"cleanup failed for {name}")
        return real_cleanup(directory_descriptor, name, identity)

    monkeypatch.setattr(tpch.os, "link", fail_third_link)
    real_cleanup = tpch._remove_owned_entry
    monkeypatch.setattr(tpch, "_remove_owned_entry", fail_cleanup)
    with pytest.raises(OSError, match="publication failed") as caught:
        tpch.generate_tpch(tpch_plan, destination, runner=fake_runner)

    assert len(cleanup_calls) == 2
    assert len(caught.value.__notes__) == 2


def test_generate_tpch_has_no_host_fallback(tmp_path, tpch_plan, monkeypatch):
    class UnavailableDockerRunner:
        def ensure_image(self, contract):
            raise RuntimeError("Docker linux/amd64 unavailable")

    assert hasattr(tpch, "DockerContainerRunner")
    monkeypatch.setattr(tpch, "DockerContainerRunner", UnavailableDockerRunner)

    with pytest.raises(RuntimeError, match="Docker linux/amd64 unavailable"):
        tpch.generate_tpch(tpch_plan, tmp_path)

    assert not list(tmp_path.glob("*.parquet"))


def test_docker_runner_rebuilds_label_drift_before_runtime_probe(tpch_plan, monkeypatch):
    contract = tpch_plan.dataset.generator
    assert contract is not None
    runner = tpch.DockerContainerRunner(repository_root=ROOT)
    matching = tpch.canonical_labels(contract)
    inspections = [
        {
            "Id": "sha256:" + "1" * 64,
            "Os": "linux",
            "Architecture": "amd64",
            "Config": {
                "Labels": {},
                "Entrypoint": list(tpch.CANONICAL_ENTRYPOINT),
                "Env": [f"{key}={value}" for key, value in tpch.canonical_environment(contract).items()],
            },
        },
        {
            "Id": "sha256:" + "2" * 64,
            "Os": "linux",
            "Architecture": "amd64",
            "Config": {
                "Labels": matching,
                "Entrypoint": list(tpch.CANONICAL_ENTRYPOINT),
                "Env": [f"{key}={value}" for key, value in tpch.canonical_environment(contract).items()],
            },
        },
    ]
    builds = []
    evidence = FakeRunner(tpch_plan).evidence
    monkeypatch.setattr(runner, "_inspect_image", lambda: inspections.pop(0))
    monkeypatch.setattr(runner, "_build_image", lambda contract: builds.append(contract))
    probes = []
    monkeypatch.setattr(
        runner,
        "_image_evidence",
        lambda contract, inspection: probes.append(inspection["Id"]) or replace(evidence, image_id=inspection["Id"]),
    )
    monkeypatch.setattr(runner, "_evidence_matches", lambda evidence, contract: True)

    assert runner.ensure_image(contract).image_id == "sha256:" + "2" * 64
    assert builds == [contract]
    assert probes == ["sha256:" + "2" * 64]


def test_docker_runner_propagates_operational_probe_failure_without_build(tpch_plan, monkeypatch):
    contract = tpch_plan.dataset.generator
    assert contract is not None
    runner = tpch.DockerContainerRunner(repository_root=ROOT)
    evidence = FakeRunner(tpch_plan).evidence
    inspection = {
        "Id": evidence.image_id,
        "Os": "linux",
        "Architecture": "amd64",
        "Config": {
            "Labels": tpch.canonical_labels(contract),
            "Entrypoint": list(tpch.CANONICAL_ENTRYPOINT),
            "Env": [f"{key}={value}" for key, value in tpch.canonical_environment(contract).items()],
        },
    }
    builds = []
    monkeypatch.setattr(runner, "_inspect_image", lambda: inspection)
    monkeypatch.setattr(
        runner,
        "_image_evidence",
        lambda contract, inspection: (_ for _ in ()).throw(RuntimeError("daemon unavailable")),
    )
    monkeypatch.setattr(runner, "_build_image", lambda contract: builds.append(contract))

    with pytest.raises(RuntimeError, match="daemon unavailable"):
        runner.ensure_image(contract)
    assert builds == []


def test_docker_runner_rebuilds_forged_matching_label_image(tpch_plan, monkeypatch):
    contract = tpch_plan.dataset.generator
    assert contract is not None
    runner = tpch.DockerContainerRunner(repository_root=ROOT)
    matching = tpch.canonical_labels(contract)
    inspection = {
        "Id": "sha256:" + "1" * 64,
        "Os": "linux",
        "Architecture": "amd64",
        "Config": {
            "Labels": matching,
            "Entrypoint": list(tpch.CANONICAL_ENTRYPOINT),
            "Env": [f"{key}={value}" for key, value in tpch.canonical_environment(contract).items()],
        },
    }
    forged = replace(
        FakeRunner(replace(tpch_plan)).evidence,
        image_id=inspection["Id"],
        tpch_extension_sha256="0" * 64,
    )
    verified = replace(forged, tpch_extension_sha256=contract.extension_sha256)
    inspections = [inspection, inspection]
    evidence = [forged, verified]
    builds = []
    monkeypatch.setattr(runner, "_inspect_image", lambda: inspections.pop(0))
    monkeypatch.setattr(runner, "_image_evidence", lambda contract, inspection: evidence.pop(0))
    monkeypatch.setattr(runner, "_build_image", lambda contract: builds.append(contract))

    assert runner.ensure_image(contract) == verified
    assert builds == [contract]


def test_docker_runner_build_command_pins_platform_dockerfile_and_labels(tpch_plan, monkeypatch):
    contract = tpch_plan.dataset.generator
    assert contract is not None
    runner = tpch.DockerContainerRunner(repository_root=ROOT)
    commands = []
    monkeypatch.setattr(runner, "_execute", lambda command: commands.append(command))

    runner._build_image(contract)

    command = commands[0]
    assert command[:7] == [
        "docker",
        "build",
        "--platform",
        "linux/amd64",
        "--file",
        "datasets/tpch-lock.Dockerfile",
        "--label",
    ]
    assert command[-3:] == ["--tag", "data-eng-lab-tpch-lock:1.5.4", "."]
    assert {command[index + 1] for index, argument in enumerate(command) if argument == "--label"} == {
        f"{key}={value}" for key, value in tpch.canonical_labels(contract).items()
    }


def test_docker_image_inspection_proves_canonical_source_and_runtime_config(tpch_plan, monkeypatch):
    contract = tpch_plan.dataset.generator
    assert contract is not None
    runner = tpch.DockerContainerRunner(repository_root=ROOT)
    monkeypatch.setattr(runner, "_inspect_base_image", lambda contract: ("sha256:base",))
    monkeypatch.setattr(
        runner,
        "_runtime_probe",
        lambda image_id, contract: {
            "duckdb_version": contract.engine_version,
            "duckdb_wheel_sha256": contract.engine_wheel_sha256,
            "uv_lock_sha256": contract.environment.uv_lock_sha256,
            "tpch_extension_sha256": contract.extension_sha256,
            "requirements_sha256": hashlib.sha256(
                (ROOT / "datasets" / "tpch-lock-requirements.txt").read_bytes()
            ).hexdigest(),
            "exporter_sha256": hashlib.sha256((ROOT / "datasets" / "tpch_lock_export.py").read_bytes()).hexdigest(),
        },
    )
    evidence = runner._image_evidence(
        contract,
        {
            "Id": "sha256:" + "1" * 64,
            "Os": "linux",
            "Architecture": "amd64",
            "RootFS": {"Layers": ["sha256:base", "sha256:application"]},
            "Config": {
                "Entrypoint": list(tpch.CANONICAL_ENTRYPOINT),
                "Env": [f"{key}={value}" for key, value in tpch.canonical_environment(contract).items()],
                "Labels": tpch.canonical_labels(contract),
            },
        },
    )

    tpch.verify_image_evidence(
        evidence,
        contract,
        tpch.VerificationContext("tpch", "tiny", "image"),
    )


def test_runtime_probe_is_network_disabled_and_bound_to_inspected_image(tpch_plan, monkeypatch):
    contract = tpch_plan.dataset.generator
    assert contract is not None
    runner = tpch.DockerContainerRunner(repository_root=ROOT)
    image_id = "sha256:" + "1" * 64
    probe = {
        "duckdb_version": contract.engine_version,
        "duckdb_wheel_sha256": contract.engine_wheel_sha256,
        "uv_lock_sha256": contract.environment.uv_lock_sha256,
        "tpch_extension_sha256": contract.extension_sha256,
        "requirements_sha256": hashlib.sha256(
            (ROOT / "datasets" / "tpch-lock-requirements.txt").read_bytes()
        ).hexdigest(),
        "exporter_sha256": hashlib.sha256((ROOT / "datasets" / "tpch_lock_export.py").read_bytes()).hexdigest(),
    }
    commands = []

    def execute(command):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, json.dumps(probe), "")

    monkeypatch.setattr(tpch.subprocess, "run", lambda command, **kwargs: execute(command))

    assert runner._runtime_probe(image_id, contract) == probe
    command = commands[0]
    assert command[:8] == [
        "docker",
        "run",
        "--rm",
        "--network=none",
        "--platform",
        "linux/amd64",
        "--entrypoint",
        "python",
    ]
    assert command[8] == image_id


@pytest.mark.parametrize(
    ("returncode", "expected_exception"),
    [(125, RuntimeError), (1, tpch._ImageProofMismatch)],
)
def test_runtime_probe_classifies_docker_cli_vs_image_proof_failure(
    tpch_plan, monkeypatch, returncode, expected_exception
):
    contract = tpch_plan.dataset.generator
    assert contract is not None
    runner = tpch.DockerContainerRunner(repository_root=ROOT)

    monkeypatch.setattr(
        tpch.subprocess,
        "run",
        lambda command, **kwargs: subprocess.CompletedProcess(
            command,
            returncode,
            "",
            "sensitive localized docker diagnostic",
        ),
    )

    with pytest.raises(expected_exception, match="runtime probe") as caught:
        runner._runtime_probe("sha256:" + "1" * 64, contract)
    assert "sensitive" not in str(caught.value)
    if returncode == 125:
        assert type(caught.value) is RuntimeError


def test_runtime_probe_program_failure_rebuilds_and_reprobes(tpch_plan, monkeypatch):
    contract = tpch_plan.dataset.generator
    assert contract is not None
    runner = tpch.DockerContainerRunner(repository_root=ROOT)
    evidence = FakeRunner(tpch_plan).evidence
    inspection = {
        "Id": evidence.image_id,
        "Os": "linux",
        "Architecture": "amd64",
        "Config": {
            "Labels": tpch.canonical_labels(contract),
            "Entrypoint": list(tpch.CANONICAL_ENTRYPOINT),
            "Env": [f"{key}={value}" for key, value in tpch.canonical_environment(contract).items()],
        },
    }
    inspections = [inspection, inspection]
    probes = [tpch._ImageProofMismatch("runtime probe program failed"), evidence]
    builds = []
    monkeypatch.setattr(runner, "_inspect_image", lambda: inspections.pop(0))
    monkeypatch.setattr(runner, "_build_image", lambda contract: builds.append(contract))

    def image_evidence(contract, inspection):
        result = probes.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    monkeypatch.setattr(runner, "_image_evidence", image_evidence)

    assert runner.ensure_image(contract) == evidence
    assert builds == [contract]
    assert probes == []


def test_runtime_probe_malformed_success_rebuilds_but_postbuild_failure_is_closed(tpch_plan, monkeypatch):
    contract = tpch_plan.dataset.generator
    assert contract is not None
    runner = tpch.DockerContainerRunner(repository_root=ROOT)
    evidence = FakeRunner(tpch_plan).evidence
    inspection = {
        "Id": evidence.image_id,
        "Os": "linux",
        "Architecture": "amd64",
        "Config": {
            "Labels": tpch.canonical_labels(contract),
            "Entrypoint": list(tpch.CANONICAL_ENTRYPOINT),
            "Env": [f"{key}={value}" for key, value in tpch.canonical_environment(contract).items()],
        },
    }
    inspections = [inspection, inspection]
    builds = []
    monkeypatch.setattr(runner, "_inspect_image", lambda: inspections.pop(0))
    monkeypatch.setattr(runner, "_build_image", lambda contract: builds.append(contract))
    monkeypatch.setattr(
        runner,
        "_image_evidence",
        lambda contract, inspection: (_ for _ in ()).throw(
            tpch._ImageProofMismatch("runtime probe returned invalid evidence")
        ),
    )

    with pytest.raises(RuntimeError, match="post-build proof"):
        runner.ensure_image(contract)
    assert builds == [contract]


def test_image_evidence_requires_pinned_base_rootfs_prefix(tpch_plan, monkeypatch):
    contract = tpch_plan.dataset.generator
    assert contract is not None
    runner = tpch.DockerContainerRunner(repository_root=ROOT)
    inspection = {
        "Id": "sha256:" + "1" * 64,
        "Os": "linux",
        "Architecture": "amd64",
        "RootFS": {"Layers": ["sha256:forged"]},
        "Config": {
            "Entrypoint": list(tpch.CANONICAL_ENTRYPOINT),
            "Env": [f"{key}={value}" for key, value in tpch.canonical_environment(contract).items()],
            "Labels": tpch.canonical_labels(contract),
        },
    }
    monkeypatch.setattr(runner, "_inspect_base_image", lambda contract: ["sha256:base"])
    monkeypatch.setattr(
        runner,
        "_runtime_probe",
        lambda image_id, contract: {
            "duckdb_version": contract.engine_version,
            "duckdb_wheel_sha256": contract.engine_wheel_sha256,
            "uv_lock_sha256": contract.environment.uv_lock_sha256,
            "tpch_extension_sha256": contract.extension_sha256,
            "requirements_sha256": hashlib.sha256(
                (ROOT / "datasets" / "tpch-lock-requirements.txt").read_bytes()
            ).hexdigest(),
            "exporter_sha256": hashlib.sha256((ROOT / "datasets" / "tpch_lock_export.py").read_bytes()).hexdigest(),
        },
    )

    evidence = runner._image_evidence(contract, inspection)
    assert evidence.base_rootfs_match is False


def test_image_evidence_rejects_substituted_canonical_exporter(tpch_plan, fake_runner):
    contract = tpch_plan.dataset.generator
    assert contract is not None
    evidence = replace(fake_runner.evidence, exporter_sha256="0" * 64)

    with pytest.raises(LockMismatch, match="exporter_sha256"):
        tpch.verify_image_evidence(
            evidence,
            contract,
            tpch.VerificationContext("tpch", "tiny", "image"),
        )


def test_docker_runner_runtime_command_is_offline_and_does_not_install(tpch_plan, tmp_path, monkeypatch):
    contract = tpch_plan.dataset.generator
    scale = tpch_plan.generator_scale
    assert contract is not None and scale is not None
    assert hasattr(tpch, "DockerContainerRunner")
    runner = tpch.DockerContainerRunner(repository_root=ROOT)
    runner._verified_image_id = "sha256:" + "1" * 64
    commands = []

    def execute(command):
        commands.append(command)
        (tmp_path / "output").mkdir()
        (tmp_path / "output" / "customer.parquet").write_bytes(b"data")
        (tmp_path / "metadata.yaml").write_bytes(b"metadata")

    monkeypatch.setattr(runner, "_execute", execute)

    runner.run(contract, scale, tmp_path, tmp_path / "metadata.yaml")

    assert commands[0] == [
        "docker",
        "run",
        "--rm",
        "--network=none",
        "--platform",
        "linux/amd64",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--env",
        "HOME=/root",
        "--env",
        "TMPDIR=/tmp",
        "--tmpfs",
        "/root:mode=0755",
        "--mount",
        "type=volume,destination=/root/.duckdb",
        "--volume",
        f"{tmp_path.resolve()}:/out",
        "sha256:" + "1" * 64,
        "--scale",
        "0.01",
        "--output-dir",
        "/out/output",
        "--metadata",
        "/out/metadata.yaml",
    ]
    assert len(commands) == 1
    assert all("INSTALL" not in argument and "pip" not in argument for command in commands for argument in command)
    assert stat.S_IMODE(tmp_path.stat().st_mode) == 0o700
    assert stat.S_IMODE((tmp_path / "customer.parquet").stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / "metadata.yaml").stat().st_mode) == 0o600


def test_generation_failure_leaves_no_host_owned_transaction_residue(tmp_path, tpch_plan, monkeypatch):
    contract = tpch_plan.dataset.generator
    assert contract is not None
    runner = tpch.DockerContainerRunner(repository_root=ROOT)
    evidence = FakeRunner(tpch_plan).evidence
    runner._verified_image_id = evidence.image_id
    monkeypatch.setattr(runner, "ensure_image", lambda contract: evidence)
    commands = []

    def fail_after_host_owned_output(command):
        commands.append(command)
        volume = command[command.index("--volume") + 1]
        output_root = Path(volume.removesuffix(":/out"))
        generated = output_root / "output"
        generated.mkdir(mode=0o700)
        (generated / "partial.parquet").write_bytes(b"partial")
        (generated / "partial.parquet").chmod(0o600)
        raise RuntimeError("generation failed")

    monkeypatch.setattr(runner, "_execute", fail_after_host_owned_output)

    with pytest.raises(RuntimeError, match="generation failed"):
        tpch.generate_tpch(tpch_plan, tmp_path, runner=runner)

    assert len(commands) == 1
    assert list(tmp_path.iterdir()) == []


def test_docker_runner_refuses_run_without_verified_image_id(tpch_plan, tmp_path):
    contract = tpch_plan.dataset.generator
    scale = tpch_plan.generator_scale
    assert contract is not None and scale is not None
    runner = tpch.DockerContainerRunner(repository_root=ROOT)

    with pytest.raises(RuntimeError, match="verified image ID"):
        runner.run(contract, scale, tmp_path, tmp_path / "metadata.yaml")


def test_host_ownership_check_rejects_root_owned_output_simulation(tmp_path, monkeypatch):
    output = tmp_path / "output"
    output.mkdir()
    artifact = output / "customer.parquet"
    artifact.write_bytes(b"data")
    real_lstat = Path.lstat

    def root_owned(path):
        status = real_lstat(path)
        if path in {output, artifact}:
            values = list(status)
            values[4] = 0
            return os.stat_result(values)
        return status

    monkeypatch.setattr(Path, "lstat", root_owned)
    with pytest.raises(RuntimeError, match="host-owned"):
        tpch._secure_host_outputs(tmp_path, output, tmp_path / "metadata.yaml")


def test_host_ownership_check_accepts_docker_desktop_group_remap(tmp_path, monkeypatch):
    output = tmp_path / "output"
    output.mkdir()
    artifact = output / "customer.parquet"
    artifact.write_bytes(b"data")
    metadata = tmp_path / "metadata.yaml"
    metadata.write_bytes(b"metadata")
    real_lstat = Path.lstat

    def remapped_group(path):
        status = real_lstat(path)
        values = list(status)
        values[5] = 0 if os.getgid() != 0 else 1
        return os.stat_result(values)

    monkeypatch.setattr(Path, "lstat", remapped_group)
    tpch._secure_host_outputs(tmp_path, output, metadata)

    assert stat.S_IMODE(artifact.stat().st_mode) == 0o600


def test_inspect_invalid_json_has_stable_error(monkeypatch):
    runner = tpch.DockerContainerRunner(repository_root=ROOT)
    monkeypatch.setattr(
        runner,
        "_execute",
        lambda command: subprocess.CompletedProcess(command, 0, "not-json", ""),
    )
    with pytest.raises(RuntimeError, match="invalid image inspection evidence"):
        runner._inspect_image()


def test_inspect_detects_missing_image_without_parsing_english_stderr(monkeypatch):
    runner = tpch.DockerContainerRunner(repository_root=ROOT)
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, "", "localized diagnostic")

    monkeypatch.setattr(tpch.subprocess, "run", run)

    assert runner._inspect_image() is None
    assert commands == [["docker", "image", "ls", "--quiet", "--no-trunc", runner.image_tag]]


def test_generate_tpch_requires_typed_generator_plan(tmp_path, tpch_plan, fake_runner):
    with pytest.raises(ValueError, match="generator contract"):
        plan = replace(tpch_plan, dataset=replace(tpch_plan.dataset, generator=None))
        tpch.generate_tpch(plan, tmp_path, fake_runner)
    with pytest.raises(ValueError, match="generator scale"):
        tpch.generate_tpch(replace(tpch_plan, generator_scale=None), tmp_path, fake_runner)
    with pytest.raises(ValueError, match="exactly eight"):
        bad_scale = GeneratorScale("tiny", 0.01, tpch_plan.generator_scale.outputs[:-1])
        tpch.generate_tpch(replace(tpch_plan, generator_scale=bad_scale), tmp_path, fake_runner)
