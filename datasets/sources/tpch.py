"""Generate locked TPC-H outputs with the canonical offline container."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import stat
import subprocess
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

import yaml

from datasets import acquisition
from datasets.registry import GeneratorContract, GeneratorOutput, GeneratorScale, ScalePlan
from datasets.schema_inspection import verify_physical_schema
from datasets.verification import (
    ExpectedObject,
    LockMismatch,
    VerificationContext,
    VerifiedFile,
    require_exact_names,
    verify_file,
)

TPCH_TABLES = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]
CANONICAL_ENTRYPOINT = ("python", "-m", "datasets.tpch_lock_export")
IMAGE_TAG = "data-eng-lab-tpch-lock:1.5.4"
_ROOT = Path(__file__).resolve().parents[2]
_DOCKERFILE = Path("datasets/tpch-lock.Dockerfile")
_REQUIREMENTS = Path("datasets/tpch-lock-requirements.txt")
_EXPORTER = Path("datasets/tpch_lock_export.py")
_LABEL_PREFIX = "io.data-eng-lab.tpch."


@dataclass(frozen=True)
class ImageEvidence:
    image_id: str
    base_image: str
    base_image_digest: str
    platform: str
    entrypoint: tuple[str, ...]
    environment: Mapping[str, str]
    labels: Mapping[str, str]
    uv_lock_sha256: str
    duckdb_version: str
    duckdb_wheel_sha256: str
    tpch_extension_sha256: str
    requirements_sha256: str
    exporter_sha256: str
    base_rootfs_match: bool
    uses_hashed_requirements: bool


@dataclass(frozen=True)
class ContainerRunArgs:
    platform: str
    network: str
    scale: str
    output_root: Path
    metadata_path: Path


@dataclass
class _Publication:
    files: tuple[VerifiedFile, ...]
    owned_links: list[tuple[Path, tuple[int, int]]]
    destination_descriptor: int
    destination_identity: tuple[int, int]
    active: bool = True

    def rollback(self, primary: BaseException) -> None:
        if not self.active:
            return
        self.active = False
        for path, identity in reversed(self.owned_links):
            try:
                _remove_owned_output(path, identity)
            except BaseException as cleanup_error:
                primary.add_note(f"TPC-H publication rollback failed for {path.name}: {cleanup_error}")
        self.close(primary)

    def close(self, primary: BaseException | None = None) -> None:
        descriptor = self.destination_descriptor
        self.destination_descriptor = -1
        if descriptor < 0:
            return
        try:
            os.close(descriptor)
        except OSError as error:
            if primary is None:
                raise
            primary.add_note(f"TPC-H destination capability release failed: {error}")

    def commit(self) -> None:
        self.active = False
        self.close()


class ContainerRunner(Protocol):
    def ensure_image(self, contract: GeneratorContract) -> ImageEvidence: ...

    def run(
        self,
        contract: GeneratorContract,
        scale: GeneratorScale,
        output_root: Path,
        metadata_path: Path,
    ) -> None: ...


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _path_identity(path: Path, *, directory: bool) -> tuple[int, int] | None:
    try:
        status = path.lstat()
    except FileNotFoundError:
        return None
    expected = stat.S_ISDIR if directory else stat.S_ISREG
    if stat.S_ISLNK(status.st_mode) or not expected(status.st_mode):
        return None
    return status.st_dev, status.st_ino


def _restore_foreign_quarantine(quarantine: Path, original: Path) -> None:
    try:
        acquisition._quarantine_path_exclusive(quarantine, original)
    except BaseException as error:
        raise RuntimeError(f"foreign replacement remains quarantined at {quarantine}") from error


def _quarantine_owned_path(
    path: Path,
    identity: tuple[int, int],
    *,
    directory: bool,
) -> None:
    current_identity = _path_identity(path, directory=directory)
    if current_identity is None:
        if os.path.lexists(path):
            raise ValueError("owned TPC-H path changed type during cleanup")
        return
    if current_identity != identity:
        raise ValueError("owned TPC-H path identity changed during cleanup")
    quarantine = path.parent / f".dataset-cleanup-tpch-{secrets.token_hex(16)}"
    acquisition._quarantine_path_exclusive(path, quarantine)
    if _path_identity(quarantine, directory=directory) != identity:
        _restore_foreign_quarantine(quarantine, path)
        raise ValueError("owned TPC-H path changed during cleanup")
    if directory:
        import shutil

        shutil.rmtree(quarantine)
    else:
        quarantine.unlink()


def _remove_owned_output(path: Path, identity: tuple[int, int]) -> None:
    _quarantine_owned_path(path, identity, directory=False)


def _verify_directory_binding(path: Path, descriptor: int, identity: tuple[int, int]) -> None:
    try:
        opened = os.fstat(descriptor)
        current = path.lstat()
    except OSError as error:
        raise ValueError("TPC-H destination directory is unavailable") from error
    if (
        not stat.S_ISDIR(opened.st_mode)
        or not stat.S_ISDIR(current.st_mode)
        or (opened.st_dev, opened.st_ino) != identity
        or (current.st_dev, current.st_ino) != identity
    ):
        raise ValueError("TPC-H destination directory changed")


def canonical_environment(contract: GeneratorContract) -> dict[str, str]:
    return {
        "LANG": contract.environment.locale,
        "LC_ALL": contract.environment.locale,
        "TZ": contract.environment.timezone,
        "PYTHONHASHSEED": "0",
    }


def canonical_labels(
    contract: GeneratorContract,
    repository_root: Path = _ROOT,
) -> dict[str, str]:
    root = Path(repository_root)
    return {
        f"{_LABEL_PREFIX}base-image": contract.environment.image,
        f"{_LABEL_PREFIX}base-digest": contract.environment.image_digest,
        f"{_LABEL_PREFIX}platform": contract.environment.platform,
        f"{_LABEL_PREFIX}duckdb-version": contract.engine_version,
        f"{_LABEL_PREFIX}duckdb-wheel-sha256": contract.engine_wheel_sha256,
        f"{_LABEL_PREFIX}extension-sha256": contract.extension_sha256,
        f"{_LABEL_PREFIX}uv-lock-sha256": contract.environment.uv_lock_sha256,
        f"{_LABEL_PREFIX}dockerfile-sha256": _sha256(root / _DOCKERFILE),
        f"{_LABEL_PREFIX}requirements-sha256": _sha256(root / _REQUIREMENTS),
        f"{_LABEL_PREFIX}exporter-sha256": _sha256(root / _EXPORTER),
    }


def expected_metadata_environment(contract: GeneratorContract) -> dict[str, object]:
    return {
        "platform": contract.environment.platform,
        "base_image_digest": contract.environment.image_digest,
        "duckdb_version": contract.engine_version,
        "duckdb_wheel_sha256": contract.engine_wheel_sha256,
        "uv_lock_sha256": contract.environment.uv_lock_sha256,
        "tpch_extension_sha256": contract.extension_sha256,
        "locale": contract.environment.locale,
        "timezone": contract.environment.timezone,
        "threads": contract.environment.threads,
        "preserve_insertion_order": contract.environment.preserve_insertion_order,
        "format": contract.export_format,
        "compression": contract.compression,
        "row_group_size": contract.row_group_size,
    }


def canonical_scale(scale_factor: float) -> str:
    scales = {0.01: "0.01", 1.0: "1", 10.0: "10"}
    try:
        return scales[scale_factor]
    except KeyError as error:
        raise ValueError(f"unsupported canonical TPC-H scale factor: {scale_factor}") from error


def _secure_host_outputs(output_root: Path, generated: Path, metadata_path: Path) -> None:
    expected_uid = os.getuid()
    expected_gid = os.getgid()
    paths = (output_root, generated, metadata_path, *tuple(generated.iterdir()))
    for path in paths:
        try:
            status = path.lstat()
        except OSError as error:
            raise RuntimeError("canonical TPC-H output ownership handoff is incomplete") from error
        if stat.S_ISLNK(status.st_mode) or status.st_uid != expected_uid or status.st_gid != expected_gid:
            raise RuntimeError(f"canonical TPC-H output is not host-owned: {path}")
        if stat.S_ISDIR(status.st_mode):
            path.chmod(0o700)
        elif stat.S_ISREG(status.st_mode):
            path.chmod(0o600)
        else:
            raise RuntimeError(f"canonical TPC-H output is not a regular file: {path}")


class DockerContainerRunner:
    """Build, inspect, and run the canonical TPC-H image through Docker."""

    def __init__(self, repository_root: Path = _ROOT, image_tag: str = IMAGE_TAG):
        self.repository_root = Path(repository_root).resolve()
        self.image_tag = image_tag
        self._verified_image_id: str | None = None

    def _execute(self, command: list[str]) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(
                command,
                cwd=self.repository_root,
                check=True,
                capture_output=True,
                text=True,
            )
        except FileNotFoundError as error:
            raise RuntimeError("Docker is required for canonical TPC-H generation") from error
        except subprocess.CalledProcessError as error:
            detail = error.stderr.strip() or error.stdout.strip() or "Docker command failed"
            raise RuntimeError(detail) from error

    def _inspect_image(self) -> dict[str, object] | None:
        try:
            result = self._execute(["docker", "image", "inspect", self.image_tag])
        except RuntimeError as error:
            if "No such image" in str(error) or "No such object" in str(error):
                return None
            raise
        try:
            document = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise RuntimeError("Docker returned invalid image inspection evidence") from error
        if not isinstance(document, list) or len(document) != 1 or not isinstance(document[0], dict):
            raise RuntimeError("Docker returned invalid image inspection evidence")
        return cast(dict[str, object], document[0])

    def _inspect_base_image(self, contract: GeneratorContract) -> tuple[str, ...]:
        reference = f"{contract.environment.image.rsplit(':', 1)[0]}@{contract.environment.image_digest}"
        try:
            result = self._execute(["docker", "image", "inspect", reference])
            document = json.loads(result.stdout)
            layers = document[0]["RootFS"]["Layers"]
        except (json.JSONDecodeError, KeyError, IndexError, TypeError) as error:
            raise RuntimeError("Docker returned invalid pinned-base inspection evidence") from error
        if not isinstance(layers, list) or not layers or not all(isinstance(item, str) for item in layers):
            raise RuntimeError("Docker returned invalid pinned-base RootFS evidence")
        return tuple(layers)

    def _runtime_probe(self, image_id: str, contract: GeneratorContract) -> dict[str, str]:
        extension_path = (
            f"/root/.duckdb/extensions/v{contract.engine_version}/linux_amd64/"
            f"{contract.extension_name}.duckdb_extension"
        )
        script = (
            "import hashlib,json,re; import duckdb; "
            "h=lambda p:hashlib.sha256(open(p,'rb').read()).hexdigest(); "
            "r=open('/tmp/requirements.txt',encoding='utf-8').read(); "
            "print(json.dumps({'duckdb_version':duckdb.__version__,"
            "'duckdb_wheel_sha256':re.search(r'duckdb==[^ ]+ --hash=sha256:([0-9a-f]{64})',r).group(1),"
            "'uv_lock_sha256':h('/workspace/uv.lock'),"
            f"'tpch_extension_sha256':h('{extension_path}'),"
            "'requirements_sha256':h('/tmp/requirements.txt'),"
            "'exporter_sha256':h('/workspace/datasets/tpch_lock_export.py')}))"
        )
        command = [
            "docker",
            "run",
            "--rm",
            "--network=none",
            "--platform",
            contract.environment.platform,
            "--entrypoint",
            "python",
            image_id,
            "-c",
            script,
        ]
        result = self._execute(command)
        try:
            document = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise RuntimeError("canonical TPC-H runtime probe returned invalid evidence") from error
        expected_fields = (
            "duckdb_version",
            "duckdb_wheel_sha256",
            "uv_lock_sha256",
            "tpch_extension_sha256",
            "requirements_sha256",
            "exporter_sha256",
        )
        if not isinstance(document, dict) or tuple(document) != expected_fields:
            raise RuntimeError("canonical TPC-H runtime probe returned invalid evidence")
        if any(type(document[field]) is not str for field in expected_fields):
            raise RuntimeError("canonical TPC-H runtime probe returned invalid evidence")
        return cast(dict[str, str], document)

    def _build_image(self, contract: GeneratorContract) -> None:
        command = [
            "docker",
            "build",
            "--platform",
            contract.environment.platform,
            "--file",
            str(_DOCKERFILE),
        ]
        for key, value in canonical_labels(contract, self.repository_root).items():
            command.extend(("--label", f"{key}={value}"))
        command.extend(("--tag", self.image_tag, "."))
        self._execute(command)

    def _source_evidence(self, contract: GeneratorContract) -> tuple[str, str, str, str, bool]:
        dockerfile = (self.repository_root / _DOCKERFILE).read_text(encoding="utf-8")
        requirements = (self.repository_root / _REQUIREMENTS).read_text(encoding="utf-8")
        base = re.search(r"^FROM --platform=([^ ]+) ([^@\s]+)@(sha256:[0-9a-f]{64})$", dockerfile, re.MULTILINE)
        wheel = re.search(r"^duckdb==([^ ]+) --hash=sha256:([0-9a-f]{64})$", requirements, re.MULTILINE)
        if base is None or wheel is None:
            raise RuntimeError("canonical TPC-H Docker inputs are malformed")
        hashed = (
            "pip install --no-cache-dir --require-hashes" in dockerfile
            and f"{_REQUIREMENTS.as_posix()} /tmp/requirements.txt" in dockerfile
            and f"{contract.environment.uv_lock_sha256}  /workspace/uv.lock" in dockerfile
            and f"{contract.extension_sha256}  /root/.duckdb/extensions/" in dockerfile
            and contract.environment.image.rsplit(":", 1)[0] == base.group(2)
        )
        return base.group(2), base.group(3), wheel.group(1), wheel.group(2), hashed

    def _image_evidence(
        self,
        contract: GeneratorContract,
        inspection: Mapping[str, object],
    ) -> ImageEvidence:
        config = inspection.get("Config")
        if not isinstance(config, Mapping):
            raise RuntimeError("Docker image inspection omitted Config")
        raw_environment = config.get("Env", ())
        raw_labels = config.get("Labels", {})
        if not isinstance(raw_environment, Sequence) or isinstance(raw_environment, str):
            raise RuntimeError("Docker image inspection returned invalid environment")
        if not isinstance(raw_labels, Mapping):
            raise RuntimeError("Docker image inspection returned invalid labels")
        image_id = inspection.get("Id")
        if not isinstance(image_id, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None:
            raise RuntimeError("Docker image inspection omitted immutable image ID")
        environment = {
            item.split("=", 1)[0]: item.split("=", 1)[1]
            for item in raw_environment
            if isinstance(item, str) and "=" in item
        }
        expected_labels = canonical_labels(contract, self.repository_root)
        labels = {key: raw_labels.get(key) for key in expected_labels}
        _base_repository, base_digest, _duckdb_version, _wheel_sha256, hashed = self._source_evidence(contract)
        rootfs = inspection.get("RootFS")
        image_layers = rootfs.get("Layers") if isinstance(rootfs, Mapping) else None
        base_layers = self._inspect_base_image(contract)
        base_rootfs_match = (
            isinstance(image_layers, list)
            and len(image_layers) >= len(base_layers)
            and tuple(image_layers[: len(base_layers)]) == base_layers
        )
        probe = self._runtime_probe(image_id, contract)
        operating_system = inspection.get("Os")
        architecture = inspection.get("Architecture")
        return ImageEvidence(
            image_id=image_id,
            base_image=cast(str, labels.get(f"{_LABEL_PREFIX}base-image")),
            base_image_digest=base_digest,
            platform=f"{operating_system}/{architecture}",
            entrypoint=tuple(config.get("Entrypoint") or ()),
            environment={key: environment.get(key, "") for key in canonical_environment(contract)},
            labels=cast(Mapping[str, str], labels),
            uv_lock_sha256=probe["uv_lock_sha256"],
            duckdb_version=probe["duckdb_version"],
            duckdb_wheel_sha256=probe["duckdb_wheel_sha256"],
            tpch_extension_sha256=probe["tpch_extension_sha256"],
            requirements_sha256=probe["requirements_sha256"],
            exporter_sha256=probe["exporter_sha256"],
            base_rootfs_match=base_rootfs_match,
            uses_hashed_requirements=hashed,
        )

    def _evidence_matches(self, evidence: ImageEvidence, contract: GeneratorContract) -> bool:
        try:
            verify_image_evidence(
                evidence,
                contract,
                VerificationContext("tpch", "image-cache", "image"),
                repository_root=self.repository_root,
            )
        except LockMismatch:
            return False
        return True

    def ensure_image(self, contract: GeneratorContract) -> ImageEvidence:
        inspection = self._inspect_image()
        expected_labels = canonical_labels(contract, self.repository_root)
        actual_labels = {}
        if inspection is not None and isinstance(inspection.get("Config"), Mapping):
            labels = cast(Mapping[str, object], inspection["Config"]).get("Labels")
            if isinstance(labels, Mapping):
                actual_labels = labels
        try:
            evidence = self._image_evidence(contract, inspection) if inspection is not None else None
        except RuntimeError:
            evidence = None
        if (
            inspection is None
            or any(actual_labels.get(key) != value for key, value in expected_labels.items())
            or evidence is None
            or not self._evidence_matches(evidence, contract)
        ):
            self._build_image(contract)
            inspection = self._inspect_image()
            if inspection is None:
                raise RuntimeError("canonical TPC-H image is unavailable after build")
            evidence = self._image_evidence(contract, inspection)
        if not self._evidence_matches(evidence, contract):
            raise RuntimeError("canonical TPC-H image failed post-build verification")
        self._verified_image_id = evidence.image_id
        return evidence

    def run(
        self,
        contract: GeneratorContract,
        scale: GeneratorScale,
        output_root: Path,
        metadata_path: Path,
    ) -> None:
        image_id = self._verified_image_id
        if image_id is None:
            raise RuntimeError("canonical TPC-H run requires a verified image ID")
        generated = output_root / "output"
        command = [
            "docker",
            "run",
            "--rm",
            "--network=none",
            "--platform",
            contract.environment.platform,
            "--volume",
            f"{output_root.resolve()}:/out",
            image_id,
            "--scale",
            canonical_scale(scale.scale_factor),
            "--output-dir",
            "/out/output",
            "--metadata",
            f"/out/{metadata_path.name}",
        ]
        primary: BaseException | None = None
        try:
            self._execute(command)
        except BaseException as error:
            primary = error
        handoff = [
            "docker",
            "run",
            "--rm",
            "--network=none",
            "--platform",
            contract.environment.platform,
            "--volume",
            f"{output_root.resolve()}:/out",
            "--entrypoint",
            "/bin/chown",
            image_id,
            "-R",
            f"{os.getuid()}:{os.getgid()}",
            "/out",
        ]
        try:
            self._execute(handoff)
        except BaseException as handoff_error:
            if primary is None:
                raise RuntimeError("canonical TPC-H output ownership handoff failed") from handoff_error
            primary.add_note(f"TPC-H output ownership handoff failed: {handoff_error}")
        if primary is not None:
            raise primary
        if not generated.is_dir():
            raise RuntimeError("canonical TPC-H container did not create its output directory")
        _secure_host_outputs(output_root, generated, metadata_path)
        for path in generated.iterdir():
            path.rename(output_root / path.name)
        generated.rmdir()


def require_generator_contract(contract: GeneratorContract | None) -> GeneratorContract:
    if contract is None:
        raise ValueError("TPC-H plan requires a generator contract")
    return contract


def require_generator_scale(scale: GeneratorScale | None) -> GeneratorScale:
    if scale is None:
        raise ValueError("TPC-H plan requires a generator scale")
    if len(scale.outputs) != 8:
        raise ValueError("TPC-H generator scale must lock exactly eight outputs")
    return scale


def _mismatch(context: VerificationContext, field: str, expected: object, actual: object) -> None:
    if type(actual) is not type(expected) or actual != expected:
        raise LockMismatch(context, field, expected, actual)


def verify_image_evidence(
    evidence: ImageEvidence,
    contract: GeneratorContract,
    context: VerificationContext,
    repository_root: Path = _ROOT,
) -> None:
    if type(evidence.image_id) is not str or re.fullmatch(r"sha256:[0-9a-f]{64}", evidence.image_id) is None:
        raise LockMismatch(context, "image_id", "immutable sha256 image ID", evidence.image_id)
    expected = {
        "base_image": contract.environment.image,
        "base_image_digest": contract.environment.image_digest,
        "platform": contract.environment.platform,
        "entrypoint": CANONICAL_ENTRYPOINT,
        "environment": canonical_environment(contract),
        "labels": canonical_labels(contract),
        "uv_lock_sha256": contract.environment.uv_lock_sha256,
        "duckdb_version": contract.engine_version,
        "duckdb_wheel_sha256": contract.engine_wheel_sha256,
        "tpch_extension_sha256": contract.extension_sha256,
        "requirements_sha256": _sha256(Path(repository_root) / _REQUIREMENTS),
        "exporter_sha256": _sha256(Path(repository_root) / _EXPORTER),
        "base_rootfs_match": True,
        "uses_hashed_requirements": True,
    }
    for field, value in expected.items():
        _mismatch(context, field, value, getattr(evidence, field))


@contextmanager
def owned_directory(destination: Path) -> Iterator[Path]:
    destination.mkdir(parents=True, exist_ok=True)
    acquisition._require_trusted_parent(destination)
    root = Path(tempfile.mkdtemp(prefix=".dataset-tpch-", dir=destination))
    status = root.lstat()
    identity = (status.st_dev, status.st_ino)
    primary: BaseException | None = None
    try:
        yield root
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            _quarantine_owned_path(root, identity, directory=True)
        except BaseException as cleanup_error:
            if primary is None:
                raise
            primary.add_note(f"TPC-H transaction cleanup failed: {cleanup_error}")


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: _UniqueKeyLoader,
    node: yaml.MappingNode,
    deep: bool = False,
) -> dict[object, object]:
    result: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in result
        except TypeError as error:
            raise yaml.constructor.ConstructorError(
                "while constructing metadata mapping",
                node.start_mark,
                "metadata keys must be scalar",
                key_node.start_mark,
            ) from error
        if duplicate:
            raise yaml.constructor.ConstructorError(
                "while constructing metadata mapping",
                node.start_mark,
                f"duplicate metadata key: {key!r}",
                key_node.start_mark,
            )
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _metadata_document(path: Path, context: VerificationContext) -> Mapping[str, object]:
    try:
        document = yaml.load(path.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
    except (OSError, UnicodeError, yaml.YAMLError) as error:
        field = "duplicate metadata key" if "duplicate metadata key" in str(error) else "metadata"
        raise LockMismatch(context, field, "valid unique-key exporter metadata", type(error).__name__) from error
    if not isinstance(document, Mapping):
        raise LockMismatch(context, "metadata", "mapping", type(document).__name__)
    return document


def _expected_object(output: GeneratorOutput) -> ExpectedObject:
    return ExpectedObject(
        object_name=output.object_name,
        size_bytes=output.size_bytes,
        sha256=output.sha256,
        schema_id=output.schema_id,
    )


def verify_tpch_outputs(
    plan: ScalePlan,
    output_root: Path,
    metadata_path: Path,
) -> tuple[VerifiedFile, ...]:
    contract = require_generator_contract(plan.dataset.generator)
    scale = require_generator_scale(plan.generator_scale)
    context = VerificationContext(plan.dataset.name, plan.scale, "generator metadata")
    expected_tables = tuple(output.table for output in scale.outputs)
    expected_names = tuple(output.object_name for output in scale.outputs)
    _mismatch(context, "tables", tuple(contract.order_by), expected_tables)
    _mismatch(context, "output_count", 8, len(expected_names))

    document = _metadata_document(metadata_path, context)
    _mismatch(
        context,
        "metadata_fields",
        ("scale_factor", "environment", "outputs"),
        tuple(document),
    )
    _mismatch(context, "scale_factor", scale.scale_factor, document.get("scale_factor"))
    raw_environment = document.get("environment")
    if not isinstance(raw_environment, Mapping):
        raise LockMismatch(context, "environment", expected_metadata_environment(contract), raw_environment)
    expected_environment = expected_metadata_environment(contract)
    for field, value in expected_environment.items():
        _mismatch(context, field, value, raw_environment.get(field))
    _mismatch(context, "environment_fields", tuple(expected_environment), tuple(raw_environment))

    raw_outputs = document.get("outputs")
    if not isinstance(raw_outputs, Mapping):
        raise LockMismatch(context, "outputs", expected_tables, raw_outputs)
    require_exact_names(expected_tables, tuple(raw_outputs), context)

    existing_names = {path.name for path in output_root.iterdir() if path != metadata_path}
    actual_names = tuple(name for name in expected_names if name in existing_names) + tuple(
        sorted(existing_names - set(expected_names))
    )
    require_exact_names(expected_names, actual_names, context)

    verified: list[VerifiedFile] = []
    for output in scale.outputs:
        output_context = VerificationContext(
            plan.dataset.name,
            plan.scale,
            "generator output",
            object_name=output.object_name,
        )
        raw_output = raw_outputs[output.table]
        if not isinstance(raw_output, Mapping):
            raise LockMismatch(output_context, "metadata", "mapping", type(raw_output).__name__)
        expected_metadata = {
            "object_name": output.object_name,
            "size_bytes": output.size_bytes,
            "sha256": output.sha256,
        }
        _mismatch(output_context, "metadata_fields", tuple(expected_metadata), tuple(raw_output))
        for field, value in expected_metadata.items():
            _mismatch(output_context, field, value, raw_output.get(field))
        try:
            schema = plan.dataset.schemas[output.schema_id]
        except KeyError:
            raise LockMismatch(
                output_context,
                "schema_id",
                tuple(plan.dataset.schemas),
                output.schema_id,
            ) from None
        item = verify_file(output_root / output.object_name, _expected_object(output), output_context)
        verify_physical_schema(item, schema, output_context)
        verified.append(item)
    return tuple(verified)


def publish_verified_files(
    files: Sequence[VerifiedFile],
    output_root: Path,
    destination: Path,
) -> _Publication:
    del output_root
    targets = tuple(destination / item.expected.object_name for item in files)
    for target in targets:
        if target.exists() or target.is_symlink():
            raise FileExistsError(target)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    destination_descriptor = os.open(destination, flags)
    opened = os.fstat(destination_descriptor)
    destination_identity = (opened.st_dev, opened.st_ino)
    publication = _Publication((), [], destination_descriptor, destination_identity)
    try:
        for item, target in zip(files, targets, strict=True):
            _verify_directory_binding(destination, destination_descriptor, destination_identity)
            source_identity = _path_identity(item.path, directory=False)
            if source_identity is None:
                raise ValueError("verified TPC-H output identity is unavailable")
            os.link(
                item.path,
                target.name,
                dst_dir_fd=destination_descriptor,
                follow_symlinks=False,
            )
            target_identity = _path_identity(target, directory=False)
            if target_identity != source_identity:
                raise ValueError("published TPC-H output identity changed")
            publication.owned_links.append((target, target_identity))
        publication.files = tuple(
            VerifiedFile(path.resolve(strict=True), item.expected) for item, path in zip(files, targets, strict=True)
        )
        return publication
    except BaseException as error:
        publication.rollback(error)
        raise


def generate_tpch(
    plan: ScalePlan,
    dest: Path,
    runner: ContainerRunner | None = None,
) -> tuple[VerifiedFile, ...]:
    """Generate, verify, and atomically publish one locked TPC-H scale."""
    contract = require_generator_contract(plan.dataset.generator)
    scale = require_generator_scale(plan.generator_scale)
    active_runner = runner or DockerContainerRunner()
    image_context = VerificationContext(plan.dataset.name, plan.scale, "image")
    evidence = active_runner.ensure_image(contract)
    verify_image_evidence(evidence, contract, image_context)
    destination = Path(dest)
    publication: _Publication | None = None
    try:
        with owned_directory(destination) as output_root:
            metadata_path = output_root / "metadata.json"
            active_runner.run(contract, scale, output_root, metadata_path)
            verified = verify_tpch_outputs(plan, output_root, metadata_path)
            publication = publish_verified_files(verified, output_root, destination)
        for item in publication.files:
            output = next(output for output in scale.outputs if output.object_name == item.expected.object_name)
            context = VerificationContext(
                plan.dataset.name,
                plan.scale,
                "published output",
                object_name=output.object_name,
            )
            verify_file(item.path, item.expected, context)
        publication.commit()
    except BaseException as error:
        if publication is not None:
            publication.rollback(error)
        raise
    return publication.files
