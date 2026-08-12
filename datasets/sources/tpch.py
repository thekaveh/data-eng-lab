"""Generate locked TPC-H outputs with the canonical offline container."""

from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import os
import re
import secrets
import stat
import subprocess
import sys
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
    verify_stream,
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


class _ImageProofMismatch(RuntimeError):
    """The inspected image ran, but its canonical proof was invalid."""


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
    owned_links: list[tuple[str, tuple[int, int]]]
    staged_links: list[tuple[str, tuple[int, int]]]
    destination_descriptor: int
    rollback_descriptor: int
    parent_descriptor: int
    parent_path: Path
    parent_identity: tuple[int, int]
    destination_name: str
    destination_identity: tuple[int, int]
    active: bool = True

    def rollback(self, primary: BaseException) -> None:
        if not self.active:
            return
        self.active = False
        for name, identity in reversed(self.owned_links + self.staged_links):
            try:
                _remove_owned_entry(self.rollback_descriptor, name, identity)
            except BaseException as cleanup_error:
                primary.add_note(f"TPC-H publication rollback failed for {name}: {cleanup_error}")
        self._close_after_rollback(primary)

    def _close_after_rollback(self, primary: BaseException) -> None:
        descriptors = (
            self.destination_descriptor,
            self.parent_descriptor,
            self.rollback_descriptor,
        )
        self.destination_descriptor = -1
        self.parent_descriptor = -1
        self.rollback_descriptor = -1
        for descriptor in descriptors:
            if descriptor < 0:
                continue
            try:
                os.close(descriptor)
            except OSError as error:
                primary.add_note(f"TPC-H destination capability release failed: {error}")

    def commit(self) -> None:
        first_error: OSError | None = None
        for field in ("destination_descriptor", "parent_descriptor"):
            descriptor = getattr(self, field)
            setattr(self, field, -1)
            if descriptor < 0:
                continue
            try:
                os.close(descriptor)
            except OSError as error:
                first_error = first_error or error
        if first_error is not None:
            raise first_error
        self.active = False
        descriptor = self.rollback_descriptor
        self.rollback_descriptor = -1
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass

    def verify_final_identities(self) -> None:
        for name, identity in self.owned_links:
            status = _entry_status(self.destination_descriptor, name)
            if status is None or not stat.S_ISREG(status.st_mode) or (status.st_dev, status.st_ino) != identity:
                raise ValueError(f"published TPC-H output identity changed: {name}")

    def verify_destination_binding(self) -> None:
        _verify_destination_binding(
            self.parent_path,
            self.parent_descriptor,
            self.parent_identity,
            self.destination_name,
            self.destination_identity,
        )


@dataclass(frozen=True)
class _OwnedDirectory:
    root: Path
    root_name: str
    root_identity: tuple[int, int]
    root_descriptor: int
    parent_path: Path
    parent_identity: tuple[int, int]
    parent_descriptor: int
    destination_name: str
    destination_identity: tuple[int, int]
    destination_descriptor: int

    def verify_bindings(self) -> None:
        _verify_parent_binding(self.parent_path, self.parent_descriptor, self.parent_identity)
        _verify_destination_binding(
            self.parent_path,
            self.parent_descriptor,
            self.parent_identity,
            self.destination_name,
            self.destination_identity,
        )
        status = os.stat(self.root_name, dir_fd=self.parent_descriptor, follow_symlinks=False)
        opened = os.fstat(self.root_descriptor)
        if (
            not stat.S_ISDIR(status.st_mode)
            or not stat.S_ISDIR(opened.st_mode)
            or (status.st_dev, status.st_ino) != self.root_identity
            or (opened.st_dev, opened.st_ino) != self.root_identity
        ):
            raise ValueError("TPC-H transaction staging changed")
        path_status = self.root.lstat()
        if (path_status.st_dev, path_status.st_ino) != self.root_identity or not stat.S_ISDIR(path_status.st_mode):
            raise ValueError("TPC-H transaction staging path changed")


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


def _renameat_noreplace(directory_descriptor: int, source: str, destination: str) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    ctypes.set_errno(0)
    source_bytes = os.fsencode(source)
    destination_bytes = os.fsencode(destination)
    if sys.platform == "darwin" and hasattr(libc, "renameatx_np"):
        rename = libc.renameatx_np
        rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        result = rename(directory_descriptor, source_bytes, directory_descriptor, destination_bytes, 0x00000004)
    elif sys.platform == "linux":
        if hasattr(libc, "renameat2"):
            rename = libc.renameat2
            rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
            rename.restype = ctypes.c_int
            result = rename(directory_descriptor, source_bytes, directory_descriptor, destination_bytes, 1)
        elif (number := acquisition._linux_renameat2_number()) is not None and hasattr(libc, "syscall"):
            syscall = libc.syscall
            syscall.restype = ctypes.c_long
            result = syscall(number, directory_descriptor, source_bytes, directory_descriptor, destination_bytes, 1)
        else:
            raise RuntimeError("secure TPC-H publication is not supported on this platform")
    else:
        raise RuntimeError("secure TPC-H publication is not supported on this platform")
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise FileExistsError(destination)
    if error_number == errno.ENOENT:
        raise FileNotFoundError(source)
    raise OSError(error_number, os.strerror(error_number), destination)


def _entry_status(directory_descriptor: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(name, dir_fd=directory_descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return None


def _directory_flags() -> int:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    return flags | getattr(os, "O_NOFOLLOW", 0)


def _verify_parent_binding(
    parent_path: Path,
    parent_descriptor: int,
    parent_identity: tuple[int, int],
) -> None:
    try:
        current = parent_path.lstat()
        opened = os.fstat(parent_descriptor)
    except OSError as error:
        raise ValueError("TPC-H destination parent changed") from error
    if (
        not stat.S_ISDIR(current.st_mode)
        or not stat.S_ISDIR(opened.st_mode)
        or (current.st_dev, current.st_ino) != parent_identity
        or (opened.st_dev, opened.st_ino) != parent_identity
    ):
        raise ValueError("TPC-H destination parent changed")


def _verify_destination_binding(
    parent_path: Path,
    parent_descriptor: int,
    parent_identity: tuple[int, int],
    destination_name: str,
    destination_identity: tuple[int, int],
) -> None:
    _verify_parent_binding(parent_path, parent_descriptor, parent_identity)
    descriptor = -1
    try:
        current = os.stat(destination_name, dir_fd=parent_descriptor, follow_symlinks=False)
        descriptor = os.open(destination_name, _directory_flags(), dir_fd=parent_descriptor)
        opened = os.fstat(descriptor)
    except OSError as error:
        raise ValueError("TPC-H destination directory changed") from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if (
        not stat.S_ISDIR(current.st_mode)
        or not stat.S_ISDIR(opened.st_mode)
        or (current.st_dev, current.st_ino) != destination_identity
        or (opened.st_dev, opened.st_ino) != destination_identity
    ):
        raise ValueError("TPC-H destination directory changed")


def _remove_owned_entry(
    directory_descriptor: int,
    name: str,
    identity: tuple[int, int],
) -> None:
    current = _entry_status(directory_descriptor, name)
    if current is None:
        return
    if (current.st_dev, current.st_ino) != identity:
        raise ValueError("owned TPC-H entry identity changed during cleanup")
    quarantine = f".dataset-cleanup-tpch-{secrets.token_hex(16)}"
    _renameat_noreplace(directory_descriptor, name, quarantine)
    quarantined = _entry_status(directory_descriptor, quarantine)
    if quarantined is None or (quarantined.st_dev, quarantined.st_ino) != identity:
        try:
            _renameat_noreplace(directory_descriptor, quarantine, name)
        except BaseException as restore_error:
            raise RuntimeError(f"foreign replacement remains quarantined as {quarantine}") from restore_error
        raise ValueError("owned TPC-H entry changed during cleanup")
    os.unlink(quarantine, dir_fd=directory_descriptor)


def _remove_tree_at(
    directory_descriptor: int,
    name: str,
    identity: tuple[int, int],
) -> None:
    status = _entry_status(directory_descriptor, name)
    if status is None:
        return
    if not stat.S_ISDIR(status.st_mode) or (status.st_dev, status.st_ino) != identity:
        raise ValueError("owned TPC-H transaction directory changed")
    quarantine = f".dataset-cleanup-tpch-{secrets.token_hex(16)}"
    _renameat_noreplace(directory_descriptor, name, quarantine)
    quarantined = _entry_status(directory_descriptor, quarantine)
    if (
        quarantined is None
        or not stat.S_ISDIR(quarantined.st_mode)
        or (quarantined.st_dev, quarantined.st_ino) != identity
    ):
        try:
            _renameat_noreplace(directory_descriptor, quarantine, name)
        except BaseException as restore_error:
            raise RuntimeError(f"foreign replacement remains quarantined as {quarantine}") from restore_error
        raise ValueError("owned TPC-H transaction directory changed during cleanup")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    child_descriptor = os.open(quarantine, flags, dir_fd=directory_descriptor)
    try:
        opened = os.fstat(child_descriptor)
        if (opened.st_dev, opened.st_ino) != identity:
            raise ValueError("owned TPC-H transaction directory changed during cleanup")
        for entry in os.scandir(child_descriptor):
            entry_status = os.stat(entry.name, dir_fd=child_descriptor, follow_symlinks=False)
            entry_identity = (entry_status.st_dev, entry_status.st_ino)
            if stat.S_ISDIR(entry_status.st_mode):
                _remove_tree_at(child_descriptor, entry.name, entry_identity)
            else:
                _remove_owned_entry(child_descriptor, entry.name, entry_identity)
    finally:
        os.close(child_descriptor)
    current = _entry_status(directory_descriptor, quarantine)
    if current is None or (current.st_dev, current.st_ino) != identity or not stat.S_ISDIR(current.st_mode):
        raise ValueError("owned TPC-H transaction directory changed during cleanup")
    os.rmdir(quarantine, dir_fd=directory_descriptor)


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
    paths = (output_root, generated, metadata_path, *tuple(generated.iterdir()))
    for path in paths:
        try:
            status = path.lstat()
        except OSError as error:
            raise RuntimeError("canonical TPC-H output ownership handoff is incomplete") from error
        if stat.S_ISLNK(status.st_mode) or status.st_uid != expected_uid:
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
        listed = self._execute(["docker", "image", "ls", "--quiet", "--no-trunc", self.image_tag])
        if not listed.stdout.strip():
            return None
        result = self._execute(["docker", "image", "inspect", self.image_tag])
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
        try:
            result = subprocess.run(
                command,
                cwd=self.repository_root,
                check=False,
                capture_output=True,
                text=True,
            )
        except (FileNotFoundError, OSError) as error:
            raise RuntimeError("canonical TPC-H runtime probe could not launch Docker") from error
        if result.returncode == 125:
            raise RuntimeError("canonical TPC-H runtime probe could not start the container")
        if result.returncode != 0:
            raise _ImageProofMismatch("canonical TPC-H runtime probe program failed")
        try:
            document = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise _ImageProofMismatch("canonical TPC-H runtime probe returned invalid evidence") from error
        expected_fields = (
            "duckdb_version",
            "duckdb_wheel_sha256",
            "uv_lock_sha256",
            "tpch_extension_sha256",
            "requirements_sha256",
            "exporter_sha256",
        )
        if not isinstance(document, dict) or tuple(document) != expected_fields:
            raise _ImageProofMismatch("canonical TPC-H runtime probe returned invalid evidence")
        if any(type(document[field]) is not str for field in expected_fields):
            raise _ImageProofMismatch("canonical TPC-H runtime probe returned invalid evidence")
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

    def _cheap_inspection_matches(
        self,
        inspection: Mapping[str, object],
        contract: GeneratorContract,
    ) -> bool:
        image_id = inspection.get("Id")
        config = inspection.get("Config")
        if (
            not isinstance(image_id, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None
            or not isinstance(config, Mapping)
            or inspection.get("Os") != "linux"
            or inspection.get("Architecture") != "amd64"
            or tuple(config.get("Entrypoint") or ()) != CANONICAL_ENTRYPOINT
        ):
            return False
        labels = config.get("Labels")
        environment = config.get("Env")
        if not isinstance(labels, Mapping) or not isinstance(environment, Sequence) or isinstance(environment, str):
            return False
        expected_labels = canonical_labels(contract, self.repository_root)
        if any(labels.get(key) != value for key, value in expected_labels.items()):
            return False
        actual_environment = {
            item.split("=", 1)[0]: item.split("=", 1)[1]
            for item in environment
            if isinstance(item, str) and "=" in item
        }
        return all(actual_environment.get(key) == value for key, value in canonical_environment(contract).items())

    def _rebuild_and_verify(self, contract: GeneratorContract) -> ImageEvidence:
        self._build_image(contract)
        inspection = self._inspect_image()
        if inspection is None:
            raise RuntimeError("canonical TPC-H image is unavailable after build")
        if not self._cheap_inspection_matches(inspection, contract):
            raise RuntimeError("canonical TPC-H image failed post-build config verification")
        try:
            evidence = self._image_evidence(contract, inspection)
        except _ImageProofMismatch as error:
            raise RuntimeError("canonical TPC-H image failed post-build proof verification") from error
        if not self._evidence_matches(evidence, contract):
            raise RuntimeError("canonical TPC-H image failed post-build verification")
        return evidence

    def ensure_image(self, contract: GeneratorContract) -> ImageEvidence:
        inspection = self._inspect_image()
        if inspection is None or not self._cheap_inspection_matches(inspection, contract):
            evidence = self._rebuild_and_verify(contract)
        else:
            try:
                evidence = self._image_evidence(contract, inspection)
            except _ImageProofMismatch:
                evidence = self._rebuild_and_verify(contract)
            else:
                if not self._evidence_matches(evidence, contract):
                    evidence = self._rebuild_and_verify(contract)
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
            f"{output_root.resolve()}:/out",
            image_id,
            "--scale",
            canonical_scale(scale.scale_factor),
            "--output-dir",
            "/out/output",
            "--metadata",
            f"/out/{metadata_path.name}",
        ]
        self._execute(command)
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
def owned_directory(destination: Path) -> Iterator[_OwnedDirectory]:
    destination = destination.absolute()
    destination.mkdir(parents=True, exist_ok=True)
    acquisition._require_trusted_parent(destination)
    parent_path = destination.parent
    acquisition._require_trusted_parent(parent_path)
    parent_descriptor = os.open(parent_path, _directory_flags())
    parent_status = os.fstat(parent_descriptor)
    parent_identity = (parent_status.st_dev, parent_status.st_ino)
    destination_descriptor = -1
    root_descriptor = -1
    root_name: str | None = None
    root_identity: tuple[int, int] | None = None
    primary: BaseException | None = None
    try:
        _verify_parent_binding(parent_path, parent_descriptor, parent_identity)
        destination_descriptor = os.open(
            destination.name,
            _directory_flags(),
            dir_fd=parent_descriptor,
        )
        destination_status = os.fstat(destination_descriptor)
        destination_identity = (destination_status.st_dev, destination_status.st_ino)
        for _ in range(100):
            candidate = f".dataset-tpch-{secrets.token_hex(16)}"
            try:
                os.mkdir(candidate, 0o700, dir_fd=parent_descriptor)
            except FileExistsError:
                continue
            root_name = candidate
            break
        if root_name is None:
            raise RuntimeError("could not allocate canonical TPC-H transaction staging")
        root_status = os.stat(root_name, dir_fd=parent_descriptor, follow_symlinks=False)
        root_identity = (root_status.st_dev, root_status.st_ino)
        root_descriptor = os.open(root_name, _directory_flags(), dir_fd=parent_descriptor)
        transaction = _OwnedDirectory(
            parent_path / root_name,
            root_name,
            root_identity,
            root_descriptor,
            parent_path,
            parent_identity,
            parent_descriptor,
            destination.name,
            destination_identity,
            destination_descriptor,
        )
        transaction.verify_bindings()
        yield transaction
    except BaseException as error:
        primary = error
        raise
    finally:
        if root_name is not None and root_identity is not None:
            try:
                _remove_tree_at(parent_descriptor, root_name, root_identity)
            except BaseException as cleanup_error:
                if primary is None:
                    primary = cleanup_error
                else:
                    primary.add_note(f"TPC-H transaction cleanup failed: {cleanup_error}")
        for descriptor in (root_descriptor, destination_descriptor, parent_descriptor):
            if descriptor < 0:
                continue
            try:
                os.close(descriptor)
            except BaseException as cleanup_error:
                if primary is None:
                    primary = cleanup_error
                else:
                    primary.add_note(f"TPC-H transaction capability release failed: {cleanup_error}")
        if primary is not None and sys.exc_info()[0] is None:
            raise primary


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


def _verify_staged_output(
    descriptor: int,
    item: VerifiedFile,
    schema: object,
    context: VerificationContext,
) -> None:
    opened = os.fstat(descriptor)
    if not stat.S_ISREG(opened.st_mode):
        raise ValueError("staged TPC-H output must be a regular file")
    stream_descriptor = os.dup(descriptor)
    try:
        stream = os.fdopen(stream_descriptor, "rb")
    except BaseException:
        os.close(stream_descriptor)
        raise
    with stream:
        verify_stream(stream, item.expected.size_bytes, item.expected.sha256, context)
    os.lseek(descriptor, 0, os.SEEK_SET)
    staged = VerifiedFile(Path(f"/dev/fd/{descriptor}"), item.expected)
    verify_physical_schema(staged, schema, context)  # type: ignore[arg-type]


def publish_verified_files(
    files: Sequence[VerifiedFile],
    transaction: _OwnedDirectory,
    destination: Path,
    schemas: Mapping[str, object],
) -> _Publication:
    del destination
    destination_descriptor = -1
    rollback_descriptor = -1
    parent_descriptor = -1
    try:
        destination_descriptor = os.dup(transaction.destination_descriptor)
        rollback_descriptor = os.dup(transaction.destination_descriptor)
        parent_descriptor = os.dup(transaction.parent_descriptor)
    except BaseException as error:
        for descriptor in (destination_descriptor, rollback_descriptor, parent_descriptor):
            if descriptor < 0:
                continue
            try:
                os.close(descriptor)
            except OSError as close_error:
                error.add_note(f"TPC-H publication capability release failed: {close_error}")
        raise
    publication = _Publication(
        (),
        [],
        [],
        destination_descriptor,
        rollback_descriptor,
        parent_descriptor,
        transaction.parent_path,
        transaction.parent_identity,
        transaction.destination_name,
        transaction.destination_identity,
    )
    try:
        source_status = os.fstat(transaction.root_descriptor)
        if (source_status.st_dev, source_status.st_ino) != transaction.root_identity:
            raise ValueError("TPC-H transaction directory identity changed")
        for item in files:
            staging_name = f".dataset-tpch-publish-{secrets.token_hex(16)}"
            os.link(
                item.expected.object_name,
                staging_name,
                src_dir_fd=transaction.root_descriptor,
                dst_dir_fd=destination_descriptor,
                follow_symlinks=False,
            )
            staged_status = os.stat(staging_name, dir_fd=destination_descriptor, follow_symlinks=False)
            staged_identity = (staged_status.st_dev, staged_status.st_ino)
            publication.staged_links.append((staging_name, staged_identity))
            if not stat.S_ISREG(staged_status.st_mode):
                raise ValueError("staged TPC-H output must be a regular file")
            open_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            staged_descriptor = os.open(staging_name, open_flags, dir_fd=destination_descriptor)
            try:
                opened = os.fstat(staged_descriptor)
                if (opened.st_dev, opened.st_ino) != staged_identity or not stat.S_ISREG(opened.st_mode):
                    raise ValueError("staged TPC-H output identity changed")
                context = VerificationContext(
                    "tpch",
                    "publication",
                    "staged output",
                    object_name=item.expected.object_name,
                )
                try:
                    schema = schemas[item.expected.schema_id]
                except KeyError:
                    raise ValueError(f"unknown staged TPC-H schema: {item.expected.schema_id}") from None
                _verify_staged_output(staged_descriptor, item, schema, context)
            finally:
                os.close(staged_descriptor)
            _renameat_noreplace(destination_descriptor, staging_name, item.expected.object_name)
            publication.staged_links.remove((staging_name, staged_identity))
            publication.owned_links.append((item.expected.object_name, staged_identity))
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
    destination = Path(dest).absolute()
    publication: _Publication | None = None
    try:
        with owned_directory(destination) as transaction:
            transaction.verify_bindings()
            metadata_path = transaction.root / "metadata.json"
            active_runner.run(contract, scale, transaction.root, metadata_path)
            transaction.verify_bindings()
            verified = verify_tpch_outputs(plan, transaction.root, metadata_path)
            publication = publish_verified_files(
                verified,
                transaction,
                destination,
                plan.dataset.schemas,
            )
        publication.verify_final_identities()
        publication.verify_destination_binding()
        publication.files = tuple(
            VerifiedFile(destination / item.expected.object_name, item.expected) for item in verified
        )
        publication.verify_destination_binding()
        publication.commit()
    except BaseException as error:
        if publication is not None:
            publication.rollback(error)
        raise
    return publication.files
