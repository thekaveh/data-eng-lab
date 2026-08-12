"""Typed HTTP artifact acquisition with lock verification and atomic return."""

from __future__ import annotations

import os
import shutil
import stat
import tempfile
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from datasets.acquisition import (
    ArchiveEntry,
    ZipLimits,
    download_bounded,
    extract_members,
    validated_zip_members,
)
from datasets.schema_inspection import verify_physical_schema
from datasets.verification import (
    ExpectedObject,
    LockMismatch,
    VerificationContext,
    VerifiedFile,
    require_exact_names,
    verify_file,
)

if TYPE_CHECKING:
    from datasets.registry import HttpArtifact, LandingObject, ScalePlan


def _context(
    plan: ScalePlan,
    stage: str,
    artifact: HttpArtifact | None = None,
    output: LandingObject | None = None,
) -> VerificationContext:
    return VerificationContext(
        dataset=plan.dataset.name,
        scale=plan.scale,
        stage=stage,
        artifact=artifact.id if artifact is not None else None,
        object_name=output.object_name if output is not None else None,
    )


def _expected(output: LandingObject) -> ExpectedObject:
    return ExpectedObject(
        object_name=output.object_name,
        size_bytes=output.size_bytes,
        sha256=output.sha256,
        schema_id=output.schema_id,
    )


def _raw_expected(artifact: HttpArtifact) -> ExpectedObject:
    return ExpectedObject(
        object_name=artifact.raw.name,
        size_bytes=artifact.raw.size_bytes,
        sha256=artifact.raw.sha256,
        schema_id="",
    )


@contextmanager
def _owned_directory(destination: Path) -> Iterator[Path]:
    destination.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix=".dataset-http-", dir=destination))
    try:
        yield root
    finally:
        shutil.rmtree(root)


def _require_direct_identity(
    plan: ScalePlan,
    artifact: HttpArtifact,
    output: LandingObject,
) -> None:
    context = _context(plan, "raw identity", artifact, output)
    expected = (
        artifact.raw.name,
        artifact.raw.size_bytes,
        artifact.raw.sha256,
        True,
        None,
    )
    actual = (
        output.object_name,
        output.size_bytes,
        output.sha256,
        output.raw_identity,
        output.member_path,
    )
    if actual != expected:
        raise LockMismatch(context, "raw_identity", expected, actual)


def _require_archive_mapping(
    plan: ScalePlan,
    artifact: HttpArtifact,
    entries: Sequence[ArchiveEntry],
) -> dict[str, ArchiveEntry]:
    context = _context(plan, "archive", artifact)
    expected_paths = tuple(output.member_path for output in artifact.outputs)
    actual_paths = tuple(entry.member_path for entry in entries)
    if any(path is None for path in expected_paths):
        raise LockMismatch(context, "member_paths", expected_paths, actual_paths)
    locked_paths = tuple(path for path in expected_paths if path is not None)
    if (
        len(set(locked_paths)) != len(locked_paths)
        or len(set(actual_paths)) != len(actual_paths)
        or set(actual_paths) != set(locked_paths)
    ):
        raise LockMismatch(context, "member_paths", locked_paths, actual_paths)
    entries_by_member = {entry.member_path: entry for entry in entries}
    expected_names = tuple(output.object_name for output in artifact.outputs)
    actual_names = tuple(entries_by_member[path].object_name for path in locked_paths)
    require_exact_names(expected_names, actual_names, context)
    return entries_by_member


def _verify_output(
    plan: ScalePlan,
    artifact: HttpArtifact,
    output: LandingObject,
    path: Path,
) -> VerifiedFile:
    context = _context(plan, "output", artifact, output)
    verified = verify_file(path, _expected(output), context)
    try:
        contract = plan.dataset.schemas[output.schema_id]
    except KeyError:
        raise LockMismatch(context, "schema_id", tuple(plan.dataset.schemas), output.schema_id) from None
    verify_physical_schema(verified, contract, context)
    return verified


def _fetch_artifact(
    plan: ScalePlan,
    artifact: HttpArtifact,
    transaction_root: Path,
    ordinal: int,
) -> list[VerifiedFile]:
    artifact_root = transaction_root / f"artifact-{ordinal}"
    artifact_root.mkdir()
    raw_path = artifact_root / artifact.raw.name
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    downloaded = download_bounded(artifact.url, raw_path, artifact.raw.size_bytes + 1)
    verify_file(downloaded.path, _raw_expected(artifact), _context(plan, "raw", artifact))

    archive_outputs = tuple(output for output in artifact.outputs if output.member_path is not None)
    if archive_outputs:
        if len(archive_outputs) != len(artifact.outputs) or any(output.raw_identity for output in artifact.outputs):
            raise LockMismatch(
                _context(plan, "artifact", artifact),
                "output_kind",
                "archive members",
                "mixed outputs",
            )
        entries = validated_zip_members(downloaded.path, ZipLimits())
        entries_by_member = _require_archive_mapping(plan, artifact, entries)
        extracted = extract_members(downloaded.path, entries, artifact_root / "members")
        try:
            paths_by_member = {
                entry.member_path: path for entry, path in zip(entries, extracted, strict=True)
            }
            return [
                _verify_output(
                    plan,
                    artifact,
                    output,
                    paths_by_member[entries_by_member[output.member_path].member_path],
                )
                for output in artifact.outputs
            ]
        finally:
            close = getattr(extracted, "close", None)
            if close is not None:
                close()

    if len(artifact.outputs) != 1:
        raise LockMismatch(
            _context(plan, "artifact", artifact),
            "outputs",
            "one raw-identity output",
            len(artifact.outputs),
        )
    output = artifact.outputs[0]
    _require_direct_identity(plan, artifact, output)
    return [_verify_output(plan, artifact, output, downloaded.path)]


def _remove_published(path: Path, identity: tuple[int, int]) -> None:
    try:
        current = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISREG(current.st_mode) and (current.st_dev, current.st_ino) == identity:
        path.unlink()


def _publish_verified_files(files: Sequence[VerifiedFile], destination: Path) -> list[VerifiedFile]:
    targets = tuple(destination / item.expected.object_name for item in files)
    for target in targets:
        if target.exists() or target.is_symlink():
            raise FileExistsError(target)

    published: list[tuple[Path, tuple[int, int]]] = []
    try:
        for item, target in zip(files, targets, strict=True):
            source_status = item.path.lstat()
            identity = (source_status.st_dev, source_status.st_ino)
            os.link(item.path, target)
            published.append((target, identity))
            status = target.lstat()
            if not stat.S_ISREG(status.st_mode) or (status.st_dev, status.st_ino) != identity:
                raise ValueError("published output identity changed")
        for path, identity in published:
            status = path.lstat()
            if not stat.S_ISREG(status.st_mode) or (status.st_dev, status.st_ino) != identity:
                raise ValueError("published output identity changed")
        return [
            VerifiedFile(path.resolve(strict=True), item.expected)
            for item, path in zip(files, targets, strict=True)
        ]
    except BaseException:
        for path, identity in reversed(published):
            _remove_published(path, identity)
        raise


def fetch_http(plan: ScalePlan, dest: Path) -> tuple[VerifiedFile, ...]:
    """Acquire, verify, and publish a registry-ordered HTTP scale transaction."""
    destination = Path(dest)
    results: list[VerifiedFile] = []
    with _owned_directory(destination) as transaction_root:
        for ordinal, artifact in enumerate(plan.artifacts):
            results.extend(_fetch_artifact(plan, artifact, transaction_root, ordinal))
        expected_names = tuple(
            output.object_name for artifact in plan.artifacts for output in artifact.outputs
        )
        actual_names = tuple(item.expected.object_name for item in results)
        require_exact_names(expected_names, actual_names, _context(plan, "result"))
        published = _publish_verified_files(results, destination)
    return tuple(published)
