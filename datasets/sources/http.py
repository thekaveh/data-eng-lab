"""Typed HTTP artifact acquisition with lock verification and atomic return."""

from __future__ import annotations

import os
import stat
import tempfile
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from datasets import acquisition
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
    verify_stream,
)

if TYPE_CHECKING:
    from datasets.registry import HttpArtifact, LandingObject, ScalePlan, SchemaContract


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


@dataclass(frozen=True)
class _BoundOutput:
    source_path: Path
    descriptor: int
    identity: tuple[int, int]
    expected: ExpectedObject
    context: VerificationContext
    schema: SchemaContract


@dataclass
class _Publication:
    files: tuple[VerifiedFile, ...]
    owned_links: list[tuple[Path, tuple[int, int]]]
    active: bool = True

    def rollback(self, primary: BaseException) -> None:
        if not self.active:
            return
        self.active = False
        for path, identity in reversed(self.owned_links):
            try:
                _remove_published(path, identity)
            except BaseException as cleanup_error:
                primary.add_note(f"HTTP publication rollback failed: {cleanup_error}")

    def commit(self) -> None:
        self.active = False


@contextmanager
def _owned_directory(destination: Path) -> Iterator[Path]:
    destination.mkdir(parents=True, exist_ok=True)
    acquisition._require_trusted_parent(destination)
    root = Path(tempfile.mkdtemp(prefix=".dataset-http-", dir=destination))
    root_status = root.lstat()
    identity = (root_status.st_dev, root_status.st_ino)
    primary: BaseException | None = None
    try:
        yield root
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            acquisition._quarantine_owned_path(root, identity, directory=True)
        except BaseException as cleanup_error:
            if primary is None:
                raise
            primary.add_note(f"HTTP transaction cleanup failed: {cleanup_error}")


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


def _output_contract(
    plan: ScalePlan,
    artifact: HttpArtifact,
    output: LandingObject,
) -> tuple[ExpectedObject, VerificationContext, SchemaContract]:
    context = _context(plan, "output", artifact, output)
    expected = _expected(output)
    try:
        contract = plan.dataset.schemas[output.schema_id]
    except KeyError:
        raise LockMismatch(context, "schema_id", tuple(plan.dataset.schemas), output.schema_id) from None
    return expected, context, contract


def _require_metadata(
    metadata: tuple[int, str],
    expected: ExpectedObject,
    context: VerificationContext,
) -> None:
    actual_size, actual_sha256 = metadata
    if actual_size != expected.size_bytes:
        raise LockMismatch(context, "size_bytes", expected.size_bytes, actual_size)
    if actual_sha256 != expected.sha256:
        raise LockMismatch(context, "sha256", expected.sha256, actual_sha256)


def _bound_download(
    downloaded: acquisition.DownloadedFile,
) -> tuple[tuple[int, str], acquisition._FileBinding]:
    return (
        acquisition._bound_download_metadata(downloaded),
        acquisition._download_binding(downloaded),
    )


def _canonical_entries(entries: list[ArchiveEntry]) -> tuple[ArchiveEntry, ...]:
    return acquisition._canonical_archive_entries(entries)


def _canonical_extracted(
    extracted: list[Path],
) -> tuple[tuple[Path, ...], tuple[acquisition._FileBinding, ...], list[tuple[int, str]]]:
    if not isinstance(extracted, acquisition._ExtractedPaths):
        raise ValueError("extracted outputs are not bound to owned files")
    canonical_paths = extracted._canonical_paths()
    bindings = extracted._bindings
    metadata = acquisition._bound_extracted_metadata(extracted)
    if len(canonical_paths) != len(bindings) or len(canonical_paths) != len(metadata):
        raise ValueError("extracted output capability has inconsistent cardinality")
    return canonical_paths, bindings, metadata


def _require_bound_identity(bound: _BoundOutput, path: Path) -> None:
    try:
        opened = os.fstat(bound.descriptor)
        current = path.lstat()
    except OSError as error:
        raise ValueError("verified output identity is unavailable") from error
    expected_identity = bound.identity
    if (
        not stat.S_ISREG(opened.st_mode)
        or not stat.S_ISREG(current.st_mode)
        or (opened.st_dev, opened.st_ino) != expected_identity
        or (current.st_dev, current.st_ino) != expected_identity
    ):
        raise ValueError("verified output identity changed")


def _verify_descriptor(bound: _BoundOutput) -> None:
    try:
        duplicate = os.dup(bound.descriptor)
    except OSError as error:
        raise ValueError("verified output descriptor is unavailable") from error
    with os.fdopen(duplicate, "rb") as stream:
        stream.seek(0)
        verify_stream(
            stream,
            bound.expected.size_bytes,
            bound.expected.sha256,
            bound.context,
        )


def _verify_bound_output(bound: _BoundOutput, path: Path | None = None) -> VerifiedFile:
    identity_path = bound.source_path if path is None else path
    _require_bound_identity(bound, identity_path)
    _verify_descriptor(bound)
    try:
        os.lseek(bound.descriptor, 0, os.SEEK_SET)
    except OSError as error:
        raise ValueError("verified output descriptor is unavailable") from error
    descriptor_path = Path(f"/dev/fd/{bound.descriptor}")
    verify_physical_schema(
        VerifiedFile(descriptor_path, bound.expected),
        bound.schema,
        bound.context,
    )
    _verify_descriptor(bound)
    _require_bound_identity(bound, identity_path)
    return VerifiedFile(identity_path.resolve(strict=True), bound.expected)


def _fetch_artifact(
    plan: ScalePlan,
    artifact: HttpArtifact,
    transaction_root: Path,
    ordinal: int,
    releases: list[Callable[[], None]],
) -> tuple[_BoundOutput, ...]:
    artifact_root = transaction_root / f"artifact-{ordinal}"
    artifact_root.mkdir()
    raw_path = artifact_root / artifact.raw.name
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    downloaded = download_bounded(artifact.url, raw_path, artifact.raw.size_bytes + 1)
    releases.append(lambda downloaded=downloaded: acquisition._unbind_download(downloaded))
    raw_context = _context(plan, "raw", artifact)
    raw_expected = _raw_expected(artifact)
    raw_metadata, raw_binding = _bound_download(downloaded)
    _require_metadata(raw_metadata, raw_expected, raw_context)

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
        canonical_entries = _canonical_entries(entries)
        archive_snapshot, _limits = entries._capability()  # type: ignore[attr-defined]
        if (
            (archive_snapshot.device, archive_snapshot.inode)
            != (raw_binding.device, raw_binding.inode)
            or (archive_snapshot.size_bytes, archive_snapshot.sha256)
            != (raw_expected.size_bytes, raw_expected.sha256)
        ):
            raise LockMismatch(
                raw_context,
                "archive_identity",
                (raw_binding.device, raw_binding.inode, raw_expected.size_bytes, raw_expected.sha256),
                (
                    archive_snapshot.device,
                    archive_snapshot.inode,
                    archive_snapshot.size_bytes,
                    archive_snapshot.sha256,
                ),
            )
        entries_by_member = _require_archive_mapping(plan, artifact, canonical_entries)
        extracted = extract_members(downloaded.path, entries, artifact_root / "members")
        releases.append(extracted.close)  # type: ignore[attr-defined]
        canonical_paths, bindings, _metadata = _canonical_extracted(extracted)
        if len(canonical_entries) != len(canonical_paths):
            raise ValueError("archive and extracted capabilities have inconsistent cardinality")
        paths_by_member = {
            entry.member_path: (path, binding)
            for entry, path, binding in zip(
                canonical_entries,
                canonical_paths,
                bindings,
                strict=True,
            )
        }
        bound_outputs: list[_BoundOutput] = []
        for output in artifact.outputs:
            member = entries_by_member[output.member_path]
            path, binding = paths_by_member[member.member_path]
            expected, context, contract = _output_contract(plan, artifact, output)
            bound = _BoundOutput(
                path,
                binding.descriptor,
                (binding.device, binding.inode),
                expected,
                context,
                contract,
            )
            _verify_bound_output(bound)
            bound_outputs.append(bound)
        return tuple(bound_outputs)

    if len(artifact.outputs) != 1:
        raise LockMismatch(
            _context(plan, "artifact", artifact),
            "outputs",
            "one raw-identity output",
            len(artifact.outputs),
        )
    output = artifact.outputs[0]
    _require_direct_identity(plan, artifact, output)
    expected, context, contract = _output_contract(plan, artifact, output)
    bound = _BoundOutput(
        downloaded.path,
        raw_binding.descriptor,
        (raw_binding.device, raw_binding.inode),
        expected,
        context,
        contract,
    )
    _verify_bound_output(bound)
    return (bound,)


def _remove_published(path: Path, identity: tuple[int, int]) -> None:
    acquisition._quarantine_owned_path(path, identity, directory=False)


def _publish_verified_files(files: Sequence[_BoundOutput], destination: Path) -> _Publication:
    targets = tuple(destination / item.expected.object_name for item in files)
    for target in targets:
        if target.exists() or target.is_symlink():
            raise FileExistsError(target)

    publication = _Publication((), [])
    try:
        for item, target in zip(files, targets, strict=True):
            _require_bound_identity(item, item.source_path)
            os.link(item.source_path, target)
            publication.owned_links.append((target, item.identity))
            _verify_bound_output(item, target)
        publication.files = tuple(
            VerifiedFile(path.resolve(strict=True), item.expected)
            for item, path in zip(files, targets, strict=True)
        )
        return publication
    except BaseException as error:
        publication.rollback(error)
        raise


def _release_all(releases: list[Callable[[], None]], primary: BaseException) -> None:
    while releases:
        release = releases.pop()
        try:
            release()
        except BaseException as cleanup_error:
            primary.add_note(f"HTTP capability release failed: {cleanup_error}")


def fetch_http(plan: ScalePlan, dest: Path) -> tuple[VerifiedFile, ...]:
    """Acquire, verify, and publish a registry-ordered HTTP scale transaction."""
    destination = Path(dest)
    results: list[_BoundOutput] = []
    releases: list[Callable[[], None]] = []
    publication: _Publication | None = None
    try:
        with _owned_directory(destination) as transaction_root:
            for ordinal, artifact in enumerate(plan.artifacts):
                results.extend(
                    _fetch_artifact(plan, artifact, transaction_root, ordinal, releases)
                )
            expected_names = tuple(
                output.object_name for artifact in plan.artifacts for output in artifact.outputs
            )
            actual_names = tuple(item.expected.object_name for item in results)
            require_exact_names(expected_names, actual_names, _context(plan, "result"))
            publication = _publish_verified_files(results, destination)
        for item, verified in zip(results, publication.files, strict=True):
            _verify_bound_output(item, verified.path)
        release_error = RuntimeError("HTTP capability release failed")
        _release_all(releases, release_error)
        if getattr(release_error, "__notes__", None):
            raise release_error
        publication.commit()
        return publication.files
    except BaseException as error:
        if publication is not None:
            publication.rollback(error)
        _release_all(releases, error)
        raise
