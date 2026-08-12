"""Typed verification for dataset provenance locks."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from datasets.locking import file_metadata


@dataclass(frozen=True)
class VerificationContext:
    dataset: str
    scale: str
    stage: str
    artifact: str | None = None
    object_name: str | None = None


@dataclass(frozen=True)
class ExpectedObject:
    object_name: str
    size_bytes: int
    sha256: str
    schema_id: str


@dataclass(frozen=True)
class VerifiedFile:
    path: Path
    expected: ExpectedObject


class LockMismatch(ValueError):
    def __init__(self, context: VerificationContext, field: str, expected: object, actual: object):
        self.context = context
        self.field = field
        self.expected = expected
        self.actual = actual
        super().__init__(f"{context.dataset}/{context.scale} {context.stage} {field} mismatch")


def verify_stream(
    stream: BinaryIO,
    expected_size: int,
    expected_sha256: str,
    context: VerificationContext,
) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    while chunk := stream.read(min(1 << 20, expected_size - size + 1)):
        size += len(chunk)
        if size > expected_size:
            raise LockMismatch(context, "size_bytes", expected_size, size)
        digest.update(chunk)
    if size != expected_size:
        raise LockMismatch(context, "size_bytes", expected_size, size)
    actual_sha256 = digest.hexdigest()
    if actual_sha256 != expected_sha256:
        raise LockMismatch(context, "sha256", expected_sha256, actual_sha256)
    return size, actual_sha256


def verify_file(path: Path, expected: ExpectedObject, context: VerificationContext) -> VerifiedFile:
    actual_size, actual_sha256 = file_metadata(path)
    if actual_size != expected.size_bytes:
        raise LockMismatch(context, "size_bytes", expected.size_bytes, actual_size)
    if actual_sha256 != expected.sha256:
        raise LockMismatch(context, "sha256", expected.sha256, actual_sha256)
    return VerifiedFile(path.resolve(strict=True), expected)


def require_exact_names(
    expected: Sequence[str], actual: Sequence[str], context: VerificationContext
) -> None:
    expected_names = tuple(expected)
    actual_names = tuple(actual)
    if (
        len(set(expected_names)) != len(expected_names)
        or len(set(actual_names)) != len(actual_names)
        or actual_names != expected_names
    ):
        raise LockMismatch(context, "object_names", expected_names, actual_names)
