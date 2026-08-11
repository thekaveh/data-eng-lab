import hashlib
import io
from pathlib import Path

import pytest

from datasets.verification import (
    ExpectedObject,
    LockMismatch,
    VerificationContext,
    require_exact_names,
    verify_file,
    verify_stream,
)

CONTEXT = VerificationContext("movielens", "small", "download", "latest_small", "ratings.csv")


def test_verify_stream_rejects_empty_input_with_structured_context():
    with pytest.raises(LockMismatch) as caught:
        verify_stream(io.BytesIO(), 1, hashlib.sha256(b"x").hexdigest(), CONTEXT)

    assert caught.value.context == CONTEXT
    assert caught.value.field == "size_bytes"
    assert caught.value.expected == 1
    assert caught.value.actual == 0


def test_verify_stream_rejects_truncation_with_structured_context():
    context = VerificationContext("movielens", "small", "download", "latest_small", "ratings.csv")
    with pytest.raises(LockMismatch) as caught:
        verify_stream(io.BytesIO(b"abc"), 4, hashlib.sha256(b"abcd").hexdigest(), context)

    assert caught.value.field == "size_bytes"
    assert caught.value.expected == 4
    assert caught.value.actual == 3


def test_verify_stream_rejects_oversized_input_after_expected_size_plus_one_byte():
    with pytest.raises(LockMismatch) as caught:
        verify_stream(io.BytesIO(b"abcde"), 4, hashlib.sha256(b"abcd").hexdigest(), CONTEXT)

    assert caught.value.field == "size_bytes"
    assert caught.value.expected == 4
    assert caught.value.actual == 5


def test_verify_stream_rejects_wrong_sha256():
    with pytest.raises(LockMismatch) as caught:
        verify_stream(io.BytesIO(b"abcd"), 4, hashlib.sha256(b"other").hexdigest(), CONTEXT)

    assert caught.value.field == "sha256"
    assert caught.value.expected == hashlib.sha256(b"other").hexdigest()
    assert caught.value.actual == hashlib.sha256(b"abcd").hexdigest()


def test_verify_stream_returns_size_and_sha256_for_exact_bytes():
    expected_sha256 = hashlib.sha256(b"abcd").hexdigest()

    assert verify_stream(io.BytesIO(b"abcd"), 4, expected_sha256, CONTEXT) == (4, expected_sha256)


def test_verify_stream_accepts_non_seekable_streams():
    class NonSeekableStream:
        def __init__(self, content: bytes):
            self._stream = io.BytesIO(content)

        def read(self, size: int = -1) -> bytes:
            return self._stream.read(size)

    content = b"non-seekable"
    assert verify_stream(NonSeekableStream(content), len(content), hashlib.sha256(content).hexdigest(), CONTEXT) == (
        len(content),
        hashlib.sha256(content).hexdigest(),
    )


def test_verify_stream_propagates_read_errors():
    class FailingStream:
        def read(self, size: int = -1) -> bytes:
            raise OSError("stream unavailable")

    with pytest.raises(OSError, match="stream unavailable"):
        verify_stream(FailingStream(), 1, hashlib.sha256(b"x").hexdigest(), CONTEXT)


def test_verify_file_returns_resolved_path_and_expected_object(tmp_path: Path):
    payload = b"locked bytes"
    path = tmp_path / "artifact.bin"
    path.write_bytes(payload)
    expected = ExpectedObject("artifact.bin", len(payload), hashlib.sha256(payload).hexdigest(), "schema-v1")

    verified = verify_file(path, expected, CONTEXT)

    assert verified.path == path.resolve(strict=True)
    assert verified.expected is expected


def test_require_exact_names_reports_missing_and_extra_together():
    with pytest.raises(LockMismatch) as caught:
        require_exact_names(("a.csv", "b.csv"), ("b.csv", "c.csv"), CONTEXT)

    assert caught.value.expected == ("a.csv", "b.csv")
    assert caught.value.actual == ("b.csv", "c.csv")


def test_require_exact_names_rejects_duplicate_expected_names():
    with pytest.raises(LockMismatch) as caught:
        require_exact_names(("a.csv", "a.csv"), ("a.csv",), CONTEXT)

    assert caught.value.field == "object_names"
    assert caught.value.expected == ("a.csv", "a.csv")
    assert caught.value.actual == ("a.csv",)


def test_require_exact_names_rejects_duplicate_actual_names():
    with pytest.raises(LockMismatch) as caught:
        require_exact_names(("a.csv",), ("a.csv", "a.csv"), CONTEXT)

    assert caught.value.field == "object_names"
    assert caught.value.expected == ("a.csv",)
    assert caught.value.actual == ("a.csv", "a.csv")


def test_require_exact_names_accepts_identical_ordered_names():
    require_exact_names(("a.csv", "b.csv"), ("a.csv", "b.csv"), CONTEXT)


def test_lock_mismatch_string_is_redacted():
    context = VerificationContext(
        "movielens",
        "small",
        "download",
        "https://secret.example.invalid/token",
        "/private/tmp/download.csv",
    )
    error = LockMismatch(context, "size_bytes", 4, 3)

    assert str(error) == "movielens/small download size_bytes mismatch"
    assert "secret" not in str(error)
    assert "/private/tmp" not in str(error)
