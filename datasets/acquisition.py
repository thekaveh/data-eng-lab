"""Hardened HTTP download and ZIP extraction boundary for dataset artifacts.

Secure extraction requires the POSIX directory-descriptor and no-follow APIs.
Unsupported platforms fail before creating an extraction destination.
"""

from __future__ import annotations

import ctypes
import errno
import hashlib
import http.client
import ipaddress
import multiprocessing
import os
import shutil
import socket
import ssl
import stat
import struct
import sys
import tempfile
import threading
import time
import weakref
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Mapping, Protocol
from urllib.parse import urljoin, urlsplit, urlunsplit

from . import schema as dataset_schema
from .locking import validate_relative_path

_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_SUPPORTED_ZIP_COMPRESSION = frozenset({zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED})
_MAX_REDIRECTS = 5
_EOCD_SIGNATURE = b"PK\x05\x06"
_ZIP64_EOCD_SIGNATURE = b"PK\x06\x06"
_ZIP64_LOCATOR_SIGNATURE = b"PK\x06\x07"
_CENTRAL_DIRECTORY_SIGNATURE = b"PK\x01\x02"
_EOCD_SIZE = 22
_ZIP64_EOCD_SIZE = 56
_ZIP64_LOCATOR_SIZE = 20
_CENTRAL_DIRECTORY_HEADER_SIZE = 46
_MAX_ARCHIVE_SNAPSHOT_BYTES = 2 * 1024 * 1024 * 1024
_SECURE_EXTRACTION_SUPPORTED = sys.platform in {"darwin", "linux"} and all(
    function in os.supports_dir_fd for function in (os.mkdir, os.open, os.link, os.stat, os.unlink, os.rmdir)
)


@dataclass(frozen=True)
class ResponseEvidence:
    etag: str | None = None
    last_modified: str | None = None


@dataclass(frozen=True)
class DownloadedFile:
    path: Path
    evidence: ResponseEvidence


@dataclass(frozen=True)
class ArchiveEntry:
    member_path: str
    object_name: str
    size_bytes: int


@dataclass(frozen=True)
class ZipLimits:
    max_entries: int = 10_000
    max_central_directory_bytes: int = 64 * 1024 * 1024
    max_total_expanded_bytes: int = 8 * 1024 * 1024 * 1024
    max_compression_ratio: int = 200
    max_member_bytes: int = 2 * 1024 * 1024 * 1024


@dataclass(frozen=True)
class _ArchiveSnapshot:
    device: int
    inode: int
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class _FileBinding:
    device: int
    inode: int
    size_bytes: int
    sha256: str
    descriptor: int


class _ValidatedEntries(list[ArchiveEntry]):
    __slots__ = ("limits", "snapshot")

    def __init__(self, entries: list[ArchiveEntry], snapshot: _ArchiveSnapshot, limits: ZipLimits) -> None:
        super().__init__(entries)
        self.snapshot = snapshot
        self.limits = limits

    def __getitem__(self, index: object) -> ArchiveEntry | _ValidatedEntries:
        selected = super().__getitem__(index)  # type: ignore[index]
        if isinstance(index, slice):
            return _ValidatedEntries(selected, self.snapshot, self.limits)
        return selected


class _ExtractedPaths(list[Path]):
    __slots__ = ("__weakref__", "bindings")

    def __init__(self, paths: list[Path], bindings: list[_FileBinding]) -> None:
        super().__init__(paths)
        self.bindings = bindings
        weakref.finalize(self, _close_bindings, bindings)


_DOWNLOAD_BINDINGS: dict[int, tuple[weakref.ReferenceType[DownloadedFile], _FileBinding]] = {}
_DOWNLOAD_BINDINGS_LOCK = threading.Lock()


def _close_bindings(bindings: list[_FileBinding]) -> None:
    for binding in bindings:
        try:
            os.close(binding.descriptor)
        except OSError:
            pass


def _bind_download(downloaded: DownloadedFile, binding: _FileBinding) -> None:
    identity = id(downloaded)

    def discard(reference: weakref.ReferenceType[DownloadedFile]) -> None:
        with _DOWNLOAD_BINDINGS_LOCK:
            current = _DOWNLOAD_BINDINGS.get(identity)
            if current is not None and current[0] is reference:
                _DOWNLOAD_BINDINGS.pop(identity, None)
                _close_bindings([current[1]])

    reference = weakref.ref(downloaded, discard)
    with _DOWNLOAD_BINDINGS_LOCK:
        previous = _DOWNLOAD_BINDINGS.pop(identity, None)
        _DOWNLOAD_BINDINGS[identity] = (reference, binding)
    if previous is not None:
        _close_bindings([previous[1]])


def _download_binding(downloaded: DownloadedFile) -> _FileBinding:
    with _DOWNLOAD_BINDINGS_LOCK:
        current = _DOWNLOAD_BINDINGS.get(id(downloaded))
    if current is None or current[0]() is not downloaded:
        raise ValueError("download is not bound to an owned file")
    return current[1]


def bound_download_metadata(downloaded: DownloadedFile) -> tuple[int, str]:
    """Return trusted metadata established from the owned download descriptor."""
    binding = _download_binding(downloaded)
    current = downloaded.path.lstat()
    if (current.st_dev, current.st_ino) != (binding.device, binding.inode):
        raise ValueError("download destination changed")
    if _metadata_descriptor(binding.descriptor) != (binding.size_bytes, binding.sha256):
        raise ValueError("download destination changed")
    return binding.size_bytes, binding.sha256


def bound_extracted_metadata(paths: list[Path]) -> list[tuple[int, str]]:
    """Verify and return metadata for extraction-owned outputs."""
    if not isinstance(paths, _ExtractedPaths):
        raise ValueError("extracted outputs are not bound to owned files")
    metadata: list[tuple[int, str]] = []
    for path, binding in zip(paths, paths.bindings, strict=True):
        current = path.lstat()
        if (current.st_dev, current.st_ino) != (binding.device, binding.inode):
            raise ValueError("extracted output changed before metadata verification")
        opened = os.fstat(binding.descriptor)
        if (opened.st_dev, opened.st_ino) != (binding.device, binding.inode):
            raise ValueError("extracted output changed before metadata verification")
        if _metadata_descriptor(binding.descriptor) != (binding.size_bytes, binding.sha256):
            raise ValueError("extracted output changed before metadata verification")
        final = path.lstat()
        if (final.st_dev, final.st_ino) != (binding.device, binding.inode):
            raise ValueError("extracted output changed before metadata verification")
        metadata.append((binding.size_bytes, binding.sha256))
    return metadata


def _metadata_descriptor(descriptor: int) -> tuple[int, str]:
    status = os.fstat(descriptor)
    digest = hashlib.sha256()
    offset = 0
    while chunk := os.pread(descriptor, 1 << 20, offset):
        digest.update(chunk)
        offset += len(chunk)
    return status.st_size, digest.hexdigest()


def _require_trusted_parent(path: Path) -> None:
    try:
        status = path.lstat()
    except OSError as error:
        raise ValueError("destination parent is unavailable") from error
    if path.is_symlink() or not stat.S_ISDIR(status.st_mode):
        raise ValueError("destination parent must be a trusted directory")
    if hasattr(os, "geteuid") and (status.st_uid != os.geteuid() or status.st_mode & 0o022):
        raise ValueError("destination parent must be owned by the current user and not group/world writable")


class _Response(Protocol):
    status: int
    headers: Mapping[str, str]
    peer_address: str

    def read1(self, amount: int, *, decode_content: bool) -> bytes: ...

    def settimeout(self, timeout: float) -> None: ...

    def close(self) -> None: ...


class _Transport(Protocol):
    trust_env: bool

    def request(
        self,
        *,
        url: str,
        address: str,
        server_hostname: str,
        host_header: str,
        headers: Mapping[str, str],
        timeout: float,
    ) -> _Response: ...


class _SocketResponse:
    def __init__(self, response: http.client.HTTPResponse, stream: ssl.SSLSocket) -> None:
        self.status = response.status
        self.headers = {key: value for key, value in response.getheaders()}
        self.peer_address = str(stream.getpeername()[0])
        self._response = response
        self._stream = stream

    def read1(self, amount: int, *, decode_content: bool) -> bytes:
        if decode_content:
            raise ValueError("content decoding is forbidden")
        return self._response.read1(amount)

    def settimeout(self, timeout: float) -> None:
        self._stream.settimeout(timeout)

    def close(self) -> None:
        try:
            self._response.close()
        finally:
            self._stream.close()


class _DirectTransport:
    """A direct-only HTTPS transport; environment proxy settings are never read."""

    trust_env = False

    def request(
        self,
        *,
        url: str,
        address: str,
        server_hostname: str,
        host_header: str,
        headers: Mapping[str, str],
        timeout: float,
    ) -> _SocketResponse:
        deadline = time.monotonic() + timeout

        def remaining() -> float:
            value = deadline - time.monotonic()
            if value <= 0:
                raise TimeoutError("HTTP transport deadline exceeded")
            return value

        parsed = urlsplit(url)
        target = parsed.path or "/"
        if parsed.query:
            target = f"{target}?{parsed.query}"
        raw_socket = socket.create_connection((address, 443), timeout=remaining())
        try:
            raw_socket.settimeout(remaining())
            context = ssl.create_default_context()
            stream = context.wrap_socket(raw_socket, server_hostname=server_hostname)
        except BaseException:
            raw_socket.close()
            raise
        try:
            request_headers = {**headers, "Host": host_header, "Connection": "close"}
            request = [f"GET {target} HTTP/1.1"]
            request.extend(f"{key}: {value}" for key, value in request_headers.items())
            stream.settimeout(remaining())
            stream.sendall(("\r\n".join(request) + "\r\n\r\n").encode("ascii"))
            stream.settimeout(remaining())
            response = http.client.HTTPResponse(stream)
            response.begin()
            return _SocketResponse(response, stream)
        except BaseException:
            stream.close()
            raise


def _validate_url(url: str) -> None:
    errors = dataset_schema._https(url, "url")
    if errors:
        raise ValueError(errors[0])


def _redacted_url(url: str) -> str:
    parsed = urlsplit(url)
    query = "<redacted>" if parsed.query else ""
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ValueError("download deadline exceeded")
    return remaining


def _resolve_worker(connection: object, host: str) -> None:
    try:
        answers = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        connection.send((True, answers))  # type: ignore[attr-defined]
    except BaseException:
        connection.send((False, None))  # type: ignore[attr-defined]
    finally:
        connection.close()  # type: ignore[attr-defined]


def _make_resolver_process(context: object, send: object, host: str) -> object:
    return context.Process(  # type: ignore[attr-defined]
        target=_resolve_worker,
        args=(send, host),
        daemon=True,
    )


def _bounded_dns_answers(host: str, deadline: float, url: str) -> list[tuple[object, ...]]:
    context = multiprocessing.get_context("spawn")
    receive, send = context.Pipe(duplex=False)
    process = _make_resolver_process(context, send, host)
    started = False
    try:
        try:
            process.start()  # type: ignore[attr-defined]
            started = True
        except Exception as error:
            try:
                process.close()  # type: ignore[attr-defined]
            except Exception:
                pass
            raise ValueError(f"could not start DNS resolver for {_redacted_url(url)}") from error
        send.close()
        remaining = _remaining(deadline)
        cleanup_budget = min(0.2, remaining / 5)
        if not receive.poll(max(remaining - cleanup_budget, 0)):
            raise ValueError("download deadline exceeded")
        succeeded, answers = receive.recv()
        if not succeeded or answers is None:
            raise ValueError(f"could not resolve {_redacted_url(url)}")
        return list(answers)
    finally:
        receive.close()
        send.close()
        if started:
            cleanup_remaining = max(deadline - time.monotonic(), 0)
            alive = process.is_alive()  # type: ignore[attr-defined]
            if alive:
                process.terminate()  # type: ignore[attr-defined]
                process.join(timeout=cleanup_remaining / 2)  # type: ignore[attr-defined]
                alive = process.is_alive()  # type: ignore[attr-defined]
            if alive:
                process.kill()  # type: ignore[attr-defined]
                process.join(timeout=max(deadline - time.monotonic(), 0))  # type: ignore[attr-defined]
                alive = process.is_alive()  # type: ignore[attr-defined]
            if not alive:
                process.join(timeout=0)  # type: ignore[attr-defined]
                process.close()  # type: ignore[attr-defined]


def _resolved_public_addresses(host: str, url: str, deadline: float) -> list[str]:
    answers = _bounded_dns_answers(host, deadline, url)
    addresses: list[str] = []
    for _family, _socket_type, _protocol, _canonical_name, socket_address in answers:
        raw_address = str(socket_address[0]).split("%", 1)[0]
        try:
            address = ipaddress.ip_address(raw_address)
        except ValueError as error:
            raise ValueError(f"DNS returned an invalid address for {_redacted_url(url)}") from error
        if not dataset_schema._is_authoritative_public_address(address):
            raise ValueError(f"DNS returned a non-public address for {_redacted_url(url)}")
        canonical = str(address)
        if canonical not in addresses:
            addresses.append(canonical)
    if not addresses:
        raise ValueError(f"DNS returned no addresses for {_redacted_url(url)}")
    return addresses


def _header(headers: Mapping[str, str], name: str) -> str | None:
    return next((value for key, value in headers.items() if key.lower() == name.lower()), None)


def _response_evidence(headers: Mapping[str, str]) -> ResponseEvidence:
    etag: str | None = None
    if raw_etag := _header(headers, "ETag"):
        candidate = raw_etag.strip()
        value = candidate[2:].lstrip() if candidate.startswith("W/") else candidate
        if len(value) >= 2 and value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        if value.strip():
            etag = candidate
    last_modified: str | None = None
    if raw_last_modified := _header(headers, "Last-Modified"):
        last_modified = raw_last_modified.strip() or None
    return ResponseEvidence(etag=etag, last_modified=last_modified)


def download_bounded(
    url: str,
    destination: Path,
    max_bytes: int,
    deadline_seconds: float = 120.0,
    transport: _Transport | None = None,
) -> DownloadedFile:
    """Download one authoritative HTTPS URL to an exclusively owned path."""
    _validate_url(url)
    if max_bytes < 0:
        raise ValueError("max_bytes must not be negative")
    if deadline_seconds <= 0:
        raise ValueError("deadline_seconds must be positive")
    active_transport = transport if transport is not None else _DirectTransport()
    if getattr(active_transport, "trust_env", False):
        raise ValueError("HTTP transport must not inherit proxy configuration")
    destination = Path(destination)
    _require_trusted_parent(destination.parent)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(destination)
    deadline = time.monotonic() + deadline_seconds
    downloaded_size = 0
    downloaded_digest = hashlib.sha256()
    staging_root = Path(tempfile.mkdtemp(prefix=".dataset-download-", dir=destination.parent))
    staging_path = staging_root / "content"
    try:
        with staging_path.open("xb") as target:
            opened_stat = os.fstat(target.fileno())
            owned_identity = (opened_stat.st_dev, opened_stat.st_ino)
            current_url = url
            redirects = 0
            while True:
                _validate_url(current_url)
                parsed = urlsplit(current_url)
                host = parsed.hostname
                if host is None:
                    raise ValueError("url: must be an authoritative HTTPS URL")
                host = host.rstrip(".").lower()
                addresses = _resolved_public_addresses(host, current_url, deadline)
                pinned_address = addresses[0]
                response: _Response | None = None
                request_timeout = _remaining(deadline)
                try:
                    try:
                        response = active_transport.request(
                            url=current_url,
                            address=pinned_address,
                            server_hostname=host,
                            host_header=parsed.netloc,
                            headers={"Accept-Encoding": "identity"},
                            timeout=request_timeout,
                        )
                    except Exception:
                        raise ValueError(f"HTTP request failed for {_redacted_url(current_url)}") from None
                    try:
                        peer = ipaddress.ip_address(response.peer_address.split("%", 1)[0])
                        pinned_peer = ipaddress.ip_address(pinned_address)
                    except ValueError as error:
                        raise ValueError(f"connected peer for {_redacted_url(current_url)} is invalid") from error
                    if peer != pinned_peer:
                        raise ValueError(f"connected peer for {_redacted_url(current_url)} did not match pinned DNS")

                    if response.status in _REDIRECT_STATUSES:
                        location = _header(response.headers, "Location")
                        if not location or not location.strip():
                            raise ValueError("redirect response is missing Location")
                        if redirects >= _MAX_REDIRECTS:
                            raise ValueError(f"too many redirects (maximum {_MAX_REDIRECTS})")
                        current_url = urljoin(current_url, location.strip())
                        _validate_url(current_url)
                        redirects += 1
                        continue
                    if not 200 <= response.status < 300:
                        raise ValueError(
                            f"HTTP request for {_redacted_url(current_url)} returned status {response.status}"
                        )

                    while True:
                        response.settimeout(_remaining(deadline))
                        chunk = response.read1(1 << 20, decode_content=False)
                        _remaining(deadline)
                        if not chunk:
                            break
                        downloaded_size += len(chunk)
                        if downloaded_size > max_bytes:
                            raise ValueError(f"download exceeds {max_bytes} bytes")
                        target.write(chunk)
                        downloaded_digest.update(chunk)
                    target.flush()
                    binding_descriptor = os.open(
                        staging_path,
                        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
                    )
                    try:
                        os.link(staging_path, destination, follow_symlinks=False)
                    except FileExistsError:
                        os.close(binding_descriptor)
                        raise ValueError("destination changed during download") from None
                    current = destination.lstat()
                    if (current.st_dev, current.st_ino) != owned_identity:
                        os.close(binding_descriptor)
                        raise ValueError("destination changed during download")
                    downloaded_file = DownloadedFile(destination, _response_evidence(response.headers))
                    _bind_download(
                        downloaded_file,
                        _FileBinding(
                            *owned_identity,
                            downloaded_size,
                            downloaded_digest.hexdigest(),
                            binding_descriptor,
                        ),
                    )
                    return downloaded_file
                finally:
                    if response is not None:
                        response.close()
    finally:
        shutil.rmtree(staging_root)


def _bounded_zip_metadata(entries: int, central_directory_size: int, limits: ZipLimits) -> None:
    if entries > limits.max_entries:
        raise ValueError(f"archive contains more than {limits.max_entries} members")
    if central_directory_size > limits.max_central_directory_bytes:
        raise ValueError(f"archive central directory exceeds {limits.max_central_directory_bytes} bytes")


def _selected_eocd(stream: BinaryIO) -> tuple[bytes, int]:
    file_size = os.fstat(stream.fileno()).st_size
    tail_start = max(file_size - (1 << 16) - _EOCD_SIZE, 0)
    stream.seek(tail_start)
    tail = stream.read()

    selected = -1
    if len(tail) >= _EOCD_SIZE and tail[-_EOCD_SIZE : -_EOCD_SIZE + 4] == _EOCD_SIGNATURE and tail[-2:] == b"\0\0":
        selected = len(tail) - _EOCD_SIZE
    else:
        selected = tail.rfind(_EOCD_SIGNATURE)
    if selected < 0 or len(tail) - selected < _EOCD_SIZE:
        raise ValueError("artifact is not a valid ZIP archive")
    record = tail[selected : selected + _EOCD_SIZE]
    comment_size = struct.unpack_from("<H", record, 20)[0]
    if selected + _EOCD_SIZE + comment_size != len(tail):
        raise ValueError("artifact has an inconsistent end-of-central-directory record")
    search_end = selected
    while search_end:
        candidate = tail.rfind(_EOCD_SIGNATURE, 0, search_end)
        if candidate < 0:
            break
        candidate_record = tail[candidate : candidate + _EOCD_SIZE]
        if len(candidate_record) == _EOCD_SIZE:
            candidate_comment_size = struct.unpack_from("<H", candidate_record, 20)[0]
            if candidate + _EOCD_SIZE + candidate_comment_size == len(tail):
                raise ValueError("artifact has an ambiguous end-of-central-directory record")
        search_end = candidate
    return record, tail_start + selected


def _zip64_metadata(stream: BinaryIO, eocd_offset: int, limits: ZipLimits) -> tuple[int, int, int] | None:
    locator_offset = eocd_offset - _ZIP64_LOCATOR_SIZE
    if locator_offset < 0:
        return None
    stream.seek(locator_offset)
    locator = stream.read(_ZIP64_LOCATOR_SIZE)
    if len(locator) != _ZIP64_LOCATOR_SIZE:
        return None
    signature, disk, zip64_offset, disk_count = struct.unpack("<4sLQL", locator)
    if signature != _ZIP64_LOCATOR_SIGNATURE:
        return None
    if disk != 0 or disk_count != 1:
        raise ValueError("multi-disk ZIP archives are not supported")
    record_offset = locator_offset - _ZIP64_EOCD_SIZE
    if record_offset < 0:
        raise ValueError("ZIP64 record layout is inconsistent")
    stream.seek(record_offset)
    record = stream.read(_ZIP64_EOCD_SIZE)
    if len(record) != _ZIP64_EOCD_SIZE:
        raise ValueError("ZIP64 record layout is inconsistent")
    (
        signature,
        record_size,
        _version_made,
        _version_needed,
        disk,
        central_directory_disk,
        entries_on_disk,
        entries,
        central_directory_size,
        central_directory_offset,
    ) = struct.unpack("<4sQ2H2L4Q", record)
    if signature != _ZIP64_EOCD_SIGNATURE or record_size != 44:
        raise ValueError("ZIP64 record layout is inconsistent")
    if disk != 0 or central_directory_disk != 0 or entries_on_disk != entries:
        raise ValueError("multi-disk ZIP archives are not supported")
    _bounded_zip_metadata(entries, central_directory_size, limits)
    if zip64_offset != central_directory_offset + central_directory_size:
        raise ValueError("ZIP64 record layout is inconsistent")
    return entries, central_directory_size, record_offset


def _stream_validate_central_directory(
    stream: BinaryIO,
    start: int,
    size: int,
    declared_entries: int,
    limits: ZipLimits,
) -> None:
    if start < 0:
        raise ValueError("artifact has an invalid central-directory offset")
    remaining = size
    actual_entries = 0
    stream.seek(start)
    while remaining:
        if remaining < _CENTRAL_DIRECTORY_HEADER_SIZE:
            raise ValueError("central directory has a truncated fixed header")
        header = stream.read(_CENTRAL_DIRECTORY_HEADER_SIZE)
        if len(header) != _CENTRAL_DIRECTORY_HEADER_SIZE:
            raise ValueError("central directory has a truncated fixed header")
        if header[:4] != _CENTRAL_DIRECTORY_SIGNATURE:
            raise ValueError("central directory has an invalid file-header signature")
        filename_size, extra_size, comment_size = struct.unpack_from("<3H", header, 28)
        variable_size = filename_size + extra_size + comment_size
        record_size = _CENTRAL_DIRECTORY_HEADER_SIZE + variable_size
        if record_size > remaining:
            raise ValueError("central directory record exceeds declared region")
        while variable_size:
            chunk = stream.read(min(variable_size, 1 << 20))
            if not chunk:
                raise ValueError("central directory record exceeds declared region")
            variable_size -= len(chunk)
        remaining -= record_size
        actual_entries += 1
        if actual_entries > limits.max_entries:
            raise ValueError(f"archive contains more than {limits.max_entries} members")
    if actual_entries != declared_entries:
        raise ValueError(f"central directory contains {actual_entries} records but declares {declared_entries}")


def _preflight_zip_stream(stream: BinaryIO, limits: ZipLimits) -> None:
    eocd, eocd_offset = _selected_eocd(stream)
    (
        _signature,
        disk,
        central_directory_disk,
        entries_on_disk,
        entries,
        central_directory_size,
        _central_directory_offset,
        _comment_size,
    ) = struct.unpack("<4s4H2LH", eocd)
    sentinel_metadata = (
        entries_on_disk == 0xFFFF
        or entries == 0xFFFF
        or central_directory_size == 0xFFFFFFFF
        or _central_directory_offset == 0xFFFFFFFF
    )
    zip64_metadata = _zip64_metadata(stream, eocd_offset, limits)
    if zip64_metadata is not None:
        entries, central_directory_size, central_directory_end = zip64_metadata
    else:
        if sentinel_metadata:
            raise ValueError("ZIP64 record layout is inconsistent")
        if disk != 0 or central_directory_disk != 0 or entries_on_disk != entries:
            raise ValueError("multi-disk ZIP archives are not supported")
        _bounded_zip_metadata(entries, central_directory_size, limits)
        central_directory_end = eocd_offset
    _stream_validate_central_directory(
        stream,
        central_directory_end - central_directory_size,
        central_directory_size,
        entries,
        limits,
    )


def _open_archive(path: Path) -> tuple[BinaryIO, os.stat_result]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ValueError("archive is unavailable") from error
    try:
        try:
            opened_stat = os.fstat(descriptor)
        except OSError as error:
            raise ValueError("archive is unavailable") from error
        try:
            current_stat = path.lstat()
        except OSError as error:
            raise ValueError("archive is unavailable") from error
        if not stat.S_ISREG(opened_stat.st_mode) or (
            opened_stat.st_dev,
            opened_stat.st_ino,
        ) != (current_stat.st_dev, current_stat.st_ino):
            raise ValueError("archive path changed while opening")
        return os.fdopen(descriptor, "rb"), opened_stat
    except BaseException:
        os.close(descriptor)
        raise


def _stable_archive(path: Path) -> tuple[BinaryIO, _ArchiveSnapshot]:
    source, opened_stat = _open_archive(path)
    snapshot_stream = tempfile.TemporaryFile()
    digest = hashlib.sha256()
    try:
        if opened_stat.st_size > _MAX_ARCHIVE_SNAPSHOT_BYTES:
            raise ValueError(f"archive exceeds {_MAX_ARCHIVE_SNAPSHOT_BYTES} bytes")
        remaining = opened_stat.st_size
        with source:
            while remaining:
                chunk = source.read(min(remaining, 1 << 20))
                if not chunk:
                    raise ValueError("archive changed while taking stable snapshot")
                snapshot_stream.write(chunk)
                digest.update(chunk)
                remaining -= len(chunk)
            if source.read(1):
                raise ValueError("archive changed while taking stable snapshot")
            try:
                final_stat = os.fstat(source.fileno())
            except OSError as error:
                raise ValueError("archive is unavailable") from error
        try:
            current_stat = path.lstat()
        except OSError as error:
            raise ValueError("archive is unavailable") from error
        identity = (opened_stat.st_dev, opened_stat.st_ino)
        if (
            (final_stat.st_dev, final_stat.st_ino) != identity
            or final_stat.st_size != opened_stat.st_size
            or (current_stat.st_dev, current_stat.st_ino) != identity
        ):
            raise ValueError("archive changed while taking stable snapshot")
        snapshot_stream.seek(0)
        return snapshot_stream, _ArchiveSnapshot(*identity, opened_stat.st_size, digest.hexdigest())
    except BaseException:
        snapshot_stream.close()
        raise


def preflight_zip(path: Path, limits: ZipLimits) -> None:
    """Validate bounded EOCD, ZIP64, and central-directory metadata."""
    stream, _archive_snapshot = _stable_archive(Path(path))
    with stream:
        _preflight_zip_stream(stream, limits)


def _unix_file_type(member: zipfile.ZipInfo) -> int:
    if member.create_system != 3:
        return 0
    return stat.S_IFMT(member.external_attr >> 16)


def _member_has_directory_attributes(member: zipfile.ZipInfo) -> bool:
    return _unix_file_type(member) == stat.S_IFDIR or bool(member.external_attr & 0x10)


def _member_is_symlink(member: zipfile.ZipInfo) -> bool:
    return member.create_system == 3 and stat.S_ISLNK(member.external_attr >> 16)


def _member_is_regular_file(member: zipfile.ZipInfo) -> bool:
    if member.is_dir():
        return False
    mode = member.external_attr >> 16
    file_type = stat.S_IFMT(mode)
    return file_type == 0 or stat.S_ISREG(mode)


def _validate_directory_member(member: zipfile.ZipInfo) -> str:
    if "//" in member.filename:
        raise ValueError("archive directory: must be a safe relative POSIX path")
    directory_path = member.filename[:-1]
    errors = validate_relative_path(directory_path, "archive directory")
    if errors:
        raise ValueError(errors[0])
    if member.file_size != 0 or member.compress_size != 0:
        raise ValueError(f"archive directory {member.filename!r} must be empty")
    file_type = _unix_file_type(member)
    if file_type not in {0, stat.S_IFDIR}:
        if file_type == stat.S_IFREG:
            raise ValueError(f"directory/file attribute ambiguity for archive member {member.filename!r}")
        raise ValueError(f"archive directory {member.filename!r} has unsafe attributes")
    if not _member_has_directory_attributes(member):
        raise ValueError(f"directory/file attribute ambiguity for archive member {member.filename!r}")
    return directory_path


def _validated_members(
    members: list[zipfile.ZipInfo],
    limits: ZipLimits,
    snapshot: _ArchiveSnapshot,
) -> _ValidatedEntries:
    if len(members) > limits.max_entries:
        raise ValueError(f"archive contains more than {limits.max_entries} members")
    total_size = 0
    object_names: set[str] = set()
    directory_paths: set[str] = set()
    file_paths: set[str] = set()
    entries: list[ArchiveEntry] = []
    for member in members:
        if member.flag_bits & ((1 << 0) | (1 << 6)):
            raise ValueError(f"encrypted archive member {member.filename} is not supported")
        if member.compress_type not in _SUPPORTED_ZIP_COMPRESSION:
            raise ValueError(
                f"archive member {member.filename} uses unsupported compression method {member.compress_type}"
            )
        if _member_is_symlink(member):
            raise ValueError(f"archive member {member.filename!r} must not be a symlink")
        if member.is_dir():
            directory_path = PurePosixPath(_validate_directory_member(member)).as_posix()
            if directory_path in directory_paths:
                raise ValueError(f"archive contains duplicate member path {member.filename!r}")
            directory_paths.add(directory_path)
            continue
        if _member_has_directory_attributes(member):
            raise ValueError(f"directory/file attribute ambiguity for archive member {member.filename!r}")
        if not _member_is_regular_file(member):
            raise ValueError(f"archive member {member.filename!r} must be a regular file")
        errors = validate_relative_path(member.filename, "archive member")
        if errors:
            raise ValueError(errors[0])
        member_path = PurePosixPath(member.filename).as_posix()
        if member_path in file_paths:
            raise ValueError(f"archive contains duplicate member path {member.filename!r}")
        file_paths.add(member_path)
        object_name = PurePosixPath(member.filename).name
        if object_name in object_names:
            raise ValueError(f"archive members flatten to duplicate object name {object_name}")
        object_names.add(object_name)
        if member.file_size > limits.max_member_bytes:
            raise ValueError(f"archive member {member.filename} exceeds {limits.max_member_bytes} bytes")
        total_size += member.file_size
        if total_size > limits.max_total_expanded_bytes:
            raise ValueError(f"archive exceeds {limits.max_total_expanded_bytes} uncompressed bytes")
        if member.file_size and (
            member.compress_size == 0 or member.file_size > limits.max_compression_ratio * member.compress_size
        ):
            raise ValueError(
                f"archive member {member.filename} exceeds compression ratio {limits.max_compression_ratio}"
            )
        entries.append(ArchiveEntry(member.filename, object_name, member.file_size))
    if ambiguous_paths := directory_paths & file_paths:
        ambiguous_path = sorted(ambiguous_paths)[0]
        raise ValueError(f"archive path {ambiguous_path!r} is both a directory and a file")
    ancestor_conflicts = sorted(
        (ancestor.as_posix(), directory_path)
        for directory_path in directory_paths
        for ancestor in PurePosixPath(directory_path).parents
        if ancestor != PurePosixPath(".") and ancestor.as_posix() in file_paths
    )
    if ancestor_conflicts:
        file_path, directory_path = ancestor_conflicts[0]
        raise ValueError(f"archive file path {file_path!r} is an ancestor of structural directory {directory_path!r}")
    if not entries:
        raise ValueError("archive must contain at least one regular file")
    return _ValidatedEntries(entries, snapshot, limits)


def validated_zip_members(path: Path, limits: ZipLimits) -> list[ArchiveEntry]:
    """Preflight and return the safe regular-file namespace of a ZIP archive."""
    path = Path(path)
    stream, archive_snapshot = _stable_archive(path)
    try:
        with stream:
            _preflight_zip_stream(stream, limits)
            stream.seek(0)
            with zipfile.ZipFile(stream) as archive:
                return _validated_members(archive.infolist(), limits, archive_snapshot)
    except zipfile.BadZipFile as error:
        raise ValueError("artifact is not a valid ZIP archive") from error


def _publish_directory_exclusive(source: Path, destination: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    source_bytes = os.fsencode(source)
    destination_bytes = os.fsencode(destination)
    if sys.platform == "darwin":
        rename = libc.renamex_np
        rename.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        result = rename(source_bytes, destination_bytes, 0x00000004)
    elif sys.platform == "linux" and hasattr(libc, "renameat2"):
        rename = libc.renameat2
        rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        result = rename(-100, source_bytes, -100, destination_bytes, 1)
    else:
        raise RuntimeError("secure extraction is not supported on this platform")
    if result != 0:
        error_number = ctypes.get_errno()
        if error_number in {errno.EEXIST, errno.ENOTDIR}:
            raise ValueError("destination changed during extraction")
        raise OSError(error_number, os.strerror(error_number), destination)


def extract_members(path: Path, entries: list[ArchiveEntry], destination: Path) -> list[Path]:
    """Extract into an atomic private staging directory under a trusted parent."""
    if not _SECURE_EXTRACTION_SUPPORTED:
        raise RuntimeError("secure extraction is not supported on this platform")
    path = Path(path)
    destination = Path(destination)
    _require_trusted_parent(destination.parent)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(destination)
    if not entries:
        raise ValueError("archive must contain at least one regular file")
    if not isinstance(entries, _ValidatedEntries):
        inherited_snapshot = getattr(entries, "snapshot", None)
        inherited_limits = getattr(entries, "limits", None)
        if inherited_snapshot is None or inherited_limits is None:
            raise ValueError("archive entries are not bound to one validated snapshot")
        entries = _ValidatedEntries(list(entries), inherited_snapshot, inherited_limits)
    if not isinstance(entries, _ValidatedEntries):
        raise ValueError("archive entries are not bound to one validated snapshot")
    expected_snapshot = entries.snapshot
    limits = entries.limits

    archive_stream, current_snapshot = _stable_archive(path)
    try:
        staging_root = Path(tempfile.mkdtemp(prefix=".dataset-extract-", dir=destination.parent))
    except OSError as error:
        archive_stream.close()
        raise ValueError("extraction parent is unavailable") from error
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        destination_descriptor: int | None = os.open(staging_root, directory_flags)
    except OSError as error:
        archive_stream.close()
        shutil.rmtree(staging_root, ignore_errors=True)
        raise ValueError("extraction parent is unavailable") from error
    owned_outputs: list[tuple[str, tuple[int, int]]] = []
    outputs: list[Path] = []
    output_bindings: list[_FileBinding] = []
    published = False

    def cleanup() -> None:
        _close_bindings(output_bindings)
        output_bindings.clear()
        if not published:
            shutil.rmtree(staging_root, ignore_errors=True)

    try:
        with archive_stream:
            if current_snapshot != expected_snapshot:
                raise ValueError("archive changed after validation")
            _preflight_zip_stream(archive_stream, limits)
            archive_stream.seek(0)
            with zipfile.ZipFile(archive_stream) as archive:
                current_entries = _validated_members(archive.infolist(), limits, current_snapshot)
                if list(current_entries) != list(entries):
                    raise ValueError("archive members changed after validation")
                infos = {info.filename: info for info in archive.infolist()}
                for entry in entries:
                    if validate_relative_path(entry.member_path, "archive member"):
                        raise ValueError("archive member: must be a safe relative POSIX path")
                    unsafe_object_name = validate_relative_path(entry.object_name, "object name")
                    if unsafe_object_name or PurePosixPath(entry.object_name).name != entry.object_name:
                        raise ValueError("object name: must be a safe relative POSIX path")
                    member = infos.get(entry.member_path)
                    if member is None or member.is_dir() or member.file_size != entry.size_bytes:
                        raise ValueError(f"archive member {entry.member_path!r} changed after validation")
                    output = destination / entry.object_name
                    member_size = 0
                    output_flags = (
                        os.O_WRONLY
                        | os.O_CREAT
                        | os.O_EXCL
                        | getattr(os, "O_CLOEXEC", 0)
                        | getattr(os, "O_NOFOLLOW", 0)
                    )
                    output_descriptor = os.open(
                        entry.object_name,
                        output_flags,
                        0o600,
                        dir_fd=destination_descriptor,
                    )
                    output_stat = os.fstat(output_descriptor)
                    owned_outputs.append((entry.object_name, (output_stat.st_dev, output_stat.st_ino)))
                    with os.fdopen(output_descriptor, "wb") as target, archive.open(member) as source:
                        for chunk in iter(lambda: source.read(1 << 20), b""):
                            member_size += len(chunk)
                            if member_size > entry.size_bytes:
                                raise ValueError(f"archive member {entry.member_path!r} changed after validation")
                            target.write(chunk)
                    if member_size != entry.size_bytes:
                        raise ValueError(f"archive member {entry.member_path!r} changed after validation")
                    current_output = os.stat(
                        entry.object_name,
                        dir_fd=destination_descriptor,
                        follow_symlinks=False,
                    )
                    if (current_output.st_dev, current_output.st_ino) != owned_outputs[-1][1]:
                        raise ValueError("archive output changed during extraction")
                    output_bindings.append(
                        _FileBinding(
                            current_output.st_dev,
                            current_output.st_ino,
                            member_size,
                            _metadata_fd(destination_descriptor, entry.object_name),
                            os.open(
                                entry.object_name,
                                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
                                dir_fd=destination_descriptor,
                            ),
                        )
                    )
                    outputs.append(output)
        for output, binding in zip(outputs, output_bindings, strict=True):
            current_output = os.stat(output.name, dir_fd=destination_descriptor, follow_symlinks=False)
            if (current_output.st_dev, current_output.st_ino) != (binding.device, binding.inode):
                raise ValueError("extracted output changed before success")
        _publish_directory_exclusive(staging_root, destination)
        published = True
    except zipfile.BadZipFile as error:
        cleanup()
        raise ValueError("artifact is not a valid ZIP archive") from error
    except BaseException:
        cleanup()
        raise
    finally:
        archive_stream.close()
        if destination_descriptor is not None:
            os.close(destination_descriptor)
    return _ExtractedPaths(outputs, output_bindings)


def _metadata_fd(directory_descriptor: int, name: str) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(name, flags, dir_fd=directory_descriptor)
    try:
        digest = hashlib.sha256()
        while chunk := os.read(descriptor, 1 << 20):
            digest.update(chunk)
        return digest.hexdigest()
    finally:
        os.close(descriptor)
