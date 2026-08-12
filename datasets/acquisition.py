"""Hardened HTTP download and ZIP extraction boundary for dataset artifacts."""

from __future__ import annotations

import http.client
import ipaddress
import socket
import ssl
import stat
import struct
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Mapping, Protocol
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


def _resolved_public_addresses(host: str, url: str) -> list[str]:
    try:
        answers = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    except OSError as error:
        raise ValueError(f"could not resolve {_redacted_url(url)}") from error
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
    deadline = time.monotonic() + deadline_seconds
    owned = False
    try:
        with destination.open("xb") as target:
            owned = True
            current_url = url
            redirects = 0
            while True:
                _validate_url(current_url)
                parsed = urlsplit(current_url)
                host = parsed.hostname
                if host is None:
                    raise ValueError("url: must be an authoritative HTTPS URL")
                host = host.rstrip(".").lower()
                addresses = _resolved_public_addresses(host, current_url)
                pinned_address = addresses[0]
                response: _Response | None = None
                try:
                    try:
                        response = active_transport.request(
                        url=current_url,
                        address=pinned_address,
                        server_hostname=host,
                        host_header=parsed.netloc,
                            headers={"Accept-Encoding": "identity"},
                            timeout=_remaining(deadline),
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

                    downloaded = 0
                    while True:
                        response.settimeout(_remaining(deadline))
                        chunk = response.read1(1 << 20, decode_content=False)
                        _remaining(deadline)
                        if not chunk:
                            break
                        downloaded += len(chunk)
                        if downloaded > max_bytes:
                            raise ValueError(f"download exceeds {max_bytes} bytes")
                        target.write(chunk)
                    target.flush()
                    return DownloadedFile(destination, _response_evidence(response.headers))
                finally:
                    if response is not None:
                        response.close()
    except BaseException:
        if owned:
            destination.unlink(missing_ok=True)
        raise


def _bounded_zip_metadata(entries: int, central_directory_size: int, limits: ZipLimits) -> None:
    if entries > limits.max_entries:
        raise ValueError(f"archive contains more than {limits.max_entries} members")
    if central_directory_size > limits.max_central_directory_bytes:
        raise ValueError(f"archive central directory exceeds {limits.max_central_directory_bytes} bytes")


def _selected_eocd(path: Path) -> tuple[bytes, int]:
    file_size = path.stat().st_size
    tail_start = max(file_size - (1 << 16) - _EOCD_SIZE, 0)
    with path.open("rb") as stream:
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


def _zip64_metadata(path: Path, eocd_offset: int, limits: ZipLimits) -> tuple[int, int, int] | None:
    locator_offset = eocd_offset - _ZIP64_LOCATOR_SIZE
    if locator_offset < 0:
        return None
    with path.open("rb") as stream:
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
    path: Path,
    start: int,
    size: int,
    declared_entries: int,
    limits: ZipLimits,
) -> None:
    if start < 0:
        raise ValueError("artifact has an invalid central-directory offset")
    remaining = size
    actual_entries = 0
    with path.open("rb") as stream:
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


def preflight_zip(path: Path, limits: ZipLimits) -> None:
    """Validate bounded EOCD, ZIP64, and central-directory metadata."""
    path = Path(path)
    eocd, eocd_offset = _selected_eocd(path)
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
    zip64_metadata = _zip64_metadata(path, eocd_offset, limits)
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
        path,
        central_directory_end - central_directory_size,
        central_directory_size,
        entries,
        limits,
    )


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


def _validated_members(members: list[zipfile.ZipInfo], limits: ZipLimits) -> list[ArchiveEntry]:
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
    return entries


def validated_zip_members(path: Path, limits: ZipLimits) -> list[ArchiveEntry]:
    """Preflight and return the safe regular-file namespace of a ZIP archive."""
    path = Path(path)
    preflight_zip(path, limits)
    try:
        with zipfile.ZipFile(path) as archive:
            return _validated_members(archive.infolist(), limits)
    except zipfile.BadZipFile as error:
        raise ValueError("artifact is not a valid ZIP archive") from error


def extract_members(path: Path, entries: list[ArchiveEntry], destination: Path) -> list[Path]:
    """Extract validated members into a newly and exclusively owned directory."""
    path = Path(path)
    destination = Path(destination)
    destination.mkdir(mode=0o700)
    outputs: list[Path] = []
    try:
        with zipfile.ZipFile(path) as archive:
            infos = {info.filename: info for info in archive.infolist()}
            total_extracted = 0
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
                with archive.open(member) as source, output.open("xb") as target:
                    for chunk in iter(lambda: source.read(1 << 20), b""):
                        member_size += len(chunk)
                        total_extracted += len(chunk)
                        if member_size > entry.size_bytes:
                            raise ValueError(f"archive member {entry.member_path!r} changed after validation")
                        target.write(chunk)
                if member_size != entry.size_bytes:
                    raise ValueError(f"archive member {entry.member_path!r} changed after validation")
                outputs.append(output)
    except BaseException:
        for output in outputs:
            output.unlink(missing_ok=True)
        for candidate in destination.iterdir():
            candidate.unlink(missing_ok=True)
        destination.rmdir()
        raise
    return outputs
