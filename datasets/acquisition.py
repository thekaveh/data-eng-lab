"""Hardened HTTP download and ZIP extraction boundary for dataset artifacts.

Secure extraction requires POSIX directory-descriptor/no-follow APIs plus an
atomically probed no-replace rename (Darwin ``renamex_np`` or Linux
``renameat2``, including its syscall fallback). Unsupported platforms fail
before download/extraction staging begins.
"""

from __future__ import annotations

import ctypes
import errno
import hashlib
import http.client
import ipaddress
import json
import multiprocessing
import os
import secrets
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
_MAX_ZIP_MEMBER_BYTES = 2 * 1024 * 1024 * 1024
_MAX_DNS_RESULT_BYTES = 64 * 1024
_SECURE_EXTRACTION_SUPPORTED = sys.platform in {"darwin", "linux"} and all(
    function in os.supports_dir_fd for function in (os.mkdir, os.open, os.stat, os.unlink, os.rmdir)
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


@dataclass(frozen=True)
class _ArchiveSnapshot:
    device: int
    inode: int
    size_bytes: int
    mtime_ns: int
    ctime_ns: int
    sha256: str


@dataclass(frozen=True)
class _FileBinding:
    device: int
    inode: int
    size_bytes: int
    sha256: str
    descriptor: int


class _ImmutableCapabilityList:
    __slots__ = ()

    def _immutable(self, *args: object, **kwargs: object) -> None:
        raise TypeError("capability-bearing list is immutable")

    append = _immutable
    extend = _immutable
    insert = _immutable
    clear = _immutable
    pop = _immutable
    remove = _immutable
    reverse = _immutable
    sort = _immutable
    __setitem__ = _immutable
    __delitem__ = _immutable
    __iadd__ = _immutable
    __imul__ = _immutable


class _ValidatedEntries(_ImmutableCapabilityList, list[ArchiveEntry]):
    __slots__ = ("__canonical", "__limits", "__snapshot")

    def __init__(self, entries: list[ArchiveEntry], snapshot: _ArchiveSnapshot, limits: ZipLimits) -> None:
        list.__init__(self, entries)
        object.__setattr__(self, "_ValidatedEntries__canonical", tuple(entries))
        object.__setattr__(self, "_ValidatedEntries__snapshot", snapshot)
        object.__setattr__(self, "_ValidatedEntries__limits", limits)

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("validated archive capability is immutable")

    def _capability(self) -> tuple[_ArchiveSnapshot, ZipLimits]:
        return self.__snapshot, self.__limits

    def _canonical_entries(self) -> tuple[ArchiveEntry, ...]:
        return self.__canonical

    def __getitem__(self, index: object) -> ArchiveEntry | _ValidatedEntries:
        selected = list.__getitem__(self, index)  # type: ignore[arg-type]
        if isinstance(index, slice):
            return _ValidatedEntries(list(self.__canonical[index]), self.__snapshot, self.__limits)
        return selected

    def copy(self) -> None:
        raise TypeError("capability-bearing validated entries cannot be copied")

    def __copy__(self) -> None:
        raise TypeError("capability-bearing validated entries cannot be copied")

    def __deepcopy__(self, memo: object) -> None:
        raise TypeError("capability-bearing validated entries cannot be copied")

    def __reduce_ex__(self, protocol: int) -> None:
        raise TypeError("capability-bearing validated entries cannot be pickled")


def _canonical_archive_entries(entries: list[ArchiveEntry]) -> tuple[ArchiveEntry, ...]:
    """Return immutable validated entries for internal security decisions."""
    if not isinstance(entries, _ValidatedEntries):
        raise ValueError("entries are not bound to validated archive entries")
    canonical = entries._canonical_entries()
    if tuple(list.__iter__(entries)) != canonical:
        raise ValueError("validated archive entries changed")
    if not canonical or not all(type(entry) is ArchiveEntry for entry in canonical):
        raise ValueError("validated archive entries are malformed")
    return canonical


class _BindingOwner:
    __slots__ = ("_active_bindings", "_bindings", "_closed", "_lock")

    def __init__(self, bindings: list[_FileBinding]) -> None:
        self._bindings = tuple(bindings)
        self._active_bindings = {binding.descriptor: binding for binding in bindings}
        self._closed = False
        self._lock = threading.Lock()

    @property
    def bindings(self) -> tuple[_FileBinding, ...]:
        return self._bindings

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            bindings = tuple(self._active_bindings.values())
            self._active_bindings.clear()
        for binding in bindings:
            try:
                current = os.fstat(binding.descriptor)
            except OSError:
                continue
            if (current.st_dev, current.st_ino) != (binding.device, binding.inode):
                continue
            try:
                os.close(binding.descriptor)
            except OSError:
                pass

    def abandon(self, descriptor: int) -> None:
        with self._lock:
            self._active_bindings.pop(descriptor, None)


class _ExtractedPaths(_ImmutableCapabilityList, list[Path]):
    __slots__ = ("__weakref__", "__canonical", "__owner", "__finalizer")

    def __init__(self, paths: list[Path], bindings: list[_FileBinding]) -> None:
        list.__init__(self, paths)
        object.__setattr__(self, "_ExtractedPaths__canonical", tuple(paths))
        owner = _BindingOwner(bindings)
        object.__setattr__(self, "_ExtractedPaths__owner", owner)
        object.__setattr__(self, "_ExtractedPaths__finalizer", weakref.finalize(self, owner.close))

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("extracted output capability is immutable")

    @property
    def _bindings(self) -> tuple[_FileBinding, ...]:
        return self.__owner.bindings

    def _binding_owner(self) -> _BindingOwner:
        return self.__owner

    def _canonical_paths(self) -> tuple[Path, ...]:
        return self.__canonical

    def close(self) -> None:
        self.__owner.close()

    def __getitem__(self, index: object) -> Path:
        if isinstance(index, slice):
            raise TypeError("capability-bearing extracted paths cannot be sliced")
        return list.__getitem__(self, index)  # type: ignore[arg-type]

    def copy(self) -> None:
        raise TypeError("capability-bearing extracted paths cannot be copied")

    def __copy__(self) -> None:
        raise TypeError("capability-bearing extracted paths cannot be copied")

    def __deepcopy__(self, memo: object) -> None:
        raise TypeError("capability-bearing extracted paths cannot be copied")

    def __reduce_ex__(self, protocol: int) -> None:
        raise TypeError("capability-bearing extracted paths cannot be pickled")


_DOWNLOAD_BINDINGS: dict[int, tuple[weakref.ReferenceType[DownloadedFile], _BindingOwner]] = {}
_DOWNLOAD_BINDINGS_LOCK = threading.Lock()


def _close_bindings(bindings: object) -> None:
    _BindingOwner(list(bindings)).close()  # type: ignore[arg-type]


def _bind_download(downloaded: DownloadedFile, binding: _FileBinding) -> None:
    identity = id(downloaded)
    owner = _BindingOwner([binding])

    def discard(reference: weakref.ReferenceType[DownloadedFile]) -> None:
        with _DOWNLOAD_BINDINGS_LOCK:
            current = _DOWNLOAD_BINDINGS.get(identity)
            if current is not None and current[0] is reference:
                _DOWNLOAD_BINDINGS.pop(identity, None)
                current[1].close()

    reference = weakref.ref(downloaded, discard)
    with _DOWNLOAD_BINDINGS_LOCK:
        previous = _DOWNLOAD_BINDINGS.pop(identity, None)
        _DOWNLOAD_BINDINGS[identity] = (reference, owner)
    if previous is not None:
        previous[1].close()


def _download_binding(downloaded: DownloadedFile) -> _FileBinding:
    with _DOWNLOAD_BINDINGS_LOCK:
        current = _DOWNLOAD_BINDINGS.get(id(downloaded))
    if current is None or current[0]() is not downloaded:
        raise ValueError("download is not bound to an owned file")
    return current[1].bindings[0]


def _unbind_download(downloaded: DownloadedFile) -> None:
    with _DOWNLOAD_BINDINGS_LOCK:
        current = _DOWNLOAD_BINDINGS.get(id(downloaded))
        if current is None or current[0]() is not downloaded:
            return
        _DOWNLOAD_BINDINGS.pop(id(downloaded), None)
    current[1].close()


def _abandon_download(downloaded: DownloadedFile) -> None:
    with _DOWNLOAD_BINDINGS_LOCK:
        current = _DOWNLOAD_BINDINGS.get(id(downloaded))
        if current is not None and current[0]() is downloaded:
            _DOWNLOAD_BINDINGS.pop(id(downloaded), None)
            current[1].abandon(current[1].bindings[0].descriptor)


def _bound_download_metadata(downloaded: DownloadedFile) -> tuple[int, str]:
    """Return trusted metadata established from the owned download descriptor."""
    binding = _download_binding(downloaded)
    try:
        opened = os.fstat(binding.descriptor)
        if (opened.st_dev, opened.st_ino) != (binding.device, binding.inode):
            _abandon_download(downloaded)
            raise ValueError("download binding is unavailable")
        current = downloaded.path.lstat()
        if (current.st_dev, current.st_ino) != (binding.device, binding.inode):
            raise ValueError("download destination changed")
        if _metadata_descriptor(binding.descriptor) != (binding.size_bytes, binding.sha256):
            raise ValueError("download destination changed")
    except OSError as error:
        if error.errno == errno.EBADF:
            _abandon_download(downloaded)
        raise ValueError("download binding is unavailable") from error
    return binding.size_bytes, binding.sha256


def _bound_extracted_metadata(paths: list[Path]) -> list[tuple[int, str]]:
    """Verify and return metadata for extraction-owned outputs."""
    if not isinstance(paths, _ExtractedPaths):
        raise ValueError("extracted outputs are not bound to owned files")
    canonical_paths = paths._canonical_paths()
    if len(canonical_paths) != len(paths._bindings):
        raise ValueError("extracted output capability is structurally invalid")
    metadata: list[tuple[int, str]] = []
    owner = paths._binding_owner()
    for path, binding in zip(canonical_paths, paths._bindings, strict=True):
        try:
            opened = os.fstat(binding.descriptor)
            if (opened.st_dev, opened.st_ino) != (binding.device, binding.inode):
                owner.abandon(binding.descriptor)
                raise ValueError("extracted output binding is unavailable")
            current = path.lstat()
            if (current.st_dev, current.st_ino) != (binding.device, binding.inode):
                raise ValueError("extracted output changed before metadata verification")
            if _metadata_descriptor(binding.descriptor) != (binding.size_bytes, binding.sha256):
                raise ValueError("extracted output changed before metadata verification")
            final = path.lstat()
            if (final.st_dev, final.st_ino) != (binding.device, binding.inode):
                raise ValueError("extracted output changed before metadata verification")
        except OSError as error:
            if error.errno == errno.EBADF:
                owner.abandon(binding.descriptor)
            raise ValueError("extracted output binding is unavailable") from error
        metadata.append((binding.size_bytes, binding.sha256))
    return metadata


def _metadata_descriptor(descriptor: int) -> tuple[int, str]:
    duplicate = os.dup(descriptor)
    with os.fdopen(duplicate, "rb") as stream:
        status = os.fstat(stream.fileno())
        stream.seek(0)
        digest = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return status.st_size, digest.hexdigest()


def _require_trusted_parent(path: Path) -> None:
    try:
        status = path.lstat()
    except OSError as error:
        raise ValueError("destination parent is unavailable") from error
    if stat.S_ISLNK(status.st_mode) or not stat.S_ISDIR(status.st_mode):
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


def _encode_dns_result(succeeded: bool, answers: object) -> bytes:
    payload = json.dumps(
        {"ok": succeeded, "answers": answers},
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")
    if len(payload) > _MAX_DNS_RESULT_BYTES:
        raise ValueError("DNS resolver result exceeds private framing limit")
    return payload


def _resolve_worker(connection: object, host: str) -> None:
    try:
        answers = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        payload = _encode_dns_result(True, answers)
    except BaseException:
        payload = _encode_dns_result(False, None)
    try:
        connection.send_bytes(payload)  # type: ignore[attr-defined]
    except (EOFError, OSError):
        pass
    finally:
        connection.close()  # type: ignore[attr-defined]


def _make_resolver_process(context: object, send: object, host: str) -> object:
    return context.Process(  # type: ignore[attr-defined]
        target=_resolve_worker,
        args=(send, host),
        daemon=True,
    )


def _decode_dns_result(payload: bytes, url: str) -> list[tuple[object, ...]]:
    try:
        decoded = json.loads(payload.decode("ascii"))
        if not isinstance(decoded, dict) or set(decoded) != {"ok", "answers"}:
            raise TypeError
        if decoded["ok"] is False and decoded["answers"] is None:
            raise ValueError(f"could not resolve {_redacted_url(url)}")
        if decoded["ok"] is not True or not isinstance(decoded["answers"], list):
            raise TypeError
        if not decoded["answers"]:
            raise TypeError
        answers: list[tuple[object, ...]] = []
        for answer in decoded["answers"]:
            if (
                not isinstance(answer, list)
                or len(answer) != 5
                or any(type(answer[index]) is not int for index in range(3))
                or answer[0] not in {socket.AF_INET, socket.AF_INET6}
                or answer[1] != socket.SOCK_STREAM
                or answer[2] != socket.IPPROTO_TCP
                or not isinstance(answer[3], str)
                or not isinstance(answer[4], list)
            ):
                raise TypeError
            expected_sockaddr_size = 2 if answer[0] == socket.AF_INET else 4
            if len(answer[4]) != expected_sockaddr_size:
                raise TypeError
            if not isinstance(answer[4][0], str) or type(answer[4][1]) is not int:
                raise TypeError
            if not 0 <= answer[4][1] <= 65535:
                raise TypeError
            if answer[0] == socket.AF_INET6 and not all(type(value) is int for value in answer[4][2:]):
                raise TypeError
            answers.append((*answer[:4], tuple(answer[4])))
        return answers
    except ValueError as error:
        if str(error).startswith("could not resolve "):
            raise
        raise ValueError("DNS resolver returned an invalid result") from error
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as error:
        raise ValueError("DNS resolver returned an invalid result") from error


def _cleanup_resolver_process(process: object, deadline: float) -> None:
    try:
        alive = bool(process.is_alive())  # type: ignore[attr-defined]
        if alive:
            process.terminate()  # type: ignore[attr-defined]
            process.join(timeout=max(deadline - time.monotonic(), 0))  # type: ignore[attr-defined]
            alive = bool(process.is_alive())  # type: ignore[attr-defined]
        if alive:
            process.kill()  # type: ignore[attr-defined]
            process.join(timeout=max(deadline - time.monotonic(), 0))  # type: ignore[attr-defined]
            alive = bool(process.is_alive())  # type: ignore[attr-defined]
        if alive:
            raise ValueError("DNS resolver process could not be reaped before deadline")
        process.join(timeout=0)  # type: ignore[attr-defined]
        process.close()  # type: ignore[attr-defined]
    except ValueError:
        raise
    except Exception as error:
        raise ValueError("DNS resolver process cleanup failed") from error


def _start_resolver_bounded(context: object, send: object, host: str, deadline: float, url: str) -> object:
    condition = threading.Condition()
    state: dict[str, object] = {}

    def launch() -> None:
        process: object | None = None
        started = False
        try:
            process = _make_resolver_process(context, send, host)
            with condition:
                cancelled_before_start = bool(state.get("cancelled"))
                if not cancelled_before_start:
                    state["launcher_owned"] = True
                    state["start_committed"] = True
            if cancelled_before_start:
                try:
                    process.close()  # type: ignore[attr-defined]
                except Exception:
                    pass
                return
            process.start()  # type: ignore[attr-defined]
            started = True
            with condition:
                if not state.get("cancelled"):
                    state["process"] = process
                    state["launcher_owned"] = False
                state["complete"] = True
                condition.notify_all()
        except BaseException as error:
            with condition:
                if not state.get("cancelled"):
                    state["error"] = error
                    state["phase"] = "start" if process is not None else "create"
                    state["complete"] = True
                    condition.notify_all()
            if process is not None and not started:
                try:
                    process.close()  # type: ignore[attr-defined]
                except Exception:
                    pass
        if started:
            with condition:
                launcher_owned = bool(state.get("launcher_owned"))
            if launcher_owned:
                try:
                    _cleanup_resolver_process(process, time.monotonic() + 0.2)
                except ValueError:
                    pass

    launcher = threading.Thread(target=launch, name="dataset-dns-resolver-start", daemon=True)
    try:
        launcher.start()
    except RuntimeError as error:
        raise ValueError(f"could not start DNS resolver launcher for {_redacted_url(url)}") from error
    with condition:
        while not state.get("complete"):
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not condition.wait(remaining):
                state["cancelled"] = True
                raise ValueError("download deadline exceeded")
        if error := state.get("error"):
            phase = state.get("phase")
            action = "create" if phase == "create" else "start"
            raise ValueError(f"could not {action} DNS resolver for {_redacted_url(url)}") from error
        process = state.pop("process", None)
        if process is None:
            raise ValueError(f"could not start DNS resolver for {_redacted_url(url)}")
        state["caller_owned"] = True
        return process


def _bounded_dns_answers(host: str, deadline: float, url: str) -> list[tuple[object, ...]]:
    try:
        context = multiprocessing.get_context("spawn")
        receive, send = context.Pipe(duplex=False)
    except Exception as error:
        raise ValueError(f"could not create DNS resolver for {_redacted_url(url)}") from error
    process: object | None = None
    started = False
    try:
        process = _start_resolver_bounded(context, send, host, deadline, url)
        started = True
        send.close()
        remaining = _remaining(deadline)
        cleanup_budget = min(0.2, remaining / 5)
        if not receive.poll(max(remaining - cleanup_budget, 0)):
            raise ValueError("download deadline exceeded")
        process.join(timeout=max(deadline - time.monotonic(), 0))  # type: ignore[attr-defined]
        if process.is_alive():  # type: ignore[attr-defined]
            raise ValueError("download deadline exceeded")
        if getattr(process, "exitcode", 0) != 0:
            raise ValueError(f"DNS resolver exited abnormally for {_redacted_url(url)}")
        try:
            payload = receive.recv_bytes(_MAX_DNS_RESULT_BYTES)
        except (EOFError, OSError) as error:
            raise ValueError("DNS resolver returned an invalid result") from error
        return _decode_dns_result(payload, url)
    finally:
        for endpoint in (receive, send):
            try:
                endpoint.close()
            except Exception:
                pass
        if started and process is not None:
            _cleanup_resolver_process(process, deadline)


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
    if not _probe_atomic_publish(destination.parent):
        raise RuntimeError("secure publication is not supported on this platform")
    deadline = time.monotonic() + deadline_seconds
    downloaded_size = 0
    downloaded_digest = hashlib.sha256()
    staging_path: Path | None = None
    target: BinaryIO | None = None
    downloaded_file: DownloadedFile | None = None
    binding_descriptor: int | None = None
    owned_identity: tuple[int, int] | None = None
    published = False
    try:
        try:
            descriptor, raw_staging_path = tempfile.mkstemp(
                prefix=".dataset-download-",
                dir=destination.parent,
            )
            staging_path = Path(raw_staging_path)
            os.fchmod(descriptor, 0o600)
            target = os.fdopen(descriptor, "wb")
        except OSError as error:
            if "descriptor" in locals():
                try:
                    os.close(descriptor)
                except OSError:
                    pass
            raise ValueError("could not create private download staging file") from error

        opened_stat = os.fstat(target.fileno())
        if not stat.S_ISREG(opened_stat.st_mode):
            raise ValueError("download staging path is not a regular file")
        owned_identity = (opened_stat.st_dev, opened_stat.st_ino)
        try:
            binding_descriptor = os.dup(target.fileno())
            retained_stat = os.fstat(binding_descriptor)
        except OSError as error:
            if binding_descriptor is not None:
                try:
                    os.close(binding_descriptor)
                except OSError:
                    pass
                binding_descriptor = None
            raise ValueError("download staging binding is unavailable") from error
        if not stat.S_ISREG(retained_stat.st_mode) or (
            retained_stat.st_dev,
            retained_stat.st_ino,
        ) != owned_identity:
            os.close(binding_descriptor)
            binding_descriptor = None
            raise ValueError("download staging binding is unavailable")
        current_url = url
        redirects = 0
        evidence = ResponseEvidence()
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
                    raise ValueError(f"HTTP request for {_redacted_url(current_url)} returned status {response.status}")

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
                evidence = _response_evidence(response.headers)
                break
            finally:
                if response is not None:
                    try:
                        response.close()
                    except OSError as error:
                        raise ValueError("response close failed") from error

        try:
            target.close()
            target = None
        except OSError as error:
            raise ValueError("target close failed") from error

        downloaded_file = DownloadedFile(destination, evidence)
        _bind_download(
            downloaded_file,
            _FileBinding(
                *owned_identity,
                downloaded_size,
                downloaded_digest.hexdigest(),
                binding_descriptor,
            ),
        )
        binding_descriptor = None
        try:
            staging_is_owned = _path_matches_identity(staging_path, owned_identity, directory=False)
        except OSError as error:
            raise ValueError("download staging path changed") from error
        if not staging_is_owned:
            raise ValueError("download staging path changed")
        try:
            _publish_path_exclusive(staging_path, destination)
        except (FileExistsError, ValueError):
            raise ValueError("destination changed during download") from None
        except OSError as error:
            raise ValueError("destination disappeared during publication") from error
        try:
            published_descriptor = _open_owned_path(destination, owned_identity, directory=False)
            os.close(published_descriptor)
        except (OSError, ValueError) as error:
            raise ValueError("download publication identity changed") from error
        published = True
        return downloaded_file
    finally:
        if target is not None:
            try:
                target.close()
            except OSError:
                pass
        if downloaded_file is not None and not published:
            _unbind_download(downloaded_file)
        if binding_descriptor is not None:
            try:
                os.close(binding_descriptor)
            except OSError:
                pass
        if staging_path is not None and owned_identity is not None and not published:
            try:
                _quarantine_owned_path(staging_path, owned_identity, directory=False)
            except OSError as error:
                raise ValueError("private download cleanup failed") from error


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
    if zip64_offset != record_offset or zip64_offset != central_directory_offset + central_directory_size:
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
    try:
        snapshot_stream = tempfile.TemporaryFile()
    except OSError as error:
        source.close()
        raise ValueError("archive snapshot storage is unavailable") from error
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
            source.seek(0)
            source_digest = hashlib.sha256()
            remaining = opened_stat.st_size
            while remaining:
                chunk = source.read(min(remaining, 1 << 20))
                if not chunk:
                    raise ValueError("archive changed while taking stable snapshot")
                source_digest.update(chunk)
                remaining -= len(chunk)
            if source.read(1) or source_digest.digest() != digest.digest():
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
        initial_state = (
            opened_stat.st_dev,
            opened_stat.st_ino,
            opened_stat.st_size,
            opened_stat.st_mtime_ns,
            opened_stat.st_ctime_ns,
        )
        if (
            (
                final_stat.st_dev,
                final_stat.st_ino,
                final_stat.st_size,
                final_stat.st_mtime_ns,
                final_stat.st_ctime_ns,
            )
            != initial_state
            or (
                current_stat.st_dev,
                current_stat.st_ino,
                current_stat.st_size,
                current_stat.st_mtime_ns,
                current_stat.st_ctime_ns,
            )
            != initial_state
        ):
            raise ValueError("archive changed while taking stable snapshot")
        snapshot_stream.seek(0)
        return snapshot_stream, _ArchiveSnapshot(
            *identity,
            opened_stat.st_size,
            opened_stat.st_mtime_ns,
            opened_stat.st_ctime_ns,
            digest.hexdigest(),
        )
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
        if member.file_size > _MAX_ZIP_MEMBER_BYTES:
            raise ValueError(f"archive member {member.filename} exceeds {_MAX_ZIP_MEMBER_BYTES} bytes")
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


def _linux_renameat2_number() -> int | None:
    machine = os.uname().machine.lower() if hasattr(os, "uname") else ""
    return {
        "x86_64": 316,
        "amd64": 316,
        "aarch64": 276,
        "arm64": 276,
        "i386": 353,
        "i686": 353,
        "ppc64": 357,
        "ppc64le": 357,
        "s390x": 347,
    }.get(machine)


def _probe_atomic_publish(parent: Path) -> bool:
    source_path: Path | None = None
    destination_path: Path | None = None
    source_descriptor: int | None = None
    destination_descriptor: int | None = None
    supported = False
    try:
        source_descriptor, source_name = tempfile.mkstemp(prefix=".dataset-rename-source-", dir=parent)
        source_path = Path(source_name)
        destination_descriptor, destination_name = tempfile.mkstemp(prefix=".dataset-rename-target-", dir=parent)
        destination_path = Path(destination_name)
        os.close(source_descriptor)
        source_descriptor = None
        os.close(destination_descriptor)
        destination_descriptor = None
        result, error_number = _rename_noreplace(os.fsencode(source_path), os.fsencode(destination_path))
        supported = result != 0 and error_number == errno.EEXIST
    except (OSError, RuntimeError):
        supported = False
    finally:
        for descriptor in (source_descriptor, destination_descriptor):
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    supported = False
        for probe_path in (source_path, destination_path):
            if probe_path is not None:
                try:
                    probe_path.unlink()
                except FileNotFoundError:
                    pass
                except OSError:
                    supported = False
    return supported


def _rename_noreplace(source_bytes: bytes, destination_bytes: bytes) -> tuple[int, int]:
    libc = ctypes.CDLL(None, use_errno=True)
    ctypes.set_errno(0)
    if sys.platform == "darwin":
        if not hasattr(libc, "renamex_np"):
            raise RuntimeError("secure publication is not supported on this platform")
        rename = libc.renamex_np
        rename.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        result = rename(source_bytes, destination_bytes, 0x00000004)
    elif sys.platform == "linux":
        if hasattr(libc, "renameat2"):
            rename = libc.renameat2
            rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
            rename.restype = ctypes.c_int
            result = rename(-100, source_bytes, -100, destination_bytes, 1)
        elif (syscall_number := _linux_renameat2_number()) is not None and hasattr(libc, "syscall"):
            syscall = libc.syscall
            syscall.restype = ctypes.c_long
            result = syscall(syscall_number, -100, source_bytes, -100, destination_bytes, 1)
        else:
            raise RuntimeError("secure publication is not supported on this platform")
    else:
        raise RuntimeError("secure publication is not supported on this platform")
    return result, ctypes.get_errno()


def _publish_path_exclusive(source: Path, destination: Path) -> None:
    result, error_number = _rename_noreplace(os.fsencode(source), os.fsencode(destination))
    if result != 0:
        if error_number in {errno.EEXIST, errno.ENOTDIR}:
            raise ValueError("destination changed during publication")
        raise OSError(error_number, os.strerror(error_number), destination)


def _quarantine_path_exclusive(source: Path, destination: Path) -> None:
    result, error_number = _rename_noreplace(os.fsencode(source), os.fsencode(destination))
    if result != 0:
        if error_number in {errno.EEXIST, errno.ENOTDIR}:
            raise ValueError("cleanup quarantine changed during publication")
        raise OSError(error_number, os.strerror(error_number), destination)


def _open_owned_path(path: Path, identity: tuple[int, int], *, directory: bool) -> int:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    if directory:
        flags |= getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ValueError("owned path identity changed") from error
    try:
        opened = os.fstat(descriptor)
        expected_type = stat.S_ISDIR if directory else stat.S_ISREG
        if not expected_type(opened.st_mode) or (opened.st_dev, opened.st_ino) != identity:
            raise ValueError("owned path identity changed")
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise
    return descriptor


def _quarantine_owned_path(path: Path, identity: tuple[int, int], *, directory: bool) -> None:
    kind = "extract" if directory else "download"
    for _attempt in range(8):
        quarantine = path.parent / f".dataset-cleanup-{kind}-{secrets.token_hex(16)}"
        try:
            _quarantine_path_exclusive(path, quarantine)
        except ValueError:
            continue
        except OSError as error:
            if error.errno == errno.ENOENT:
                return
            raise
        break
    else:
        raise OSError(errno.EEXIST, "could not reserve private cleanup quarantine")

    try:
        descriptor = _open_owned_path(quarantine, identity, directory=directory)
    except ValueError:
        return
    try:
        current = quarantine.lstat()
        expected_type = stat.S_ISDIR if directory else stat.S_ISREG
        if not expected_type(current.st_mode) or (current.st_dev, current.st_ino) != identity:
            return
    finally:
        os.close(descriptor)
    if directory:
        shutil.rmtree(quarantine)
    else:
        os.unlink(quarantine)


def _path_matches_identity(path: Path, identity: tuple[int, int], *, directory: bool) -> bool:
    try:
        current = path.lstat()
    except FileNotFoundError:
        return False
    expected_type = stat.S_ISDIR if directory else stat.S_ISREG
    return expected_type(current.st_mode) and (current.st_dev, current.st_ino) == identity


def extract_members(path: Path, entries: list[ArchiveEntry], destination: Path) -> list[Path]:
    """Extract into an atomic private staging directory under a trusted parent."""
    path = Path(path)
    destination = Path(destination)
    _require_trusted_parent(destination.parent)
    if not _SECURE_EXTRACTION_SUPPORTED or not _probe_atomic_publish(destination.parent):
        raise RuntimeError("secure extraction is not supported on this platform")
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(destination)
    if not entries:
        raise ValueError("archive must contain at least one regular file")
    if not isinstance(entries, _ValidatedEntries):
        inherited_snapshot = getattr(entries, "_snapshot", None)
        inherited_limits = getattr(entries, "_limits", None)
        if inherited_snapshot is None or inherited_limits is None:
            raise ValueError("archive entries are not bound to one validated snapshot")
        entries = _ValidatedEntries(list(entries), inherited_snapshot, inherited_limits)
    if not isinstance(entries, _ValidatedEntries):
        raise ValueError("archive entries are not bound to one validated snapshot")
    expected_snapshot, limits = entries._capability()
    canonical_entries = _canonical_archive_entries(entries)

    archive_stream, current_snapshot = _stable_archive(path)
    staging_root: Path | None = None
    staging_identity: tuple[int, int] | None = None
    try:
        staging_root = Path(tempfile.mkdtemp(prefix=".dataset-extract-", dir=destination.parent))
        staging_status = staging_root.lstat()
        if not stat.S_ISDIR(staging_status.st_mode):
            raise OSError("private extraction staging path is not a directory")
        staging_identity = (staging_status.st_dev, staging_status.st_ino)
    except OSError as error:
        cleanup_failed = False
        try:
            archive_stream.close()
        except OSError:
            cleanup_failed = True
        if staging_root is not None:
            if staging_identity is None:
                try:
                    recovered = staging_root.lstat()
                    if stat.S_ISDIR(recovered.st_mode):
                        staging_identity = (recovered.st_dev, recovered.st_ino)
                except OSError:
                    pass
            if staging_identity is not None:
                try:
                    _quarantine_owned_path(staging_root, staging_identity, directory=True)
                except OSError:
                    cleanup_failed = True
        if cleanup_failed:
            raise ValueError("extraction cleanup failed") from error
        raise ValueError("extraction parent is unavailable") from error
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    destination_descriptor: int | None = None
    try:
        destination_descriptor = os.open(staging_root, directory_flags)
        opened_staging = os.fstat(destination_descriptor)
        if not stat.S_ISDIR(opened_staging.st_mode) or (
            opened_staging.st_dev,
            opened_staging.st_ino,
        ) != staging_identity:
            raise OSError("private extraction staging path changed while opening")
    except OSError as error:
        cleanup_failed = False
        if destination_descriptor is not None:
            try:
                os.close(destination_descriptor)
            except OSError:
                cleanup_failed = True
        try:
            archive_stream.close()
        except OSError:
            cleanup_failed = True
        try:
            if staging_identity is not None:
                _quarantine_owned_path(staging_root, staging_identity, directory=True)
        except OSError:
            cleanup_failed = True
        if cleanup_failed:
            raise ValueError("extraction cleanup failed") from error
        raise ValueError("extraction parent is unavailable") from error
    owned_outputs: list[tuple[str, tuple[int, int]]] = []
    outputs: list[Path] = []
    output_bindings: list[_FileBinding] = []
    capability: _ExtractedPaths | None = None

    def cleanup(original: BaseException) -> None:
        cleanup_error: BaseException | None = None
        if capability is not None:
            try:
                capability.close()
            except BaseException as error:
                cleanup_error = error
                _close_bindings(output_bindings)
        else:
            _close_bindings(output_bindings)
        if destination_descriptor is not None:
            try:
                os.close(destination_descriptor)
            except OSError as error:
                cleanup_error = error
        try:
            archive_stream.close()
        except OSError as error:
            cleanup_error = cleanup_error or error
        try:
            if staging_identity is not None:
                _quarantine_owned_path(staging_root, staging_identity, directory=True)
        except OSError as error:
            cleanup_error = cleanup_error or error
        if cleanup_error is not None:
            raise ValueError("extraction cleanup failed") from original

    try:
        with archive_stream:
            if current_snapshot != expected_snapshot:
                raise ValueError("archive changed after validation")
            _preflight_zip_stream(archive_stream, limits)
            archive_stream.seek(0)
            with zipfile.ZipFile(archive_stream) as archive:
                current_entries = _validated_members(archive.infolist(), limits, current_snapshot)
                fresh_entries = current_entries._canonical_entries()
                if fresh_entries != canonical_entries:
                    raise ValueError("archive members changed after validation")
                infos = {info.filename: info for info in archive.infolist()}
                for entry in canonical_entries:
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
        capability = _ExtractedPaths(outputs, output_bindings)
        archive_stream.close()
    except zipfile.BadZipFile as error:
        cleanup(error)
        raise ValueError("artifact is not a valid ZIP archive") from error
    except BaseException as error:
        cleanup(error)
        raise

    if capability is None:
        error = ValueError("extracted output capability is unavailable")
        cleanup(error)
        raise error
    try:
        staging_is_owned = _path_matches_identity(staging_root, staging_identity, directory=True)
    except OSError as error:
        changed = ValueError("extraction staging path changed")
        cleanup(changed)
        raise changed from error
    if not staging_is_owned:
        changed = ValueError("extraction staging path changed")
        cleanup(changed)
        raise changed
    try:
        _publish_path_exclusive(staging_root, destination)
    except ValueError as error:
        cleanup(error)
        raise ValueError("destination changed during extraction") from None
    except BaseException as error:
        cleanup(error)
        raise
    try:
        published_descriptor = _open_owned_path(destination, staging_identity, directory=True)
        os.close(published_descriptor)
        os.close(destination_descriptor)
        destination_descriptor = None
    except (OSError, ValueError) as error:
        changed = ValueError("extraction publication identity changed")
        cleanup(changed)
        raise changed from error
    return capability


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
