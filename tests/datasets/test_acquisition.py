from __future__ import annotations

import copy
import dataclasses
import errno
import gc
import hashlib
import json
import os
import pickle
import socket
import stat
import struct
import time
import zipfile
from pathlib import Path

import pytest

from datasets import acquisition
from datasets.acquisition import ZipLimits, download_bounded, extract_members, validated_zip_members


def _stalled_dns_worker(connection: object) -> None:
    time.sleep(2)
    connection.close()  # type: ignore[attr-defined]


def _static_dns_worker(connection: object) -> None:
    connection.send_bytes(  # type: ignore[attr-defined]
        acquisition._encode_dns_result(
            True,
            [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.0.9", 443))],
        )
    )
    connection.close()  # type: ignore[attr-defined]


@pytest.fixture(autouse=True)
def inline_dns_resolver(monkeypatch: pytest.MonkeyPatch):
    def process_factory(context: object, send: object, host: str):
        class InlineProcess:
            def start(self) -> None:
                acquisition._resolve_worker(send, host)

            def is_alive(self) -> bool:
                return False

            def join(self, timeout: float = 0) -> None:
                pass

            def close(self) -> None:
                pass

        return InlineProcess()

    monkeypatch.setattr(acquisition, "_make_resolver_process", process_factory)


def _zip(path: Path, members: dict[str, bytes]) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)
    return path


class FakeResponse:
    status = 200

    def __init__(
        self,
        body: bytes = b"locked",
        *,
        peer: str = "192.0.0.9",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.headers = headers or {}
        self.peer_address = peer
        self._chunks = iter((body, b""))
        self.timeouts: list[float] = []

    def read1(self, amount: int, *, decode_content: bool) -> bytes:
        assert amount == 1 << 20
        assert decode_content is False
        return next(self._chunks)

    def settimeout(self, timeout: float) -> None:
        self.timeouts.append(timeout)

    def close(self) -> None:
        pass


def _reuse_descriptor(descriptor: int, path: Path) -> int:
    source = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    reused = os.dup2(source, descriptor)
    if source != reused:
        os.close(source)
    return reused


def _fstat_or_none(raw_descriptor: str) -> os.stat_result | None:
    try:
        return os.fstat(int(raw_descriptor))
    except (OSError, ValueError):
        return None


class FakeTransport:
    def __init__(self, responses: list[FakeResponse] | None = None) -> None:
        self.responses = iter(responses or [FakeResponse()])
        self.requests: list[dict[str, object]] = []
        self.trust_env = False

    def request(self, **kwargs: object) -> FakeResponse:
        self.requests.append(kwargs)
        return next(self.responses)


def _public_dns(monkeypatch: pytest.MonkeyPatch, *addresses: str) -> None:
    monkeypatch.setattr(
        acquisition.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [
            (socket.AF_INET6 if ":" in address else socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 443))
            for address in addresses
        ],
    )


def test_download_pins_public_dns_and_rejects_private_peer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _public_dns(monkeypatch, "192.0.0.9")
    transport = FakeTransport([FakeResponse(peer="127.0.0.1")])

    with pytest.raises(ValueError, match="connected peer"):
        download_bounded("https://example.test/file", tmp_path / "target", 10, transport=transport)

    assert not (tmp_path / "target").exists()


def test_download_requires_every_a_and_aaaa_answer_to_be_public(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9", "127.0.0.1")
    transport = FakeTransport()

    with pytest.raises(ValueError, match="non-public address"):
        download_bounded("https://example.test/file", tmp_path / "target", 10, transport=transport)

    assert transport.requests == []


def test_download_pins_address_while_preserving_tls_and_http_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    transport = FakeTransport()

    result = download_bounded("https://example.test/file?token=secret", tmp_path / "target", 10, transport=transport)

    assert result.path.read_bytes() == b"locked"
    assert transport.trust_env is False
    assert transport.requests == [
        {
            "url": "https://example.test/file?token=secret",
            "address": "192.0.0.9",
            "server_hostname": "example.test",
            "host_header": "example.test",
            "headers": {"Accept-Encoding": "identity"},
            "timeout": pytest.approx(120, abs=1),
        }
    ]


def test_download_rejects_transport_that_inherits_proxy_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    transport = FakeTransport()
    transport.trust_env = True

    with pytest.raises(ValueError, match="must not inherit proxy"):
        download_bounded("https://example.test/file", tmp_path / "target", 10, transport=transport)

    assert not (tmp_path / "target").exists()


def test_download_redacts_query_when_transport_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _public_dns(monkeypatch, "192.0.0.9")

    class FailingTransport(FakeTransport):
        def request(self, **kwargs: object) -> FakeResponse:
            raise OSError("failure exposed token=secret")

    with pytest.raises(ValueError, match=r"file\?<redacted>") as error:
        download_bounded(
            "https://example.test/file?token=secret",
            tmp_path / "target",
            10,
            transport=FailingTransport(),
        )

    assert "secret" not in str(error.value)
    assert not (tmp_path / "target").exists()


def test_download_cleans_only_its_partial_destination_on_size_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    sibling = tmp_path / "keep"
    sibling.write_bytes(b"caller owned")

    with pytest.raises(ValueError, match="download exceeds 5 bytes"):
        download_bounded(
            "https://example.test/file",
            tmp_path / "target",
            5,
            transport=FakeTransport([FakeResponse(body=b"123456")]),
        )

    assert not (tmp_path / "target").exists()
    assert sibling.read_bytes() == b"caller owned"


def test_download_does_not_unlink_replacement_after_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    destination = tmp_path / "target"
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"replacement")

    class ReplacingResponse(FakeResponse):
        def read1(self, amount: int, *, decode_content: bool) -> bytes:
            replacement.replace(destination)
            return b"too large"

    with pytest.raises(ValueError, match="download exceeds 1 bytes"):
        download_bounded(
            "https://example.test/file",
            destination,
            1,
            transport=FakeTransport([ReplacingResponse()]),
        )

    assert destination.read_bytes() == b"replacement"


def test_download_deadline_bounds_dns_resolution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    def process_factory(context: object, send: object, host: str):
        return context.Process(target=_stalled_dns_worker, args=(send,), daemon=True)  # type: ignore[attr-defined]

    monkeypatch.setattr(acquisition, "_make_resolver_process", process_factory)
    started = time.monotonic()

    with pytest.raises(ValueError, match="download deadline exceeded"):
        download_bounded(
            "https://example.test/file",
            tmp_path / "target",
            10,
            deadline_seconds=0.05,
            transport=FakeTransport(),
        )

    assert time.monotonic() - started < 1
    assert not (tmp_path / "target").exists()


def test_dns_resolver_uses_spawn_context_and_closes_process_handle(monkeypatch: pytest.MonkeyPatch):
    observed: dict[str, object] = {}

    class FinishedProcess:
        def start(self) -> None:
            observed["started"] = True

        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float = 0) -> None:
            observed.setdefault("joins", []).append(timeout)  # type: ignore[union-attr]

        def close(self) -> None:
            observed["closed"] = True

    def process_factory(context: object, send: object, host: str):
        observed["method"] = context.get_start_method()  # type: ignore[attr-defined]
        send.send_bytes(  # type: ignore[attr-defined]
            acquisition._encode_dns_result(
                True,
                [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.0.9", 443))],
            )
        )
        return FinishedProcess()

    monkeypatch.setattr(acquisition, "_make_resolver_process", process_factory)

    answers = acquisition._bounded_dns_answers("example.test", time.monotonic() + 1, "https://example.test")

    assert answers[0][-1] == ("192.0.0.9", 443)
    assert observed["method"] == "spawn"
    assert observed["started"] is True
    assert len(observed["joins"]) == 2  # type: ignore[arg-type]
    assert observed["joins"][-1] == 0  # type: ignore[index]
    assert observed["closed"] is True


def test_dns_resolver_start_failure_does_not_join_unstarted_process(monkeypatch: pytest.MonkeyPatch):
    calls: list[str] = []

    class StartFailureProcess:
        def start(self) -> None:
            calls.append("start")
            raise RuntimeError("spawn failed")

        def is_alive(self) -> bool:
            calls.append("is_alive")
            raise AssertionError("unstarted process inspected")

        def join(self, timeout: float = 0) -> None:
            calls.append("join")
            raise AssertionError("unstarted process joined")

        def close(self) -> None:
            calls.append("close")

    monkeypatch.setattr(acquisition, "_make_resolver_process", lambda *args: StartFailureProcess())

    with pytest.raises(ValueError, match="could not start DNS resolver") as error:
        acquisition._bounded_dns_answers("example.test", time.monotonic() + 1, "https://example.test")

    assert isinstance(error.value.__cause__, RuntimeError)
    assert calls == ["start", "close"]


def test_dns_resolver_factory_failure_closes_both_pipe_endpoints(monkeypatch: pytest.MonkeyPatch):
    closed: list[str] = []

    class Endpoint:
        def close(self) -> None:
            closed.append(self.name)

        def __init__(self, name: str) -> None:
            self.name = name

    class Context:
        def Pipe(self, *, duplex: bool):
            assert duplex is False
            return Endpoint("receive"), Endpoint("send")

    monkeypatch.setattr(acquisition.multiprocessing, "get_context", lambda method: Context())
    monkeypatch.setattr(
        acquisition,
        "_make_resolver_process",
        lambda *args: (_ for _ in ()).throw(RuntimeError("factory failed")),
    )

    with pytest.raises(ValueError, match="could not create DNS resolver") as error:
        acquisition._bounded_dns_answers("example.test", time.monotonic() + 1, "https://example.test")

    assert isinstance(error.value.__cause__, RuntimeError)
    assert closed == ["receive", "send"]


def test_dns_resolver_stuck_child_cleanup_is_deadline_bounded(monkeypatch: pytest.MonkeyPatch):
    calls: list[object] = []

    class Receive:
        def poll(self, timeout: float) -> bool:
            calls.append(("poll", timeout))
            return False

        def close(self) -> None:
            calls.append("receive.close")

    class Send:
        def close(self) -> None:
            calls.append("send.close")

    class StuckProcess:
        def start(self) -> None:
            calls.append("start")

        def is_alive(self) -> bool:
            calls.append("is_alive")
            return True

        def terminate(self) -> None:
            calls.append("terminate")

        def kill(self) -> None:
            calls.append("kill")

        def join(self, timeout: float = 0) -> None:
            calls.append(("join", timeout))

        def close(self) -> None:
            raise AssertionError("a live process handle cannot be closed")

    class Context:
        def Pipe(self, *, duplex: bool):
            return Receive(), Send()

    monkeypatch.setattr(acquisition.multiprocessing, "get_context", lambda method: Context())
    monkeypatch.setattr(acquisition, "_make_resolver_process", lambda *args: StuckProcess())
    started = time.monotonic()

    with pytest.raises(ValueError, match="could not be reaped before deadline"):
        acquisition._bounded_dns_answers("example.test", time.monotonic() + 0.02, "https://example.test")

    assert time.monotonic() - started < 0.5
    assert "terminate" in calls and "kill" in calls
    assert all(item[1] <= 0.02 for item in calls if isinstance(item, tuple) and item[0] == "join")


def test_dns_resolver_spawn_process_completes_without_handle_leak(monkeypatch: pytest.MonkeyPatch):
    before = {process.pid for process in acquisition.multiprocessing.active_children()}

    def process_factory(context: object, send: object, host: str):
        return context.Process(target=_static_dns_worker, args=(send,), daemon=True)  # type: ignore[attr-defined]

    monkeypatch.setattr(acquisition, "_make_resolver_process", process_factory)

    answers = acquisition._bounded_dns_answers("example.test", time.monotonic() + 3, "https://example.test")

    assert answers[0][-1] == ("192.0.0.9", 443)
    assert {process.pid for process in acquisition.multiprocessing.active_children()} == before


@pytest.mark.parametrize("payload", [b"", b"not-json", b'{"ok":true'])
def test_dns_resolver_rejects_eof_or_partial_invalid_payload(monkeypatch: pytest.MonkeyPatch, payload: bytes):
    class PayloadProcess:
        def __init__(self, send: object) -> None:
            self.send = send

        def start(self) -> None:
            if payload:
                self.send.send_bytes(payload)  # type: ignore[attr-defined]
            self.send.close()  # type: ignore[attr-defined]

        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float = 0) -> None:
            pass

        def close(self) -> None:
            pass

    monkeypatch.setattr(acquisition, "_make_resolver_process", lambda context, send, host: PayloadProcess(send))

    with pytest.raises(ValueError, match="DNS resolver returned an invalid result"):
        acquisition._bounded_dns_answers("example.test", time.monotonic() + 1, "https://example.test")


@pytest.mark.parametrize(
    "answers",
    [
        [],
        [[socket.AF_UNIX, socket.SOCK_STREAM, 0, "", ["192.0.0.9", 443]]],
        [[socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP, "", ["192.0.0.9", 443]]],
        [[socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", []]],
        [[socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", [9, "443"]]],
        [[socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ["192.0.0.9", True]]],
        [[2.0, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ["192.0.0.9", 443]]],
        [[socket.AF_INET, True, socket.IPPROTO_TCP, "", ["192.0.0.9", 443]]],
        [[socket.AF_INET, socket.SOCK_STREAM, 6.0, "", ["192.0.0.9", 443]]],
        [[socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ["2001:4860:4860::8888", 443]]],
    ],
)
def test_dns_decoder_rejects_empty_or_malformed_answer_shapes(answers: list[object]):
    payload = json.dumps({"ok": True, "answers": answers}).encode()

    with pytest.raises(ValueError, match="DNS resolver returned an invalid result") as error:
        acquisition._decode_dns_result(payload, "https://example.test/file?token=secret")

    assert "secret" not in str(error.value)


def test_dns_resolver_rejects_valid_frame_from_abnormal_worker(monkeypatch: pytest.MonkeyPatch):
    class AbnormalProcess:
        exitcode = 9

        def __init__(self, send: object) -> None:
            self.send = send

        def start(self) -> None:
            self.send.send_bytes(  # type: ignore[attr-defined]
                acquisition._encode_dns_result(
                    True,
                    [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("192.0.0.9", 443))],
                )
            )

        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float = 0) -> None:
            pass

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        acquisition,
        "_make_resolver_process",
        lambda context, send, host: AbnormalProcess(send),
    )

    with pytest.raises(ValueError, match="DNS resolver exited abnormally"):
        acquisition._bounded_dns_answers("example.test", time.monotonic() + 1, "https://example.test")


def test_dns_resolver_factory_is_bounded_by_deadline(monkeypatch: pytest.MonkeyPatch):
    completed = acquisition.threading.Event()

    class NeverStarted:
        def close(self) -> None:
            completed.set()

    def slow_factory(*args: object):
        time.sleep(0.08)
        return NeverStarted()

    monkeypatch.setattr(acquisition, "_make_resolver_process", slow_factory)
    started = time.monotonic()

    with pytest.raises(ValueError, match="download deadline exceeded"):
        acquisition._bounded_dns_answers("example.test", time.monotonic() + 0.01, "https://example.test")

    assert time.monotonic() - started < 0.05
    assert completed.wait(0.2)


def test_dns_resolver_late_factory_result_is_closed_without_start(monkeypatch: pytest.MonkeyPatch):
    release_factory = acquisition.threading.Event()
    closed = acquisition.threading.Event()
    starts: list[str] = []

    class LateProcess:
        def start(self) -> None:
            starts.append("start")

        def close(self) -> None:
            closed.set()

    def delayed_factory(*args: object):
        release_factory.wait()
        return LateProcess()

    monkeypatch.setattr(acquisition, "_make_resolver_process", delayed_factory)

    with pytest.raises(ValueError, match="download deadline exceeded"):
        acquisition._bounded_dns_answers("example.test", time.monotonic() + 0.01, "https://example.test")

    release_factory.set()
    assert closed.wait(0.2)
    assert starts == []


def test_repeated_resolver_timeouts_do_not_retain_completed_launcher_threads(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(acquisition, "_make_resolver_process", lambda *args: time.sleep(0.02))

    for _ in range(3):
        with pytest.raises(ValueError, match="download deadline exceeded"):
            acquisition._bounded_dns_answers("example.test", time.monotonic() + 0.005, "https://example.test")
    time.sleep(0.05)

    assert not [thread for thread in acquisition.threading.enumerate() if thread.name == "dataset-dns-resolver-start"]


def test_dns_resolver_start_is_bounded_and_late_process_is_reaped(monkeypatch: pytest.MonkeyPatch):
    cleaned = acquisition.threading.Event()

    class SlowStart:
        exitcode = 0

        def start(self) -> None:
            time.sleep(0.08)

        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float = 0) -> None:
            pass

        def close(self) -> None:
            cleaned.set()

    monkeypatch.setattr(acquisition, "_make_resolver_process", lambda *args: SlowStart())
    started = time.monotonic()

    with pytest.raises(ValueError, match="download deadline exceeded"):
        acquisition._bounded_dns_answers("example.test", time.monotonic() + 0.01, "https://example.test")

    assert time.monotonic() - started < 0.05
    assert cleaned.wait(0.2)


def test_dns_resolver_timeout_boundary_has_exactly_one_process_owner(monkeypatch: pytest.MonkeyPatch):
    start_entered = acquisition.threading.Event()
    release_start = acquisition.threading.Event()
    cleaned = acquisition.threading.Event()
    calls: list[str] = []

    class BoundaryProcess:
        exitcode = 0

        def start(self) -> None:
            start_entered.set()
            release_start.wait()

        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float = 0) -> None:
            calls.append("join")

        def close(self) -> None:
            calls.append("close")
            cleaned.set()

    monkeypatch.setattr(acquisition, "_make_resolver_process", lambda *args: BoundaryProcess())

    with pytest.raises(ValueError, match="download deadline exceeded"):
        acquisition._bounded_dns_answers("example.test", time.monotonic() + 0.01, "https://example.test")

    assert start_entered.is_set()
    release_start.set()
    assert cleaned.wait(0.2)
    assert calls.count("close") == 1
    assert calls.count("join") == 1


def test_dns_resolver_start_commit_precedes_timeout_ownership_transfer(monkeypatch: pytest.MonkeyPatch):
    start_entered = acquisition.threading.Event()
    release_start = acquisition.threading.Event()
    closed = acquisition.threading.Event()
    calls: list[str] = []

    class CommittedStartProcess:
        exitcode = 0

        def start(self) -> None:
            calls.append("start")
            start_entered.set()
            release_start.wait()

        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float = 0) -> None:
            calls.append("join")

        def close(self) -> None:
            calls.append("close")
            closed.set()

    monkeypatch.setattr(acquisition, "_make_resolver_process", lambda *args: CommittedStartProcess())

    with pytest.raises(ValueError, match="download deadline exceeded"):
        acquisition._bounded_dns_answers("example.test", time.monotonic() + 0.01, "https://example.test")

    assert start_entered.is_set()
    assert calls == ["start"]
    release_start.set()
    assert closed.wait(0.2)
    assert calls == ["start", "join", "close"]


def test_dns_resolver_launcher_thread_start_failure_is_normalized(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        acquisition.threading.Thread,
        "start",
        lambda self: (_ for _ in ()).throw(RuntimeError("thread unavailable")),
    )

    with pytest.raises(ValueError, match="could not start DNS resolver launcher") as error:
        acquisition._bounded_dns_answers("example.test", time.monotonic() + 1, "https://example.test")

    assert isinstance(error.value.__cause__, RuntimeError)


def test_download_rejects_path_replacement_before_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    destination = tmp_path / "target"
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"replacement")

    class ReplacingSuccessResponse(FakeResponse):
        def read1(self, amount: int, *, decode_content: bool) -> bytes:
            chunk = super().read1(amount, decode_content=decode_content)
            if not chunk:
                replacement.replace(destination)
            return chunk

    with pytest.raises(ValueError, match="destination changed during download"):
        download_bounded(
            "https://example.test/file",
            destination,
            10,
            transport=FakeTransport([ReplacingSuccessResponse()]),
        )

    assert destination.read_bytes() == b"replacement"


def test_bound_download_metadata_rejects_replacement_and_same_inode_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    destination = tmp_path / "target"
    downloaded = download_bounded(
        "https://example.test/file",
        destination,
        10,
        transport=FakeTransport(),
    )
    destination.write_bytes(b"mutate")

    with pytest.raises(ValueError, match="download destination changed"):
        acquisition._bound_download_metadata(downloaded)

    destination.unlink()
    destination.write_bytes(b"locked")
    with pytest.raises(ValueError, match="download destination changed"):
        acquisition._bound_download_metadata(downloaded)


def test_download_publishes_mode_0600_independent_of_umask(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _public_dns(monkeypatch, "192.0.0.9")
    previous_umask = os.umask(0)
    try:
        result = download_bounded(
            "https://example.test/file",
            tmp_path / "target",
            10,
            transport=FakeTransport(),
        )
    finally:
        os.umask(previous_umask)

    assert stat.S_IMODE(result.path.stat().st_mode) == 0o600


def test_download_response_close_failure_prevents_publication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _public_dns(monkeypatch, "192.0.0.9")

    class CloseFailureResponse(FakeResponse):
        def close(self) -> None:
            raise OSError("response close failed")

    destination = tmp_path / "target"
    with pytest.raises(ValueError, match="response close failed"):
        download_bounded(
            "https://example.test/file",
            destination,
            10,
            transport=FakeTransport([CloseFailureResponse()]),
        )

    assert not destination.exists()


def test_download_target_close_failure_prevents_publication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _public_dns(monkeypatch, "192.0.0.9")
    real_fdopen = acquisition.os.fdopen

    class CloseFailure:
        def __init__(self, stream: object) -> None:
            self.stream = stream

        def __getattr__(self, name: str):
            return getattr(self.stream, name)

        def close(self) -> None:
            self.stream.close()  # type: ignore[attr-defined]
            raise OSError("target close failed")

    def failing_fdopen(descriptor: int, mode: str):
        stream = real_fdopen(descriptor, mode)
        return CloseFailure(stream) if mode == "wb" else stream

    monkeypatch.setattr(acquisition.os, "fdopen", failing_fdopen)
    destination = tmp_path / "target"

    with pytest.raises(ValueError, match="target close failed"):
        download_bounded("https://example.test/file", destination, 10, transport=FakeTransport())

    assert not destination.exists()


def test_download_private_cleanup_failure_prevents_publication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _public_dns(monkeypatch, "192.0.0.9")

    class CloseFailureResponse(FakeResponse):
        def close(self) -> None:
            raise OSError("response close failed")

    real_unlink = acquisition.os.unlink

    def fail_cleanup(path: object, *args: object, **kwargs: object) -> None:
        if Path(path).name.startswith(".dataset-cleanup-download-"):
            raise OSError("cleanup failed")
        real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(acquisition.os, "unlink", fail_cleanup)
    destination = tmp_path / "target"

    with pytest.raises(ValueError, match="private download cleanup failed"):
        download_bounded(
            "https://example.test/file",
            destination,
            10,
            transport=FakeTransport([CloseFailureResponse()]),
        )

    assert not destination.exists()


def test_download_rejects_staging_path_swap_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    real_bind = acquisition._bind_download
    moved_staging = tmp_path / "moved-download"
    replacement: list[Path] = []

    def swap_after_binding(downloaded: object, binding: object) -> None:
        real_bind(downloaded, binding)  # type: ignore[arg-type]
        staging = next(tmp_path.glob(".dataset-download-*"))
        staging.replace(moved_staging)
        staging.write_bytes(b"foreign replacement")
        replacement.append(staging)

    monkeypatch.setattr(acquisition, "_bind_download", swap_after_binding)
    destination = tmp_path / "target"

    with pytest.raises(ValueError, match="download staging path changed"):
        download_bounded("https://example.test/file", destination, 10, transport=FakeTransport())

    assert not destination.exists()
    assert not replacement[0].exists()
    assert next(tmp_path.glob(".dataset-cleanup-download-*")).read_bytes() == b"foreign replacement"


def test_download_cleanup_preserves_swapped_staging_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    moved_staging = tmp_path / "moved-download"
    replacement: list[Path] = []

    class SwappingCloseResponse(FakeResponse):
        def close(self) -> None:
            staging = next(tmp_path.glob(".dataset-download-*"))
            staging.replace(moved_staging)
            staging.write_bytes(b"foreign replacement")
            replacement.append(staging)
            raise OSError("response close failed")

    destination = tmp_path / "target"
    with pytest.raises(ValueError, match="response close failed"):
        download_bounded(
            "https://example.test/file",
            destination,
            10,
            transport=FakeTransport([SwappingCloseResponse()]),
        )

    assert not destination.exists()
    assert not replacement[0].exists()
    assert next(tmp_path.glob(".dataset-cleanup-download-*")).read_bytes() == b"foreign replacement"


def test_open_owned_path_closes_descriptor_when_path_identity_mismatches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    owned = tmp_path / "owned"
    owned.write_bytes(b"owned")
    owned_status = owned.stat()
    owned.replace(tmp_path / "moved-owned")
    owned.write_bytes(b"foreign")
    opened: list[int] = []
    real_open = acquisition.os.open

    def tracking_open(*args: object, **kwargs: object) -> int:
        descriptor = real_open(*args, **kwargs)
        opened.append(descriptor)
        return descriptor

    monkeypatch.setattr(acquisition.os, "open", tracking_open)

    with pytest.raises(ValueError, match="owned path identity changed"):
        acquisition._open_owned_path(
            owned,
            (owned_status.st_dev, owned_status.st_ino),
            directory=False,
        )

    assert opened
    with pytest.raises(OSError):
        os.fstat(opened[-1])


def test_download_rejects_swap_between_staging_check_and_publication_without_fd_leak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    destination = tmp_path / "target"
    moved_staging = tmp_path / "moved-download"
    retained: list[int] = []
    staging_identity: list[tuple[int, int]] = []
    real_bind = acquisition._bind_download
    real_quarantine = acquisition._quarantine_path_exclusive

    def track_binding(downloaded: object, binding: object) -> None:
        retained.append(binding.descriptor)  # type: ignore[attr-defined]
        real_bind(downloaded, binding)  # type: ignore[arg-type]

    def swap_during_publication(source: Path, target: Path) -> None:
        source_status = source.stat()
        staging_identity.append((source_status.st_dev, source_status.st_ino))
        source.replace(moved_staging)
        source.write_bytes(b"foreign replacement")
        real_quarantine(source, target)

    monkeypatch.setattr(acquisition, "_bind_download", track_binding)
    monkeypatch.setattr(acquisition, "_publish_path_exclusive", swap_during_publication)

    with pytest.raises(ValueError, match="download publication identity changed"):
        download_bounded("https://example.test/file", destination, 10, transport=FakeTransport())

    assert destination.read_bytes() == b"foreign replacement"
    assert retained
    for descriptor in retained:
        with pytest.raises(OSError):
            os.fstat(descriptor)
    open_identities = {
        (status.st_dev, status.st_ino)
        for raw_descriptor in os.listdir("/dev/fd")
        if (status := _fstat_or_none(raw_descriptor)) is not None
    }
    assert staging_identity[0] not in open_identities


def test_download_cleanup_quarantines_before_identity_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    real_publish = acquisition._publish_path_exclusive
    quarantined: list[Path] = []
    moved_staging = tmp_path / "moved-download"

    def swap_during_quarantine(source: Path, target: Path) -> None:
        if target.name.startswith(".dataset-cleanup-download-"):
            source.replace(moved_staging)
            source.write_bytes(b"foreign replacement")
            quarantined.append(target)
        real_publish(source, target)

    class CloseFailureResponse(FakeResponse):
        def close(self) -> None:
            raise OSError("response close failed")

    monkeypatch.setattr(acquisition, "_quarantine_path_exclusive", swap_during_quarantine)

    with pytest.raises(ValueError, match="response close failed"):
        download_bounded(
            "https://example.test/file",
            tmp_path / "target",
            10,
            transport=FakeTransport([CloseFailureResponse()]),
        )

    assert quarantined
    assert quarantined[0].read_bytes() == b"foreign replacement"


@pytest.mark.parametrize("failure", ["open", "fstat", "close"])
def test_download_committed_owned_publication_ignores_verification_housekeeping_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
):
    _public_dns(monkeypatch, "192.0.0.9")
    destination = tmp_path / "target"
    committed = False
    injected = False
    real_publish = acquisition._publish_path_exclusive
    real_open = acquisition.os.open
    real_fstat = acquisition.os.fstat
    real_close = acquisition.os.close

    def publish(source: Path, target: Path) -> None:
        nonlocal committed
        real_publish(source, target)
        committed = True

    def maybe_open(path: object, *args: object, **kwargs: object) -> int:
        nonlocal injected
        if failure == "open" and committed and Path(path) == destination and not injected:
            injected = True
            raise OSError("post-rename open failed")
        return real_open(path, *args, **kwargs)

    def maybe_fstat(descriptor: int):
        nonlocal injected
        status = real_fstat(descriptor)
        if failure == "fstat" and committed and not injected and stat.S_ISREG(status.st_mode):
            injected = True
            raise OSError("post-rename fstat failed")
        return status

    def maybe_close(descriptor: int) -> None:
        nonlocal injected
        if failure == "close" and committed and not injected:
            injected = True
            real_close(descriptor)
            raise OSError("post-rename close failed")
        real_close(descriptor)

    monkeypatch.setattr(acquisition, "_publish_path_exclusive", publish)
    monkeypatch.setattr(acquisition.os, "open", maybe_open)
    monkeypatch.setattr(acquisition.os, "fstat", maybe_fstat)
    monkeypatch.setattr(acquisition.os, "close", maybe_close)

    downloaded = download_bounded("https://example.test/file", destination, 10, transport=FakeTransport())

    assert injected
    assert downloaded.path.read_bytes() == b"locked"
    assert acquisition._bound_download_metadata(downloaded) == (6, hashlib.sha256(b"locked").hexdigest())


def test_download_publication_disappearance_is_normalized_without_binding_leak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    real_publish = acquisition._publish_path_exclusive
    destination = tmp_path / "target"
    before = len(acquisition._DOWNLOAD_BINDINGS)

    def disappearing_publish(source: Path, target: Path):
        real_publish(source, target)
        destination.unlink()
        raise OSError(errno.ENOENT, "publication disappeared")

    monkeypatch.setattr(acquisition, "_publish_path_exclusive", disappearing_publish)

    with pytest.raises(ValueError, match="destination disappeared during publication"):
        download_bounded("https://example.test/file", destination, 10, transport=FakeTransport())

    assert len(acquisition._DOWNLOAD_BINDINGS) == before



def test_stale_download_descriptor_is_normalized(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _public_dns(monkeypatch, "192.0.0.9")
    downloaded = download_bounded(
        "https://example.test/file",
        tmp_path / "target",
        10,
        transport=FakeTransport(),
    )
    os.close(acquisition._download_binding(downloaded).descriptor)

    with pytest.raises(ValueError, match="download binding is unavailable"):
        acquisition._bound_download_metadata(downloaded)


def test_stale_download_binding_finalizer_never_closes_reused_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    downloaded = download_bounded(
        "https://example.test/file",
        tmp_path / "target",
        10,
        transport=FakeTransport(),
    )
    stale = acquisition._download_binding(downloaded).descriptor
    os.close(stale)
    foreign = _reuse_descriptor(stale, tmp_path / "foreign")

    with pytest.raises(ValueError, match="download binding is unavailable"):
        acquisition._bound_download_metadata(downloaded)
    del downloaded
    gc.collect()

    os.fstat(foreign)
    os.close(foreign)


def test_download_finalizer_directly_abandons_reused_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    downloaded = download_bounded(
        "https://example.test/file",
        tmp_path / "target",
        10,
        transport=FakeTransport(),
    )
    stale = acquisition._download_binding(downloaded).descriptor
    os.close(stale)
    foreign = _reuse_descriptor(stale, tmp_path / "foreign")

    del downloaded
    gc.collect()

    os.fstat(foreign)
    os.close(foreign)


def test_download_revalidates_redirect_dns_and_redacts_query_from_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    answers = {
        "example.test": "192.0.0.9",
        "redirect.test": "127.0.0.1",
    }
    monkeypatch.setattr(
        acquisition.socket,
        "getaddrinfo",
        lambda host, *args, **kwargs: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (answers[host], 443))],
    )
    transport = FakeTransport(
        [FakeResponse(peer="192.0.0.9", headers={"Location": "https://redirect.test/file?token=secret"})]
    )
    transport.responses = iter(
        [
            type(
                "Redirect",
                (FakeResponse,),
                {"status": 302},
            )(peer="192.0.0.9", headers={"Location": "https://redirect.test/file?token=secret"})
        ]
    )

    with pytest.raises(ValueError, match="non-public address") as error:
        download_bounded("https://example.test/file", tmp_path / "target", 10, transport=transport)

    assert "secret" not in str(error.value)
    assert len(transport.requests) == 1


def test_download_uses_exclusive_destination_and_never_removes_unowned_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _public_dns(monkeypatch, "192.0.0.9")
    destination = tmp_path / "target"
    destination.write_bytes(b"owned by caller")

    with pytest.raises(FileExistsError):
        download_bounded("https://example.test/file", destination, 10, transport=FakeTransport())

    assert destination.read_bytes() == b"owned by caller"


def test_validated_zip_members_preserve_paths_and_exclude_structural_directories(tmp_path: Path):
    archive = _zip(
        tmp_path / "data.zip",
        {"root/": b"", "root/data.csv": b"a", "other.txt": b"other"},
    )

    assert validated_zip_members(archive, ZipLimits()) == [
        acquisition.ArchiveEntry("root/data.csv", "data.csv", 1),
        acquisition.ArchiveEntry("other.txt", "other.txt", 5),
    ]


@pytest.mark.parametrize("name", ["../escape.csv", "/absolute.csv", "a\\data.csv"])
def test_validated_zip_members_reject_unsafe_paths(tmp_path: Path, name: str):
    archive = _zip(tmp_path / "data.zip", {name: b"unsafe"})

    with pytest.raises(ValueError, match="safe relative POSIX path"):
        validated_zip_members(archive, ZipLimits())


def test_validated_zip_members_reject_symlink_and_flattened_collision(tmp_path: Path):
    archive = tmp_path / "data.zip"
    with zipfile.ZipFile(archive, "w") as stream:
        symlink = zipfile.ZipInfo("link.csv")
        symlink.create_system = 3
        symlink.external_attr = (stat.S_IFLNK | 0o777) << 16
        stream.writestr(symlink, "target.csv")

    with pytest.raises(ValueError, match="symlink"):
        validated_zip_members(archive, ZipLimits())

    _zip(archive, {"a/data.csv": b"a", "b/data.csv": b"b"})
    with pytest.raises(ValueError, match="flatten to duplicate object name"):
        validated_zip_members(archive, ZipLimits())


def test_extract_members_uses_exclusive_owned_paths(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"root/data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    destination = tmp_path / "members"
    destination.mkdir()
    (destination / "data.csv").write_bytes(b"owned by caller")

    with pytest.raises(FileExistsError):
        extract_members(archive, entries, destination)

    assert (destination / "data.csv").read_bytes() == b"owned by caller"


def test_extract_members_rejects_archive_replacement_after_validation(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"first!"})
    entries = validated_zip_members(archive, ZipLimits())
    replacement = _zip(tmp_path / "replacement.zip", {"data.csv": b"second"})
    replacement.replace(archive)

    with pytest.raises(ValueError, match="changed after validation"):
        extract_members(archive, entries, tmp_path / "members")

    assert not (tmp_path / "members").exists()


def test_extract_members_rejects_same_inode_archive_mutation_after_validation(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"first!"})
    entries = validated_zip_members(archive, ZipLimits())
    replacement_bytes = _zip(tmp_path / "replacement.zip", {"data.csv": b"second"}).read_bytes()
    archive.write_bytes(replacement_bytes)

    with pytest.raises(ValueError, match="changed after validation"):
        extract_members(archive, entries, tmp_path / "members")

    assert not (tmp_path / "members").exists()


def test_extract_members_rejects_requested_subset_of_validated_namespace(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"a.csv": b"a", "b.csv": b"b"})
    entries = validated_zip_members(archive, ZipLimits())

    with pytest.raises(ValueError, match="members changed after validation"):
        extract_members(archive, entries[:1], tmp_path / "members")

    assert not (tmp_path / "members").exists()


def test_extract_members_translates_crc_error_and_cleans_owned_destination(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    payload = bytearray(archive.read_bytes())
    local_header = payload.index(b"PK\x03\x04")
    filename_size, extra_size = struct.unpack_from("<2H", payload, local_header + 26)
    payload[local_header + 30 + filename_size + extra_size] ^= 0xFF
    archive.write_bytes(payload)
    entries = validated_zip_members(archive, ZipLimits())

    with pytest.raises(ValueError, match="artifact is not a valid ZIP archive"):
        extract_members(archive, entries, tmp_path / "members")

    assert not (tmp_path / "members").exists()


def test_extract_members_does_not_follow_replaced_destination_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    destination = tmp_path / "members"
    attacker = tmp_path / "attacker"
    attacker.mkdir()
    real_publish = acquisition._publish_path_exclusive

    def replacing_publish(source: Path, target: Path):
        destination.symlink_to(attacker, target_is_directory=True)
        return real_publish(source, target)

    monkeypatch.setattr(acquisition, "_publish_path_exclusive", replacing_publish)

    with pytest.raises(ValueError, match="destination changed during extraction"):
        extract_members(archive, entries, destination)

    assert destination.is_symlink()
    assert list(attacker.iterdir()) == []


def test_extract_failure_does_not_delete_foreign_destination_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"a.csv": b"a", "b.csv": b"b"})
    entries = validated_zip_members(archive, ZipLimits())[:1]
    destination = tmp_path / "members"
    real_rmtree = acquisition.shutil.rmtree

    def replacement_cleanup(path: Path) -> None:
        destination.mkdir()
        (destination / "foreign").write_bytes(b"attacker owned")
        real_rmtree(path)

    monkeypatch.setattr(acquisition.shutil, "rmtree", replacement_cleanup)

    with pytest.raises(ValueError, match="members changed after validation"):
        extract_members(archive, entries, destination)

    assert (destination / "foreign").read_bytes() == b"attacker owned"


def test_extract_rejects_staging_directory_swap_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    real_paths = acquisition._ExtractedPaths
    moved_staging = tmp_path / "moved-extraction"
    replacement: list[Path] = []

    def swap_after_capability(paths: list[Path], bindings: list[object]):
        capability = real_paths(paths, bindings)
        staging = next(tmp_path.glob(".dataset-extract-*"))
        staging.replace(moved_staging)
        staging.mkdir()
        (staging / "foreign").write_bytes(b"foreign replacement")
        replacement.append(staging)
        return capability

    monkeypatch.setattr(acquisition, "_ExtractedPaths", swap_after_capability)
    destination = tmp_path / "members"

    with pytest.raises(ValueError, match="extraction staging path changed"):
        extract_members(archive, entries, destination)

    assert not destination.exists()
    assert not replacement[0].exists()
    quarantine = next(tmp_path.glob(".dataset-cleanup-extract-*"))
    assert (quarantine / "foreign").read_bytes() == b"foreign replacement"


def test_extract_cleanup_preserves_swapped_staging_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"a.csv": b"a", "b.csv": b"b"})
    entries = validated_zip_members(archive, ZipLimits())[:1]
    real_validated_members = acquisition._validated_members
    moved_staging = tmp_path / "moved-extraction"
    replacement: list[Path] = []

    def swap_before_mismatch(*args: object, **kwargs: object):
        current = real_validated_members(*args, **kwargs)
        staging = next(tmp_path.glob(".dataset-extract-*"))
        staging.replace(moved_staging)
        staging.mkdir()
        (staging / "foreign").write_bytes(b"foreign replacement")
        replacement.append(staging)
        return current

    monkeypatch.setattr(acquisition, "_validated_members", swap_before_mismatch)

    with pytest.raises(ValueError, match="members changed after validation"):
        extract_members(archive, entries, tmp_path / "members")

    assert not replacement[0].exists()
    quarantine = next(tmp_path.glob(".dataset-cleanup-extract-*"))
    assert (quarantine / "foreign").read_bytes() == b"foreign replacement"


def test_extract_rejects_swap_between_staging_check_and_publication_without_fd_leak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    destination = tmp_path / "members"
    moved_staging = tmp_path / "moved-extraction"
    observed: list[acquisition._ExtractedPaths] = []
    staging_identity: list[tuple[int, int]] = []
    real_paths = acquisition._ExtractedPaths
    real_quarantine = acquisition._quarantine_path_exclusive

    def track_capability(paths: list[Path], bindings: list[object]):
        capability = real_paths(paths, bindings)
        observed.append(capability)
        return capability

    def swap_during_publication(source: Path, target: Path) -> None:
        source_status = source.stat()
        staging_identity.append((source_status.st_dev, source_status.st_ino))
        source.replace(moved_staging)
        source.mkdir()
        (source / "foreign").write_bytes(b"foreign replacement")
        real_quarantine(source, target)

    monkeypatch.setattr(acquisition, "_ExtractedPaths", track_capability)
    monkeypatch.setattr(acquisition, "_publish_path_exclusive", swap_during_publication)

    with pytest.raises(ValueError, match="extraction publication identity changed"):
        extract_members(archive, entries, destination)

    assert (destination / "foreign").read_bytes() == b"foreign replacement"
    assert observed
    for binding in observed[0]._bindings:
        with pytest.raises(OSError):
            os.fstat(binding.descriptor)
    open_identities = {
        (status.st_dev, status.st_ino)
        for raw_descriptor in os.listdir("/dev/fd")
        if (status := _fstat_or_none(raw_descriptor)) is not None
    }
    assert staging_identity[0] not in open_identities


def test_extract_cleanup_quarantines_before_identity_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"a.csv": b"a", "b.csv": b"b"})
    entries = validated_zip_members(archive, ZipLimits())[:1]
    real_publish = acquisition._publish_path_exclusive
    quarantined: list[Path] = []
    moved_staging = tmp_path / "moved-extraction"

    def swap_during_quarantine(source: Path, target: Path) -> None:
        if target.name.startswith(".dataset-cleanup-extract-"):
            source.replace(moved_staging)
            source.mkdir()
            (source / "foreign").write_bytes(b"foreign replacement")
            quarantined.append(target)
        real_publish(source, target)

    monkeypatch.setattr(acquisition, "_quarantine_path_exclusive", swap_during_quarantine)

    with pytest.raises(ValueError, match="members changed after validation"):
        extract_members(archive, entries, tmp_path / "members")

    assert quarantined
    assert (quarantined[0] / "foreign").read_bytes() == b"foreign replacement"


@pytest.mark.parametrize("failure", ["open", "fstat", "verification_close", "staging_close"])
def test_extract_committed_owned_publication_ignores_verification_housekeeping_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    destination = tmp_path / "members"
    committed = False
    post_commit_closes = 0
    injected = False
    real_publish = acquisition._publish_path_exclusive
    real_open = acquisition.os.open
    real_fstat = acquisition.os.fstat
    real_close = acquisition.os.close

    def publish(source: Path, target: Path) -> None:
        nonlocal committed
        real_publish(source, target)
        committed = True

    def maybe_open(path: object, *args: object, **kwargs: object) -> int:
        nonlocal injected
        if failure == "open" and committed and Path(path) == destination and not injected:
            injected = True
            raise OSError("post-rename open failed")
        return real_open(path, *args, **kwargs)

    def maybe_fstat(descriptor: int):
        nonlocal injected
        status = real_fstat(descriptor)
        if failure == "fstat" and committed and not injected and stat.S_ISDIR(status.st_mode):
            injected = True
            raise OSError("post-rename fstat failed")
        return status

    def maybe_close(descriptor: int) -> None:
        nonlocal injected, post_commit_closes
        if committed:
            post_commit_closes += 1
            expected = 1 if failure == "verification_close" else 2
            if failure in {"verification_close", "staging_close"} and post_commit_closes == expected:
                injected = True
                real_close(descriptor)
                raise OSError("post-rename close failed")
        real_close(descriptor)

    monkeypatch.setattr(acquisition, "_publish_path_exclusive", publish)
    monkeypatch.setattr(acquisition.os, "open", maybe_open)
    monkeypatch.setattr(acquisition.os, "fstat", maybe_fstat)
    monkeypatch.setattr(acquisition.os, "close", maybe_close)

    paths = extract_members(archive, entries, destination)

    assert injected
    assert paths[0].read_bytes() == b"locked"
    assert acquisition._bound_extracted_metadata(paths) == [(6, hashlib.sha256(b"locked").hexdigest())]


def test_archive_entry_public_constructor_remains_three_fields():
    assert acquisition.ArchiveEntry("a/data.csv", "data.csv", 1) == acquisition.ArchiveEntry(
        member_path="a/data.csv",
        object_name="data.csv",
        size_bytes=1,
    )


@pytest.mark.parametrize(
    "value",
    [
        acquisition.ArchiveEntry("a/data.csv", "data.csv", 1),
        acquisition.DownloadedFile(Path("download"), acquisition.ResponseEvidence(etag='"locked"')),
        acquisition.ResponseEvidence(etag='"locked"', last_modified="Mon, 01 Jan 2024 00:00:00 GMT"),
    ],
)
def test_public_dataclasses_have_exact_serialization_fields(value: object):
    expected_fields = {
        acquisition.ArchiveEntry: ["member_path", "object_name", "size_bytes"],
        acquisition.DownloadedFile: ["path", "evidence"],
        acquisition.ResponseEvidence: ["etag", "last_modified"],
    }[type(value)]

    assert [field.name for field in dataclasses.fields(value)] == expected_fields
    assert list(dataclasses.asdict(value)) == expected_fields
    assert copy.copy(value) == value
    assert copy.deepcopy(value) == value
    assert pickle.loads(pickle.dumps(value)) == value
    assert hash(pickle.loads(pickle.dumps(value))) == hash(value)


def test_zip_limits_has_exact_four_field_public_surface():
    limits = ZipLimits(1, 2, 3, 4)

    assert [field.name for field in dataclasses.fields(limits)] == [
        "max_entries",
        "max_central_directory_bytes",
        "max_total_expanded_bytes",
        "max_compression_ratio",
    ]
    assert dataclasses.asdict(limits) == {
        "max_entries": 1,
        "max_central_directory_bytes": 2,
        "max_total_expanded_bytes": 3,
        "max_compression_ratio": 4,
    }
    assert copy.copy(limits) == limits
    assert copy.deepcopy(limits) == limits
    assert pickle.loads(pickle.dumps(limits)) == limits


def test_archive_snapshot_rejects_raw_file_over_explicit_bound(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    archive = tmp_path / "oversized.zip"
    archive.touch()
    os.truncate(archive, 11)
    monkeypatch.setattr(acquisition, "_MAX_ARCHIVE_SNAPSHOT_BYTES", 10)

    with pytest.raises(ValueError, match="archive exceeds 10 bytes"):
        acquisition.preflight_zip(archive, ZipLimits())


def test_archive_disappearance_is_stable_value_error(tmp_path: Path):
    missing = tmp_path / "missing.zip"

    with pytest.raises(ValueError, match="archive is unavailable"):
        acquisition.preflight_zip(missing, ZipLimits())


def test_secure_extraction_has_explicit_supported_platform_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    monkeypatch.setattr(acquisition, "_SECURE_EXTRACTION_SUPPORTED", False)

    with pytest.raises(RuntimeError, match="secure extraction is not supported"):
        extract_members(archive, entries, tmp_path / "members")

    assert not (tmp_path / "members").exists()


def test_secure_extraction_preflights_atomic_publish_before_archive_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    observed: list[Path] = []

    def unsupported(parent: Path) -> bool:
        observed.append(parent)
        return False

    monkeypatch.setattr(acquisition, "_probe_atomic_publish", unsupported)
    monkeypatch.setattr(
        acquisition,
        "_stable_archive",
        lambda path: (_ for _ in ()).throw(AssertionError("archive work began")),
    )

    with pytest.raises(RuntimeError, match="secure extraction is not supported"):
        extract_members(archive, entries, tmp_path / "members")

    assert observed == [tmp_path]
    assert not (tmp_path / "members").exists()


def test_atomic_publish_probe_uses_private_names_on_destination_filesystem(tmp_path: Path):
    before = set(tmp_path.iterdir())

    assert acquisition._probe_atomic_publish(tmp_path) is True

    assert set(tmp_path.iterdir()) == before


def test_extract_constructs_capability_and_closes_nonretained_fds_before_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    observed: dict[str, object] = {}
    real_paths = acquisition._ExtractedPaths
    real_publish = acquisition._publish_path_exclusive

    class TrackingPaths(real_paths):
        def __init__(self, paths: list[Path], bindings: list[object]) -> None:
            super().__init__(paths, bindings)
            observed["capability"] = self

    def checked_publish(source: Path, destination: Path) -> None:
        capability = observed["capability"]
        assert isinstance(capability, real_paths)
        archive_identity = (archive.stat().st_dev, archive.stat().st_ino)
        open_identities: set[tuple[int, int]] = set()
        for raw_descriptor in os.listdir("/dev/fd"):
            try:
                status = os.fstat(int(raw_descriptor))
            except (OSError, ValueError):
                continue
            open_identities.add((status.st_dev, status.st_ino))
        assert archive_identity not in open_identities
        real_publish(source, destination)

    monkeypatch.setattr(acquisition, "_ExtractedPaths", TrackingPaths)
    monkeypatch.setattr(acquisition, "_publish_path_exclusive", checked_publish)

    paths = extract_members(archive, entries, tmp_path / "members")

    assert paths[0].read_bytes() == b"locked"


def test_extract_capability_construction_failure_prevents_publication_and_closes_bindings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    retained: list[int] = []
    real_open = acquisition.os.open

    def tracking_open(*args: object, **kwargs: object) -> int:
        descriptor = real_open(*args, **kwargs)
        if args and args[0] == "data.csv" and args[1] & os.O_RDONLY == os.O_RDONLY:
            retained.append(descriptor)
        return descriptor

    monkeypatch.setattr(acquisition.os, "open", tracking_open)
    monkeypatch.setattr(
        acquisition,
        "_ExtractedPaths",
        lambda *args: (_ for _ in ()).throw(MemoryError("capability failed")),
    )

    with pytest.raises(MemoryError, match="capability failed"):
        extract_members(archive, entries, tmp_path / "members")

    assert not (tmp_path / "members").exists()
    assert retained
    for descriptor in retained:
        with pytest.raises(OSError):
            os.fstat(descriptor)


def test_extract_publication_failure_closes_preconstructed_capability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    observed: dict[str, acquisition._ExtractedPaths] = {}
    real_paths = acquisition._ExtractedPaths

    def construct(paths: list[Path], bindings: list[object]):
        capability = real_paths(paths, bindings)
        observed["capability"] = capability
        return capability

    monkeypatch.setattr(acquisition, "_ExtractedPaths", construct)
    monkeypatch.setattr(
        acquisition,
        "_publish_path_exclusive",
        lambda *args: (_ for _ in ()).throw(ValueError("publish failed")),
    )

    with pytest.raises(ValueError, match="destination changed during extraction"):
        extract_members(archive, entries, tmp_path / "members")

    assert not (tmp_path / "members").exists()
    for binding in observed["capability"]._bindings:
        with pytest.raises(OSError):
            os.fstat(binding.descriptor)


def test_extract_capability_close_failure_falls_back_to_binding_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    observed: dict[str, acquisition._ExtractedPaths] = {}
    real_paths = acquisition._ExtractedPaths

    def construct(paths: list[Path], bindings: list[object]):
        capability = real_paths(paths, bindings)
        observed["capability"] = capability
        return capability

    monkeypatch.setattr(acquisition, "_ExtractedPaths", construct)
    monkeypatch.setattr(real_paths, "close", lambda self: (_ for _ in ()).throw(OSError("close failed")))
    monkeypatch.setattr(
        acquisition,
        "_publish_path_exclusive",
        lambda *args: (_ for _ in ()).throw(ValueError("publish failed")),
    )

    with pytest.raises(ValueError, match="extraction cleanup failed") as error:
        extract_members(archive, entries, tmp_path / "members")

    assert isinstance(error.value.__cause__, ValueError)
    assert "publish failed" in str(error.value.__cause__)
    assert not (tmp_path / "members").exists()
    for binding in observed["capability"]._bindings:
        with pytest.raises(OSError):
            os.fstat(binding.descriptor)


def test_extract_cleanup_failure_is_controlled_and_preserves_original_cause(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"a.csv": b"a", "b.csv": b"b"})
    entries = validated_zip_members(archive, ZipLimits())[:1]
    monkeypatch.setattr(
        acquisition.shutil,
        "rmtree",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("cleanup failed")),
    )

    with pytest.raises(ValueError, match="extraction cleanup failed") as error:
        extract_members(archive, entries, tmp_path / "members")

    assert isinstance(error.value.__cause__, ValueError)
    assert "members changed after validation" in str(error.value.__cause__)
    assert not (tmp_path / "members").exists()


def test_extract_staging_open_cleanup_failure_is_controlled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    real_open = acquisition.os.open

    def failing_open(path: object, *args: object, **kwargs: object) -> int:
        if isinstance(path, Path) and path.name.startswith(".dataset-extract-"):
            raise OSError("staging open failed")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(acquisition.os, "open", failing_open)
    monkeypatch.setattr(
        acquisition.shutil,
        "rmtree",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("cleanup failed")),
    )

    with pytest.raises(ValueError, match="extraction cleanup failed") as error:
        extract_members(archive, entries, tmp_path / "members")

    assert isinstance(error.value.__cause__, OSError)
    assert "staging open failed" in str(error.value.__cause__)


def test_extract_first_staging_identity_failure_preserves_unknown_path_and_original(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    real_lstat = acquisition.Path.lstat
    moved_staging = tmp_path / "moved-extraction"
    foreign_staging: list[Path] = []
    failed = False

    def fail_first_staging_lstat(path: Path):
        nonlocal failed
        if path.name.startswith(".dataset-extract-") and not failed:
            failed = True
            path.replace(moved_staging)
            path.mkdir()
            (path / "foreign").write_bytes(b"foreign replacement")
            foreign_staging.append(path)
            raise OSError("first staging identity observation failed")
        return real_lstat(path)

    monkeypatch.setattr(acquisition.Path, "lstat", fail_first_staging_lstat)

    with pytest.raises(ValueError, match="cleanup ownership is uncertain") as error:
        extract_members(archive, entries, tmp_path / "members")

    assert failed
    assert isinstance(error.value.__cause__, OSError)
    assert "first staging identity observation failed" in str(error.value.__cause__)
    assert moved_staging.is_dir()
    assert (foreign_staging[0] / "foreign").read_bytes() == b"foreign replacement"
    assert list(tmp_path.glob(".dataset-cleanup-extract-*")) == []


def test_download_requires_owned_non_writable_trusted_parent(tmp_path: Path):
    untrusted = tmp_path / "untrusted"
    untrusted.mkdir(mode=0o777)
    untrusted.chmod(0o777)

    with pytest.raises(ValueError, match="not group/world writable"):
        download_bounded("https://example.test/file", untrusted / "target", 10, transport=FakeTransport())


def test_shared_zip_policy_enforces_member_and_total_expanded_limits(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"a.csv": b"aa", "b.csv": b"bb"})

    with pytest.raises(ValueError, match="archive exceeds 3 uncompressed bytes"):
        validated_zip_members(archive, ZipLimits(max_total_expanded_bytes=3))


def test_shared_zip_policy_enforces_internal_member_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    archive = _zip(tmp_path / "data.zip", {"a.csv": b"aa"})
    monkeypatch.setattr(acquisition, "_MAX_ZIP_MEMBER_BYTES", 1)

    with pytest.raises(ValueError, match="member a.csv exceeds 1 bytes"):
        validated_zip_members(archive, ZipLimits())


def test_shared_zip_policy_rejects_duplicate_exact_member_path(tmp_path: Path):
    archive = tmp_path / "data.zip"
    with pytest.warns(UserWarning, match="Duplicate name"):
        with zipfile.ZipFile(archive, "w") as stream:
            stream.writestr("data.csv", b"first")
            stream.writestr("data.csv", b"second")

    with pytest.raises(ValueError, match="duplicate member path"):
        validated_zip_members(archive, ZipLimits())


def test_validated_entries_reject_copy_deepcopy_and_pickle(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())

    for operation in (copy.copy, copy.deepcopy, pickle.dumps):
        with pytest.raises(TypeError, match="capability-bearing"):
            operation(entries)


def test_capability_results_preserve_runtime_list_contract(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    paths = extract_members(archive, entries, tmp_path / "members")

    assert isinstance(entries, list)
    assert list(entries) == [entries[0]]
    assert entries == [entries[0]]
    assert isinstance(paths, list)
    assert list(paths) == [paths[0]]
    assert paths == [paths[0]]
    assert not hasattr(entries, "__dict__")
    assert not hasattr(paths, "__dict__")


def test_extract_rejects_pre_call_unbound_validated_list_mutation(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    list.__setitem__(entries, 0, acquisition.ArchiveEntry("data.csv", "renamed.csv", 6))

    with pytest.raises(ValueError, match="validated archive entries changed"):
        extract_members(archive, entries, tmp_path / "members")

    assert not (tmp_path / "members").exists()


def test_canonical_archive_entries_helper_rejects_plain_or_mutated_inputs(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())

    assert acquisition._canonical_archive_entries(entries) == (
        acquisition.ArchiveEntry("data.csv", "data.csv", 6),
    )
    with pytest.raises(ValueError, match="not bound to validated archive entries"):
        acquisition._canonical_archive_entries(list(entries))
    list.__setitem__(entries, 0, acquisition.ArchiveEntry("data.csv", "renamed.csv", 6))
    with pytest.raises(ValueError, match="validated archive entries changed"):
        acquisition._canonical_archive_entries(entries)


def test_extract_uses_canonical_entries_during_mid_call_unbound_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    entries = validated_zip_members(archive, ZipLimits())
    real_validate = acquisition._validated_members

    def mutating_validation(*args: object, **kwargs: object):
        fresh = real_validate(*args, **kwargs)
        list.__setitem__(entries, 0, acquisition.ArchiveEntry("data.csv", "renamed.csv", 6))
        return fresh

    monkeypatch.setattr(acquisition, "_validated_members", mutating_validation)

    paths = extract_members(archive, entries, tmp_path / "members")

    assert paths == [tmp_path / "members" / "data.csv"]
    assert paths[0].read_bytes() == b"locked"
    assert not (tmp_path / "members" / "renamed.csv").exists()


def test_extracted_metadata_uses_canonical_paths_after_unbound_mutation(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    paths = extract_members(archive, validated_zip_members(archive, ZipLimits()), tmp_path / "members")
    foreign = tmp_path / "foreign"
    foreign.write_bytes(b"foreign")
    list.__setitem__(paths, 0, foreign)

    assert acquisition._bound_extracted_metadata(paths) == [(6, hashlib.sha256(b"locked").hexdigest())]


@pytest.mark.parametrize(
    "operation",
    [
        lambda value: value.append(value[0]),
        lambda value: value.extend(value),
        lambda value: value.insert(0, value[0]),
        lambda value: value.clear(),
        lambda value: value.pop(),
        lambda value: value.remove(value[0]),
        lambda value: value.reverse(),
        lambda value: value.sort(key=lambda item: item.member_path),
        lambda value: value.__setitem__(0, value[0]),
        lambda value: value.__delitem__(0),
        lambda value: value.__iadd__(value),
        lambda value: value.__imul__(2),
    ],
)
def test_validated_entries_reject_all_list_mutators(tmp_path: Path, operation: object):
    entries = validated_zip_members(_zip(tmp_path / "data.zip", {"data.csv": b"locked"}), ZipLimits())

    with pytest.raises(TypeError, match="immutable"):
        operation(entries)  # type: ignore[operator]


def test_extracted_paths_reject_copy_deepcopy_pickle_and_binding_mutation(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    paths = extract_members(archive, validated_zip_members(archive, ZipLimits()), tmp_path / "members")

    for operation in (copy.copy, copy.deepcopy, pickle.dumps):
        with pytest.raises(TypeError, match="capability-bearing"):
            operation(paths)
    with pytest.raises(AttributeError):
        paths.bindings = ()
    with pytest.raises(TypeError):
        paths._bindings[0] = paths._bindings[0]


@pytest.mark.parametrize(
    "operation",
    [
        lambda value: value.append(value[0]),
        lambda value: value.extend(value),
        lambda value: value.insert(0, value[0]),
        lambda value: value.clear(),
        lambda value: value.pop(),
        lambda value: value.remove(value[0]),
        lambda value: value.reverse(),
        lambda value: value.sort(),
        lambda value: value.__setitem__(0, value[0]),
        lambda value: value.__delitem__(0),
        lambda value: value.__iadd__(value),
        lambda value: value.__imul__(2),
    ],
)
def test_extracted_paths_reject_all_list_mutators(tmp_path: Path, operation: object):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    paths = extract_members(archive, validated_zip_members(archive, ZipLimits()), tmp_path / "members")

    with pytest.raises(TypeError, match="immutable"):
        operation(paths)  # type: ignore[operator]


def test_stale_extracted_descriptor_is_normalized(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    paths = extract_members(archive, validated_zip_members(archive, ZipLimits()), tmp_path / "members")
    os.close(paths._bindings[0].descriptor)

    with pytest.raises(ValueError, match="extracted output binding is unavailable"):
        acquisition._bound_extracted_metadata(paths)


def test_stale_extracted_binding_finalizer_never_closes_reused_descriptor(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    paths = extract_members(archive, validated_zip_members(archive, ZipLimits()), tmp_path / "members")
    stale = paths._bindings[0].descriptor
    os.close(stale)
    foreign = _reuse_descriptor(stale, tmp_path / "foreign")

    with pytest.raises(ValueError, match="extracted output binding is unavailable"):
        acquisition._bound_extracted_metadata(paths)
    del paths
    gc.collect()

    os.fstat(foreign)
    os.close(foreign)


def test_extracted_paths_finalizer_directly_abandons_reused_descriptor(tmp_path: Path):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    paths = extract_members(archive, validated_zip_members(archive, ZipLimits()), tmp_path / "members")
    stale = paths._bindings[0].descriptor
    os.close(stale)
    foreign = _reuse_descriptor(stale, tmp_path / "foreign")

    del paths
    gc.collect()

    os.fstat(foreign)
    os.close(foreign)


def test_archive_snapshot_rejects_same_size_mutation_and_restore_during_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    original = archive.read_bytes()
    real_open_archive = acquisition._open_archive

    class MutatingStream:
        def __init__(self, stream: object) -> None:
            self.stream = stream
            self.mutated = False

        def __getattr__(self, name: str):
            return getattr(self.stream, name)

        def __enter__(self):
            self.stream.__enter__()
            return self

        def __exit__(self, *args: object):
            return self.stream.__exit__(*args)

        def read(self, amount: int = -1) -> bytes:
            chunk = self.stream.read(amount)
            if chunk and not self.mutated:
                self.mutated = True
                archive.write_bytes(bytes([original[0] ^ 0xFF]) + original[1:])
                archive.write_bytes(original)
            return chunk

    def mutating_open(path: Path):
        stream, status = real_open_archive(path)
        return MutatingStream(stream), status

    monkeypatch.setattr(acquisition, "_open_archive", mutating_open)

    with pytest.raises(ValueError, match="archive changed while taking stable snapshot"):
        acquisition.preflight_zip(archive, ZipLimits())


def test_zip64_locator_must_point_to_adjacent_record_before_zipfile_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    archive = _zip(tmp_path / "data.zip", {"data.csv": b"locked"})
    payload = archive.read_bytes()
    eocd_offset = payload.rindex(b"PK\x05\x06")
    (
        _signature,
        _disk,
        _central_directory_disk,
        _entries_on_disk,
        entries,
        central_directory_size,
        central_directory_offset,
        _comment_size,
    ) = struct.unpack_from("<4s4H2LH", payload, eocd_offset)
    record_offset = eocd_offset
    contradictory_offset = record_offset - 1
    zip64_record = struct.pack(
        "<4sQ2H2L4Q",
        b"PK\x06\x06",
        44,
        45,
        45,
        0,
        0,
        entries,
        entries,
        central_directory_size,
        central_directory_offset - 1,
    )
    locator = struct.pack("<4sLQL", b"PK\x06\x07", 0, contradictory_offset, 1)
    sentinel_eocd = struct.pack(
        "<4s4H2LH",
        b"PK\x05\x06",
        0,
        0,
        0xFFFF,
        0xFFFF,
        0xFFFFFFFF,
        0xFFFFFFFF,
        0,
    )
    archive.write_bytes(payload[:eocd_offset] + zip64_record + locator + sentinel_eocd)
    monkeypatch.setattr(
        acquisition.zipfile,
        "ZipFile",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("ZipFile constructed")),
    )

    with pytest.raises(ValueError, match="ZIP64 record layout is inconsistent"):
        validated_zip_members(archive, ZipLimits())
