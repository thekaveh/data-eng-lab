"""Fixed-origin streaming lease lifecycle for Jupyter notebook projections."""

from __future__ import annotations

import json
import os
import re
import threading
import urllib.error
import urllib.request
from types import MappingProxyType
from typing import Callable, Mapping


class NotebookLeaseFailure(ValueError):
    """A bounded failure safe for notebook output."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


_ORIGIN = "http://checkpoint-retention:8080"
_MAX_BODY = 65_536
_IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")
_ROUTES = {
    "acquire": "/v1/leases/acquire",
    "heartbeat": "/v1/leases/heartbeat",
    "terminal": "/v1/leases/terminal",
}


class StreamingLease:
    """Acquire before query start, heartbeat while active, terminalize on exit."""

    def __init__(
        self,
        *,
        checkpoint_id: str,
        prefix: str,
        workload: str,
        owner_id: str,
        session_id: str,
        terminal_state: str,
        terminal_evidence: Mapping[str, object],
        post: Callable[[str, Mapping[str, object]], Mapping[str, object]] | None = None,
        heartbeat_seconds: float = 60,
    ) -> None:
        for value in (checkpoint_id, workload, owner_id, session_id):
            if not isinstance(value, str) or _IDENTITY.fullmatch(value) is None:
                raise NotebookLeaseFailure("lease_config_invalid")
        if not isinstance(prefix, str) or not prefix or len(prefix.encode("ascii", errors="ignore")) != len(prefix):
            raise NotebookLeaseFailure("lease_config_invalid")
        if terminal_state not in {"stopped", "completed", "retired"}:
            raise NotebookLeaseFailure("lease_config_invalid")
        if not isinstance(terminal_evidence, Mapping):
            raise NotebookLeaseFailure("lease_config_invalid")
        if (
            not isinstance(heartbeat_seconds, (int, float))
            or isinstance(heartbeat_seconds, bool)
            or heartbeat_seconds <= 0
        ):
            raise NotebookLeaseFailure("lease_config_invalid")
        self._checkpoint_id = checkpoint_id
        self._prefix = prefix
        self._workload = workload
        self._owner_id = owner_id
        self._session_id = session_id
        self._terminal_state = terminal_state
        self._terminal_evidence = MappingProxyType(dict(terminal_evidence))
        self._post = _post if post is None else post
        self._heartbeat_seconds = heartbeat_seconds
        self._epoch: str | None = None
        self._query: object | None = None
        self._stop = threading.Event()
        self._worker: threading.Thread | None = None
        self._worker_failure: NotebookLeaseFailure | None = None

    def __enter__(self) -> StreamingLease:
        response = self._call(
            "acquire",
            {
                "checkpoint_id": self._checkpoint_id,
                "owner_id": self._owner_id,
                "prefix": self._prefix,
                "session_id": self._session_id,
                "workload": self._workload,
            },
        )
        epoch = response.get("epoch")
        if not isinstance(epoch, str) or _UUID.fullmatch(epoch) is None:
            raise NotebookLeaseFailure("lease_response_invalid")
        self._epoch = epoch
        return self

    def bind_query(self, query: object) -> None:
        if self._epoch is None or self._query is not None or not hasattr(query, "stop"):
            raise NotebookLeaseFailure("query_invalid")
        self._query = query
        self._worker = threading.Thread(target=self._heartbeat_loop, name="checkpoint-lease", daemon=True)
        self._worker.start()

    def __exit__(self, exception_type, exception, traceback) -> bool:
        primary = exception
        self._stop.set()
        if self._worker is not None:
            self._worker.join(timeout=min(float(self._heartbeat_seconds) + 1, 5))
        query = self._query
        if query is not None and getattr(query, "isActive", False):
            try:
                query.stop()
            except (KeyboardInterrupt, SystemExit):
                if primary is None:
                    raise
            except BaseException:
                if primary is None:
                    primary = NotebookLeaseFailure("query_stop_failed")
        cleanup = self._finalize()
        if primary is None:
            primary = self._worker_failure or cleanup
        if primary is not None and exception is None:
            raise primary
        return False

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self._heartbeat_seconds):
            try:
                self._heartbeat()
            except (KeyboardInterrupt, SystemExit):
                self._worker_failure = NotebookLeaseFailure("heartbeat_failed")
            except BaseException:
                self._worker_failure = NotebookLeaseFailure("heartbeat_failed")
            if self._worker_failure is not None:
                query = self._query
                if query is not None:
                    try:
                        query.stop()
                    except BaseException:
                        pass
                self._stop.set()
                return

    def _heartbeat(self) -> None:
        self._call(
            "heartbeat",
            {"checkpoint_id": self._checkpoint_id, "epoch": self._require_epoch(), "prefix": self._prefix},
        )

    def _finalize(self) -> NotebookLeaseFailure | None:
        failure: NotebookLeaseFailure | None = None
        try:
            self._heartbeat()
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            failure = NotebookLeaseFailure("final_heartbeat_failed")
        try:
            self._call(
                "terminal",
                {
                    "checkpoint_id": self._checkpoint_id,
                    "epoch": self._require_epoch(),
                    "evidence": dict(self._terminal_evidence),
                    "prefix": self._prefix,
                    "state": self._terminal_state,
                },
            )
        except (KeyboardInterrupt, SystemExit):
            if failure is None:
                raise
        except BaseException:
            if failure is None:
                failure = NotebookLeaseFailure("terminal_failed")
        return failure

    def _call(self, action: str, payload: Mapping[str, object]) -> Mapping[str, object]:
        try:
            response = self._post(action, payload)
        except (KeyboardInterrupt, SystemExit):
            raise
        except NotebookLeaseFailure:
            raise
        except BaseException:
            raise NotebookLeaseFailure(f"{action}_failed") from None
        if not isinstance(response, Mapping):
            raise NotebookLeaseFailure("lease_response_invalid")
        return response

    def _require_epoch(self) -> str:
        if self._epoch is None:
            raise NotebookLeaseFailure("lease_not_acquired")
        return self._epoch


def _post(action: str, payload: Mapping[str, object]) -> Mapping[str, object]:
    path = _ROUTES.get(action)
    token = os.environ.get("CHECKPOINT_RETENTION_LEASE_TOKEN")
    origin = os.environ.get("CHECKPOINT_RETENTION_URI", _ORIGIN)
    if path is None or origin != _ORIGIN or not isinstance(token, str) or not token or len(token.encode()) > 256:
        raise NotebookLeaseFailure("lease_configuration_invalid")
    try:
        body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    except (TypeError, ValueError, UnicodeError):
        raise NotebookLeaseFailure("lease_request_invalid") from None
    if not body or len(body) > _MAX_BODY:
        raise NotebookLeaseFailure("lease_request_invalid")
    request = urllib.request.Request(
        _ORIGIN + path,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Length": str(len(body)),
            "Content-Type": "application/json",
        },
    )
    response = None
    primary: BaseException | None = None
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        response = opener.open(request, timeout=30)
        raw = response.read(_MAX_BODY + 1)
        if type(raw) is not bytes or len(raw) > _MAX_BODY:
            raise NotebookLeaseFailure("lease_response_invalid")
        value = json.loads(raw.decode("utf-8"))
        if not isinstance(value, dict):
            raise NotebookLeaseFailure("lease_response_invalid")
        return value
    except (KeyboardInterrupt, SystemExit, NotebookLeaseFailure) as error:
        primary = error
        raise
    except (urllib.error.URLError, UnicodeError, json.JSONDecodeError, RecursionError):
        primary = NotebookLeaseFailure("lease_request_failed")
        raise primary from None
    finally:
        if response is not None:
            try:
                response.close()
            except (KeyboardInterrupt, SystemExit):
                if primary is None:
                    raise
            except BaseException:
                if primary is None:
                    raise NotebookLeaseFailure("lease_response_close_failed") from None
