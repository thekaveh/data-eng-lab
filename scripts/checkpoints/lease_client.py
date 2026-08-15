"""Writer-side lease session lifecycle shared by notebook projections."""

from __future__ import annotations

from types import TracebackType
from typing import Mapping, Protocol

from scripts.checkpoints.leases import (
    AcquireRequest,
    HeartbeatRequest,
    LeaseFailure,
    TerminalRequest,
)


class _LeaseApi(Protocol):
    def acquire(self, request: AcquireRequest): ...

    def heartbeat(self, request: HeartbeatRequest): ...

    def terminal(self, request: TerminalRequest): ...


class LeaseSession:
    """Acquire before query start and always heartbeat/terminalize on exit."""

    def __init__(
        self,
        api: _LeaseApi,
        request: AcquireRequest,
        *,
        terminal_state: str,
        terminal_evidence: Mapping[str, object],
    ) -> None:
        self._api = api
        self._request = request
        self._terminal_state = terminal_state
        self._terminal_evidence = terminal_evidence
        self._epoch: str | None = None
        self._query: object | None = None

    def __enter__(self) -> LeaseSession:
        result = self._api.acquire(self._request)
        epoch = getattr(result, "epoch", None)
        if not isinstance(epoch, str):
            raise LeaseFailure("lease_response_invalid")
        self._epoch = epoch
        return self

    def bind_query(self, query: object) -> None:
        if not hasattr(query, "stop"):
            raise LeaseFailure("query_invalid")
        self._query = query

    def heartbeat(self) -> None:
        request = self._heartbeat_request()
        try:
            self._api.heartbeat(request)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as error:
            if self._query is not None:
                try:
                    self._query.stop()
                except BaseException:
                    pass
            if isinstance(error, LeaseFailure):
                raise
            raise LeaseFailure("heartbeat_failed") from None

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        cleanup_failure: BaseException | None = None
        try:
            self._api.heartbeat(self._heartbeat_request())
        except (KeyboardInterrupt, SystemExit) as error:
            cleanup_failure = error
        except BaseException:
            cleanup_failure = LeaseFailure("final_heartbeat_failed")
        try:
            self._api.terminal(
                TerminalRequest(
                    self._request.checkpoint_id,
                    self._request.prefix,
                    self._require_epoch(),
                    self._terminal_state,
                    self._terminal_evidence,
                )
            )
        except (KeyboardInterrupt, SystemExit) as error:
            if cleanup_failure is None:
                cleanup_failure = error
        except BaseException:
            if cleanup_failure is None:
                cleanup_failure = LeaseFailure("terminal_failed")
        if exception is not None:
            return False
        if cleanup_failure is not None:
            raise cleanup_failure
        return False

    def _heartbeat_request(self) -> HeartbeatRequest:
        return HeartbeatRequest(self._request.checkpoint_id, self._request.prefix, self._require_epoch())

    def _require_epoch(self) -> str:
        if self._epoch is None:
            raise LeaseFailure("lease_not_acquired")
        return self._epoch
