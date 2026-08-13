"""Bounded, same-origin Trino statement protocol client."""

from __future__ import annotations

import json
import math
import re
import time
from collections.abc import Callable, Mapping
from typing import Any, NamedTuple
from urllib.parse import urlparse

from trino_bi.contracts import QUERIES, QueryName, QuerySpec

REQUEST_TIMEOUT_SECONDS = 30
QUERY_DEADLINE_SECONDS = 120
MAX_RESPONSE_BYTES = 256 * 1024
MAX_TOTAL_BYTES = 1024 * 1024
MAX_PAGES = 32
MAX_REQUESTS = 33
MAX_COLUMNS = 32
MAX_CELL_BYTES = 64 * 1024
MAX_JSON_DEPTH = 16

_BASE_URL = "http://trino:8080"
_STATEMENT_URL = f"{_BASE_URL}/v1/statement"
_QUERY_ID = re.compile(r"^[A-Za-z0-9_]+$")
_HEADERS = {
    "X-Trino-User": "data_eng_lab_bi",
    "X-Trino-Source": "data-eng-lab-airflow",
    "X-Trino-Catalog": "lakehouse",
}


class TrinoProtocolError(RuntimeError):
    """The fixed query could not be completed within the protocol contract."""


class QueryResult(NamedTuple):
    query_id: str
    columns: tuple[tuple[str, str], ...]
    rows: tuple[tuple[Any, ...], ...]


def _default_hook_factory(*, method: str, http_conn_id: str):
    # Airflow is intentionally imported only while a task executes.
    from airflow.providers.http.hooks.http import HttpHook

    return HttpHook(method=method, http_conn_id=http_conn_id)


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise ValueError("non-finite JSON number")


def _json_depth(value: Any, depth: int = 0) -> int:
    if depth > MAX_JSON_DEPTH:
        raise TrinoProtocolError("Trino response exceeds JSON depth bound")
    if isinstance(value, Mapping):
        for child in value.values():
            _json_depth(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            _json_depth(child, depth + 1)
    return depth


def _validate_base_url(value: Any) -> None:
    if value != _BASE_URL:
        raise TrinoProtocolError("Trino connection origin does not match the internal service")
    parsed = urlparse(value)
    if (
        parsed.scheme != "http"
        or parsed.hostname != "trino"
        or parsed.port != 8080
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ("", "/")
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise TrinoProtocolError("Trino connection origin does not match the internal service")


def _validate_next_uri(value: Any) -> str:
    if not isinstance(value, str):
        raise TrinoProtocolError("Trino next page URI is invalid")
    parsed = urlparse(value)
    if (
        parsed.scheme != "http"
        or parsed.hostname != "trino"
        or parsed.port != 8080
        or parsed.username is not None
        or parsed.password is not None
        or not parsed.path.startswith("/v1/statement/")
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise TrinoProtocolError("Trino next page URI leaves the reviewed origin or path")
    return value


class TrinoHttpClient:
    """Execute only reviewed registry queries through a bounded Trino protocol."""

    def __init__(
        self,
        *,
        hook_factory: Callable[..., Any] = _default_hook_factory,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._hook_factory = hook_factory
        self._monotonic = monotonic
        self.max_total_bytes = MAX_TOTAL_BYTES

    def execute(self, name: QueryName) -> QueryResult:
        try:
            spec = QUERIES[name]
        except (KeyError, TypeError) as error:
            raise TrinoProtocolError("query is absent from the reviewed registry") from error

        hook = self._hook_factory(method="POST", http_conn_id="trino_default")
        headers = dict(_HEADERS)
        if spec.schema is not None:
            headers["X-Trino-Schema"] = spec.schema
        session = hook.get_conn(headers=headers)
        try:
            _validate_base_url(hook.base_url)
            return self._execute(session, spec)
        except TrinoProtocolError:
            # _execute owns cancellation because it has the current valid URI.
            raise
        except Exception as error:
            raise TrinoProtocolError(f"{spec.name.value}: Trino transport failed") from error
        finally:
            session.close()

    def _execute(self, session: Any, spec: QuerySpec) -> QueryResult:
        started = self._monotonic()
        request_url = _STATEMENT_URL
        method = "POST"
        seen: set[str] = set()
        cancel_uri: str | None = None
        total_bytes = 0
        page_count = 0
        request_count = 0
        query_id: str | None = None
        columns: tuple[tuple[str, str], ...] | None = None
        rows: list[tuple[Any, ...]] = []

        try:
            while True:
                if page_count >= MAX_PAGES:
                    raise TrinoProtocolError("Trino response exceeds page bound")
                if request_count >= MAX_REQUESTS:
                    raise TrinoProtocolError("Trino response exceeds request bound")
                if method == "GET":
                    if request_url in seen:
                        raise TrinoProtocolError("Trino returned a repeated next page URI")
                    seen.add(request_url)

                response = None
                try:
                    kwargs = {
                        "allow_redirects": False,
                        "stream": True,
                        "timeout": REQUEST_TIMEOUT_SECONDS,
                    }
                    if method == "POST":
                        response = session.post(request_url, data=spec.sql.encode("utf-8"), **kwargs)
                    else:
                        response = session.get(request_url, **kwargs)
                    request_count += 1
                    if response.status_code != 200:
                        raise TrinoProtocolError(
                            f"{spec.name.value}: Trino returned HTTP status {response.status_code}"
                        )
                    payload = bytearray()
                    for chunk in response.iter_content(chunk_size=16 * 1024):
                        if not chunk:
                            continue
                        if len(payload) + len(chunk) > MAX_RESPONSE_BYTES:
                            raise TrinoProtocolError("Trino response exceeds response byte bound")
                        if total_bytes + len(chunk) > self.max_total_bytes:
                            raise TrinoProtocolError("Trino response exceeds total byte bound")
                        payload.extend(chunk)
                        total_bytes += len(chunk)
                finally:
                    if response is not None:
                        response.close()

                page_count += 1
                document = self._decode_document(bytes(payload))
                current_query_id = document.get("id")
                if not isinstance(current_query_id, str) or not _QUERY_ID.fullmatch(current_query_id):
                    raise TrinoProtocolError("Trino response has an invalid query ID")
                if query_id is None:
                    query_id = current_query_id
                elif current_query_id != query_id:
                    raise TrinoProtocolError("Trino query ID changed between pages")
                if "error" in document:
                    raise TrinoProtocolError(f"{spec.name.value}: Trino query failed")

                page_columns = self._parse_columns(document.get("columns"), spec, optional=True)
                if page_columns is not None:
                    if columns is not None and page_columns != columns:
                        raise TrinoProtocolError("Trino columns changed between pages")
                    columns = page_columns
                page_rows = self._parse_rows(document.get("data"), columns, spec)
                rows.extend(page_rows)
                if len(rows) > spec.max_rows:
                    raise TrinoProtocolError("Trino result exceeds row bound")

                stats = document.get("stats")
                state = stats.get("state") if isinstance(stats, Mapping) else None
                if not isinstance(state, str):
                    raise TrinoProtocolError("Trino response has invalid query state")

                raw_next = document.get("nextUri")
                if raw_next is None:
                    if state != "FINISHED":
                        raise TrinoProtocolError("Trino response did not reach terminal FINISHED state")
                    if columns is None:
                        raise TrinoProtocolError("Trino response is missing columns")
                    cancel_uri = None
                    return QueryResult(query_id, columns, tuple(rows))

                next_uri = _validate_next_uri(raw_next)
                if next_uri in seen:
                    raise TrinoProtocolError("Trino returned a repeated next page URI")
                cancel_uri = next_uri
                if self._monotonic() - started > QUERY_DEADLINE_SECONDS:
                    raise TrinoProtocolError("Trino query exceeded deadline")
                request_url = next_uri
                method = "GET"
        except Exception:
            if cancel_uri is not None:
                self._cancel(session, cancel_uri)
            raise

    @staticmethod
    def _decode_document(payload: bytes) -> dict[str, Any]:
        try:
            value = json.loads(
                payload.decode("utf-8"),
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise TrinoProtocolError("Trino returned malformed JSON") from error
        _json_depth(value)
        if not isinstance(value, dict):
            raise TrinoProtocolError("Trino response must be a JSON object")
        return value

    @staticmethod
    def _parse_columns(
        value: Any, spec: QuerySpec, *, optional: bool
    ) -> tuple[tuple[str, str], ...] | None:
        if value is None and optional:
            return None
        if not isinstance(value, list):
            raise TrinoProtocolError("Trino response columns have invalid shape")
        if len(value) > MAX_COLUMNS:
            raise TrinoProtocolError("Trino result exceeds column bound")
        parsed: list[tuple[str, str]] = []
        for item in value:
            if not isinstance(item, Mapping):
                raise TrinoProtocolError("Trino response columns have invalid shape")
            name = item.get("name")
            data_type = item.get("type")
            if not isinstance(name, str) or not isinstance(data_type, str):
                raise TrinoProtocolError("Trino response columns have invalid shape")
            parsed.append((name, data_type))
        result = tuple(parsed)
        if result != spec.columns:
            raise TrinoProtocolError("Trino response columns do not match the reviewed query")
        return result

    @staticmethod
    def _parse_rows(
        value: Any, columns: tuple[tuple[str, str], ...] | None, spec: QuerySpec
    ) -> tuple[tuple[Any, ...], ...]:
        if value is None:
            return ()
        if not isinstance(value, list):
            raise TrinoProtocolError("Trino response data rows have invalid shape")
        if columns is None:
            raise TrinoProtocolError("Trino response data precedes columns")
        if len(value) > spec.max_rows:
            raise TrinoProtocolError("Trino result exceeds row bound")
        parsed: list[tuple[Any, ...]] = []
        for row in value:
            if not isinstance(row, list):
                raise TrinoProtocolError("Trino response data rows have invalid shape")
            if len(row) != len(columns):
                raise TrinoProtocolError("Trino response row width does not match columns")
            for cell in row:
                if isinstance(cell, float) and not math.isfinite(cell):
                    raise TrinoProtocolError("Trino response cell is not finite")
                try:
                    size = len(
                        json.dumps(cell, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                    )
                except (TypeError, ValueError) as error:
                    raise TrinoProtocolError("Trino response cell has invalid type") from error
                if size > MAX_CELL_BYTES:
                    raise TrinoProtocolError("Trino result exceeds cell bound")
            parsed.append(tuple(row))
        return tuple(parsed)

    @staticmethod
    def _cancel(session: Any, uri: str) -> None:
        response = None
        try:
            response = session.delete(
                uri,
                allow_redirects=False,
                stream=True,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except Exception:
            return
        finally:
            if response is not None:
                response.close()
