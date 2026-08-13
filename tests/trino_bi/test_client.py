from __future__ import annotations

import importlib
import json
import sys
from collections.abc import Iterable
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
AIRFLOW_DAGS = ROOT / "airflow-dags"
MODULE = AIRFLOW_DAGS / "trino_bi" / "client.py"


def _load_client():
    assert MODULE.is_file(), "bounded Trino HTTP client has not been implemented"
    sys.path.insert(0, str(AIRFLOW_DAGS))
    try:
        return importlib.import_module("trino_bi.client")
    finally:
        sys.path.remove(str(AIRFLOW_DAGS))


class FakeResponse:
    def __init__(self, document=None, *, status=200, chunks=None):
        encoded = json.dumps(document).encode() if chunks is None else None
        self._chunks = list(chunks if chunks is not None else [encoded])
        self.status_code = status
        self.closed = False

    def iter_content(self, chunk_size: int) -> Iterable[bytes]:
        assert chunk_size > 0
        yield from self._chunks

    def close(self) -> None:
        self.closed = True


class FakeSession:
    def __init__(self, responses, *, delete_error=None):
        self.responses = list(responses)
        self.calls = []
        self.delete_error = delete_error
        self.closed = False

    def post(self, url, **kwargs):
        self.calls.append(("POST", url, kwargs))
        response = self.responses.pop(0)
        return response if isinstance(response, FakeResponse) else FakeResponse(response)

    def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        response = self.responses.pop(0)
        return response if isinstance(response, FakeResponse) else FakeResponse(response)

    def delete(self, url, **kwargs):
        self.calls.append(("DELETE", url, kwargs))
        if self.delete_error:
            raise self.delete_error
        return FakeResponse({}, status=204)

    def close(self):
        self.closed = True


class FakeHook:
    def __init__(self, session, *, base_url="http://trino:8080"):
        self.session = session
        self.base_url = base_url
        self.headers = None

    def get_conn(self, headers=None):
        self.headers = headers
        return self.session


def _factory(hook, calls):
    def make(*, method, http_conn_id):
        calls.append((method, http_conn_id))
        return hook

    return make


def _doc(*, query_id="20260812_000001_00001_x", next_uri=None, columns=None, data=None, state="FINISHED"):
    result = {"id": query_id, "stats": {"state": state}}
    if next_uri is not None:
        result["nextUri"] = next_uri
    if columns is not None:
        result["columns"] = [{"name": name, "type": data_type} for name, data_type in columns]
    if data is not None:
        result["data"] = data
    return result


def _run(responses, *, base_url="http://trino:8080", name="nyc_source_count", clock=None, delete_error=None):
    module = _load_client()
    session = FakeSession(responses, delete_error=delete_error)
    hook = FakeHook(session, base_url=base_url)
    factory_calls = []
    client = module.TrinoHttpClient(
        hook_factory=_factory(hook, factory_calls),
        monotonic=clock or (lambda: 0.0),
    )
    return module, client, session, hook, factory_calls, lambda: client.execute(module.QueryName(name))


def test_single_page_query_uses_exact_connection_headers_and_fixed_sql() -> None:
    module, _client, session, hook, factory_calls, execute = _run(
        [_doc(columns=(("source_count", "bigint"),), data=[[17]])]
    )
    result = execute()
    assert factory_calls == [("POST", "trino_default")]
    assert hook.headers == {
        "X-Trino-User": "data_eng_lab_bi",
        "X-Trino-Source": "data-eng-lab-airflow",
        "X-Trino-Catalog": "lakehouse",
        "X-Trino-Schema": "bronze",
    }
    method, url, kwargs = session.calls[0]
    assert (method, url) == ("POST", "http://trino:8080/v1/statement")
    assert kwargs["data"] == module.QUERIES[module.QueryName.NYC_SOURCE_COUNT].sql.encode("utf-8")
    assert kwargs["allow_redirects"] is False and kwargs["stream"] is True
    assert kwargs["timeout"] == module.REQUEST_TIMEOUT_SECONDS
    assert result.query_id == "20260812_000001_00001_x"
    assert result.columns == (("source_count", "bigint"),)
    assert result.rows == ((17,),)
    assert session.closed is True


def test_multipage_query_follows_same_origin_until_finished() -> None:
    next_one = "http://trino:8080/v1/statement/q/1"
    next_two = "http://trino:8080/v1/statement/q/2"
    responses = [
        _doc(next_uri=next_one, columns=(("source_count", "bigint"),), state="QUEUED"),
        _doc(next_uri=next_two, data=[[17]], state="RUNNING"),
        _doc(state="FINISHED"),
    ]
    _module, _client, session, _hook, _calls, execute = _run(responses)
    result = execute()
    assert result.rows == ((17,),)
    assert [(method, url) for method, url, _ in session.calls] == [
        ("POST", "http://trino:8080/v1/statement"),
        ("GET", next_one),
        ("GET", next_two),
    ]


@pytest.mark.parametrize(
    "base_url",
    [
        "https://trino:8080",
        "http://localhost:8080",
        "http://trino:8081",
        "http://user@trino:8080",
        "http://trino:8080/path",
        "http://trino:8080?token=secret",
    ],
)
def test_connection_rejects_every_noncanonical_origin(base_url: str) -> None:
    module, _client, session, _hook, _calls, execute = _run([], base_url=base_url)
    with pytest.raises(module.TrinoProtocolError, match="connection origin") as failure:
        execute()
    assert "secret" not in str(failure.value) and base_url not in str(failure.value)
    assert session.calls == []


@pytest.mark.parametrize(
    "next_uri",
    [
        "https://trino:8080/v1/statement/q/1",
        "http://other:8080/v1/statement/q/1",
        "http://trino:8081/v1/statement/q/1",
        "http://user@trino:8080/v1/statement/q/1",
        "http://trino:8080/v1/info",
        "http://trino:8080/v1/statement/q/1?token=secret",
        "http://trino:8080/v1/statement/q/1#fragment",
        "/v1/statement/q/1",
    ],
)
def test_pagination_rejects_origin_or_path_escape_and_cancels(next_uri: str) -> None:
    valid_cancel = "http://trino:8080/v1/statement/q/0"
    first = _doc(next_uri=valid_cancel, columns=(("source_count", "bigint"),), state="QUEUED")
    second = _doc(next_uri=next_uri, state="RUNNING")
    module, _client, session, _hook, _calls, execute = _run([first, second])
    with pytest.raises(module.TrinoProtocolError, match="next page") as failure:
        execute()
    assert next_uri not in str(failure.value) and "secret" not in str(failure.value)
    assert session.calls[-1][0:2] == ("DELETE", valid_cancel)
    assert session.closed is True


def test_repeated_next_uri_fails_without_looping_and_cancels() -> None:
    next_uri = "http://trino:8080/v1/statement/q/1"
    responses = [
        _doc(next_uri=next_uri, columns=(("source_count", "bigint"),), state="QUEUED"),
        _doc(next_uri=next_uri, state="RUNNING"),
    ]
    module, _client, session, _hook, _calls, execute = _run(responses)
    with pytest.raises(module.TrinoProtocolError, match="repeated next page"):
        execute()
    assert len([call for call in session.calls if call[0] == "GET"]) == 1
    assert session.calls[-1][0] == "DELETE"


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ([], "JSON object"),
        ({"stats": {"state": "FINISHED"}}, "query ID"),
        (_doc(query_id="bad query id"), "query ID"),
        ({**_doc(), "error": {"message": "secret SQL body"}}, "query failed"),
        (_doc(state="RUNNING"), "terminal FINISHED"),
        (_doc(columns=(("wrong", "bigint"),), data=[[1]]), "columns"),
        (_doc(columns=(("source_count", "varchar"),), data=[["1"]]), "columns"),
        (_doc(columns=(("source_count", "bigint"),), data=[[1, 2]]), "row width"),
        (_doc(columns=(("source_count", "bigint"),), data={"bad": "shape"}), "data rows"),
    ],
)
def test_protocol_document_and_result_shape_fail_closed(document, message: str) -> None:
    module, _client, session, _hook, _calls, execute = _run([document])
    with pytest.raises(module.TrinoProtocolError, match=message) as failure:
        execute()
    assert "secret SQL body" not in str(failure.value)
    assert session.closed is True


def test_query_id_cannot_change_between_pages() -> None:
    next_uri = "http://trino:8080/v1/statement/q/1"
    responses = [
        _doc(next_uri=next_uri, columns=(("source_count", "bigint"),), state="QUEUED"),
        _doc(query_id="20260812_000002_00001_x", data=[[1]]),
    ]
    module, _client, session, _hook, _calls, execute = _run(responses)
    with pytest.raises(module.TrinoProtocolError, match="query ID changed"):
        execute()
    assert session.calls[-1][0] == "DELETE"


def test_each_streamed_response_is_bounded_before_the_next_chunk() -> None:
    chunks = [b"{" + b"x" * 65_536 for _ in range(5)]
    response = FakeResponse(chunks=chunks)
    module, _client, session, _hook, _calls, execute = _run([response])
    with pytest.raises(module.TrinoProtocolError, match="response byte bound"):
        execute()
    assert response.closed is True and session.closed is True


def test_total_page_row_column_cell_depth_and_deadline_bounds_are_fail_closed() -> None:
    module = _load_client()
    next_uri = "http://trino:8080/v1/statement/q/1"

    too_many_rows = _doc(
        columns=(("source_count", "bigint"),),
        data=[[index] for index in range(module.QUERIES[module.QueryName.NYC_SOURCE_COUNT].max_rows + 1)],
    )
    _, _, _, _, _, execute_rows = _run([too_many_rows])
    with pytest.raises(module.TrinoProtocolError, match="row bound"):
        execute_rows()

    too_many_columns = _doc(columns=tuple((f"c{x}", "bigint") for x in range(module.MAX_COLUMNS + 1)))
    _, _, _, _, _, execute_columns = _run([too_many_columns])
    with pytest.raises(module.TrinoProtocolError, match="column bound"):
        execute_columns()

    huge_cell = _doc(columns=(("source_count", "bigint"),), data=[["x" * (module.MAX_CELL_BYTES + 1)]])
    _, _, _, _, _, execute_cell = _run([huge_cell])
    with pytest.raises(module.TrinoProtocolError, match="cell bound"):
        execute_cell()

    deep = 0
    for _ in range(module.MAX_JSON_DEPTH + 1):
        deep = [deep]
    deep_doc = {**_doc(columns=(("source_count", "bigint"),)), "unused": deep}
    _, _, _, _, _, execute_depth = _run([deep_doc])
    with pytest.raises(module.TrinoProtocolError, match="JSON depth"):
        execute_depth()

    responses = [
        _doc(next_uri=next_uri, columns=(("source_count", "bigint"),), state="QUEUED"),
        _doc(data=[[1]], state="FINISHED"),
    ]
    clock_values = iter([0.0, module.QUERY_DEADLINE_SECONDS + 1])
    _, _, deadline_session, _, _, execute_deadline = _run(responses, clock=lambda: next(clock_values))
    with pytest.raises(module.TrinoProtocolError, match="deadline"):
        execute_deadline()
    assert deadline_session.calls[-1][0] == "DELETE"


def test_page_and_total_byte_bounds_cannot_allocate_or_loop_unboundedly() -> None:
    module = _load_client()
    uris = [f"http://trino:8080/v1/statement/q/{index}" for index in range(module.MAX_PAGES + 1)]
    docs = [
        _doc(
            next_uri=uri,
            columns=(("source_count", "bigint"),) if index == 0 else None,
            state="RUNNING",
        )
        for index, uri in enumerate(uris)
    ]
    _, _, session, _, _, execute = _run(docs)
    with pytest.raises(module.TrinoProtocolError, match="page bound"):
        execute()
    assert len(session.calls) <= module.MAX_REQUESTS + 1
    assert session.calls[-1][0] == "DELETE"

    chunk = b" " * 1024
    responses = [FakeResponse(chunks=[chunk]), FakeResponse(chunks=[chunk])]
    # First response is invalid JSON, so exercise the aggregate bound through a smaller patched limit.
    _, client, _, _, _, _ = _run(responses)
    client.max_total_bytes = len(chunk) - 1
    with pytest.raises(module.TrinoProtocolError, match="total byte bound"):
        client.execute(module.QueryName.NYC_SOURCE_COUNT)


def test_http_status_redirect_malformed_json_and_transport_errors_are_redacted() -> None:
    module, _client, session, _hook, _calls, execute = _run([FakeResponse({}, status=302)])
    with pytest.raises(module.TrinoProtocolError, match="HTTP status 302"):
        execute()
    assert session.closed

    malformed = FakeResponse(chunks=[b'{"secret":"token"'])
    module, _client, session, _hook, _calls, execute = _run([malformed])
    with pytest.raises(module.TrinoProtocolError, match="malformed JSON") as failure:
        execute()
    assert "secret" not in str(failure.value) and "token" not in str(failure.value)


def test_cancellation_cleanup_failure_preserves_primary_protocol_error() -> None:
    next_uri = "http://trino:8080/v1/statement/q/1"
    responses = [
        _doc(next_uri=next_uri, columns=(("source_count", "bigint"),), state="QUEUED"),
        _doc(query_id="changed", state="FINISHED"),
    ]
    module, _client, session, _hook, _calls, execute = _run(
        responses, delete_error=RuntimeError("cleanup secret")
    )
    with pytest.raises(module.TrinoProtocolError, match="query ID changed") as failure:
        execute()
    assert "cleanup secret" not in str(failure.value)
    assert session.closed is True
