from __future__ import annotations

import io
import json

import pytest

from scripts.checkpoints import retention


class Response:
    def __init__(self, body, status=200, close_error=None):
        self.body = io.BytesIO(body)
        self.status = status
        self.close_count = 0
        self.close_error = close_error

    def read(self, size):
        return self.body.read(size)

    def close(self):
        self.close_count += 1
        if self.close_error is not None:
            raise self.close_error


def test_cli_plan_uses_fixed_origin_token_header_and_canonical_output(monkeypatch, tmp_path, capsys):
    facts = tmp_path / "facts.json"
    facts.write_text('{"actor":"acceptance-engineering"}', encoding="utf-8")
    output = tmp_path / "plan.json"
    captured = {}
    response = Response(b'{"state":"accepted","plan_sha256":"' + b"a" * 64 + b'"}')

    def urlopen(request, timeout):
        captured.update(url=request.full_url, headers=dict(request.header_items()), body=request.data, timeout=timeout)
        return response

    monkeypatch.setenv("CHECKPOINT_RETENTION_API_TOKEN", "api-secret-token")
    monkeypatch.setenv("CHECKPOINT_RETENTION_URI", "http://checkpoint-retention:8080")
    monkeypatch.setattr(retention, "_open", urlopen)

    code = retention.main(
        [
            "plan",
            "--checkpoint-id",
            "go-live-streaming-test-v1",
            "--prefix",
            "streaming_test/550e8400-e29b-41d4-a716-446655440000/",
            "--facts",
            str(facts),
            "--output",
            str(output),
        ]
    )

    assert code == 0
    assert captured["url"] == "http://checkpoint-retention:8080/v1/plans"
    assert captured["timeout"] == 30
    assert captured["headers"]["Authorization"] == "Bearer api-secret-token"
    assert b"api-secret-token" not in captured["body"]
    assert output.read_bytes() == b'{"plan_sha256":"' + b"a" * 64 + b'","state":"accepted"}'
    assert capsys.readouterr().out == output.read_text() + "\n"
    assert response.close_count == 1


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"state": "not_ready"}, 3),
        ({"state": "refused"}, 3),
        ({"state": "partial"}, 4),
    ],
)
def test_cli_stable_state_exit_codes(monkeypatch, capsys, payload, expected):
    monkeypatch.setenv("CHECKPOINT_RETENTION_API_TOKEN", "api-token")
    monkeypatch.setenv("CHECKPOINT_RETENTION_URI", "http://checkpoint-retention:8080")
    monkeypatch.setattr(
        retention,
        "_open",
        lambda *_args, **_kwargs: Response(json.dumps(payload, separators=(",", ":")).encode()),
    )
    code = retention.main(["status", "--operation-id", "550e8400-e29b-41d4-a716-446655440000"])
    assert code == expected
    assert capsys.readouterr().err == ""


def test_cli_rejects_origin_override_invalid_env_and_never_prints_token(monkeypatch, capsys):
    monkeypatch.setenv("CHECKPOINT_RETENTION_API_TOKEN", "never-print-this-token")
    monkeypatch.setenv("CHECKPOINT_RETENTION_URI", "http://attacker.invalid:8080")

    code = retention.main(["status", "--operation-id", "550e8400-e29b-41d4-a716-446655440000"])

    captured = capsys.readouterr()
    assert code == 2
    assert "never-print-this-token" not in captured.err
    assert "attacker.invalid" not in captured.err
    with pytest.raises(SystemExit):
        retention.main(["--url", "http://attacker.invalid"])


@pytest.mark.parametrize("close_error", [RuntimeError("secret cleanup"), KeyboardInterrupt()])
def test_cli_response_close_failure_is_sanitized_and_control_flow_preserved(monkeypatch, close_error, capsys):
    monkeypatch.setenv("CHECKPOINT_RETENTION_API_TOKEN", "api-token")
    monkeypatch.setenv("CHECKPOINT_RETENTION_URI", "http://checkpoint-retention:8080")
    monkeypatch.setattr(
        retention, "_open", lambda *_args, **_kwargs: Response(b'{"state":"accepted"}', close_error=close_error)
    )

    arguments = ["status", "--operation-id", "550e8400-e29b-41d4-a716-446655440000"]
    if isinstance(close_error, KeyboardInterrupt):
        with pytest.raises(KeyboardInterrupt) as failure:
            retention.main(arguments)
        assert failure.value is close_error
    else:
        assert retention.main(arguments) == 5
        assert "secret cleanup" not in capsys.readouterr().err


def test_cli_rejects_duplicate_or_oversized_response_before_output(monkeypatch, capsys):
    monkeypatch.setenv("CHECKPOINT_RETENTION_API_TOKEN", "api-token")
    monkeypatch.setenv("CHECKPOINT_RETENTION_URI", "http://checkpoint-retention:8080")
    arguments = ["status", "--operation-id", "550e8400-e29b-41d4-a716-446655440000"]
    for body in (b'{"state":"accepted","state":"partial"}', b"x" * 65_537):
        monkeypatch.setattr(retention, "_open", lambda *_args, _body=body, **_kwargs: Response(_body))
        assert retention.main(arguments) == 5
    assert "accepted" not in capsys.readouterr().out
