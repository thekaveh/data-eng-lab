"""Tests for safe GitHub wiki synchronization."""

import subprocess
from pathlib import Path

import pytest

from scripts.docs.push_wiki import _is_ssh_remote, push_wiki, sync_wiki


def test_sync_wiki_preserves_git_and_removes_stale_files(tmp_path: Path):
    source = tmp_path / "source"
    clone = tmp_path / "clone"
    source.mkdir()
    (source / "Home.md").write_text("home", encoding="utf-8")
    (clone / ".git").mkdir(parents=True)
    (clone / "stale.md").write_text("stale", encoding="utf-8")

    sync_wiki(source, clone)

    assert (clone / ".git").is_dir()
    assert not (clone / "stale.md").exists()
    assert (clone / "Home.md").read_text(encoding="utf-8") == "home"


class FakeGit:
    def __init__(self, *, unchanged: bool = False):
        self.calls: list[tuple[list[str], Path | None, dict[str, str]]] = []
        self.unchanged = unchanged

    def __call__(self, command, *, cwd=None, env=None, check=None, **_kwargs):
        args = list(command)
        self.calls.append((args, cwd, dict(env or {})))
        if args[:2] == ["git", "clone"]:
            clone = Path(args[-1])
            (clone / ".git").mkdir(parents=True)
        return_code = 0 if self.unchanged and args[:4] == ["git", "diff", "--cached", "--quiet"] else 1
        if args[:4] != ["git", "diff", "--cached", "--quiet"]:
            return_code = 0
        if check and return_code:
            raise subprocess.CalledProcessError(return_code, args)
        return subprocess.CompletedProcess(args, return_code)

    @property
    def push_call(self):
        return next(call for call in self.calls if call[0][:2] == ["git", "push"])


@pytest.fixture
def source(tmp_path: Path) -> Path:
    path = tmp_path / "source"
    path.mkdir()
    (path / "Home.md").write_text("home\n", encoding="utf-8")
    return path


def test_push_command_targets_master_and_supplies_default_identity(
    monkeypatch: pytest.MonkeyPatch, source: Path, tmp_path: Path
):
    fake_git = FakeGit()
    monkeypatch.setattr(subprocess, "run", fake_git)

    push_wiki(source, "git@github.com:thekaveh/data-eng-lab.wiki.git", tmp_path / "key", push=True)

    command, _, env = fake_git.push_call
    assert command[-1] == "HEAD:master"
    assert env["GIT_AUTHOR_NAME"] == "data-eng-lab docs bot"
    assert env["GIT_AUTHOR_EMAIL"] == "docs-bot@users.noreply.github.com"
    assert env["GIT_COMMITTER_NAME"] == "data-eng-lab docs bot"
    assert env["GIT_COMMITTER_EMAIL"] == "docs-bot@users.noreply.github.com"
    assert env["GIT_SSH_COMMAND"].startswith("ssh -i ")


def test_https_remote_does_not_require_an_ssh_key(
    monkeypatch: pytest.MonkeyPatch, source: Path
):
    fake_git = FakeGit()
    monkeypatch.setattr(subprocess, "run", fake_git)

    push_wiki(
        source,
        "https://x-access-token:token@github.com/thekaveh/data-eng-lab.wiki.git",
        None,
        push=True,
    )

    command, _, env = fake_git.push_call
    assert command[-1] == "HEAD:master"
    assert "GIT_SSH_COMMAND" not in env


def test_https_remote_removes_inherited_ssh_command(
    monkeypatch: pytest.MonkeyPatch, source: Path
):
    fake_git = FakeGit()
    monkeypatch.setenv("GIT_SSH_COMMAND", "ssh inherited")
    monkeypatch.setattr(subprocess, "run", fake_git)

    push_wiki(
        source,
        "https://x-access-token:token@github.com/thekaveh/data-eng-lab.wiki.git",
        None,
        push=True,
    )

    assert "GIT_SSH_COMMAND" not in fake_git.push_call[2]


def test_scp_style_ssh_remote_uses_supplied_key(
    monkeypatch: pytest.MonkeyPatch, source: Path, tmp_path: Path
):
    fake_git = FakeGit()
    monkeypatch.setattr(subprocess, "run", fake_git)

    push_wiki(
        source,
        "deploy@github.com:thekaveh/data-eng-lab.wiki.git",
        tmp_path / "deploy key",
        push=True,
    )

    assert fake_git.push_call[2]["GIT_SSH_COMMAND"].startswith("ssh -i ")


def test_scp_style_ssh_remote_without_user_uses_supplied_key(
    monkeypatch: pytest.MonkeyPatch, source: Path, tmp_path: Path
):
    fake_git = FakeGit()
    monkeypatch.setattr(subprocess, "run", fake_git)

    push_wiki(
        source,
        "github.com:thekaveh/data-eng-lab.wiki.git",
        tmp_path / "deploy-key",
        push=True,
    )

    assert fake_git.push_call[2]["GIT_SSH_COMMAND"].startswith("ssh -i ")


@pytest.mark.parametrize(
    "remote",
    [
        "https://github.com/thekaveh/data-eng-lab.wiki.git",
        "file:///tmp/data-eng-lab.wiki.git",
        "C:/repos/data-eng-lab.wiki.git",
        r"C:\repos\data-eng-lab.wiki.git",
        r"C:repos\data-eng-lab.wiki.git",
        r"c:repos\data-eng-lab.wiki.git",
        "/tmp/data-eng-lab.wiki.git",
    ],
)
def test_non_scp_remote_forms_are_not_classified_as_ssh(remote: str):
    assert not _is_ssh_remote(remote)


def test_noop_index_skips_commit_and_push(monkeypatch: pytest.MonkeyPatch, source: Path):
    fake_git = FakeGit(unchanged=True)
    monkeypatch.setattr(subprocess, "run", fake_git)

    push_wiki(source, "git@github.com:thekaveh/data-eng-lab.wiki.git", None, push=True)

    commands = [call[0][1] for call in fake_git.calls]
    assert "commit" not in commands
    assert "push" not in commands


def test_check_mode_validates_without_running_git(
    monkeypatch: pytest.MonkeyPatch, source: Path
):
    def fail(*_args, **_kwargs):
        raise AssertionError("check mode must not invoke git")

    monkeypatch.setattr(subprocess, "run", fail)
    push_wiki(source, "", None, push=False)


def test_empty_wiki_source_is_rejected(tmp_path: Path):
    source = tmp_path / "empty"
    source.mkdir()

    with pytest.raises(ValueError, match="generated wiki is empty"):
        push_wiki(source, "", None, push=False)


def test_local_remote_receives_master_commit_and_second_push_is_noop(
    source: Path, tmp_path: Path
):
    remote = tmp_path / "wiki.git"
    subprocess.run(
        ["git", "init", "--bare", "--initial-branch=master", str(remote)],
        check=True,
        capture_output=True,
    )

    push_wiki(source, str(remote), None, push=True)
    first = subprocess.run(
        ["git", "--git-dir", str(remote), "rev-parse", "refs/heads/master"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    content = subprocess.run(
        ["git", "--git-dir", str(remote), "show", "master:Home.md"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout

    push_wiki(source, str(remote), None, push=True)
    second = subprocess.run(
        ["git", "--git-dir", str(remote), "rev-parse", "refs/heads/master"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    assert content == "home\n"
    assert second == first
