"""Safely synchronize the generated wiki and optionally push wiki ``master``."""

from __future__ import annotations

import argparse
import os
import re
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path

_BOT_NAME = "data-eng-lab docs bot"
_BOT_EMAIL = "docs-bot@users.noreply.github.com"
_DEFAULT_REMOTE = "git@github.com:thekaveh/data-eng-lab.wiki.git"


def sync_wiki(source: Path, clone: Path) -> None:
    """Replace a wiki clone's working tree while preserving its ``.git`` directory."""
    if not source.is_dir():
        raise FileNotFoundError(f"generated wiki directory missing: {source}")
    clone.mkdir(parents=True, exist_ok=True)
    for target in clone.iterdir():
        if target.name == ".git":
            continue
        if target.is_symlink() or target.is_file():
            target.unlink()
        else:
            shutil.rmtree(target)
    shutil.copytree(
        source,
        clone,
        dirs_exist_ok=True,
        symlinks=True,
        ignore=shutil.ignore_patterns(".git"),
    )


def push_wiki(source: Path, remote: str, key_path: Path | None, *, push: bool) -> None:
    """Validate *source* or commit and push its contents to ``HEAD:master``."""
    _validate_source(source)
    if not push:
        with tempfile.TemporaryDirectory() as directory:
            clone = Path(directory) / "wiki"
            (clone / ".git").mkdir(parents=True)
            sync_wiki(source, clone)
        return
    if not remote:
        raise ValueError("wiki remote is required when pushing")

    env = os.environ.copy()
    env.pop("GIT_SSH_COMMAND", None)
    env.update(
        {
            "GIT_AUTHOR_NAME": _BOT_NAME,
            "GIT_AUTHOR_EMAIL": _BOT_EMAIL,
            "GIT_COMMITTER_NAME": _BOT_NAME,
            "GIT_COMMITTER_EMAIL": _BOT_EMAIL,
        }
    )
    if _is_ssh_remote(remote) and key_path is not None:
        env["GIT_SSH_COMMAND"] = f"ssh -i {shlex.quote(str(key_path))} -o IdentitiesOnly=yes"

    with tempfile.TemporaryDirectory() as directory:
        clone = Path(directory) / "wiki"
        subprocess.run(["git", "clone", remote, str(clone)], env=env, check=True)
        sync_wiki(source, clone)
        subprocess.run(["git", "add", "--all"], cwd=clone, env=env, check=True)
        diff = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=clone,
            env=env,
            check=False,
        )
        if diff.returncode == 0:
            return
        if diff.returncode != 1:
            diff.check_returncode()
        subprocess.run(
            ["git", "commit", "-m", "docs: sync generated wiki"],
            cwd=clone,
            env=env,
            check=True,
        )
        subprocess.run(
            ["git", "push", "origin", "HEAD:master"],
            cwd=clone,
            env=env,
            check=True,
        )


def _validate_source(source: Path) -> None:
    if not source.is_dir():
        raise FileNotFoundError(f"generated wiki directory missing: {source}")
    if not any(path.is_file() for path in source.rglob("*")):
        raise ValueError(f"generated wiki is empty: {source}")


def _is_ssh_remote(remote: str) -> bool:
    if re.match(r"^[A-Za-z]:[\\/]", remote):
        return False
    uri = re.match(r"^(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*)://", remote)
    if uri is not None:
        return uri.group("scheme").lower() in {"ssh", "git+ssh"}
    return re.match(r"^(?:[^/@:\s]+@)?[^/\\:\s]+:[^\s]+$", remote) is not None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=Path.cwd(), type=Path)
    parser.add_argument("--remote", default=os.environ.get("WIKI_REMOTE", _DEFAULT_REMOTE))
    parser.add_argument(
        "--key-path",
        type=Path,
        default=Path(os.environ["WIKI_SSH_KEY"]) if os.environ.get("WIKI_SSH_KEY") else None,
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--push", action="store_true")
    args = parser.parse_args(argv)
    push_wiki(
        args.root.resolve() / "generated/wiki",
        args.remote,
        args.key_path,
        push=args.push,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
