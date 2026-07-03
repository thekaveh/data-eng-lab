"""HTTP source fetcher: download raw dataset files (with optional unzip) to a local dir."""
from __future__ import annotations

import zipfile
from pathlib import Path
from urllib.parse import urlparse

import requests


def _download(url: str, dest: Path) -> Path:
    name = Path(urlparse(url).path).name
    out = dest / name
    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with open(out, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    return out


def _unzip(path: Path, dest: Path) -> list[Path]:
    extracted: list[Path] = []
    with zipfile.ZipFile(path) as z:
        for member in z.namelist():
            if member.endswith("/"):
                continue
            target = dest / Path(member).name  # flatten
            with z.open(member) as src, open(target, "wb") as out:
                out.write(src.read())
            extracted.append(target)
    path.unlink()  # drop the zip once extracted
    return extracted


def fetch_http(plan, dest: Path) -> list[Path]:
    dest.mkdir(parents=True, exist_ok=True)
    files: list[Path] = []
    for url in plan.urls:
        downloaded = _download(url, dest)
        if plan.dataset.unzip and downloaded.suffix == ".zip":
            files.extend(_unzip(downloaded, dest))
        else:
            files.append(downloaded)
    return files
