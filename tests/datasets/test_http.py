import io
import zipfile
from pathlib import Path

import responses

# Package imports (NOT importlib-by-path): registry.py defines frozen dataclasses, which
# require the module to be registered in sys.modules — a by-path load breaks them.
from datasets import registry as reg
from datasets.sources import http


def _plan(urls, unzip=False):
    artifacts = {
        f"source_{index}": reg.HttpArtifact(
            id=f"source_{index}",
            url=url,
            version=reg.SourceVersion(kind="revision", value=f"source-{index}"),
            stability="mutable",
            evidence={},
            raw=reg.RawArtifact(name=Path(url).name, size_bytes=1, sha256="a" * 64),
            outputs=(),
        )
        for index, url in enumerate(urls)
    }
    ds = reg.Dataset(
        "d",
        "d",
        "csv",
        "l",
        "d",
        "http",
        unzip,
        {"tiny": tuple(artifacts)},
        artifacts=artifacts,
    )
    return reg.resolve_scale(ds, "tiny")


@responses.activate
def test_fetch_downloads_each_url(tmp_path: Path):
    responses.add(responses.GET, "https://x/a.parquet", body=b"AAA", status=200)
    responses.add(responses.GET, "https://x/b.parquet", body=b"BBB", status=200)
    files = http.fetch_http(_plan(["https://x/a.parquet", "https://x/b.parquet"]), tmp_path)
    assert sorted(f.name for f in files) == ["a.parquet", "b.parquet"]
    assert (tmp_path / "a.parquet").read_bytes() == b"AAA"


@responses.activate
def test_fetch_unzips_when_flagged(tmp_path: Path):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("ml/ratings.csv", "u,m,r\n1,2,3\n")
    responses.add(responses.GET, "https://x/ml.zip", body=buf.getvalue(), status=200)
    files = http.fetch_http(_plan(["https://x/ml.zip"], unzip=True), tmp_path)
    names = [f.name for f in files]
    assert "ratings.csv" in names
    assert not any(f.suffix == ".zip" for f in files)
