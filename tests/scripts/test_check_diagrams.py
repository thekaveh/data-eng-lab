"""Tests for canonical HTML-master and PNG diagram validation."""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "check_diagrams", ROOT / "scripts" / "check_diagrams.py"
)


def _load():
    module = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(module)
    return module


def _master(*, width: int = 800, height: int = 400, svg_count: int = 1) -> str:
    svg = f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}"></svg>'
    return f"<html><body>{svg * svg_count}</body></html>"


def _png(*, width: int = 800, height: int = 400) -> bytes:
    return b"\x89PNG\r\n\x1a\n" + b"\x00\x00\x00\rIHDR" + width.to_bytes(4, "big") + height.to_bytes(4, "big")


def _setup(tmp_path: Path, entries: dict[str, tuple[str, bytes]], *, master_overrides=None) -> None:
    masters = tmp_path / "docs" / "diagrams"
    images = masters / "img"
    images.mkdir(parents=True)
    master_overrides = master_overrides or {}
    diagrams = []
    for identifier, (html, png) in entries.items():
        master = master_overrides.get(identifier, f"docs/diagrams/{identifier}.html")
        diagrams.append(f"  - {{id: {identifier}, master: {master}}}")
        (masters / f"{identifier}.html").write_text(html, encoding="utf-8")
        (images / f"{identifier}.png").write_bytes(png)
    manifest = (
        "surfaces: [repo, site, wiki]\n"
        "numbering: baked\n"
        "internal_roots: []\n"
        "sections: []\n"
        "diagrams:\n"
        + "\n".join(diagrams)
        + "\n"
    )
    (tmp_path / "docs" / "manifest.yaml").write_text(manifest, encoding="utf-8")


def test_canonical_master_and_png_pass(tmp_path):
    checker = _load()
    _setup(tmp_path, {"overview": (_master(), _png())})
    assert checker.main(["--root", str(tmp_path)]) == 0


def test_empty_manifest_cannot_make_the_gate_vacuous(tmp_path):
    checker = _load()
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "manifest.yaml").write_text(
        "surfaces: [repo, site, wiki]\n"
        "numbering: baked\n"
        "internal_roots: []\n"
        "sections: []\n"
        "diagrams: []\n",
        encoding="utf-8",
    )
    assert checker.main(["--root", str(tmp_path)]) == 1


def test_missing_master_or_png_fails(tmp_path):
    checker = _load()
    _setup(tmp_path, {"overview": (_master(), _png())})
    (tmp_path / "docs" / "diagrams" / "overview.html").unlink()
    assert checker.main(["--root", str(tmp_path)]) == 1

    (tmp_path / "docs" / "diagrams" / "overview.html").write_text(_master(), encoding="utf-8")
    (tmp_path / "docs" / "diagrams" / "img" / "overview.png").unlink()
    assert checker.main(["--root", str(tmp_path)]) == 1


def test_mismatched_manifest_master_and_asset_id_sets_fail(tmp_path):
    checker = _load()
    _setup(
        tmp_path,
        {"overview": (_master(), _png())},
        master_overrides={"overview": "docs/diagrams/wrong.html"},
    )
    (tmp_path / "docs" / "diagrams" / "extra.html").write_text(_master(), encoding="utf-8")
    (tmp_path / "docs" / "diagrams" / "img" / "extra.png").write_bytes(_png())
    assert checker.main(["--root", str(tmp_path)]) == 1


def test_non_landscape_or_multiple_svg_master_fails(tmp_path):
    checker = _load()
    _setup(
        tmp_path,
        {
            "portrait": (_master(width=400, height=800), _png()),
            "multiple": (_master(svg_count=2), _png()),
        },
    )
    assert checker.main(["--root", str(tmp_path)]) == 1


def test_invalid_or_zero_dimension_png_fails(tmp_path):
    checker = _load()
    _setup(
        tmp_path,
        {
            "invalid": (_master(), b"not png"),
            "zero": (_master(), _png(width=0, height=400)),
        },
    )
    assert checker.main(["--root", str(tmp_path)]) == 1


def test_current_repository_passes():
    checker = _load()
    assert checker.main(["--root", str(ROOT)]) == 0
