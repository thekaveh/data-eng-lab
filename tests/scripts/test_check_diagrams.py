"""Tests for scripts/check_diagrams.py — landscape + script-free SVG assertions."""
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "check_diagrams", ROOT / "scripts" / "check_diagrams.py"
)


def _load():
    m = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(m)
    return m


def _svg(w, h):
    return (
        f'<?xml version="1.0"?>'
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}"></svg>'
    )


def _setup(tmp_path, entries):
    """Create docs/architectures/*.svg + a minimal manifest mirroring `entries`."""
    arch = tmp_path / "docs" / "architectures"
    arch.mkdir(parents=True)
    for name, body in entries.items():
        (arch / name).write_text(body)
    sp = tmp_path / "scripts" / "diagrams_manifest.yaml"
    sp.parent.mkdir(parents=True)
    # Manifest is declarative input for generation (Task 11); the checker scans
    # docs/architectures/ directly, but we write it so the dir layout is realistic.
    lines = ["orientation: landscape", "diagrams:"]
    lines += [f"  - {{output: {name}}}" for name in entries]
    sp.write_text("\n".join(lines) + "\n")


def test_landscape_passes(tmp_path):
    cs = _load()
    _setup(tmp_path, {"a.svg": _svg(800, 400)})
    assert cs.main(["--root", str(tmp_path)]) == 0


def test_portrait_or_script_fails(tmp_path):
    cs = _load()
    _setup(
        tmp_path,
        {
            "a.svg": _svg(400, 800),                              # portrait
            "b.svg": _svg(800, 400) + "<script>x</script>",       # script
        },
    )
    assert cs.main(["--root", str(tmp_path)]) == 1


def test_empty_dir_passes(tmp_path):
    """No SVGs yet (the post-Task-10, pre-Task-11 state) must not error."""
    cs = _load()
    arch = tmp_path / "docs" / "architectures"
    arch.mkdir(parents=True)
    sp = tmp_path / "scripts" / "diagrams_manifest.yaml"
    sp.parent.mkdir(parents=True)
    sp.write_text("orientation: landscape\ndiagrams: []\n")
    assert cs.main(["--root", str(tmp_path)]) == 0


def test_missing_dir_fails(tmp_path):
    """No docs/architectures/ at all is a configuration error."""
    cs = _load()
    assert cs.main(["--root", str(tmp_path)]) == 1
