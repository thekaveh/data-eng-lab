from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = [
    "pyproject.toml",
    ".python-version",
    ".gitignore",
    "LICENSE",
    "README.md",
    "CONTRIBUTING.md",
    "CHANGELOG.md",
    "docs/atlas-enablement.md",
    "docs/superpowers/specs/2026-07-02-data-eng-lab-design.md",
]


def test_required_top_level_files_exist():
    missing = [f for f in REQUIRED_FILES if not (ROOT / f).exists()]
    assert not missing, f"missing required files: {missing}"


def test_python_version_is_311():
    assert (ROOT / ".python-version").read_text().strip() == "3.11"
