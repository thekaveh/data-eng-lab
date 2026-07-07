"""Auto-extract a per-scenario notebook doc from the .ipynb + .zpln."""
from __future__ import annotations

import json
import re
from pathlib import Path

import nbformat

NOTEBOOK_SECTIONS = ["1. Overview", "2. Setup", "3. Read", "4. Transform", "5. Write", "6. Verify"]
_H = re.compile(r"^##\s+(\d+\.\s+.*)$")
# A bare numbered title like "1. Overview" (Zeppelin paragraph title field).
_TITLE_NUM = re.compile(r"^(\d+\.\s+.+)$")
# Trailing annotation like ' (code)' on Zeppelin paragraph titles.
_TITLE_SUFFIX = re.compile(r"\s*\([^()]*\)\s*$")
# A leading section-header line '## N. Section' inside notebook markdown.
_SECTION_HEADER_LINE = re.compile(r"^#{1,6}\s+\d+\.\s+")
# Leading notebook section number, e.g. '2. ' in '2. Setup'.
_NUM_PREFIX = re.compile(r"^\d+\.\s+")


def _normalize_title(title: str) -> str:
    """Strip a trailing annotation: '2. Setup (code)' -> '2. Setup'."""
    return _TITLE_SUFFIX.sub("", title.strip())


def _strip_section_headers(md: str) -> str:
    """Drop leading '## N. Section' header lines from notebook markdown so the
    generated '### N. Section' heading isn't immediately followed by its source twin."""
    lines = md.splitlines()
    while lines and _SECTION_HEADER_LINE.match(lines[0]):
        lines.pop(0)
    return "\n".join(lines).strip()


def _section_name(sec: str) -> str:
    """'2. Setup' -> 'Setup' (strip the notebook's leading section number)."""
    return _NUM_PREFIX.sub("", sec)


def _ipython_cells(path: Path) -> list[tuple[str, str, str]]:
    """Return [(section_title, cell_type, source_text)] grouped under ## N. headers."""
    nb = nbformat.read(path, as_version=4)
    out: list[tuple[str, str, str]] = []
    cur = "0. Preamble"
    for c in nb.cells:
        if c.cell_type == "markdown":
            src = "".join(c.source) if isinstance(c.source, list) else c.source
            m = _H.match(src.strip())
            if m:
                cur = m.group(1).strip()
            out.append((cur, "markdown", src))
        elif c.cell_type == "code":
            src = "".join(c.source) if isinstance(c.source, list) else c.source
            out.append((cur, "code", src))
    return out


def _zeppelin_cells(path: Path) -> list[tuple[str, str, str]]:
    """Return [(section_title, lang, text)] for each Zeppelin paragraph.

    Section is detected from the paragraph ``title`` (a bare numbered heading
    like "1. Overview", after stripping a trailing ' (code)' annotation) — the
    text body begins with a Zeppelin interpreter directive (``%md``/``%spark``)
    that would mask any ``##`` header from a start-anchored regex.
    """
    doc = json.loads(path.read_text(encoding="utf-8"))
    out: list[tuple[str, str, str]] = []
    cur = "0. Preamble"
    for p in doc.get("paragraphs", []):
        text = p.get("text", "")
        title = p.get("title", "")
        title_base = _normalize_title(title) if title else ""
        tm = _TITLE_NUM.match(title_base) if title_base else None
        if tm:
            cur = tm.group(1).strip()
        else:
            m = _H.match(_strip_interp(text))
            if m:
                cur = m.group(1).strip()
        lang = "markdown" if text.lstrip().startswith("%md") else "code"
        out.append((cur, lang, text))
    return out


def _strip_interp(text: str) -> str:
    # drop zeppelin %md / %spark / %trino leading directives
    return re.sub(r"^(%\w+\s*)+", "", text).strip()


def extract_notebook_doc(scenario_name: str, jupyter_path: Path, zeppelin_path: Path) -> str:
    py = _ipython_cells(jupyter_path)
    sc = _zeppelin_cells(zeppelin_path)
    sections_seen: dict[str, dict[str, str]] = {}
    for sec, lang, src in py:
        v = sections_seen.setdefault(sec, {})
        for k in ("py_md", "py_code", "sc_md", "sc_code"):
            v.setdefault(k, "")
        key = "py_md" if lang == "markdown" else "py_code"
        if src.strip():
            v[key] += src.rstrip() + "\n"
    for sec, lang, src in sc:
        v = sections_seen.setdefault(sec, {})
        for k in ("py_md", "py_code", "sc_md", "sc_code"):
            v.setdefault(k, "")
        clean = _strip_interp(src)
        key = "sc_md" if lang == "markdown" else "sc_code"
        if clean:
            v[key] += clean + "\n"

    lines = [f"# Notebooks — {scenario_name}\n",
             "Auto-extracted from `jupyter/notebook.ipynb` and `zeppelin/notebook.zpln`.\n",
             "Both notebooks implement identical logic in PySpark and Scala.\n\n",
             "## 1. Section map\n\n",
             "| Subsection | Scala (Zeppelin) | PySpark (Jupyter) |\n|---|---|---|\n"]
    visible: list[tuple[str, str, str, str, str]] = []
    for sec in NOTEBOOK_SECTIONS:
        v = sections_seen.get(sec, {})
        sc_code = v.get("sc_code", "").strip()
        py_code = v.get("py_code", "").strip()
        sc_md = _strip_section_headers(v.get("sc_md", "")).strip()
        py_md = _strip_section_headers(v.get("py_md", "")).strip()
        if sc_code or py_code or sc_md or py_md:
            visible.append((sec, sc_code, py_code, sc_md, py_md))
    for i, (sec, sc_code, py_code, _sc_md, _py_md) in enumerate(visible, 1):
        lines.append(f"| 2.{i} {_section_name(sec)} | {'✓' if sc_code else '—'} | {'✓' if py_code else '—'} |\n")
    lines.append("\n## 2. Walkthrough\n\n")
    for i, (sec, sc_code, py_code, sc_md, py_md) in enumerate(visible, 1):
        lines.append(f"### 2.{i} {_section_name(sec)}\n\n")
        if sc_code:
            lines.append("**Scala (Zeppelin):**\n\n```scala\n" + sc_code + "\n```\n\n")
        if py_code:
            lines.append("**PySpark (Jupyter):**\n\n```python\n" + py_code + "\n```\n\n")
        prose = sc_md or py_md
        if prose:
            lines.append(prose + "\n\n")
    lines.append("## 3. Scala / PySpark parity\n\n")
    lines.append("Both notebooks share the same numbered sections and produce identical Iceberg tables; "
                 "only the language and interpreter differ.\n\n")
    lines.append("## 4. How to run\n\n")
    lines.append("Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or "
                 "`jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.\n")
    return "".join(lines)
