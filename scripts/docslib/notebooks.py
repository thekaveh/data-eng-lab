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

    Section is detected from the paragraph ``title`` field when it is a bare
    numbered heading (e.g. "1. Overview"); otherwise we look for a ``## N.``
    header inside the stripped text. The title is preferred because the text
    body usually begins with a Zeppelin interpreter directive (``%md``,
    ``%spark``) that would mask any ``##`` header from a start-anchored regex.
    """
    doc = json.loads(path.read_text(encoding="utf-8"))
    out: list[tuple[str, str, str]] = []
    cur = "0. Preamble"
    for p in doc.get("paragraphs", []):
        text = p.get("text", "")
        title = p.get("title", "")
        tm = _TITLE_NUM.match(title.strip()) if title else None
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
        sections_seen.setdefault(sec, {"py_md": "", "py_code": ""})
        key = "py_md" if lang == "markdown" else "py_code"
        if src.strip():
            sections_seen[sec][key] += src.rstrip() + "\n"
    for sec, lang, src in sc:
        sections_seen.setdefault(sec, {"py_md": "", "py_code": "", "sc_md": "", "sc_code": ""})
        clean = _strip_interp(src)
        key = "sc_md" if lang == "markdown" else "sc_code"
        if clean:
            sections_seen[sec].setdefault(key, "")
            sections_seen[sec][key] += clean + "\n"

    lines = [f"# Notebooks — {scenario_name}\n",
             "Auto-extracted from `jupyter/notebook.ipynb` and `zeppelin/notebook.zpln`.\n",
             "Both notebooks implement identical logic in PySpark and Scala.\n\n",
             "## 2. Section map\n\n",
             "| Section | Scala (Zeppelin) | PySpark (Jupyter) |\n|---|---|---|\n"]
    for sec in NOTEBOOK_SECTIONS:
        has_sc = "✓" if any(s == sec and (v.get("sc_md") or v.get("sc_code"))
                            for s, v in sections_seen.items()) else "—"
        has_py = "✓" if any(s == sec and (v.get("py_md") or v.get("py_code"))
                            for s, v in sections_seen.items()) else "—"
        lines.append(f"| {sec} | {has_sc} | {has_py} |\n")
    lines.append("\n## 3. Walkthrough\n\n")
    for sec in NOTEBOOK_SECTIONS:
        v = sections_seen.get(sec)
        if not v:
            continue
        lines.append(f"### {sec}\n\n")
        sc_code = v.get("sc_code", "").strip()
        py_code = v.get("py_code", "").strip()
        if sc_code or py_code:
            lines.append("**Scala (Zeppelin):**\n\n```scala\n" + sc_code + "\n```\n\n")
            lines.append("**PySpark (Jupyter):**\n\n```python\n" + py_code + "\n```\n\n")
        sc_md = v.get("sc_md", "").strip()
        py_md = v.get("py_md", "").strip()
        if sc_md:
            lines.append(sc_md + "\n\n")
        elif py_md:
            lines.append(py_md + "\n\n")
    lines.append("## 4. Scala / PySpark parity\n\n")
    lines.append("Both notebooks share the same numbered sections and produce identical Iceberg tables; "
                 "only the language and interpreter differ.\n\n")
    lines.append("## 5. How to run\n\n")
    lines.append("Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or "
                 "`jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.\n")
    return "".join(lines)
