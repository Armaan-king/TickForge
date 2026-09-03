"""Docs stay internally consistent.

Link rot from renames is the one documentation failure a machine can catch.
Whether the content is still *accurate* is not checkable — that's what the
update discipline in docs/knowledge/INDEX.md is for.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKIP_DIRS = {".git", ".venv", "node_modules", "data", "__pycache__"}

MD_LINK = re.compile(r"\]\(([^)\s]+)\)")
CLAUDE_IMPORT = re.compile(r"@([A-Za-z0-9_./-]+\.md)")


def markdown_files() -> list[Path]:
    return [
        p
        for p in ROOT.rglob("*.md")
        if not SKIP_DIRS.intersection(p.relative_to(ROOT).parts)
    ]


def test_markdown_files_are_found() -> None:
    """Guards the other tests: a bad filter would make them pass vacuously."""
    assert len(markdown_files()) >= 5


def test_relative_links_resolve() -> None:
    broken = []
    for md in markdown_files():
        for target in MD_LINK.findall(md.read_text(encoding="utf-8")):
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            path = md.parent / target.split("#")[0]
            if not path.exists():
                broken.append(f"{md.relative_to(ROOT)} -> {target}")
    assert not broken, "Dangling markdown links:\n  " + "\n  ".join(broken)


def test_claude_imports_resolve() -> None:
    text = (ROOT / "CLAUDE.md").read_text(encoding="utf-8")
    broken = [t for t in CLAUDE_IMPORT.findall(text) if not (ROOT / t).exists()]
    assert not broken, "Dangling @imports in CLAUDE.md:\n  " + "\n  ".join(broken)
