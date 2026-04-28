from __future__ import annotations

try:
    import tomllib  # py311+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_pyproject_tomllib_parse_valid() -> None:
    parsed = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert "project" in parsed
    assert "dependencies" in parsed["project"]


def test_pyproject_has_single_dependencies_key_under_project() -> None:
    content = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    after_project = content.split("[project]", 1)[1].splitlines()
    section_lines: list[str] = []
    for line in after_project:
        if line.startswith("["):
            break
        section_lines.append(line)
    project_section = "\n".join(section_lines)
    assert project_section.count("dependencies =") == 1
