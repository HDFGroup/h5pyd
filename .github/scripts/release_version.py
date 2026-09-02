#!/usr/bin/env python3
"""Resolve the release version from the repo-local single source of truth.

SSOT: the `version` field of [project] in pyproject.toml.

Everything else in the release pipeline keys off what this prints, so a tag
that disagrees with the source tree stops the run before anything is built or
published.
"""
import os
import re
import sys
import tomllib
from pathlib import Path

PYPROJECT = Path("pyproject.toml")
VERSION_PY = Path("h5pyd/version.py")
DIST_NAME = "h5pyd"

# PEP 440 pre-release / dev-release markers.
PRERELEASE_RE = re.compile(r"(a|b|rc|alpha|beta|dev)\d*$", re.IGNORECASE)


def set_output(name, value):
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as f:
            f.write(f"{name}={value}\n")
    print(f"{name}={value}")


def fail(msg):
    print(f"::error::{msg}", file=sys.stderr)
    raise SystemExit(1)


def read_pyproject_version():
    if not PYPROJECT.is_file():
        fail(f"{PYPROJECT} not found")
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    project = data.get("project", {})
    if "version" not in project:
        fail(
            f"{PYPROJECT} [project] has no static `version`. If the project has "
            "moved to a dynamic version, point this script at the new SSOT."
        )
    return str(project["version"]).strip()


def check_version_py(version):
    """Hard check: h5pyd/version.py must agree with pyproject.toml.

    h5pyd/__init__.py does `__version__ = version.version`, so version.py is
    what the installed library reports at runtime, while pyproject.toml is what
    names the wheel. If the two drift, `pip install h5pyd==X` gives you a
    package whose own `__version__` says something else - so this fails the
    release rather than shipping the disagreement.
    """
    if not VERSION_PY.is_file():
        fail(f"{VERSION_PY} not found - the version SSOT check cannot run")
    match = re.search(
        r"^version\s*=\s*[\"']([^\"']+)[\"']",
        VERSION_PY.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    if not match:
        fail(f"{VERSION_PY} does not define a `version = \"...\"` string")
    if match.group(1) != version:
        fail(
            f"version mismatch: {PYPROJECT} says {version}, "
            f"{VERSION_PY} says {match.group(1)}. Both must be bumped together - "
            "pyproject.toml names the wheel, version.py is what "
            "h5pyd.__version__ reports at runtime."
        )


def main():
    version = read_pyproject_version()
    check_version_py(version)

    tag = f"v{version}"
    if os.environ.get("EVENT_NAME") == "push":
        ref = os.environ.get("GITHUB_REF_NAME", "")
        if ref != tag:
            fail(
                f"tag {ref!r} does not match the version in {PYPROJECT} "
                f"({version}, expected tag {tag!r}). Bump the SSOT and re-tag."
            )

    set_output("version", version)
    set_output("tag", tag)
    set_output("dist_name", DIST_NAME)
    set_output("prerelease", "true" if PRERELEASE_RE.search(version) else "false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
