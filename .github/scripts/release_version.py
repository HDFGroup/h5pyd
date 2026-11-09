#!/usr/bin/env python3
"""Resolve the release version from the repo-local single source of truth.

SSOT: the `version` field of [project] in pyproject.toml.

Everything else in the release pipeline keys off what this prints, so a
malformed version, or one that disagrees with h5pyd/version.py, stops the run
before anything is built or published.
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

# PEP 440 version syntax, from the spec's Appendix B (the same pattern
# `packaging.version` uses). `packaging` is not installed in the job that runs
# this script, so the pattern is inlined. Always applied with fullmatch().
PEP440_RE = re.compile(
    r"""
    v?
    (?:
        (?:(?P<epoch>[0-9]+)!)?                           # epoch
        (?P<release>[0-9]+(?:\.[0-9]+)*)                  # release segment
        (?P<pre>                                          # pre-release
            [-_\.]?
            (?P<pre_l>(a|b|c|rc|alpha|beta|pre|preview))
            [-_\.]?
            (?P<pre_n>[0-9]+)?
        )?
        (?P<post>                                         # post release
            (?:-(?P<post_n1>[0-9]+))
            |
            (?:
                [-_\.]?
                (?P<post_l>post|rev|r)
                [-_\.]?
                (?P<post_n2>[0-9]+)?
            )
        )?
        (?P<dev>                                          # dev release
            [-_\.]?
            (?P<dev_l>dev)
            [-_\.]?
            (?P<dev_n>[0-9]+)?
        )?
    )
    (?:\+(?P<local>[a-z0-9]+(?:[-_\.][a-z0-9]+)*))?       # local version
    """,
    re.VERBOSE | re.IGNORECASE,
)


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
    return str(project["version"])


def validate_version(version):
    """Fail unless the version is a well-formed PEP 440 version with no whitespace."""
    if any(c.isspace() for c in version) or not PEP440_RE.fullmatch(version):
        fail(
            f"{PYPROJECT} [project] version {version!r} is not a valid PEP 440 "
            "version (whitespace and newlines are not allowed)"
        )


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
    validate_version(version)
    check_version_py(version)

    tag = f"v{version}"
    set_output("version", version)
    set_output("tag", tag)
    set_output("dist_name", DIST_NAME)
    set_output("prerelease", "true" if PRERELEASE_RE.search(version) else "false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
