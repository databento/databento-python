"""
Tests specific to releasing a version of databento-python.
"""

import operator
import re
from datetime import date

import pytest
import tomli

import databento
from tests import PROJECT_ROOT


CHANGELOG_RELEASE_TITLE = re.compile(r"## (?P<version>\d+\.\d+\.\d+) - (?P<date>.+)\n")


@pytest.fixture(name="changelog")
def fixture_changelog() -> str:
    """
    Fixture for the text of CHANGELOG.md.

    Returns
    -------
    str

    """
    # Arrange, Act, Assert
    with open(PROJECT_ROOT / "CHANGELOG.md") as changelog:
        return changelog.read()


@pytest.fixture(name="pyproject_version")
def fixture_pyproject_version() -> str:
    """
    Fixture for the version text of project.toml.

    Returns
    -------
    str

    """
    # Arrange, Act, Assert
    with open(PROJECT_ROOT / "pyproject.toml", "rb") as pyproject:
        data = tomli.load(pyproject)
    return data["project"]["version"]


@pytest.mark.release
def test_release_changelog(changelog: str, pyproject_version: str) -> None:
    """
    Test that CHANGELOG.md and pyproject.toml contain correct version
    information.

    This test verifies that:
        - The version in `version.py` matches the latest release note
        - The version in `version.py` matches the version in `pyproject.toml`
        - The versions are unique.
        - The versions are ascending.
        - The release dates are chronological.

    """
    # Arrange, Act
    releases = CHANGELOG_RELEASE_TITLE.findall(changelog)

    try:
        versions = list(map(operator.itemgetter(0), releases))
        version_tuples = [tuple(map(int, v.split("."))) for v in versions]
    except Exception:
        # This could happen if we have an irregular version string.
        raise AssertionError("Failed to parse version from CHANGELOG.md")

    try:
        date_strings = list(map(operator.itemgetter(1), releases))
        dates = list(map(date.fromisoformat, date_strings))
    except Exception:
        # This could happen if we have TBD as the release date.
        raise AssertionError("Failed to parse release date from CHANGELOG.md")

    # Assert
    # Ensure latest version matches `__version__`
    assert databento.__version__ == versions[0]

    # Ensure latest version matches pyproject.toml
    assert databento.__version__ == pyproject_version

    # Ensure versions are unique
    assert len(versions) == len(set(versions))

    # Ensure version is ascending
    assert version_tuples == sorted(version_tuples, reverse=True)

    # Ensure release dates are chronological
    assert dates == sorted(dates, reverse=True)
