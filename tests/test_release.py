"""Tests specific to releasing a version of databento-python"""
import operator
import re
from datetime import date

import databento
import pytest

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
    with open(PROJECT_ROOT / "CHANGELOG.md", mode="r", encoding="utf-8") as changelog:
        return changelog.read()


@pytest.mark.release
def test_release_changelog(changelog: str) -> None:
    """
    Test that CHANGELOG.md contains correct version information.
    This test verifies that:
        - The version in `version.py` matches the latest release note.
        - The versions are unique.
        - The versions are ascending.
        - The release dates are chronological.

    """
    releases = CHANGELOG_RELEASE_TITLE.findall(changelog)

    try:
        versions = list(map(operator.itemgetter(0), releases))
        version_tuples = [tuple(map(int, v.split("."))) for v in versions]
    except Exception as exc:
        # This could happen if we have an irregular version string.
        raise AssertionError("Failed to parse version from CHANGELOG.md") from exc

    try:
        date_strings = list(map(operator.itemgetter(1), releases))
        dates = list(map(date.fromisoformat, date_strings))
    except Exception as exc:
        # This could happen if we have TBD as the release date.
        raise AssertionError("Failed to parse release date from CHANGELOG.md") from exc

    # Ensure latest version matches `__version__`
    assert databento.__version__ == versions[0]

    # Ensure versions are unique
    assert len(versions) == len(set(versions))

    # Ensure version is ascending
    assert version_tuples == sorted(version_tuples, reverse=True)

    # Ensure release dates are chronological
    assert dates == sorted(dates, reverse=True)
