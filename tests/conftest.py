"""Pytest fixtures"""
import pathlib
from typing import Callable, Iterable

import pytest
from databento import Schema

from tests import TESTS_ROOT


def pytest_addoption(parser: pytest.Parser) -> None:
    """
    Function to customize pytest cli options.
    This should not be invoked directly.

    Parameters
    ----------
    parser : pytest.Parser
        The pytest argument parser.

    See Also
    --------
    pytest.addoption

    """
    # Add a --release flag
    parser.addoption(
        "--release",
        action="store_true",
        help="indicates release tests should be run",
    )


def pytest_configure(config: pytest.Config) -> None:
    """
    Function to configure pytest.
    This should not be invoked directly.

    Parameters
    ----------
    config : pytest.Config
        The pytest configuration.

    """
    # Add custom mark for `release`
    config.addinivalue_line(
        "markers",
        "release: mark tests as release tests (run with --release)",
    )


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: Iterable[pytest.Item],
) -> None:
    """
    Function to customize test items.
    This should not be invoked directly.

    Parameters
    ----------
    config : pytest.Config
        The pytest configuration.
    items : Iterable[pytest.Item]
        The pytest test item.

    """
    skip_release = pytest.mark.skip(
        reason="skipping release test (invoke pytest with --release to execute)",
    )

    for item in items:
        # Skip release tests if `--release` was not specified
        if "release" in item.keywords and not config.getoption("--release"):
            item.add_marker(skip_release)


@pytest.fixture(name="test_data_path")
def fixture_test_data_path() -> Callable[[Schema], pathlib.Path]:
    """
    Factory fixture for retrieving stub data paths.

    Parameters
    ----------
    schema : Schema
        The schema of the stub data path to request.

    Returns
    -------
    Callable

    See Also
    --------
    test_data

    """

    def func(schema: Schema) -> pathlib.Path:
        return pathlib.Path(TESTS_ROOT) / "data" / f"test_data.{schema}.dbn.zst"

    return func


@pytest.fixture(name="test_data")
def fixture_test_data(
    test_data_path: Callable[[Schema], pathlib.Path],
) -> Callable[[Schema], bytes]:
    """
    Factory fixture for retrieving stub test data.

    Parameters
    ----------
    test_data_path : Callable
        The test_data_path fixture.

    Returns
    -------
    Callable

    See Also
    --------
    test_data_path

    """

    def func(schema: Schema) -> bytes:
        return test_data_path(schema).read_bytes()

    return func
