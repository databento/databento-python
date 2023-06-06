"""Pytest fixtures"""
import asyncio
import pathlib
import random
import string
from typing import AsyncGenerator, Callable, Generator, Iterable

import databento.live
import pytest
import pytest_asyncio
from databento.common.enums import Schema
from databento.live import client

from tests import TESTS_ROOT
from tests.mock_live_server import MockLiveServer


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


@pytest.fixture(name="test_api_key")
def fixture_test_api_key() -> str:
    """
    Generates a random API key for testing.
    API keys are 32 characters in length, the first three of
    which are "db-".

    Returns
    -------
    str

    """
    chars = string.ascii_letters + string.digits
    random_str = "".join(random.choice(chars) for _ in range(29))
    return f"db-{random_str}"


@pytest_asyncio.fixture(name="mock_live_server")
async def fixture_mock_live_server(
    test_api_key: str,
    caplog: pytest.LogCaptureFixture,
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[MockLiveServer, None]:
    """
    Fixture for a MockLiveServer instance.

    Yields
    ------
    MockLiveServer

    """
    monkeypatch.setenv(
        name="DATABENTO_API_KEY",
        value=test_api_key,
    )
    monkeypatch.setattr(
        databento.live,
        "AUTH_TIMEOUT_SECONDS",
        1,
    )
    monkeypatch.setattr(
        databento.live,
        "CONNECT_TIMEOUT_SECONDS",
        1,
    )

    with caplog.at_level("DEBUG"):
        mock_live_server = await MockLiveServer.create(
            host="127.0.0.1",
            port=unused_tcp_port,
            dbn_path=TESTS_ROOT / "data",
        )
        await mock_live_server.start()
        yield mock_live_server
        await mock_live_server.stop()


@pytest.fixture(name="live_client")
def fixture_live_client(
    test_api_key: str,
    mock_live_server: MockLiveServer,
) -> Generator[client.Live, None, None]:
    """
    Fixture for a Live client to connect to the MockLiveServer.

    Yields
    ------
    Live

    """
    test_client = client.Live(
        key=test_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
    )
    yield test_client
    if test_client.is_connected():
        test_client.stop()
