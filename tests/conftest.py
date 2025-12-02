"""
Pytest fixtures.
"""

import asyncio
import logging
import pathlib
import random
import string
from collections.abc import AsyncGenerator
from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Iterable

import pytest
from databento_dbn import Schema

import databento.live.session
from databento import historical
from databento import live
from databento import reference
from databento.common.publishers import Dataset
from tests import TESTS_ROOT
from tests.mockliveserver.fixture import MockLiveServerInterface
from tests.mockliveserver.fixture import fixture_mock_live_server  # noqa


def pytest_addoption(parser: pytest.Parser) -> None:
    """
    Customize pytest cli options. This should not be invoked directly.

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
    Configure pytest. This should not be invoked directly.

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
    Customize test items. This should not be invoked directly.

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


@pytest.fixture(autouse=True)
def fixture_log_capture(
    caplog: pytest.LogCaptureFixture,
) -> Generator[None, None, None]:
    with caplog.at_level(logging.DEBUG):
        yield


@pytest.fixture(name="event_loop", scope="module")
def fixture_event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(name="live_test_data_path")
def fixture_live_test_data_path() -> pathlib.Path:
    """
    Fixture to retrieve the live stub data path.

    Returns
    -------
    pathlib.Path

    See Also
    --------
    live_test_data

    """
    return TESTS_ROOT / "data" / "LIVE" / "test_data.live.dbn.zst"


@pytest.fixture(name="test_data_path")
def fixture_test_data_path() -> Callable[[Dataset, Schema], pathlib.Path]:
    """
    Fixture to retrieve stub data paths.

    Parameters
    ----------
    dataset: Dataset,
        The dataset of the stub data to request.
    schema : Schema
        The schema of the stub data path to request.

    Returns
    -------
    Callable[[Dataset, Schema], pathlib.Path]

    See Also
    --------
    test_data

    """

    def func(dataset: Dataset, schema: Schema) -> pathlib.Path:
        path = TESTS_ROOT / "data" / dataset / f"test_data.{schema}.dbn.zst"
        if not path.exists():
            pytest.skip(f"no test data for {dataset} {schema}")
        return path

    return func


@pytest.fixture(name="live_test_data")
def fixture_live_test_data(
    live_test_data_path: pathlib.Path,
) -> bytes:
    """
    Fixture to retrieve live stub test data.

    Returns
    -------
    bytes

    See Also
    --------
    live_test_data_path

    """
    return live_test_data_path.read_bytes()


@pytest.fixture(name="test_data")
def fixture_test_data(
    test_data_path: Callable[[Dataset, Schema], pathlib.Path],
) -> Callable[[Dataset, Schema], bytes]:
    """
    Fixture to retrieve stub test data.

    Parameters
    ----------
    test_data_path : Callable
        The test_data_path fixture.

    Returns
    -------
    Callable[[Dataset, Schema], bytes]

    See Also
    --------
    test_data_path

    """

    def func(dataset: Dataset, schema: Schema) -> bytes:
        return test_data_path(dataset, schema).read_bytes()

    return func


@pytest.fixture(name="test_api_key")
def fixture_test_api_key() -> str:
    """
    Generate a random API key for testing. API keys are 32 characters in
    length, the first three of which are "db-".

    Returns
    -------
    str

    """
    chars = string.ascii_letters + string.digits
    random_str = "".join(random.choice(chars) for _ in range(29))  # noqa: S311
    return f"db-{random_str}"


@pytest.fixture(name="test_live_api_key")
async def fixture_test_live_api_key(
    test_api_key: str,
    mock_live_server: MockLiveServerInterface,
) -> AsyncGenerator[str, None]:
    async with mock_live_server.api_key_context(test_api_key):
        yield test_api_key


@pytest.fixture(name="historical_client")
def fixture_historical_client(
    test_api_key: str,
) -> Generator[historical.client.Historical, None, None]:
    """
    Fixture for a Historical client.

    Yields
    ------
    Historical

    """
    test_client = historical.client.Historical(
        key=test_api_key,
        gateway="localhost",
    )
    yield test_client


@pytest.fixture(name="reference_client")
def fixture_reference_client(
    test_api_key: str,
) -> Generator[reference.client.Reference, None, None]:
    """
    Fixture for a Reference client.

    Yields
    ------
    Reference

    """
    test_client = reference.client.Reference(
        key=test_api_key,
        gateway="localhost",
    )
    yield test_client


@pytest.fixture(name="live_client")
async def fixture_live_client(
    test_live_api_key: str,
    mock_live_server: MockLiveServerInterface,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[live.client.Live, None]:
    """
    Fixture for a Live client to connect to the MockLiveServer.

    Yields
    ------
    Live

    """
    monkeypatch.setattr(
        databento.live.session,
        "AUTH_TIMEOUT_SECONDS",
        0.5,
    )
    monkeypatch.setattr(
        databento.live.session,
        "CONNECT_TIMEOUT_SECONDS",
        0.5,
    )

    test_client = live.client.Live(
        key=test_live_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
    )

    with mock_live_server.test_context():
        yield test_client

    if test_client.is_connected():
        test_client.stop()
        await test_client.wait_for_close()
