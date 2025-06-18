"""
Unit tests for the Live client.
"""

from __future__ import annotations

import pathlib
import platform
import random
import string
from io import BytesIO
from typing import Callable
from unittest.mock import MagicMock

import databento_dbn
import pytest
import zstandard
from databento.common.constants import ALL_SYMBOLS
from databento.common.constants import SCHEMA_STRUCT_MAP
from databento.common.cram import BUCKET_ID_LENGTH
from databento.common.dbnstore import DBNStore
from databento.common.error import BentoError
from databento.common.publishers import Dataset
from databento.common.types import DBNRecord
from databento.live import client
from databento.live import gateway
from databento.live import protocol
from databento.live import session
from databento_dbn import Encoding
from databento_dbn import Schema
from databento_dbn import SType

from tests.mockliveserver.fixture import MockLiveServerInterface


# TODO(nm): Remove when stable
if platform.system() == "Windows":
    pytest.skip(reason="Skip on Windows due to flakiness", allow_module_level=True)


def test_live_connection_refused(
    test_api_key: str,
) -> None:
    """
    Test that a refused connection raises a BentoError.
    """
    # Arrange
    live_client = client.Live(
        key=test_api_key,
        # Connect to something that does not exist
        gateway="localhost",
        port=0,
    )

    # Act, Assert
    with pytest.raises(BentoError) as exc:
        live_client.subscribe(
            dataset=Dataset.GLBX_MDP3,
            schema=Schema.MBO,
        )

    exc.match(r"Connection to .+ failed.")


def test_live_connection_timeout(
    monkeypatch: pytest.MonkeyPatch,
    live_client: client.Live,
) -> None:
    """
    Test that a timeout raises a BentoError.

    Mock the create_connection function so that it never completes and
    set a timeout of 0.

    """
    # Arrange
    monkeypatch.setattr(
        session,
        "CONNECT_TIMEOUT_SECONDS",
        0,
    )

    # Act, Assert
    with pytest.raises(BentoError) as exc:
        live_client.subscribe(
            dataset=Dataset.GLBX_MDP3,
            schema=Schema.MBO,
        )

    # Ensure this was a timeout error
    exc.match(r"Connection to .+ timed out after 0 second\(s\)\.")


@pytest.mark.parametrize(
    "gateway",
    [
        pytest.param("", id="empty"),
        pytest.param(" ", id="space"),
    ],
)
def test_live_invalid_gateway(
    mock_live_server: MockLiveServerInterface,
    test_api_key: str,
    gateway: str,
) -> None:
    """
    Test that specifying an invalid gateway raises a ValueError.
    """
    # Arrange, Act, Assert
    with pytest.raises(ValueError):
        client.Live(
            key=test_api_key,
            gateway=gateway,
            port=mock_live_server.port,
        )


@pytest.mark.parametrize(
    "port",
    [
        pytest.param("12345", id="str"),
        pytest.param(0.5, id="float"),
    ],
)
def test_live_invalid_port(
    mock_live_server: MockLiveServerInterface,
    test_api_key: str,
    port: object,
) -> None:
    """
    Test that specifying an invalid port raises a ValueError.
    """
    # Arrange, Act, Assert
    with pytest.raises(ValueError):
        client.Live(
            key=test_api_key,
            gateway=mock_live_server.host,
            port=port,  # type: ignore
        )


def test_live_connection_cram_failure(
    mock_live_server: MockLiveServerInterface,
) -> None:
    """
    Test that a failed auth message due to an incorrect CRAM raises a
    BentoError.
    """
    invalid_key = "db-invalidkey00000000000000FFFFF"

    live_client = client.Live(
        key=invalid_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
    )

    # Act, Assert
    with pytest.raises(BentoError) as exc:
        live_client.subscribe(
            dataset=Dataset.GLBX_MDP3,
            schema=Schema.MBO,
        )

    # Ensure this was an authentication error
    exc.match(r"User authentication failed:")


@pytest.mark.parametrize(
    "start",
    [
        "now",
        "2022-06-10T12:00",
        1671717080706865759,
    ],
)
def test_live_subscription_with_snapshot_failed(
    mock_live_server: MockLiveServerInterface,
    test_api_key: str,
    start: str | int,
) -> None:
    """
    Test that an invalid snapshot subscription raises a ValueError.
    """
    # Arrange
    live_client = client.Live(
        key=test_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
    )

    # Act, Assert
    with pytest.raises(ValueError) as exc:
        live_client.subscribe(
            dataset=Dataset.GLBX_MDP3,
            schema=Schema.MBO,
            symbols=ALL_SYMBOLS,
            start=start,
            snapshot=True,
        )

    # Ensure this was an authentication error
    exc.match(r"Subscription with snapshot expects start=None")


@pytest.mark.parametrize(
    "dataset",
    [pytest.param(dataset, id=str(dataset)) for dataset in Dataset],
)
def test_live_creation(
    mock_live_server: MockLiveServerInterface,
    live_client: client.Live,
    test_api_key: str,
    dataset: Dataset,
) -> None:
    """
    Test the live constructor and successful connection to the
    MockLiveServerInterface.
    """
    # Arrange, Act
    live_client.subscribe(
        dataset=dataset,
        schema=Schema.MBO,
    )

    # Assert
    assert live_client.gateway == mock_live_server.host
    assert live_client.port == mock_live_server.port
    assert live_client._key == test_api_key
    assert live_client.dataset == dataset
    assert live_client.is_connected() is True


async def test_live_connect_auth(
    mock_live_server: MockLiveServerInterface,
    live_client: client.Live,
) -> None:
    """
    Test the live sent a correct AuthenticationRequest message after
    connecting.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    # Act
    message = await mock_live_server.wait_for_message_of_type(
        message_type=gateway.AuthenticationRequest,
    )

    # Assert
    assert message.auth.endswith(live_client.key[-BUCKET_ID_LENGTH:])
    assert message.dataset == live_client.dataset
    assert message.encoding == Encoding.DBN


async def test_live_connect_auth_with_heartbeat_interval(
    mock_live_server: MockLiveServerInterface,
    test_live_api_key: str,
) -> None:
    """
    Test that setting `heartbeat_interval_s` on a Live client sends that field
    to the gateway.
    """
    # Arrange
    live_client = client.Live(
        key=test_live_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
        heartbeat_interval_s=10,
    )

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    # Act
    message = await mock_live_server.wait_for_message_of_type(
        message_type=gateway.AuthenticationRequest,
    )

    # Assert
    assert message.auth.endswith(live_client.key[-BUCKET_ID_LENGTH:])
    assert message.dataset == live_client.dataset
    assert message.encoding == Encoding.DBN
    assert message.heartbeat_interval_s == "10"


async def test_live_connect_auth_two_clients(
    mock_live_server: MockLiveServerInterface,
    test_live_api_key: str,
) -> None:
    """
    Test the live sent a correct AuthenticationRequest message after connecting
    two distinct clients.
    """
    # Arrange
    first = client.Live(
        key=test_live_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
    )

    second = client.Live(
        key=test_live_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
    )

    # Act
    first.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    first_auth = await mock_live_server.wait_for_message_of_type(
        message_type=gateway.AuthenticationRequest,
    )

    # Assert
    assert first_auth.auth.endswith(first.key[-BUCKET_ID_LENGTH:])
    assert first_auth.dataset == first.dataset
    assert first_auth.encoding == Encoding.DBN

    second.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    second_auth = await mock_live_server.wait_for_message_of_type(
        message_type=gateway.AuthenticationRequest,
    )

    assert second_auth.auth.endswith(second.key[-BUCKET_ID_LENGTH:])
    assert second_auth.dataset == second.dataset
    assert second_auth.encoding == Encoding.DBN


async def test_live_start(
    live_client: client.Live,
    mock_live_server: MockLiveServerInterface,
) -> None:
    """
    Test the live sends a SesssionStart message upon calling start().
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    assert live_client.is_connected() is True

    # Act
    live_client.start()

    message = await mock_live_server.wait_for_message_of_type(
        message_type=gateway.SessionStart,
    )

    # Assert
    assert message.start_session


async def test_live_start_twice(
    live_client: client.Live,
    mock_live_server: MockLiveServerInterface,
) -> None:
    """
    Test that calling start() twice raises a ValueError.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    # Act
    live_client.start()

    _ = await mock_live_server.wait_for_message_of_type(
        message_type=gateway.SessionStart,
    )

    # Assert
    with pytest.raises(ValueError):
        live_client.start()


def test_live_start_before_subscribe(
    live_client: client.Live,
) -> None:
    """
    Test that calling start() before subscribe raises a ValueError.
    """
    # Arrange, Act, Assert
    with pytest.raises(ValueError):
        live_client.start()


async def test_live_iteration_after_start(
    live_client: client.Live,
    mock_live_server: MockLiveServerInterface,
) -> None:
    """
    Test that iterating the Live client after it is started raises a
    ValueError.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    # Act
    live_client.start()

    _ = await mock_live_server.wait_for_message_of_type(
        message_type=gateway.SessionStart,
    )

    # Assert
    with pytest.raises(ValueError):
        iter(live_client)


async def test_live_async_iteration_after_start(
    live_client: client.Live,
    mock_live_server: MockLiveServerInterface,
) -> None:
    """
    Test that async-iterating the Live client after it is started raises a
    ValueError.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    # Act
    live_client.start()

    _ = await mock_live_server.wait_for_message_of_type(
        message_type=gateway.SessionStart,
    )

    # Assert
    with pytest.raises(ValueError):
        live_client.__aiter__()


@pytest.mark.parametrize(
    "schema",
    [pytest.param(schema, id=str(schema)) for schema in Schema.variants()],
)
@pytest.mark.parametrize(
    "stype_in",
    [pytest.param(stype, id=str(stype)) for stype in SType.variants()],
)
@pytest.mark.parametrize(
    "symbols",
    [
        pytest.param("NVDA", id="str"),
        pytest.param("ES,CL", id="str-list"),
        pytest.param(ALL_SYMBOLS, id="all-symbols"),
    ],
)
@pytest.mark.parametrize(
    "start",
    [
        None,
        "1680736543000000000",
    ],
)
async def test_live_subscribe(
    live_client: client.Live,
    mock_live_server: MockLiveServerInterface,
    schema: Schema,
    stype_in: SType,
    symbols: str,
    start: str,
) -> None:
    """
    Test various combination of subscription messages are serialized and
    correctly deserialized by the MockLiveServerInterface.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=schema,
        stype_in=stype_in,
        symbols=symbols,
        start=start,
    )

    # Act
    message = await mock_live_server.wait_for_message_of_type(
        message_type=gateway.SubscriptionRequest,
    )

    if symbols is None:
        symbols = ALL_SYMBOLS

    # Assert
    assert message.schema == schema
    assert message.stype_in == stype_in
    assert message.symbols == symbols
    assert message.start == start
    assert message.snapshot == "0"


@pytest.mark.parametrize(
    "snapshot",
    [
        False,
        True,
    ],
)
async def test_live_subscribe_snapshot(
    live_client: client.Live,
    mock_live_server: MockLiveServerInterface,
    snapshot: bool,
) -> None:
    """
    Test that snapshot parameter is assigned correctly.
    """
    # Arrange

    schema = Schema.MBO
    stype_in = SType.RAW_SYMBOL
    symbols = ALL_SYMBOLS
    start = None

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=schema,
        stype_in=stype_in,
        symbols=symbols,
        start=start,
        snapshot=snapshot,
    )

    # Act
    message = await mock_live_server.wait_for_message_of_type(
        gateway.SubscriptionRequest,
        timeout=1,
    )

    # Assert
    assert message.schema == schema
    assert message.stype_in == stype_in
    assert message.symbols == symbols
    assert message.start == start
    assert message.snapshot == str(int(snapshot))


@pytest.mark.usefixtures("mock_live_server")
async def test_live_subscribe_session_id(
    live_client: client.Live,
) -> None:
    """
    Test that a session ID is assigned after the connection is authenticated.
    """
    # Arrange
    old_session_id = live_client._session.session_id

    # Act
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols=ALL_SYMBOLS,
    )

    # Assert
    assert live_client._session.session_id != old_session_id
    assert live_client._session.session_id != 0


async def test_live_subscribe_large_symbol_list(
    live_client: client.Live,
    mock_live_server: MockLiveServerInterface,
) -> None:
    """
    Test that sending a subscription with a large symbol list breaks that list
    up into multiple messages.
    """
    # Arrange
    large_symbol_list = list(
        random.choices(string.ascii_uppercase, k=3950),  # noqa: S311
    )

    # Act
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols=large_symbol_list,
    )

    reconstructed: list[str] = []
    for i in range(8):
        message = await mock_live_server.wait_for_message_of_type(
            message_type=gateway.SubscriptionRequest,
        )
        assert int(message.is_last) == int(i == 7)
        reconstructed.extend(message.symbols.split(","))

    # Assert
    assert reconstructed == large_symbol_list


async def test_live_subscribe_from_callback(
    live_client: client.Live,
    mock_live_server: MockLiveServerInterface,
) -> None:
    """
    Test that `Live.subscribe` can be called from a callback.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.OHLCV_1H,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST0",
    )

    def cb_sub(_: DBNRecord) -> None:
        live_client.subscribe(
            dataset=Dataset.GLBX_MDP3,
            schema=Schema.MBO,
            stype_in=SType.RAW_SYMBOL,
            symbols="TEST1",
        )

    live_client.add_callback(cb_sub)

    # Act
    first_sub = await mock_live_server.wait_for_message_of_type(
        message_type=gateway.SubscriptionRequest,
    )

    live_client.start()

    await live_client.wait_for_close()

    second_sub = await mock_live_server.wait_for_message_of_type(
        message_type=gateway.SubscriptionRequest,
    )

    # Assert
    assert first_sub.symbols == "TEST0"
    assert second_sub.symbols == "TEST1"


async def test_live_subscribe_different_dataset(
    live_client: client.Live,
    mock_live_server: MockLiveServerInterface,
) -> None:
    """
    Test that once a Live client is disconnected, it can be used with a
    different subscription dataset.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    # Act
    _ = await mock_live_server.wait_for_message_of_type(
        message_type=gateway.SubscriptionRequest,
    )

    live_client.start()
    await live_client.wait_for_close()

    # Assert
    live_client.subscribe(
        dataset=Dataset.XNAS_ITCH,
        schema=Schema.MBO,
    )


@pytest.mark.usefixtures("mock_live_server")
def test_live_stop(
    live_client: client.Live,
) -> None:
    """
    Test that calling start() and stop() appropriately update the client state.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    # Act, Assert
    assert live_client.is_connected() is True

    live_client.start()

    live_client.stop()
    live_client.block_for_close()

    assert live_client.is_connected() is False


@pytest.mark.usefixtures("mock_live_server")
def test_live_shutdown_remove_closed_stream(
    tmp_path: pathlib.Path,
    live_client: client.Live,
) -> None:
    """
    Test that closed streams are removed upon disconnection.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    output = tmp_path / "output.dbn"

    # Act, Assert
    with output.open("wb") as out:
        live_client.add_stream(out)

        assert live_client.is_connected() is True

        live_client.start()

    live_client.stop()
    live_client.block_for_close()

    assert live_client._session._user_streams == []


def test_live_stop_twice(
    live_client: client.Live,
) -> None:
    """
    Test that calling stop() twice does not raise an exception.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    # Act, Assert
    live_client.stop()
    live_client.stop()


@pytest.mark.usefixtures("mock_live_server")
def test_live_block_for_close(
    live_client: client.Live,
) -> None:
    """
    Test that block_for_close unblocks when the connection is closed.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.INSTRUMENT_ID,
        symbols="ALL_SYMBOLS",
        start=None,
    )

    # Act, Assert
    live_client.start()

    live_client.block_for_close()

    assert not live_client.is_connected()


async def test_live_block_for_close_timeout(
    live_client: client.Live,
    monkeypatch: pytest.MonkeyPatch,
    mock_live_server: MockLiveServerInterface,
) -> None:
    """
    Test that block_for_close terminates the session when the timeout is
    reached.
    """
    # Arrange
    monkeypatch.setattr(live_client, "terminate", MagicMock())
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.INSTRUMENT_ID,
        symbols="ALL_SYMBOLS",
        start=None,
    )

    # Act, Assert
    _ = await mock_live_server.wait_for_message_of_type(
        message_type=gateway.SubscriptionRequest,
    )

    live_client.block_for_close(timeout=0)
    live_client.terminate.assert_called_once()  # type: ignore


@pytest.mark.usefixtures("mock_live_server")
async def test_live_block_for_close_timeout_stream(
    live_client: client.Live,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    mock_live_server: MockLiveServerInterface,
) -> None:
    """
    Test that block_for_close flushes user streams on timeout.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.INSTRUMENT_ID,
        symbols="ALL_SYMBOLS",
        start=None,
    )
    path = tmp_path / "test.dbn"
    stream = path.open("wb")
    monkeypatch.setattr(stream, "flush", MagicMock())
    live_client.add_stream(stream)

    # Act, Assert
    _ = await mock_live_server.wait_for_message_of_type(
        message_type=gateway.SubscriptionRequest,
    )

    live_client.block_for_close(timeout=0)
    stream.flush.assert_called()  # type: ignore [attr-defined]


@pytest.mark.usefixtures("mock_live_server")
async def test_live_wait_for_close(
    live_client: client.Live,
) -> None:
    """
    Test that wait_for_close unblocks when the connection is closed.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.INSTRUMENT_ID,
        symbols="ALL_SYMBOLS",
        start=None,
    )

    # Act
    live_client.start()
    await live_client.wait_for_close()

    # Assert
    assert not live_client.is_connected()


@pytest.mark.usefixtures("mock_live_server")
async def test_live_wait_for_close_timeout(
    live_client: client.Live,
    monkeypatch: pytest.MonkeyPatch,
    mock_live_server: MockLiveServerInterface,
) -> None:
    """
    Test that wait_for_close terminates the session when the timeout is
    reached.
    """
    # Arrange
    monkeypatch.setattr(live_client, "terminate", MagicMock())

    # Act
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.INSTRUMENT_ID,
        symbols="ALL_SYMBOLS",
        start=None,
    )

    _ = await mock_live_server.wait_for_message_of_type(
        message_type=gateway.SubscriptionRequest,
    )

    await live_client.wait_for_close(timeout=0)

    # Assert
    live_client.terminate.assert_called_once()  # type: ignore


@pytest.mark.usefixtures("mock_live_server")
async def test_live_wait_for_close_timeout_stream(
    live_client: client.Live,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    mock_live_server: MockLiveServerInterface,
) -> None:
    """
    Test that wait_for_close flushes user streams on timeout.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.INSTRUMENT_ID,
        symbols="ALL_SYMBOLS",
        start=None,
    )

    path = tmp_path / "test.dbn"
    stream = path.open("wb")
    monkeypatch.setattr(stream, "flush", MagicMock())
    live_client.add_stream(stream)

    # Act
    _ = await mock_live_server.wait_for_message_of_type(
        message_type=gateway.SubscriptionRequest,
    )

    await live_client.wait_for_close(timeout=0)

    # Assert
    stream.flush.assert_called()  # type: ignore [attr-defined]


def test_live_add_callback(
    live_client: client.Live,
) -> None:
    """
    Test that calling add_callback adds that callback to the client.
    """

    # Arrange
    def callback(_: object) -> None:
        pass

    # Act
    live_client.add_callback(callback)

    # Assert
    assert len(live_client._session._user_callbacks) == 2  # include map_symbols callback
    assert (callback, None) in live_client._session._user_callbacks


def test_live_add_stream(
    live_client: client.Live,
) -> None:
    """
    Test that calling add_stream adds that stream to the client.
    """
    # Arrange
    stream = BytesIO()

    # Act
    live_client.add_stream(stream)

    # Assert
    assert len(live_client._session._user_streams) == 1
    assert (stream, None) in live_client._session._user_streams


def test_live_add_stream_invalid(
    tmp_path: pathlib.Path,
    live_client: client.Live,
) -> None:
    """
    Test that passing a non-writable stream to add_stream raises a ValueError.
    """
    # Arrange, Act
    with pytest.raises(ValueError):
        live_client.add_stream(object)  # type: ignore

    readable_file = tmp_path / "nope.txt"
    readable_file.touch()

    # Assert
    with pytest.raises(ValueError):
        live_client.add_stream(readable_file.open(mode="rb"))


def test_live_add_stream_path_directory(
    tmp_path: pathlib.Path,
    live_client: client.Live,
) -> None:
    """
    Test that passing a path to a directory raises an OSError.
    """
    # Arrange, Act, Assert
    with pytest.raises(OSError):
        live_client.add_stream(tmp_path)


async def test_live_async_iteration(
    live_client: client.Live,
) -> None:
    """
    Test async-iteration of DBN records.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    records: list[DBNRecord] = []

    # Act
    async for record in live_client:
        records.append(record)

    # Assert
    assert len(records) == 4
    assert isinstance(records[0], databento_dbn.MBOMsg)
    assert isinstance(records[1], databento_dbn.MBOMsg)
    assert isinstance(records[2], databento_dbn.MBOMsg)
    assert isinstance(records[3], databento_dbn.MBOMsg)


async def test_live_async_iteration_backpressure(
    monkeypatch: pytest.MonkeyPatch,
    live_client: client.Live,
) -> None:
    """
    Test that a full queue disables reading on the transport but will resume it
    when the queue is depleted when iterating asynchronously.
    """
    # Arrange
    monkeypatch.setattr(session, "DBN_QUEUE_CAPACITY", 2)

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    monkeypatch.setattr(
        live_client._session._transport,
        "pause_reading",
        pause_mock := MagicMock(),
    )

    # Act
    live_it = iter(live_client)
    await live_client.wait_for_close()

    pause_mock.assert_called()

    records: list[DBNRecord] = list(live_it)

    # Assert
    assert len(records) == 4
    assert live_client._session._dbn_queue.empty()


async def test_live_async_iteration_dropped(
    monkeypatch: pytest.MonkeyPatch,
    live_client: client.Live,
    test_api_key: str,
) -> None:
    """
    Test that an artificially small queue size will not drop messages when
    full.
    """
    # Arrange
    monkeypatch.setattr(session, "DBN_QUEUE_CAPACITY", 1)

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    monkeypatch.setattr(
        live_client._session._transport,
        "pause_reading",
        pause_mock := MagicMock(),
    )

    # Act
    live_it = iter(live_client)
    await live_client.wait_for_close()

    pause_mock.assert_called()

    records = list(live_it)

    # Assert
    assert len(records) == 4
    assert live_client._session._dbn_queue.empty()


async def test_live_async_iteration_stop(
    live_client: client.Live,
) -> None:
    """
    Test that stopping in the middle of iteration does not prevent iterating
    the queue to completion.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )
    records = []

    # Act
    async for record in live_client:
        records.append(record)
        live_client.stop()

    # Assert
    assert len(records) > 1
    assert live_client._session._dbn_queue.empty()


def test_live_sync_iteration(
    live_client: client.Live,
) -> None:
    """
    Test synchronous iteration of DBN records.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )
    records = []

    # Act
    for record in live_client:
        records.append(record)

    # Assert
    assert len(records) == 4
    assert isinstance(records[0], databento_dbn.MBOMsg)
    assert isinstance(records[1], databento_dbn.MBOMsg)
    assert isinstance(records[2], databento_dbn.MBOMsg)
    assert isinstance(records[3], databento_dbn.MBOMsg)


async def test_live_callback(
    live_client: client.Live,
) -> None:
    """
    Test callback dispatch of DBN records.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )
    records = []

    def callback(record: DBNRecord) -> None:
        nonlocal records
        records.append(record)

    # Act
    live_client.add_callback(callback)

    live_client.start()

    await live_client.wait_for_close()

    # Assert
    assert len(records) == 4
    assert isinstance(records[0], databento_dbn.MBOMsg)
    assert isinstance(records[1], databento_dbn.MBOMsg)
    assert isinstance(records[2], databento_dbn.MBOMsg)
    assert isinstance(records[3], databento_dbn.MBOMsg)


@pytest.mark.parametrize(
    "dataset",
    [
        Dataset.GLBX_MDP3,
        Dataset.XNAS_ITCH,
        Dataset.OPRA_PILLAR,
        Dataset.EQUS_MINI,
        Dataset.IFEU_IMPACT,
        Dataset.NDEX_IMPACT,
    ],
)
@pytest.mark.parametrize(
    "schema",
    (pytest.param(schema, id=str(schema)) for schema in Schema.variants()),
)
async def test_live_stream_to_dbn(
    tmp_path: pathlib.Path,
    test_data_path: Callable[[Dataset, Schema], pathlib.Path],
    live_client: client.Live,
    dataset: Dataset,
    schema: Schema,
) -> None:
    """
    Test that DBN data streamed by the MockLiveServerInterface is properly re-
    constructed client side.
    """
    # Arrange
    output = tmp_path / "output.dbn"

    live_client.subscribe(
        dataset=dataset,
        schema=schema,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )
    live_client.add_stream(output.open("wb", buffering=0))

    # Act
    live_client.start()

    await live_client.wait_for_close()

    expected_data = BytesIO(
        zstandard.ZstdDecompressor()
        .stream_reader(test_data_path(dataset, schema).open("rb"))
        .read(),
    )
    expected_data.seek(0)  # rewind

    # Assert
    assert output.read_bytes() == expected_data.read()


@pytest.mark.parametrize(
    "dataset",
    [
        Dataset.GLBX_MDP3,
        Dataset.XNAS_ITCH,
        Dataset.OPRA_PILLAR,
        Dataset.EQUS_MINI,
        Dataset.IFEU_IMPACT,
        Dataset.NDEX_IMPACT,
    ],
)
@pytest.mark.parametrize(
    "schema",
    (pytest.param(schema, id=str(schema)) for schema in Schema.variants()),
)
async def test_live_stream_to_dbn_from_path(
    tmp_path: pathlib.Path,
    test_data_path: Callable[[Dataset, Schema], pathlib.Path],
    live_client: client.Live,
    dataset: Dataset,
    schema: Schema,
) -> None:
    """
    Test that DBN data streamed by the MockLiveServerInterface is properly re-
    constructed client side when specifying a file as a path.
    """
    # Arrange
    output = tmp_path / "output.dbn"

    live_client.subscribe(
        dataset=dataset,
        schema=schema,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )
    live_client.add_stream(output)

    # Act
    live_client.start()

    await live_client.wait_for_close()

    expected_data = BytesIO(
        zstandard.ZstdDecompressor()
        .stream_reader(test_data_path(dataset, schema).open("rb"))
        .read(),
    )
    expected_data.seek(0)  # rewind

    # Assert
    assert output.read_bytes() == expected_data.read()


@pytest.mark.parametrize(
    "schema",
    (pytest.param(schema, id=str(schema)) for schema in Schema.variants()),
)
@pytest.mark.parametrize(
    "buffer_size",
    (
        1,
        2,
        3,
    ),
)
async def test_live_stream_to_dbn_with_tiny_buffer(
    tmp_path: pathlib.Path,
    test_data_path: Callable[[Dataset, Schema], pathlib.Path],
    live_client: client.Live,
    schema: Schema,
    monkeypatch: pytest.MonkeyPatch,
    buffer_size: int,
) -> None:
    """
    Test that DBN data streamed by the MockLiveServerInterface is properly re-
    constructed client side when using the small values for RECV_BUFFER_SIZE.
    """
    # Arrange
    monkeypatch.setattr(protocol, "RECV_BUFFER_SIZE", buffer_size)
    output = tmp_path / "output.dbn"

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=schema,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )
    live_client.add_stream(output.open("wb", buffering=0))

    # Act
    live_client.start()

    await live_client.wait_for_close()

    expected_data = BytesIO(
        zstandard.ZstdDecompressor()
        .stream_reader(test_data_path(Dataset.GLBX_MDP3, schema).open("rb"))
        .read(),
    )
    expected_data.seek(0)  # rewind

    # Assert
    assert output.read_bytes() == expected_data.read()


async def test_live_disconnect_async(
    live_client: client.Live,
) -> None:
    """
    Simulates a disconnection event with an exception.

    This tests that wait_for_close properly raises a BentoError from the
    exception.

    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema="mbo",
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )
    live_client.start()
    assert live_client._session is not None

    wait = live_client.wait_for_close()

    # Act
    protocol = live_client._session._protocol
    protocol.disconnected.set_exception(Exception("test"))

    # Assert
    with pytest.raises(BentoError) as exc:
        await wait

    exc.match(r"test")


def test_live_disconnect(
    live_client: client.Live,
) -> None:
    """
    Simulates a disconnection event with an exception.

    This tests that block_for_close properly raises a BentoError from
    the exception.

    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema="mbo",
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )
    live_client.start()
    assert live_client._session is not None

    # Act
    protocol = live_client._session._protocol
    protocol.disconnected.set_exception(Exception("test"))

    # Assert
    with pytest.raises(BentoError) as exc:
        live_client.block_for_close()

    exc.match(r"test")


async def test_live_terminate(
    live_client: client.Live,
) -> None:
    """
    Test that terminate closes the connection.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    # Act
    live_client.terminate()
    await live_client.wait_for_close()

    # Assert
    assert not live_client.is_connected()


@pytest.mark.parametrize(
    "schema",
    (pytest.param(schema, id=str(schema)) for schema in Schema.variants()),
)
async def test_live_iteration_with_reuse(
    live_client: client.Live,
    test_data_path: Callable[[Dataset, Schema], pathlib.Path],
    schema: Schema,
) -> None:
    """
    Test that the client can be reused while iterating.

    The iteration should yield every record.

    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=schema,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    assert live_client.is_connected()
    assert live_client.dataset == Dataset.GLBX_MDP3

    my_iter = iter(live_client)

    await live_client.wait_for_close()

    assert not live_client.is_connected()

    # Act
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=schema,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    live_client.start()

    await live_client.wait_for_close()

    assert not live_client.is_connected()

    expected_data = BytesIO(
        zstandard.ZstdDecompressor()
        .stream_reader(test_data_path(Dataset.GLBX_MDP3, schema).open("rb"))
        .read(),
    )
    dbn = DBNStore.from_bytes(expected_data)

    # Assert
    records = list(my_iter)
    assert len(records) == 2 * len(list(dbn))
    for record in records:
        assert isinstance(record, SCHEMA_STRUCT_MAP[schema])


@pytest.mark.parametrize(
    "schema",
    (pytest.param(schema, id=str(schema)) for schema in Schema.variants()),
)
async def test_live_callback_with_reuse(
    live_client: client.Live,
    test_data_path: Callable[[Dataset, Schema], pathlib.Path],
    schema: Schema,
) -> None:
    """
    Test that the client can be reused with a callback.

    That callback should emit every record, but needs to be re-added.

    """
    # Arrange
    records: list[DBNRecord] = []

    # Act, Assert
    for _ in range(5):
        live_client.add_callback(records.append)
        live_client.subscribe(
            dataset=Dataset.GLBX_MDP3,
            schema=schema,
            stype_in=SType.RAW_SYMBOL,
            symbols="TEST",
        )

        assert live_client.is_connected()
        assert live_client.dataset == Dataset.GLBX_MDP3

        live_client.start()

        await live_client.wait_for_close()
        assert not live_client.is_connected()

    expected_data = BytesIO(
        zstandard.ZstdDecompressor()
        .stream_reader(test_data_path(Dataset.GLBX_MDP3, schema).open("rb"))
        .read(),
    )
    dbn = DBNStore.from_bytes(expected_data)
    assert len(records) == 5 * len(list(dbn))

    for record in records:
        assert isinstance(record, SCHEMA_STRUCT_MAP[schema])


@pytest.mark.parametrize(
    "schema",
    (pytest.param(schema, id=str(schema)) for schema in Schema.variants()),
)
async def test_live_stream_with_reuse(
    tmp_path: pathlib.Path,
    live_client: client.Live,
    schema: Schema,
) -> None:
    """
    Test that the client can be reused with an output stream.

    That output stream should be a valid DBN stream.

    """
    # Arrange
    if schema in (
        "ohlcv-eod",
        "imbalance",
        "cbbo",
        "cbbo-1s",
        "cbbo-1m",
        "tcbbo",
        "cmbp-1",
        "bbo-1s",
        "bbo-1m",
    ):
        pytest.skip(f"no stub data for {schema} schema")

    output = tmp_path / "output.dbn"
    live_client.add_stream(output.open("wb"))

    # Act
    for _ in range(3):
        live_client.subscribe(
            dataset=Dataset.GLBX_MDP3,
            schema=schema,
            stype_in=SType.RAW_SYMBOL,
            symbols="TEST",
        )

        assert live_client.is_connected()
        assert live_client.dataset == Dataset.GLBX_MDP3

        live_client.start()

        await live_client.wait_for_close()
        assert not live_client.is_connected()

    data = DBNStore.from_file(output)

    # Assert
    records = list(data)
    for record in records:
        assert isinstance(record, SCHEMA_STRUCT_MAP[schema])


async def test_live_connection_reuse_cram_failure(
    mock_live_server: MockLiveServerInterface,
    test_api_key: str,
) -> None:
    """
    Test that a client with a failed connection can be reused.
    """
    # Arrange
    live_client = client.Live(
        key=test_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
    )

    # Act, Assert
    with pytest.raises(BentoError) as exc:
        live_client.subscribe(
            dataset=Dataset.GLBX_MDP3,
            schema=Schema.MBO,
        )

    # Ensure this was an authentication error
    exc.match(r"User authentication failed:")

    async with mock_live_server.api_key_context(test_api_key):
        live_client.subscribe(
            dataset=Dataset.GLBX_MDP3,
            schema=Schema.MBO,
        )

        assert live_client.is_connected()


async def test_live_callback_exception_handler(
    live_client: client.Live,
) -> None:
    """
    Test exceptions that occur during callbacks are dispatched to the assigned
    exception handler.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    exceptions: list[Exception] = []

    def callback(_: DBNRecord) -> None:
        raise RuntimeError("this is a test")

    live_client.add_callback(callback, exceptions.append)

    # Act
    live_client.start()
    await live_client.wait_for_close()

    # Assert
    assert len(exceptions) == 4


async def test_live_stream_exception_handler(
    live_client: client.Live,
) -> None:
    """
    Test exceptions that occur during stream writes are dispatched to the
    assigned exception handler.
    """
    # Arrange
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    exceptions: list[Exception] = []

    stream = BytesIO()
    live_client.add_stream(stream, exceptions.append)
    stream.close()

    # Act
    live_client.start()

    # Assert
    await live_client.wait_for_close()
    assert exceptions
