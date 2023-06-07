"""Unit tests for the Live client."""
import pathlib
import platform
from io import BytesIO
from typing import Callable, List
from unittest.mock import MagicMock

import databento_dbn
import pytest
import zstandard
from databento.common.cram import BUCKET_ID_LENGTH
from databento.common.dbnstore import DBNStore
from databento.common.enums import Dataset, Encoding, Schema, SType
from databento.common.error import BentoError
from databento.common.symbology import ALL_SYMBOLS
from databento.live import DBNRecord, client, gateway, protocol, session

from tests.mock_live_server import MockLiveServer


def test_live_connection_refused(
    test_api_key: str,
) -> None:
    """
    Test that a refused connection raises a BentoError.
    """
    live_client = client.Live(
        key=test_api_key,
        # Connect to something that does not exist
        gateway="localhost",
        port=0,
    )

    with pytest.raises(BentoError) as exc:
        live_client.subscribe(
            dataset=Dataset.GLBX_MDP3,
            schema=Schema.MBO,
        )

    exc.match(r"Connection to .+ failed.")


def test_live_connection_timeout(
    monkeypatch: pytest.MonkeyPatch,
    mock_live_server: MockLiveServer,
    test_api_key: str,
) -> None:
    """
    Test that a timeout raises a BentoError. Mock the create_connection
    function so that it never completes and set a timeout of 0.
    """
    monkeypatch.setattr(
        session,
        "CONNECT_TIMEOUT_SECONDS",
        0,
    )

    live_client = client.Live(
        key=test_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
    )

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
    mock_live_server: MockLiveServer,
    test_api_key: str,
    gateway: str,
) -> None:
    """
    Test that specifying an invalid gateway raises a
    ValueError.
    """
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
    mock_live_server: MockLiveServer,
    test_api_key: str,
    port: object,
) -> None:
    """
    Test that specifying an invalid port raises a
    ValueError.
    """
    with pytest.raises(ValueError):
        client.Live(
            key=test_api_key,
            gateway=mock_live_server.host,
            port=port,  # type: ignore
        )


def test_live_connection_cram_failure(
    mock_live_server: MockLiveServer,
    monkeypatch: pytest.MonkeyPatch,
    test_api_key: str,
) -> None:
    """
    Test that a failed auth message due to an incorrect CRAM
    raies a BentoError.
    """

    # Dork up the API key in the mock client to fail CRAM
    bucket_id = test_api_key[-BUCKET_ID_LENGTH:]
    invalid_key = "db-invalidkey00000000000000FFFFF"
    monkeypatch.setitem(mock_live_server._user_api_keys, bucket_id, invalid_key)

    live_client = client.Live(
        key=test_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
    )

    with pytest.raises(BentoError) as exc:
        live_client.subscribe(
            dataset=Dataset.GLBX_MDP3,
            schema=Schema.MBO,
        )

    # Ensure this was an authentication error
    exc.match(r"User authentication failed:")


@pytest.mark.parametrize(
    "dataset",
    [pytest.param(dataset, id=str(dataset)) for dataset in Dataset],
)
def test_live_creation(
    test_api_key: str,
    mock_live_server: MockLiveServer,
    dataset: Dataset,
) -> None:
    """
    Test the live constructor and successful connection to
    the MockLiveServer.
    """
    live_client = client.Live(
        key=test_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
    )

    live_client.subscribe(
        dataset=dataset,
        schema=Schema.MBO,
    )

    assert live_client.gateway == mock_live_server.host
    assert live_client.port == mock_live_server.port
    assert live_client._key == test_api_key
    assert live_client.dataset == dataset
    assert live_client.is_connected() is True


def test_live_connect_auth(
    mock_live_server: MockLiveServer,
    live_client: client.Live,
) -> None:
    """
    Test the live sent a correct AuthenticationRequest message
    after connecting.
    """
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    message = mock_live_server.get_message_of_type(
        gateway.AuthenticationRequest,
        timeout=1,
    )

    assert message.auth.endswith(live_client.key[-BUCKET_ID_LENGTH:])
    assert message.dataset == live_client.dataset
    assert message.encoding == Encoding.DBN


def test_live_connect_auth_two_clients(
    mock_live_server: MockLiveServer,
    test_api_key: str,
) -> None:
    """
    Test the live sent a correct AuthenticationRequest message
    after connecting two distinct clients.
    """
    first = client.Live(
        key=test_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
    )

    second = client.Live(
        key=test_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
    )

    first.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    first_auth = mock_live_server.get_message_of_type(
        gateway.AuthenticationRequest,
        timeout=1,
    )
    assert first_auth.auth.endswith(first.key[-BUCKET_ID_LENGTH:])
    assert first_auth.dataset == first.dataset
    assert first_auth.encoding == Encoding.DBN

    second.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    second_auth = mock_live_server.get_message_of_type(
        gateway.AuthenticationRequest,
        timeout=1,
    )

    assert second_auth.auth.endswith(second.key[-BUCKET_ID_LENGTH:])
    assert second_auth.dataset == second.dataset
    assert second_auth.encoding == Encoding.DBN


def test_live_start(
    live_client: client.Live,
    mock_live_server: MockLiveServer,
) -> None:
    """
    Test the live sends a SesssionStart message upon calling
    start().
    """
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    assert live_client.is_connected() is True

    live_client.start()

    live_client.block_for_close()

    message = mock_live_server.get_message_of_type(
        gateway.SessionStart,
        timeout=1,
    )

    assert message.start_session


def test_live_start_twice(
    live_client: client.Live,
) -> None:
    """
    Test that calling start() twice raises a ValueError.
    """
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    live_client.start()

    with pytest.raises(ValueError):
        live_client.start()


@pytest.mark.parametrize(
    "schema",
    [pytest.param(schema, id=str(schema)) for schema in Schema],
)
@pytest.mark.parametrize(
    "stype_in",
    [pytest.param(stype, id=str(stype)) for stype in SType],
)
@pytest.mark.parametrize(
    "symbols",
    [
        pytest.param("NVDA", id="str"),
        pytest.param("ES,CL", id="str-list"),
        pytest.param(None, id="all-symbols"),
    ],
)
@pytest.mark.parametrize(
    "start",
    [
        None,
        "1680736543000000000",
    ],
)
def test_live_subscribe(
    live_client: client.Live,
    mock_live_server: MockLiveServer,
    schema: Schema,
    stype_in: SType,
    symbols: str,
    start: str,
) -> None:
    """
    Test various combination of subscription messages are serialized and
    correctly deserialized by the MockLiveServer.
    """
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=schema,
        stype_in=stype_in,
        symbols=symbols,
        start=start,
    )

    message = mock_live_server.get_message_of_type(
        gateway.SubscriptionRequest,
        timeout=1,
    )

    if symbols is None:
        symbols = ALL_SYMBOLS

    assert message.schema == schema
    assert message.stype_in == stype_in
    assert message.symbols == symbols
    assert message.start == start


@pytest.mark.usefixtures("mock_live_server")
def test_live_stop(
    live_client: client.Live,
) -> None:
    """
    Test that calling start() and stop() appropriately update the
    client state.
    """
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    assert live_client.is_connected() is True

    live_client.start()

    live_client.stop()
    live_client.block_for_close()


def test_live_stop_twice(
    live_client: client.Live,
) -> None:
    """
    Test that calling stop() twice does not raise an exception.
    """
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
    )

    live_client.stop()
    live_client.stop()


@pytest.mark.usefixtures("mock_live_server")
def test_live_block_for_close(
    live_client: client.Live,
) -> None:
    """
    Test that block_for_close unblocks when the connection
    is closed.
    """
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.INSTRUMENT_ID,
        symbols="ALL_SYMBOLS",
        start=None,
    )

    live_client.start()

    live_client.block_for_close()

    assert not live_client.is_connected()


def test_live_block_for_close_timeout(
    live_client: client.Live,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Test that block_for_close terminates the session when
    the timeout is reached.
    """
    monkeypatch.setattr(live_client, "terminate", MagicMock())
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.INSTRUMENT_ID,
        symbols="ALL_SYMBOLS",
        start=None,
    )
    live_client.block_for_close(timeout=0)
    live_client.terminate.assert_called_once()  # type: ignore


@pytest.mark.usefixtures("mock_live_server")
async def test_live_wait_for_close(
    live_client: client.Live,
) -> None:
    """
    Test that wait_for_close unblocks when the connection
    is closed.
    """
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.INSTRUMENT_ID,
        symbols="ALL_SYMBOLS",
        start=None,
    )

    live_client.start()

    await live_client.wait_for_close()

    assert not live_client.is_connected()


@pytest.mark.usefixtures("mock_live_server")
async def test_live_wait_for_close_timeout(
    live_client: client.Live,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Test that wait_for_close terminates the session when
    the timeout is reached.
    """
    monkeypatch.setattr(live_client, "terminate", MagicMock())

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.INSTRUMENT_ID,
        symbols="ALL_SYMBOLS",
        start=None,
    )

    await live_client.wait_for_close(timeout=0)

    live_client.terminate.assert_called_once()  # type: ignore


def test_live_add_callback(
    live_client: client.Live,
) -> None:
    """
    Test that calling add_callback adds that callback to the client.
    """

    def callback(_: object) -> None:
        pass

    live_client.add_callback(callback)
    assert live_client._user_callbacks == [callback]
    assert live_client._user_streams == []


def test_live_add_stream(
    live_client: client.Live,
) -> None:
    """
    Test that calling add_stream adds that stream to the client.
    """

    stream = BytesIO()

    live_client.add_stream(stream)
    assert live_client._user_callbacks == []
    assert live_client._user_streams == [stream]


def test_live_add_stream_invalid(
    tmp_path: pathlib.Path,
    live_client: client.Live,
) -> None:
    """
    Test that passing a non-writable stream to add_stream raises
    a ValueError.
    """
    with pytest.raises(ValueError):
        live_client.add_stream(object)  # type: ignore

    readable_file = tmp_path / "nope.txt"
    readable_file.touch()
    with pytest.raises(ValueError):
        live_client.add_stream(readable_file.open(mode="rb"))


@pytest.mark.skipif(platform.system() == "Windows", reason="flaky on windows runner")
async def test_live_async_iteration(
    live_client: client.Live,
) -> None:
    """
    Test async-iteration of DBN records.
    """
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    live_client.start()

    records: List[DBNRecord] = []
    async for record in live_client:
        records.append(record)

    assert len(records) == 2
    assert isinstance(records[0], databento_dbn.MBOMsg)
    assert isinstance(records[1], databento_dbn.MBOMsg)


@pytest.mark.skipif(platform.system() == "Windows", reason="flaky on windows runner")
async def test_live_async_iteration_backpressure(
    monkeypatch: pytest.MonkeyPatch,
    mock_live_server: MockLiveServer,
    test_api_key: str,
) -> None:
    """
    Test that a full queue disables reading on the
    transport but will resume it when the queue is
    depleted when iterating asynchronously.
    """
    monkeypatch.setattr(client, "DEFAULT_QUEUE_SIZE", 2)

    live_client = client.Live(
        key=test_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
    )

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    live_client.start()

    records: List[DBNRecord] = []
    async for record in live_client:
        records.append(record)

    assert len(records) == 2
    assert isinstance(records[0], databento_dbn.MBOMsg)
    assert isinstance(records[1], databento_dbn.MBOMsg)
    assert live_client._dbn_queue.empty()

@pytest.mark.skipif(platform.system() == "Windows", reason="flaky on windows runner")
async def test_live_async_iteration_dropped(
    monkeypatch: pytest.MonkeyPatch,
    mock_live_server: MockLiveServer,
    test_api_key: str,
) -> None:
    """
    Test that an artificially small queue size will
    drop messages when full.
    """
    monkeypatch.setattr(client, "DEFAULT_QUEUE_SIZE", 1)

    live_client = client.Live(
        key=test_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
    )

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    live_client.start()

    records: List[DBNRecord] = []
    async for record in live_client:
        records.append(record)

    assert len(records) == 1
    assert isinstance(records[0], databento_dbn.MBOMsg)
    assert live_client._dbn_queue.empty()


@pytest.mark.skipif(platform.system() == "Windows", reason="flaky on windows runner")
async def test_live_async_iteration_stop(
    live_client: client.Live,
) -> None:
    """
    Test that stopping in the middle of iteration does
    not prevent iterating the queue to completion.
    """
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    live_client.start()

    records = []
    async for record in live_client:
        records.append(record)
        live_client.stop()

    assert len(records) == 2
    assert isinstance(records[0], databento_dbn.MBOMsg)
    assert isinstance(records[1], databento_dbn.MBOMsg)

@pytest.mark.skipif(platform.system() == "Windows", reason="flaky on windows runner")
def test_live_sync_iteration(
    live_client: client.Live,
) -> None:
    """
    Test synchronous iteration of DBN records.
    """
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    live_client.start()

    records = []
    for record in live_client:
        records.append(record)

    assert len(records) == 2
    assert isinstance(records[0], databento_dbn.MBOMsg)
    assert isinstance(records[1], databento_dbn.MBOMsg)


async def test_live_callback(
    live_client: client.Live,
) -> None:
    """
    Test callback dispatch of DBN records.
    """
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

    live_client.add_callback(callback)

    live_client.start()

    await live_client.wait_for_close()

    assert len(records) == 2
    assert isinstance(records[0], databento_dbn.MBOMsg)
    assert isinstance(records[1], databento_dbn.MBOMsg)


@pytest.mark.parametrize(
    "schema",
    (pytest.param(schema, id=str(schema)) for schema in Schema),
)
async def test_live_stream_to_dbn(
    tmp_path: pathlib.Path,
    test_data_path: Callable[[Schema], pathlib.Path],
    live_client: client.Live,
    schema: Schema,
) -> None:
    """
    Test that DBN data streamed by the MockLiveServer is properly
    re-constructed client side.
    """
    output = tmp_path / "output.dbn"

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=schema,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )
    live_client.add_stream(output.open("wb", buffering=0))

    live_client.start()

    await live_client.wait_for_close()

    expected_data = BytesIO(
        zstandard.ZstdDecompressor()
        .stream_reader(test_data_path(schema).open("rb"))
        .read(),
    )
    expected_data.seek(0)  # rewind

    assert output.read_bytes() == expected_data.read()


@pytest.mark.parametrize(
    "schema",
    (pytest.param(schema, id=str(schema)) for schema in Schema),
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
    test_data_path: Callable[[Schema], pathlib.Path],
    live_client: client.Live,
    schema: Schema,
    monkeypatch: pytest.MonkeyPatch,
    buffer_size: int,
) -> None:
    """
    Test that DBN data streamed by the MockLiveServer is properly
    re-constructed client side when using the small values for MIN_BUFFER_SIZE.
    """
    monkeypatch.setattr(protocol, "MIN_BUFFER_SIZE", buffer_size)
    output = tmp_path / "output.dbn"

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=schema,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )
    live_client.add_stream(output.open("wb", buffering=0))

    live_client.start()

    await live_client.wait_for_close()

    expected_data = BytesIO(
        zstandard.ZstdDecompressor()
        .stream_reader(test_data_path(schema).open("rb"))
        .read(),
    )
    expected_data.seek(0)  # rewind

    assert output.read_bytes() == expected_data.read()


async def test_live_disconnect_async(
    live_client: client.Live,
) -> None:
    """
    Simulates a disconnection event with an exception.
    This tests that wait_for_close properly raises a
    BentoError from the exception.
    """
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema="mbo",
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )
    live_client.start()
    assert live_client._session is not None

    wait = live_client.wait_for_close()

    protocol = live_client._session._protocol
    protocol.disconnected.set_exception(Exception("test"))

    with pytest.raises(BentoError) as exc:
        await wait

    exc.match(r"connection lost")


def test_live_disconnect(
    live_client: client.Live,
) -> None:
    """
    Simulates a disconnection event with an exception.
    This tests that block_for_close properly raises a
    BentoError from the exception.
    """
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema="mbo",
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )
    live_client.start()
    assert live_client._session is not None

    protocol = live_client._session._protocol
    protocol.disconnected.set_exception(Exception("test"))

    with pytest.raises(BentoError) as exc:
        live_client.block_for_close()

    exc.match(r"connection lost")


async def test_live_terminate(
    live_client: client.Live,
) -> None:
    """
    Test that terminate closes the connection.
    """
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    live_client.terminate()

    await live_client.wait_for_close()
    assert not live_client.is_connected()


@pytest.mark.parametrize(
    "schema",
    (pytest.param(schema, id=str(schema)) for schema in Schema),
)
async def test_live_iteration_with_reconnect(
    live_client: client.Live,
    test_data_path: Callable[[Schema], pathlib.Path],
    schema: Schema,
) -> None:
    """
    Test that the client can reconnect to the same subscription
    while iterating. The iteration should yield every record.
    """
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=schema,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    assert live_client.is_connected()
    assert live_client.dataset == Dataset.GLBX_MDP3

    live_client.start()
    my_iter = iter(live_client)

    await live_client.wait_for_close()

    assert not live_client.is_connected()

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
        .stream_reader(test_data_path(schema).open("rb"))
        .read(),
    )
    dbn = DBNStore.from_bytes(expected_data)

    records = list(my_iter)
    assert len(records) == 2 * len(list(dbn))
    for record in records:
        assert isinstance(record, schema.get_record_type())


@pytest.mark.parametrize(
    "schema",
    (pytest.param(schema, id=str(schema)) for schema in Schema),
)
async def test_live_callback_with_reconnect(
    live_client: client.Live,
    test_data_path: Callable[[Schema], pathlib.Path],
    schema: Schema,
) -> None:
    """
    Test that the client can reconnect to the same subscription
    with a callback. That callback should emit every record.
    """
    records: List[DBNRecord] = []
    live_client.add_callback(records.append)

    for _ in range(5):
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
        .stream_reader(test_data_path(schema).open("rb"))
        .read(),
    )
    dbn = DBNStore.from_bytes(expected_data)
    assert len(records) == 5 * len(list(dbn))

    for record in records:
        assert isinstance(record, schema.get_record_type())


@pytest.mark.parametrize(
    "schema",
    (pytest.param(schema, id=str(schema)) for schema in Schema),
)
async def test_live_stream_with_reconnect(
    tmp_path: pathlib.Path,
    live_client: client.Live,
    schema: Schema,
) -> None:
    """
    Test that the client can reconnect to the same subscription
    with an output stream. That output stream should be readable.
    """
    output = tmp_path / "output.dbn"
    live_client.add_stream(output.open("wb", buffering=0))

    for _ in range(5):
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

    records = list(data)
    for record in records:
        assert isinstance(record, schema.get_record_type())
