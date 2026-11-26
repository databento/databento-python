from __future__ import annotations

import asyncio
import platform
from unittest.mock import MagicMock

import pandas as pd
import pytest
from databento_dbn import DBNRecord

from databento import Dataset
from databento import Schema
from databento import SType
from databento.common.constants import ALL_SYMBOLS
from databento.common.enums import ReconnectPolicy
from databento.live import client
from databento.live import session
from databento.live.gateway import AuthenticationRequest
from databento.live.gateway import SessionStart
from databento.live.gateway import SubscriptionRequest
from databento.live.gateway import UnsubscribeRequest
from databento.live.protocol import DatabentoLiveProtocol
from tests.mockliveserver.fixture import MockLiveServerInterface


# TODO(nm): Remove when stable
if platform.system() == "Windows":
    pytest.skip(reason="Skip on Windows due to flakiness", allow_module_level=True)


async def test_reconnect_policy_none(
    test_live_api_key: str,
    mock_live_server: MockLiveServerInterface,
) -> None:
    """
    Test that a reconnect policy of "none" does not reconnect the client.
    """
    # Arrange
    live_client = client.Live(
        key=test_live_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
        reconnect_policy=ReconnectPolicy.NONE,
    )

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )
    live_client._loop.call_soon_threadsafe(
        live_client._session._protocol.disconnected.set_exception,
        ConnectionResetError,
    )

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    # Act
    await mock_live_server.disconnect(
        session_id=live_client._session.session_id,
    )

    # Assert
    with pytest.raises(TimeoutError):
        await mock_live_server.wait_for_message_of_type(AuthenticationRequest)


async def test_reconnect_before_start(
    test_live_api_key: str,
    mock_live_server: MockLiveServerInterface,
    reconnect_policy: ReconnectPolicy = ReconnectPolicy.RECONNECT,
) -> None:
    """
    Test that a reconnect policy of "reconnect" reconnects a client but does
    not send the session start command if the session was not streaming
    previously.
    """
    # Arrange
    live_client = client.Live(
        key=test_live_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
        reconnect_policy=reconnect_policy,
    )

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    records: list[DBNRecord] = []
    live_client.add_callback(records.append)

    # Act
    live_client._loop.call_soon_threadsafe(
        live_client._session._protocol.disconnected.set_exception,
        ConnectionResetError,
    )
    await mock_live_server.disconnect(
        session_id=live_client._session.session_id,
    )

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    live_client.stop()

    # Assert
    with pytest.raises(TimeoutError):
        await mock_live_server.wait_for_message_of_type(SessionStart)


@pytest.mark.parametrize(
    "schema",
    (pytest.param(schema, id=str(schema)) for schema in Schema.variants()),
)
@pytest.mark.parametrize(
    "stype_in",
    (pytest.param(stype_in, id=str(stype_in)) for stype_in in SType.variants()),
)
@pytest.mark.parametrize(
    "symbols",
    [
        ("TEST0",),
        ("TEST0", "TEST1"),
        ("TEST0", "TEST1", "TEST2"),
    ],
)
@pytest.mark.parametrize(
    "start,snapshot",
    [
        (0, False),
        (None, True),
    ],
)
async def test_reconnect_subscriptions(
    test_live_api_key: str,
    mock_live_server: MockLiveServerInterface,
    schema: Schema,
    stype_in: SType,
    symbols: tuple[str, ...],
    start: int | None,
    snapshot: bool,
    reconnect_policy: ReconnectPolicy = ReconnectPolicy.RECONNECT,
) -> None:
    """
    Test that a reconnect policy of "reconnect" re-sends the subscriptions as a
    single request with a start of `None`.
    """
    # Arrange
    live_client = client.Live(
        key=test_live_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
        reconnect_policy=reconnect_policy,
    )

    for symbol in symbols:
        live_client.subscribe(
            dataset=Dataset.GLBX_MDP3,
            schema=schema,
            stype_in=stype_in,
            symbols=symbol,
            start=start,
            snapshot=snapshot,
        )

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    # Act
    live_client._loop.call_soon_threadsafe(
        live_client._session._protocol.disconnected.set_exception,
        ConnectionResetError,
    )
    await mock_live_server.disconnect(
        session_id=live_client._session.session_id,
    )

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    sub = await mock_live_server.wait_for_message_of_type(SubscriptionRequest)

    live_client.stop()

    # Assert
    assert sub.schema == schema
    assert sub.stype_in == stype_in
    assert sub.symbols == ",".join(symbols)
    assert sub.start is None
    assert sub.snapshot == str(int(snapshot))
    assert int(sub.is_last) == 1
    assert sub.id is None


async def test_reconnect_combines_requests(
    test_live_api_key: str,
    mock_live_server: MockLiveServerInterface,
) -> None:
    """
    Test that a reconnect policy of "reconnect" recreates the session with the
    fewest subscription and unsubscribe requests that leave the gateway with
    the same symbols.
    """
    # Arrange
    live_client = client.Live(
        key=test_live_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
        reconnect_policy=ReconnectPolicy.RECONNECT,
    )
    large_symbol_list = [f"TEST{i}" for i in range(1200)]

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols=large_symbol_list,
        start=0,
    )
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.TRADES,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST0",
        start=0,
    )
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols=["TEST0", "OTHER"],
    )
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.PARENT,
        symbols="ES.FUT",
    )
    live_client.unsubscribe(
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST0",
    )
    live_client.unsubscribe(
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST1",
    )
    live_client.unsubscribe(
        schema=Schema.MBO,
        stype_in=SType.PARENT,
        symbols="ES.FUT",
    )
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.TRADES,
        stype_in=SType.RAW_SYMBOL,
    )
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST2",
    )

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    # Act
    live_client._loop.call_soon_threadsafe(
        live_client._session._protocol.disconnected.set_exception,
        ConnectionResetError,
    )
    await mock_live_server.disconnect(
        session_id=live_client._session.session_id,
    )

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    # Messages of other types are skipped while waiting, so a request sent out
    # of order causes a later wait to time out
    mbo_subs = [
        await mock_live_server.wait_for_message_of_type(SubscriptionRequest) for _ in range(4)
    ]
    mbo_unsubs = [
        await mock_live_server.wait_for_message_of_type(UnsubscribeRequest) for _ in range(2)
    ]
    last_subs = [
        await mock_live_server.wait_for_message_of_type(SubscriptionRequest) for _ in range(3)
    ]

    live_client.stop()

    # Assert
    subs = [*mbo_subs, *last_subs]
    assert all(sub.start is None for sub in subs)
    assert all(sub.id is None for sub in subs)

    # The raw symbol subscriptions made before the first unsubscribe are
    # combined into one request, without the repeated symbol
    assert [sub.schema for sub in mbo_subs] == ["mbo"] * 4
    assert [sub.stype_in for sub in mbo_subs] == ["raw_symbol"] * 3 + ["parent"]
    assert ",".join(sub.symbols for sub in mbo_subs[:3]).split(",") == [
        *large_symbol_list,
        "OTHER",
    ]
    assert [int(sub.is_last) for sub in mbo_subs] == [0, 0, 1, 1]
    assert mbo_subs[3].symbols == "ES.FUT"

    # Only consecutive unsubscribes with the same stype_in are combined
    assert [unsub.schema for unsub in mbo_unsubs] == ["mbo"] * 2
    assert [unsub.stype_in for unsub in mbo_unsubs] == ["raw_symbol", "parent"]
    assert [unsub.symbols for unsub in mbo_unsubs] == ["TEST0,TEST1", "ES.FUT"]
    assert [int(unsub.is_last) for unsub in mbo_unsubs] == [1, 1]

    # A subscription sent after an unsubscribe for its schema stays after it,
    # and a subscription for all symbols is not combined with other symbols
    assert [sub.schema for sub in last_subs] == ["mbo", "trades", "trades"]
    assert [sub.symbols for sub in last_subs] == ["TEST2", "TEST0", "ALL_SYMBOLS"]
    assert [int(sub.is_last) for sub in last_subs] == [1, 1, 1]

    # The stored requests are left as they were originally sent
    stored_subs = [sub for batch in live_client.subscription_requests for sub in batch]
    assert [sub.id for sub in stored_subs] == [0, 0, 0, 1, 2, 3, 4, 5]
    assert [sub.start for sub in stored_subs] == [0, 0, 0, 0, None, None, None, None]


async def test_reconnect_keeps_incompatible_requests_separate(
    test_live_api_key: str,
    mock_live_server: MockLiveServerInterface,
) -> None:
    """
    Test that a reconnect policy of "reconnect" does not combine subscriptions
    with a different `snapshot`, or an unsubscribe for all symbols with other
    symbols.
    """
    # Arrange
    live_client = client.Live(
        key=test_live_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
        reconnect_policy=ReconnectPolicy.RECONNECT,
    )

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST0",
    )
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST1",
        snapshot=True,
    )
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST2",
    )
    live_client.unsubscribe(
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST0",
    )
    live_client.unsubscribe(
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols=["TEST0", "TEST1"],
    )
    live_client.unsubscribe(
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols=ALL_SYMBOLS,
    )
    live_client.unsubscribe(
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST2",
    )

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    # Act
    live_client._loop.call_soon_threadsafe(
        live_client._session._protocol.disconnected.set_exception,
        ConnectionResetError,
    )
    await mock_live_server.disconnect(
        session_id=live_client._session.session_id,
    )

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    subs = [await mock_live_server.wait_for_message_of_type(SubscriptionRequest) for _ in range(2)]
    unsubs = [await mock_live_server.wait_for_message_of_type(UnsubscribeRequest) for _ in range(3)]

    live_client.stop()

    # Assert
    assert [sub.symbols for sub in subs] == ["TEST0,TEST2", "TEST1"]
    assert [sub.snapshot for sub in subs] == ["0", "1"]
    assert [int(sub.is_last) for sub in subs] == [1, 1]

    # The repeated symbol is only sent once
    assert [unsub.symbols for unsub in unsubs] == ["TEST0,TEST1", "ALL_SYMBOLS", "TEST2"]
    assert [int(unsub.is_last) for unsub in unsubs] == [1, 1, 1]


async def test_reconnect_batches_combined_subscriptions(
    test_live_api_key: str,
    mock_live_server: MockLiveServerInterface,
) -> None:
    """
    Test that a reconnect policy of "reconnect" breaks a subscription combined
    from several requests into multiple messages when it has a large symbol
    list.
    """
    # Arrange
    live_client = client.Live(
        key=test_live_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
        reconnect_policy=ReconnectPolicy.RECONNECT,
    )
    symbols = [f"TEST{i}" for i in range(700)]

    # Each subscription fits in a single message and the second repeats 100
    # symbols of the first
    for symbol_list in (symbols[:300], symbols[200:500], symbols[500:]):
        live_client.subscribe(
            dataset=Dataset.GLBX_MDP3,
            schema=Schema.MBO,
            stype_in=SType.RAW_SYMBOL,
            symbols=symbol_list,
        )

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    # Act
    live_client._loop.call_soon_threadsafe(
        live_client._session._protocol.disconnected.set_exception,
        ConnectionResetError,
    )
    await mock_live_server.disconnect(
        session_id=live_client._session.session_id,
    )

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    subs = [await mock_live_server.wait_for_message_of_type(SubscriptionRequest) for _ in range(2)]

    live_client.stop()

    # Assert
    assert [len(sub.symbols.split(",")) for sub in subs] == [500, 200]
    assert ",".join(sub.symbols for sub in subs).split(",") == symbols
    assert [int(sub.is_last) for sub in subs] == [0, 1]


async def test_reconnect_replays_unsubscriptions_in_order(
    test_live_api_key: str,
    mock_live_server: MockLiveServerInterface,
) -> None:
    """
    Test that a reconnect policy of "reconnect" re-sends subscription and
    unsubscribe requests for the same schema in the order they were originally
    sent.
    """
    # Arrange
    live_client = client.Live(
        key=test_live_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
        reconnect_policy=ReconnectPolicy.RECONNECT,
    )

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST0",
    )
    live_client.unsubscribe(
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST0",
    )
    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST1",
    )

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    # Act
    live_client._loop.call_soon_threadsafe(
        live_client._session._protocol.disconnected.set_exception,
        ConnectionResetError,
    )
    await mock_live_server.disconnect(
        session_id=live_client._session.session_id,
    )

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    # Messages of other types are skipped while waiting, so a request sent out
    # of order causes a later wait to time out
    first_sub = await mock_live_server.wait_for_message_of_type(SubscriptionRequest)
    unsub = await mock_live_server.wait_for_message_of_type(UnsubscribeRequest)
    second_sub = await mock_live_server.wait_for_message_of_type(SubscriptionRequest)

    live_client.stop()

    # Assert
    assert first_sub.symbols == "TEST0"
    assert unsub.symbols == "TEST0"
    assert second_sub.symbols == "TEST1"
    # Nothing is combined because the request types alternate
    assert int(first_sub.is_last) == 1
    assert int(unsub.is_last) == 1
    assert int(second_sub.is_last) == 1


async def test_reconnect_callback(
    test_live_api_key: str,
    mock_live_server: MockLiveServerInterface,
    reconnect_policy: ReconnectPolicy = ReconnectPolicy.RECONNECT,
) -> None:
    """
    Test that a reconnect policy of "reconnect" will cause a user supplied
    reconnection callback to be executed when a reconnection occurs.
    """
    # Arrange
    live_client = client.Live(
        key=test_live_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
        reconnect_policy=reconnect_policy,
    )

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    reconnect_callback = MagicMock()
    live_client.add_reconnect_callback(reconnect_callback)

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    # Act
    live_client.start()

    live_client._loop.call_soon_threadsafe(
        live_client._session._protocol.disconnected.set_exception,
        ConnectionResetError,
    )
    await mock_live_server.wait_for_message_of_type(SessionStart)

    await live_client.wait_for_close()

    # Assert
    reconnect_callback.assert_called()
    args, _ = reconnect_callback.call_args
    gap_start, gap_end = args
    assert isinstance(gap_start, pd.Timestamp)
    assert isinstance(gap_end, pd.Timestamp)


async def test_reconnect_after_heartbeat_timeout(
    test_live_api_key: str,
    mock_live_server: MockLiveServerInterface,
    monkeypatch: pytest.MonkeyPatch,
    reconnect_policy: ReconnectPolicy = ReconnectPolicy.RECONNECT,
) -> None:
    """
    Test that the heartbeat monitor reconnects the client every time the
    gateway times out, not just the first time.
    """
    # Arrange
    # this mock will make the connection go silent instead of disconnecting
    monkeypatch.setattr(DatabentoLiveProtocol, "eof_received", lambda _: True)
    monkeypatch.setattr(session, "CLIENT_TIMEOUT_MARGIN_SECONDS", 0)

    live_client = client.Live(
        key=test_live_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
        heartbeat_interval_s=1,
        reconnect_policy=reconnect_policy,
    )

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    # Act
    live_client.start()

    await mock_live_server.wait_for_message_of_type(SessionStart)

    # Assert
    for _ in range(2):
        await mock_live_server.wait_for_message_of_type(
            AuthenticationRequest,
            timeout=10,
        )
        await mock_live_server.wait_for_message_of_type(
            SessionStart,
            timeout=10,
        )

    live_client.stop()


async def test_reconnect_does_not_block_session_lock(
    test_live_api_key: str,
    mock_live_server: MockLiveServerInterface,
    monkeypatch: pytest.MonkeyPatch,
    reconnect_policy: ReconnectPolicy = ReconnectPolicy.RECONNECT,
) -> None:
    """
    Test that the session lock is not held while a reconnection attempt is in
    progress, so calls like `is_connected` from another thread do not block.
    """
    # Arrange
    live_client = client.Live(
        key=test_live_api_key,
        gateway=mock_live_server.host,
        port=mock_live_server.port,
        reconnect_policy=reconnect_policy,
    )

    live_client.subscribe(
        dataset=Dataset.GLBX_MDP3,
        schema=Schema.MBO,
        stype_in=SType.RAW_SYMBOL,
        symbols="TEST",
    )

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    # hold the reconnection attempt open until we release it
    connect_started = asyncio.Event()
    connect_release = asyncio.Event()
    test_loop = asyncio.get_running_loop()
    original_connect_task = session.LiveSession._connect_task

    async def slow_connect_task(self: session.LiveSession, dataset: str) -> object:
        test_loop.call_soon_threadsafe(connect_started.set)
        await asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(connect_release.wait(), test_loop),
        )
        return await original_connect_task(self, dataset=dataset)

    monkeypatch.setattr(session.LiveSession, "_connect_task", slow_connect_task)

    # Act
    live_client._loop.call_soon_threadsafe(
        live_client._session._protocol.disconnected.set_exception,
        ConnectionResetError,
    )
    await mock_live_server.disconnect(
        session_id=live_client._session.session_id,
    )
    await asyncio.wait_for(connect_started.wait(), timeout=5)

    # Assert
    await asyncio.wait_for(
        test_loop.run_in_executor(None, live_client.is_connected),
        timeout=1,
    )

    connect_release.set()
    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)
    live_client.stop()
