from __future__ import annotations

import asyncio
import platform
from unittest.mock import MagicMock

import pandas as pd
import pytest

from databento import Dataset
from databento import Schema
from databento import SType
from databento.common.enums import ReconnectPolicy
from databento.common.types import DBNRecord
from databento.live import client
from databento.live.gateway import AuthenticationRequest
from databento.live.gateway import SessionStart
from databento.live.gateway import SubscriptionRequest
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
    live_client._session._protocol.disconnected.set_exception(ConnectionResetError)

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    # Act
    await mock_live_server.disconnect(
        session_id=live_client._session.session_id,
    )

    # Assert
    with pytest.raises(asyncio.TimeoutError):
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
    live_client._session._protocol.disconnected.set_exception(ConnectionResetError)
    await mock_live_server.disconnect(
        session_id=live_client._session.session_id,
    )

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    live_client.stop()

    # Assert
    with pytest.raises(asyncio.TimeoutError):
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
    Test that a reconnect policy of "reconnect" re-sends the subscription
    requests with a start of `None`.
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
    live_client._session._protocol.disconnected.set_exception(ConnectionResetError)
    await mock_live_server.disconnect(
        session_id=live_client._session.session_id,
    )

    await mock_live_server.wait_for_message_of_type(AuthenticationRequest)

    reconnect_subscriptions: list[SubscriptionRequest] = []
    for _ in range(len(symbols)):
        request = await mock_live_server.wait_for_message_of_type(SubscriptionRequest)
        reconnect_subscriptions.append(request)

    live_client.stop()

    # Assert
    for i, symbol in enumerate(symbols):
        sub = reconnect_subscriptions[i]
        assert sub.schema == schema
        assert sub.stype_in == stype_in
        assert sub.symbols == symbol
        assert sub.start is None
        assert sub.snapshot == str(int(snapshot))


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
    live_client._session._protocol.disconnected.set_exception(ConnectionResetError)

    await mock_live_server.wait_for_message_of_type(SessionStart)

    await live_client.wait_for_close()

    # Assert
    reconnect_callback.assert_called()
    args, _ = reconnect_callback.call_args
    gap_start, gap_end = args
    assert isinstance(gap_start, pd.Timestamp)
    assert isinstance(gap_end, pd.Timestamp)
