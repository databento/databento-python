import asyncio
from unittest.mock import MagicMock

import pytest
from databento.common.enums import Schema
from databento.common.enums import SType
from databento.live.protocol import DatabentoLiveProtocol

from tests.mock_live_server import MockLiveServer


async def test_protocol_connection(
    mock_live_server: MockLiveServer,
    test_api_key: str,
) -> None:
    """
    Test the low-level DatabentoLiveProtocol can be used to establish a
    connection to the live subscription gateway.
    """
    transport, protocol = await asyncio.get_event_loop().create_connection(
        protocol_factory=lambda: DatabentoLiveProtocol(
            api_key=test_api_key,
            dataset="TEST",
        ),
        host=mock_live_server.host,
        port=mock_live_server.port,
    )

    await asyncio.wait_for(protocol.authenticated, timeout=1)

    transport.close()

    await asyncio.wait_for(protocol.disconnected, timeout=1)


async def test_protocol_connection_streaming(
    monkeypatch: pytest.MonkeyPatch,
    mock_live_server: MockLiveServer,
    test_api_key: str,
) -> None:
    """
    Test the low-level DatabentoLiveProtocol can be used to stream DBN records
    from the live subscription gateway.
    """
    monkeypatch.setattr(DatabentoLiveProtocol, "received_metadata", metadata_mock := MagicMock())
    monkeypatch.setattr(DatabentoLiveProtocol, "received_record", record_mock := MagicMock())

    transport, protocol = await asyncio.get_event_loop().create_connection(
        protocol_factory=lambda: DatabentoLiveProtocol(
            api_key=test_api_key,
            dataset="TEST",
        ),
        host=mock_live_server.host,
        port=mock_live_server.port,
    )

    await asyncio.wait_for(protocol.authenticated, timeout=1)

    protocol.subscribe(
        schema=Schema.MBO,
        symbols="TEST",
        stype_in=SType.RAW_SYMBOL,
    )

    protocol.start()
    await asyncio.wait_for(protocol.started.wait(), timeout=1)

    await asyncio.wait_for(protocol.disconnected, timeout=1)

    assert metadata_mock.call_count == 1
    assert record_mock.call_count == 4
