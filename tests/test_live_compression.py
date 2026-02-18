from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest
from databento_dbn import Compression
from databento_dbn import Schema

from databento.common.publishers import Dataset
from databento.live.protocol import DatabentoLiveProtocol
from tests.mockliveserver.fixture import MockLiveServerInterface


@pytest.mark.parametrize("compression", [Compression.NONE, Compression.ZSTD])
async def test_protocol_connection_with_compression(
    mock_live_server: MockLiveServerInterface,
    test_live_api_key: str,
    compression: Compression,
) -> None:
    """
    Test protocol connection with different compression settings.
    """
    # Arrange
    transport, protocol = await asyncio.get_event_loop().create_connection(
        protocol_factory=lambda: DatabentoLiveProtocol(
            api_key=test_live_api_key,
            dataset=Dataset.GLBX_MDP3,
            compression=compression,
        ),
        host=mock_live_server.host,
        port=mock_live_server.port,
    )

    # Act, Assert
    await asyncio.wait_for(protocol.authenticated, timeout=1)
    transport.close()
    await asyncio.wait_for(protocol.disconnected, timeout=1)


@pytest.mark.parametrize("compression", [Compression.NONE, Compression.ZSTD])
async def test_protocol_streaming_with_compression(
    monkeypatch: pytest.MonkeyPatch,
    mock_live_server: MockLiveServerInterface,
    test_live_api_key: str,
    compression: Compression,
) -> None:
    """
    Test streaming records with different compression settings.
    """
    # Arrange
    monkeypatch.setattr(
        DatabentoLiveProtocol,
        "received_metadata",
        metadata_mock := MagicMock(),
    )
    monkeypatch.setattr(
        DatabentoLiveProtocol,
        "received_record",
        record_mock := MagicMock(),
    )

    _, protocol = await asyncio.get_event_loop().create_connection(
        protocol_factory=lambda: DatabentoLiveProtocol(
            api_key=test_live_api_key,
            dataset=Dataset.GLBX_MDP3,
            compression=compression,
        ),
        host=mock_live_server.host,
        port=mock_live_server.port,
    )

    await asyncio.wait_for(protocol.authenticated, timeout=1)

    # Act
    protocol.subscribe(
        schema=Schema.MBO,
        symbols="ESM4",
    )
    protocol.start()

    # Assert
    await asyncio.wait_for(protocol.disconnected, timeout=5)
    metadata_mock.assert_called()
    record_mock.assert_called()
