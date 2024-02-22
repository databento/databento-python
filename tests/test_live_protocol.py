import asyncio
from unittest.mock import MagicMock

import pytest
from databento.common.publishers import Dataset
from databento.live.protocol import DatabentoLiveProtocol
from databento_dbn import Schema
from databento_dbn import SType

from tests.mock_live_server import MockLiveServer


@pytest.mark.parametrize(
    "dataset",
    [
        Dataset.GLBX_MDP3,
        Dataset.XNAS_ITCH,
        Dataset.OPRA_PILLAR,
        Dataset.DBEQ_BASIC,
        Dataset.IFEU_IMPACT,
        Dataset.NDEX_IMPACT,
    ],
)
async def test_protocol_connection(
    mock_live_server: MockLiveServer,
    test_api_key: str,
    dataset: Dataset,
) -> None:
    """
    Test the low-level DatabentoLiveProtocol can be used to establish a
    connection to the live subscription gateway.
    """
    # Arrange
    transport, protocol = await asyncio.get_event_loop().create_connection(
        protocol_factory=lambda: DatabentoLiveProtocol(
            api_key=test_api_key,
            dataset=dataset,
        ),
        host=mock_live_server.host,
        port=mock_live_server.port,
    )

    # Act, Assert
    await asyncio.wait_for(protocol.authenticated, timeout=1)
    transport.close()
    await asyncio.wait_for(protocol.disconnected, timeout=1)


@pytest.mark.parametrize(
    "dataset",
    [
        Dataset.GLBX_MDP3,
        Dataset.XNAS_ITCH,
        Dataset.OPRA_PILLAR,
        Dataset.DBEQ_BASIC,
        Dataset.IFEU_IMPACT,
        Dataset.NDEX_IMPACT,
    ],
)
async def test_protocol_connection_streaming(
    monkeypatch: pytest.MonkeyPatch,
    mock_live_server: MockLiveServer,
    test_api_key: str,
    dataset: Dataset,
) -> None:
    """
    Test the low-level DatabentoLiveProtocol can be used to stream DBN records
    from the live subscription gateway.
    """
    # Arrange
    monkeypatch.setattr(
        DatabentoLiveProtocol, "received_metadata", metadata_mock := MagicMock(),
    )
    monkeypatch.setattr(
        DatabentoLiveProtocol, "received_record", record_mock := MagicMock(),
    )

    _, protocol = await asyncio.get_event_loop().create_connection(
        protocol_factory=lambda: DatabentoLiveProtocol(
            api_key=test_api_key,
            dataset=dataset,
        ),
        host=mock_live_server.host,
        port=mock_live_server.port,
    )

    await asyncio.wait_for(protocol.authenticated, timeout=1)

    # Act
    protocol.subscribe(
        schema=Schema.TRADES,
        symbols="TEST",
        stype_in=SType.RAW_SYMBOL,
    )

    protocol.start()
    await asyncio.wait_for(protocol.disconnected, timeout=1)

    # Assert
    assert metadata_mock.call_count == 1
    assert record_mock.call_count == 4
