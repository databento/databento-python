from __future__ import annotations

import pathlib
from typing import Callable
from unittest.mock import MagicMock

import databento as db
import pytest
import requests
from databento import DBNStore
from databento import Historical
from databento.common.enums import HistoricalGateway
from databento.common.enums import Schema


def test_key_returns_expected() -> None:
    # Arrange
    key = "DUMMY_API_KEY"

    # Act
    client = db.Historical(key=key)

    # Assert
    assert client.key == "DUMMY_API_KEY"


def test_default_host_returns_expected() -> None:
    # Arrange, Act
    historical_client = db.Historical(key="DUMMY_API_KEY")

    # Assert
    assert historical_client.gateway == "https://hist.databento.com"


@pytest.mark.parametrize(
    "gateway, expected",
    [
        [HistoricalGateway.BO1, "https://hist.databento.com"],
        ["bo1", "https://hist.databento.com"],
    ],
)
def test_gateway_nearest_and_bo1_map_to_hist_databento(
    gateway: HistoricalGateway | str,
    expected: str,
) -> None:
    # Arrange, Act
    client = db.Historical(key="DUMMY_API_KEY", gateway=gateway)

    # Assert
    assert client.gateway == expected


def test_custom_gateway_returns_expected() -> None:
    # Arrange
    ny4_gateway = "ny4.databento.com"

    # Act
    client = db.Historical(key="DUMMY_API_KEY", gateway=ny4_gateway)

    # Assert
    assert client.gateway == "https://ny4.databento.com"


@pytest.mark.parametrize(
    "gateway",
    [
        "//",
        "",
    ],
)
def test_custom_gateway_error(
    gateway: str,
) -> None:
    """
    Test that setting a custom gateway to an invalid url raises an exception.
    """
    # Arrange, Act, Assert
    with pytest.raises(ValueError):
        db.Historical(key="DUMMY_API_KEY", gateway=gateway)


@pytest.mark.parametrize(
    "gateway, expected",
    [
        ["hist.databento.com", "https://hist.databento.com"],
        ["http://hist.databento.com", "https://hist.databento.com"],
    ],
)
def test_custom_gateway_force_https(
    gateway: str,
    expected: str,
) -> None:
    """
    Test that custom gateways are forced to the https scheme.
    """
    # Arrange Act
    client = db.Historical(key="DUMMY_API_KEY", gateway=gateway)

    # Assert
    assert client.gateway == expected


def test_re_request_symbology_makes_expected_request(
    test_data_path: Callable[[Schema], pathlib.Path],
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())

    bento = DBNStore.from_file(path=test_data_path(Schema.MBO))

    # Act
    bento.request_symbology(historical_client)

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"]
        == f"{historical_client.gateway}/v{db.API_VERSION}/symbology.resolve"
    )
    assert call["params"] == [
        ("dataset", "GLBX.MDP3"),
        ("symbols", "ESH1"),
        ("stype_in", "raw_symbol"),
        ("stype_out", "instrument_id"),
        ("start_date", "2020-12-28"),
        ("end_date", "2020-12-29"),
        ("default_value", ""),
    ]
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


def test_request_full_definitions_expected_request(
    test_data: Callable[[Schema], bytes],
    test_data_path: Callable[[Schema], pathlib.Path],
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())

    # Create an MBO bento
    bento = DBNStore.from_file(path=test_data_path(Schema.MBO))

    # Mock from_bytes with the definition stub
    stream_bytes = test_data(Schema.DEFINITION)
    monkeypatch.setattr(
        DBNStore,
        "from_bytes",
        MagicMock(return_value=DBNStore.from_bytes(stream_bytes)),
    )

    # Act
    definition_bento = bento.request_full_definitions(historical_client)

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"]
        == f"{historical_client.gateway}/v{db.API_VERSION}/timeseries.get_range"
    )
    assert call["params"] == [
        ("dataset", "GLBX.MDP3"),
        ("start", "2020-12-28T13:00:00+00:00"),
        ("end", "2020-12-29T13:01:00+00:00"),
        ("symbols", "ESH1"),
        ("schema", "definition"),
        ("stype_in", "raw_symbol"),
        ("stype_out", "instrument_id"),
        ("encoding", "dbn"),
        ("compression", "zstd"),
    ]
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)
    assert len(stream_bytes) == definition_bento.nbytes
