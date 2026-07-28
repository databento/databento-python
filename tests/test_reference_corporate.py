from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
import requests
import zstandard

import databento as db
from databento.reference.client import Reference
from tests import TESTS_ROOT


@pytest.mark.parametrize(
    (
        "events",
        "countries",
        "security_types",
        "expected_events",
        "expected_countries",
        "expected_security_types",
    ),
    [
        [
            None,
            None,
            None,
            None,
            None,
            None,
        ],
        [
            [],
            [],
            [],
            None,
            None,
            None,
        ],
        [
            "DIV",
            "US",
            "EQS",
            "DIV",
            "US",
            "EQS",
        ],
        [
            "DIV,LIQ",
            "US,CA",
            "EQS,ETF",
            "DIV,LIQ",
            "US,CA",
            "EQS,ETF",
        ],
        [
            ["DIV", "LIQ"],
            ["US", "CA"],
            ["EQS", "ETF"],
            "DIV,LIQ",
            "US,CA",
            "EQS,ETF",
        ],
    ],
)
def test_corporate_actions_get_range_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    reference_client: Reference,
    events: Iterable[str] | str | None,
    countries: Iterable[str] | str | None,
    security_types: Iterable[str] | str | None,
    expected_events: str,
    expected_countries: str,
    expected_security_types: str,
) -> None:
    # Arrange
    mock_response = MagicMock()
    mock_response.content = zstandard.compress(b"{}")
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__ = MagicMock()
    monkeypatch.setattr(requests, "post", mock_post := MagicMock(return_value=mock_response))

    # Act
    reference_client.corporate_actions.get_range(
        symbols="AAPL",
        stype_in="raw_symbol",
        start="2024-01",
        end="2024-04",
        events=events,
        countries=countries,
        security_types=security_types,
    )

    # Assert
    call = mock_post.call_args.kwargs
    assert (
        call["url"] == f"{reference_client.gateway}/v{db.API_VERSION}/corporate_actions.get_range"
    )
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["data"] == {
        "start": "2024-01",
        "end": "2024-04",
        "index": "event_date",
        "symbols": "AAPL",
        "stype_in": "raw_symbol",
        "events": expected_events,
        "countries": expected_countries,
        "security_types": expected_security_types,
        "compression": "zstd",
    }
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


def test_corporate_actions_get_range_when_empty_response(
    monkeypatch: pytest.MonkeyPatch,
    reference_client: Reference,
) -> None:
    # Arrange
    mock_response = MagicMock()
    mock_response.content = zstandard.compress(b"")
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__ = MagicMock()
    monkeypatch.setattr(requests, "post", MagicMock(return_value=mock_response))

    # Act
    df_raw = reference_client.corporate_actions.get_range(
        symbols="AAPL",
        stype_in="raw_symbol",
        start="2024-01",
        end="2024-04",
    )

    # Assert
    assert df_raw.empty


def test_corporate_actions_get_range_response_parsing_as_pit(
    monkeypatch: pytest.MonkeyPatch,
    reference_client: Reference,
) -> None:
    # Arrange
    data_path = Path(TESTS_ROOT) / "data" / "REFERENCE" / "test_data.corporate-actions.jsonl"
    mock_response = MagicMock()
    mock_response.content = zstandard.compress(data_path.read_bytes())
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__ = MagicMock()
    monkeypatch.setattr(requests, "post", MagicMock(return_value=mock_response))

    # Act
    df_raw = reference_client.corporate_actions.get_range(
        symbols="AAPL",
        stype_in="raw_symbol",
        start="2024-01",
        end="2024-04",
        pit=True,
    )

    # Assert
    assert len(df_raw) == 2
    assert df_raw.index.name == "event_date"
    assert df_raw.index.dtype == "O"
    assert df_raw.index.is_monotonic_increasing
    # Assert the columns were dropped
    for col in ["date_info", "rate_info", "event_info"]:
        assert col not in df_raw.columns


def test_corporate_actions_get_range_response(
    monkeypatch: pytest.MonkeyPatch,
    reference_client: Reference,
) -> None:
    # Arrange
    data_path = Path(TESTS_ROOT) / "data" / "REFERENCE" / "test_data.corporate-actions-pit.jsonl"
    mock_response = MagicMock()
    mock_response.content = zstandard.compress(data_path.read_bytes())
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__ = MagicMock()
    monkeypatch.setattr(requests, "post", MagicMock(return_value=mock_response))

    # Act
    df_raw = reference_client.corporate_actions.get_range(
        symbols="AAPL",
        index="ts_record",
        start="2024-01",
        end="2024-04",
        pit=False,
    )

    # Assert
    assert len(df_raw) == 1
    assert df_raw.index[0] == pd.Timestamp("2023-11-01 00:00:00", tz="UTC")


def test_corporate_actions_get_range_with_ts_record_index(
    monkeypatch: pytest.MonkeyPatch,
    reference_client: Reference,
) -> None:
    # Arrange
    data_path = Path(TESTS_ROOT) / "data" / "REFERENCE" / "test_data.corporate-actions.jsonl"
    mock_response = MagicMock()
    mock_response.content = zstandard.compress(data_path.read_bytes())
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__ = MagicMock()
    monkeypatch.setattr(requests, "post", MagicMock(return_value=mock_response))

    # Act
    df_raw = reference_client.corporate_actions.get_range(
        symbols="AAPL",
        stype_in="raw_symbol",
        index="ts_record",
        start="2024-01",
        end="2024-04",
    )

    expected_index = pd.DatetimeIndex(
        [
            "2023-10-10 04:37:14+00:00",
            "2023-10-10 04:37:14+00:00",
        ],
        name="ts_record",
    )

    # Assert
    assert len(df_raw) == 2
    assert df_raw.index.equals(expected_index)


def test_corporate_actions_get_range_without_flattening(
    monkeypatch: pytest.MonkeyPatch,
    reference_client: Reference,
) -> None:
    # Arrange
    data_path = Path(TESTS_ROOT) / "data" / "REFERENCE" / "test_data.corporate-actions.jsonl"
    mock_response = MagicMock()
    mock_response.content = zstandard.compress(data_path.read_bytes())
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__ = MagicMock()
    monkeypatch.setattr(requests, "post", MagicMock(return_value=mock_response))

    # Act
    df_raw = reference_client.corporate_actions.get_range(
        symbols="AAPL",
        stype_in="raw_symbol",
        start="2024-01",
        end="2024-04",
        flatten=False,
    )

    # Assert
    assert len(df_raw) == 2
    # Assert the columns were retained
    for col in ["date_info", "rate_info", "event_info"]:
        assert col in df_raw.columns


LIST_EVENTS_RESPONSE = {
    "AGM": {
        "calendar_dates": [
            {"alias": "meeting_date", "name": "event_date"},
            {"alias": None, "name": "record_date"},
        ],
        "category": "proposals",
        "code": "AGM",
        "description": "Annual General meeting of shareholders.",
        "fields": [
            {
                "description": "Company Meeting Number",
                "group": "event_info",
                "name": "meeting_number",
            },
        ],
        "level": "issuer",
        "name": "Company Meeting",
        "participation": "voluntary",
        "subtypes": [
            {"code": "AGM", "description": "Annual General Meeting"},
        ],
    },
}


def test_corporate_actions_list_events_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    reference_client: Reference,
) -> None:
    # Arrange
    mock_response = MagicMock()
    mock_response.json.return_value = LIST_EVENTS_RESPONSE
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__ = MagicMock()
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock(return_value=mock_response))

    # Act
    reference_client.corporate_actions.list_events()

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"] == f"{reference_client.gateway}/v{db.API_VERSION}/corporate_actions.list_events"
    )
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["timeout"] == (100, 100)


def test_corporate_actions_list_events_response(
    monkeypatch: pytest.MonkeyPatch,
    reference_client: Reference,
) -> None:
    # Arrange
    mock_response = MagicMock()
    mock_response.json.return_value = LIST_EVENTS_RESPONSE
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__ = MagicMock()
    monkeypatch.setattr(requests, "get", MagicMock(return_value=mock_response))

    # Act
    data = reference_client.corporate_actions.list_events()

    # Assert
    assert data == LIST_EVENTS_RESPONSE


LIST_ENUMS_RESPONSE = {
    "ACTION": [
        {"code": "C", "description": "Cancelled"},
        {"code": "I", "description": "Inserted"},
    ],
}


def test_corporate_actions_list_enums_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    reference_client: Reference,
) -> None:
    # Arrange
    mock_response = MagicMock()
    mock_response.json.return_value = LIST_ENUMS_RESPONSE
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__ = MagicMock()
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock(return_value=mock_response))

    # Act
    reference_client.corporate_actions.list_enums()

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"] == f"{reference_client.gateway}/v{db.API_VERSION}/corporate_actions.list_enums"
    )
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["timeout"] == (100, 100)


def test_corporate_actions_list_enums_response(
    monkeypatch: pytest.MonkeyPatch,
    reference_client: Reference,
) -> None:
    # Arrange
    mock_response = MagicMock()
    mock_response.json.return_value = LIST_ENUMS_RESPONSE
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__ = MagicMock()
    monkeypatch.setattr(requests, "get", MagicMock(return_value=mock_response))

    # Act
    data = reference_client.corporate_actions.list_enums()

    # Assert
    assert data == LIST_ENUMS_RESPONSE
