from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from unittest.mock import MagicMock

import databento as db
import pandas as pd
import pytest
import requests
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
    mock_response.text = "{}"
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
    }
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


def test_corporate_actions_get_range_response_parsing_as_pit(
    monkeypatch: pytest.MonkeyPatch,
    reference_client: Reference,
) -> None:
    # Arrange
    data_path = Path(TESTS_ROOT) / "data" / "REFERENCE" / "test_data.corporate-actions.ndjson"
    mock_response = MagicMock()
    mock_response.text = data_path.read_text()
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
    assert df_raw.index.is_monotonic_increasing
    # Assert the columns were dropped
    for col in ["date_info", "rate_info", "event_info"]:
        assert col not in df_raw.columns


def test_corporate_actions_get_range_response(
    monkeypatch: pytest.MonkeyPatch,
    reference_client: Reference,
) -> None:
    # Arrange
    data_path = Path(TESTS_ROOT) / "data" / "REFERENCE" / "test_data.corporate-actions-pit.ndjson"
    mock_response = MagicMock()
    mock_response.text = data_path.read_text()
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
    data_path = Path(TESTS_ROOT) / "data" / "REFERENCE" / "test_data.corporate-actions.ndjson"
    mock_response = MagicMock()
    mock_response.text = data_path.read_text()
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
    data_path = Path(TESTS_ROOT) / "data" / "REFERENCE" / "test_data.corporate-actions.ndjson"
    mock_response = MagicMock()
    mock_response.text = data_path.read_text()
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
