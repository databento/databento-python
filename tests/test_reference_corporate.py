from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from unittest.mock import MagicMock

import databento as db
import pytest
import requests
from databento.reference.client import Reference

from tests import TESTS_ROOT


@pytest.mark.parametrize(
    ("events", "data_events"),
    [
        [
            None,
            None,
        ],
        [
            [],
            None,
        ],
        [
            "DIV",
            "DIV",
        ],
        [
            "DIV,LIQ",
            "DIV,LIQ",
        ],
        [
            ["DIV", "LIQ"],
            "DIV,LIQ",
        ],
    ],
)
def test_corporate_actions_get_range_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    reference_client: Reference,
    events: Iterable[str] | str | None,
    data_events: str,
) -> None:
    # Arrange
    mock_response = MagicMock()
    mock_response.text = "{}"
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__ = MagicMock()
    monkeypatch.setattr(requests, "post", mock_post := MagicMock(return_value=mock_response))

    # Act
    reference_client.corporate_actions.get_range(
        dataset=None,
        symbols="AAPL",
        stype_in="raw_symbol",
        start_date="2024-01",
        end_date="2024-04",
        events=events,
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
        "dataset": None,
        "start_date": "2024-01",
        "end_date": "2024-04",
        "symbols": "AAPL",
        "stype_in": "raw_symbol",
        "events": data_events,
    }
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


def test_corporate_actions_get_range_response_parsing(
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
        dataset=None,
        symbols="AAPL",
        stype_in="raw_symbol",
        start_date="2024-01",
        end_date="2024-04",
    )

    # Assert
    assert len(df_raw) == 2
