from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
import zstandard

import databento as db
from databento.reference.client import Reference
from tests import TESTS_ROOT


@pytest.mark.parametrize(
    (
        "countries",
        "security_types",
        "expected_countries",
        "expected_security_types",
    ),
    [
        [
            None,
            None,
            None,
            None,
        ],
        [
            [],
            [],
            None,
            None,
        ],
        [
            "US",
            "EQS",
            "US",
            "EQS",
        ],
        [
            "US,CA",
            "EQS,ETF",
            "US,CA",
            "EQS,ETF",
        ],
        [
            ["US", "CA"],
            ["EQS", "ETF"],
            "US,CA",
            "EQS,ETF",
        ],
    ],
)
def test_adjustment_factors_get_range_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    reference_client: Reference,
    countries: Iterable[str] | str | None,
    security_types: Iterable[str] | str | None,
    expected_countries: str,
    expected_security_types: str,
) -> None:
    # Arrange
    mock_response = MagicMock()
    mock_response.content = zstandard.compress(b'{"ex_date":"1970-01-01"}\n')
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__ = MagicMock()
    monkeypatch.setattr(requests, "post", mock_post := MagicMock(return_value=mock_response))

    # Act
    reference_client.adjustment_factors.get_range(
        symbols="AAPL",
        stype_in="raw_symbol",
        start="2024-01",
        end="2024-04",
        countries=countries,
        security_types=security_types,
    )

    # Assert
    call = mock_post.call_args.kwargs
    assert (
        call["url"] == f"{reference_client.gateway}/v{db.API_VERSION}/adjustment_factors.get_range"
    )
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["data"] == {
        "start": "2024-01",
        "end": "2024-04",
        "symbols": "AAPL",
        "stype_in": "raw_symbol",
        "countries": expected_countries,
        "security_types": expected_security_types,
        "compression": "zstd",
    }
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


def test_adjustment_factors_get_range_when_empty_response(
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
    df_raw = reference_client.adjustment_factors.get_range(
        symbols="AAPL",
        stype_in="raw_symbol",
        start="2024-01",
        end="2024-04",
    )

    # Assert
    assert df_raw.empty


def test_adjustment_factors_get_range_response(
    monkeypatch: pytest.MonkeyPatch,
    reference_client: Reference,
) -> None:
    # Arrange
    mock_response = MagicMock()
    data_path = Path(TESTS_ROOT) / "data" / "REFERENCE" / "test_data.adjustment-factors.jsonl"
    mock_response.content = zstandard.compress(data_path.read_bytes())
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__ = MagicMock()
    monkeypatch.setattr(requests, "post", MagicMock(return_value=mock_response))

    # Act
    df_raw = reference_client.adjustment_factors.get_range(
        symbols="AAPL",
        stype_in="raw_symbol",
        start="2024-01",
        end="2024-04",
    )

    # Assert
    assert len(df_raw) == 2
    assert df_raw.index.name == "ex_date"
