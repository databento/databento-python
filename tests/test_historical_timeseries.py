from pathlib import Path
from typing import Callable
from unittest.mock import MagicMock

import databento as db
import pytest
import requests
from databento import DBNStore
from databento.common.enums import Schema
from databento.common.error import BentoServerError
from databento.historical.client import Historical


def test_get_range_given_invalid_schema_raises_error(
    historical_client: Historical,
) -> None:
    # Arrange, Act, Assert
    with pytest.raises(ValueError):
        historical_client.timeseries.get_range(
            dataset="GLBX.MDP3",
            symbols="ESH1",
            schema="ticks",  # <--- invalid
            start="2020-12-28",
            end="2020-12-28T23:00",
        )


def test_get_range_given_invalid_stype_in_raises_error(
    historical_client: Historical,
) -> None:
    # Arrange, Act, Assert
    with pytest.raises(ValueError):
        historical_client.timeseries.get_range(
            dataset="GLBX.MDP3",
            symbols="ESH1",
            schema="mbo",
            start="2020-12-28",
            end="2020-12-28T23:00",
            stype_in="zzz",  # <--- invalid
        )


def test_get_range_given_invalid_stype_out_raises_error(
    historical_client: Historical,
) -> None:
    # Arrange, Act, Assert
    with pytest.raises(ValueError):
        historical_client.timeseries.get_range(
            dataset="GLBX.MDP3",
            symbols="ESH1",
            schema="mbo",
            start="2020-12-28",
            end="2020-12-28T23:00",
            stype_out="zzz",  # <--- invalid
        )


def test_get_range_error_no_file_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    historical_client: Historical,
) -> None:
    # Arrange
    mocked_response = MagicMock()
    mocked_response.__enter__.return_value = MagicMock(status_code=500)
    monkeypatch.setattr(requests, "get", MagicMock(return_value=mocked_response))

    output_file = tmp_path / "output.dbn"

    # Act
    with pytest.raises(BentoServerError):
        historical_client.timeseries.get_range(
            dataset="GLBX.MDP3",
            symbols="ES.c.0",
            stype_in="continuous",
            schema="trades",
            start="2020-12-28T12:00",
            end="2020-12-29",
            path=output_file,
        )

    # Assert
    assert not output_file.exists()


def test_get_range_sends_expected_request(
    test_data: Callable[[Schema], bytes],
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())
    stream_bytes = test_data(Schema.TRADES)

    monkeypatch.setattr(
        DBNStore,
        "from_bytes",
        MagicMock(return_value=DBNStore.from_bytes(stream_bytes)),
    )

    # Act
    historical_client.timeseries.get_range(
        dataset="GLBX.MDP3",
        symbols="ES.c.0",
        stype_in="continuous",
        schema="trades",
        start="2020-12-28T12:00",
        end="2020-12-29",
    )

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"]
        == f"{historical_client.gateway}/v{db.API_VERSION}/timeseries.get_range"
    )
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["params"] == [
        ("dataset", "GLBX.MDP3"),
        ("start", "2020-12-28T12:00"),
        ("end", "2020-12-29"),
        ("symbols", "ES.c.0"),
        ("schema", "trades"),
        ("stype_in", "continuous"),
        ("stype_out", "instrument_id"),
        ("encoding", "dbn"),
        ("compression", "zstd"),
    ]
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


def test_get_range_with_limit_sends_expected_request(
    test_data: Callable[[Schema], bytes],
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())

    # Mock from_bytes with the definition stub
    stream_bytes = test_data(Schema.TRADES)
    monkeypatch.setattr(
        DBNStore,
        "from_bytes",
        MagicMock(return_value=DBNStore.from_bytes(stream_bytes)),
    )

    # Act
    historical_client.timeseries.get_range(
        dataset="GLBX.MDP3",
        symbols="ESH1",
        schema="trades",
        start="2020-12-28T12:00",
        end="2020-12-29",
        limit=1000000,
    )

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"]
        == f"{historical_client.gateway}/v{db.API_VERSION}/timeseries.get_range"
    )
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["params"] == [
        ("dataset", "GLBX.MDP3"),
        ("start", "2020-12-28T12:00"),
        ("end", "2020-12-29"),
        ("symbols", "ESH1"),
        ("schema", "trades"),
        ("stype_in", "raw_symbol"),
        ("stype_out", "instrument_id"),
        ("encoding", "dbn"),
        ("compression", "zstd"),
        ("limit", "1000000"),
    ]
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)
