from __future__ import annotations

from unittest.mock import MagicMock

import databento as db
import pytest
import requests
from databento.common.enums import Dataset
from databento.historical.client import Historical


def test_list_publishers_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())

    # Act
    historical_client.metadata.list_publishers()

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"]
        == f"{historical_client.gateway}/v{db.API_VERSION}/metadata.list_publishers"
    )
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


def test_list_datasets_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())

    # Act
    historical_client.metadata.list_datasets(
        start_date="2018-01-01",
        end_date="2020-01-01",
    )

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"]
        == f"{historical_client.gateway}/v{db.API_VERSION}/metadata.list_datasets"
    )
    assert ("start_date", "2018-01-01") in call["params"]
    assert ("end_date", "2020-01-01") in call["params"]
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


def test_list_schemas_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())

    # Act
    historical_client.metadata.list_schemas(dataset="GLBX.MDP3")

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"]
        == f"{historical_client.gateway}/v{db.API_VERSION}/metadata.list_schemas"
    )
    assert ("dataset", "GLBX.MDP3") in call["params"]
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


def test_list_fields_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())

    # Act
    historical_client.metadata.list_fields(
        schema="mbo",
        encoding="dbn",
    )

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"]
        == f"{historical_client.gateway}/v{db.API_VERSION}/metadata.list_fields"
    )
    assert ("schema", "mbo") in call["params"]
    assert ("encoding", "dbn") in call["params"]
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


@pytest.mark.parametrize(
    "dataset",
    [
        "GLBX.MDP3",
        Dataset.GLBX_MDP3,
    ],
)
def test_list_unit_price_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
    dataset: Dataset | str,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())

    # Act
    historical_client.metadata.list_unit_prices(dataset=dataset)

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"]
        == f"{historical_client.gateway}/v{db.API_VERSION}/metadata.list_unit_prices"
    )
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["params"] == [
        ("dataset", "GLBX.MDP3"),
    ]
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


def test_get_dataset_condition_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())

    # Act
    historical_client.metadata.get_dataset_condition(
        dataset="GLBX.MDP3",
        start_date="2018-01-01",
        end_date="2020-01-01",
    )

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"]
        == f"{historical_client.gateway}/v{db.API_VERSION}/metadata.get_dataset_condition"
    )
    assert ("dataset", "GLBX.MDP3") in call["params"]
    assert ("start_date", "2018-01-01") in call["params"]
    assert ("end_date", "2020-01-01") in call["params"]
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


def test_get_dataset_range_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())

    # Act
    historical_client.metadata.get_dataset_range(dataset="GLBX.MDP3")

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"]
        == f"{historical_client.gateway}/v{db.API_VERSION}/metadata.get_dataset_range"
    )
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["params"] == [
        ("dataset", "GLBX.MDP3"),
    ]
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


def test_get_record_count_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())

    # Act
    historical_client.metadata.get_record_count(
        dataset="GLBX.MDP3",
        symbols=["ESH1"],
        schema="mbo",
        start="2020-12-28T12:00",
        end="2020-12-29",
        limit=1000000,
    )

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"]
        == f"{historical_client.gateway}/v{db.API_VERSION}/metadata.get_record_count"
    )
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["params"] == [
        ("dataset", "GLBX.MDP3"),
        ("symbols", "ESH1"),
        ("schema", "mbo"),
        ("start", "2020-12-28T12:00"),
        ("end", "2020-12-29"),
        ("stype_in", "raw_symbol"),
        ("limit", "1000000"),
    ]
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


def test_get_billable_size_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())

    # Act
    historical_client.metadata.get_billable_size(
        dataset="GLBX.MDP3",
        symbols=["ESH1"],
        schema="mbo",
        start="2020-12-28T12:00",
        end="2020-12-29",
        limit=1000000,
    )

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"]
        == f"{historical_client.gateway}/v{db.API_VERSION}/metadata.get_billable_size"
    )
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["params"] == [
        ("dataset", "GLBX.MDP3"),
        ("start", "2020-12-28T12:00"),
        ("end", "2020-12-29"),
        ("symbols", "ESH1"),
        ("schema", "mbo"),
        ("stype_in", "raw_symbol"),
        ("stype_out", "instrument_id"),
        ("limit", "1000000"),
    ]
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


def test_get_cost_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())

    # Act
    historical_client.metadata.get_cost(
        dataset="GLBX.MDP3",
        symbols=["ESH1"],
        schema="mbo",
        start="2020-12-28T12:00",
        end="2020-12-29",
        limit=1000000,
    )

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"]
        == f"{historical_client.gateway}/v{db.API_VERSION}/metadata.get_cost"
    )
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["params"] == [
        ("dataset", "GLBX.MDP3"),
        ("start", "2020-12-28T12:00"),
        ("end", "2020-12-29"),
        ("symbols", "ESH1"),
        ("schema", "mbo"),
        ("stype_in", "raw_symbol"),
        ("stype_out", "instrument_id"),
        ("mode", "historical-streaming"),
        ("limit", "1000000"),
    ]
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)
