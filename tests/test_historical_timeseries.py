import sys
from typing import Callable
from unittest.mock import MagicMock

import databento as db
import pytest
import requests
from databento import DBNStore
from databento.common.enums import Schema
from pytest_mock import MockerFixture


class TestHistoricalTimeSeries:
    def setup_method(self) -> None:
        key = "DUMMY_API_KEY"
        self.client = db.Historical(key=key)

    def test_get_range_given_invalid_schema_raises_error(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(ValueError):
            self.client.timeseries.get_range(
                dataset="GLBX.MDP3",
                symbols="ESH1",
                schema="ticks",  # <--- invalid
                start="2020-12-28",
                end="2020-12-28T23:00",
            )

    def test_get_range_given_invalid_stype_in_raises_error(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(ValueError):
            self.client.timeseries.get_range(
                dataset="GLBX.MDP3",
                symbols="ESH1",
                schema="mbo",
                start="2020-12-28",
                end="2020-12-28T23:00",
                stype_in="zzz",  # <--- invalid
            )

    def test_get_range_given_invalid_stype_out_raises_error(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(ValueError):
            self.client.timeseries.get_range(
                dataset="GLBX.MDP3",
                symbols="ESH1",
                schema="mbo",
                start="2020-12-28",
                end="2020-12-28T23:00",
                stype_out="zzz",  # <--- invalid
            )

    def test_get_range_sends_expected_request(
        self,
        test_data: Callable[[Schema], bytes],
        mocker: MockerFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")
        stream_bytes = test_data(Schema.TRADES)

        monkeypatch.setattr(
            DBNStore,
            "from_bytes",
            MagicMock(return_value=DBNStore.from_bytes(stream_bytes)),
        )

        # Act
        self.client.timeseries.get_range(
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
            == f"https://hist.databento.com/v{db.API_VERSION}/timeseries.get_range"
        )
        assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
        assert call["headers"]["accept"] == "application/json"
        assert all(
            v in call["headers"]["user-agent"] for v in ("Databento/", "Python/")
        )
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
        self,
        test_data: Callable[[Schema], bytes],
        mocker: MockerFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Mock from_bytes with the definition stub
        stream_bytes = test_data(Schema.TRADES)
        monkeypatch.setattr(
            DBNStore,
            "from_bytes",
            MagicMock(return_value=DBNStore.from_bytes(stream_bytes)),
        )

        # Act
        self.client.timeseries.get_range(
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
            == f"https://hist.databento.com/v{db.API_VERSION}/timeseries.get_range"
        )
        assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
        assert call["headers"]["accept"] == "application/json"
        assert all(
            v in call["headers"]["user-agent"] for v in ("Databento/", "Python/")
        )
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
