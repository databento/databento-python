import sys

import databento as db
import pytest
import requests
from pytest_mock import MockerFixture


class TestHistoricalTimeSeries:
    def setup(self) -> None:
        key = "DUMMY_API_KEY"
        self.client = db.Historical(key=key)

    def test_stream_given_invalid_schema_raises_error(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(ValueError):
            self.client.timeseries.stream(
                dataset="GLBX.MDP3",
                symbols="ESH1",
                schema="ticks",  # <--- invalid
                start="2020-12-28",
                end="2020-12-28T23:00",
            )

    def test_stream_given_invalid_stype_in_raises_error(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(ValueError):
            self.client.timeseries.stream(
                dataset="GLBX.MDP3",
                symbols="ESH1",
                schema="mbo",
                start="2020-12-28",
                end="2020-12-28T23:00",
                stype_in="zzz",  # <--- invalid
            )

    def test_stream_given_invalid_stype_out_raises_error(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(ValueError):
            self.client.timeseries.stream(
                dataset="GLBX.MDP3",
                symbols="ESH1",
                schema="mbo",
                start="2020-12-28",
                end="2020-12-28T23:00",
                stype_out="zzz",  # <--- invalid
            )

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_stream_sends_expected_request(self, mocker: MockerFixture) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.timeseries.stream(
            dataset="GLBX.MDP3",
            symbols="ESH1",
            schema="trades",
            start="2020-12-28T12:00",
            end="2020-12-29",
        )

        # Assert
        call = mocked_get.call_args.kwargs
        assert (
            call["url"]
            == f"https://hist.databento.com/v{db.API_VERSION}/timeseries.stream"
        )
        assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
        assert call["headers"]["accept"] == "application/json"
        assert all(
            v in call["headers"]["user-agent"] for v in ("Databento/", "Python/")
        )
        assert call["params"] == [
            ("dataset", "glbx.mdp3"),
            ("symbols", "ESH1"),
            ("schema", "trades"),
            ("start", "2020-12-28T12:00:00"),
            ("end", "2020-12-29T00:00:00"),
            ("stype_in", "native"),
            ("stype_out", "product_id"),
            ("encoding", "dbz"),
        ]
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_stream_with_limit_sends_expected_request(
        self, mocker: MockerFixture
    ) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.timeseries.stream(
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
            == f"https://hist.databento.com/v{db.API_VERSION}/timeseries.stream"
        )
        assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
        assert call["headers"]["accept"] == "application/json"
        assert all(
            v in call["headers"]["user-agent"] for v in ("Databento/", "Python/")
        )
        assert call["params"] == [
            ("dataset", "glbx.mdp3"),
            ("symbols", "ESH1"),
            ("schema", "trades"),
            ("start", "2020-12-28T12:00:00"),
            ("end", "2020-12-29T00:00:00"),
            ("stype_in", "native"),
            ("stype_out", "product_id"),
            ("limit", "1000000"),
            ("encoding", "dbz"),
        ]
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)
