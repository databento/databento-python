import sys

import databento as db
import pytest
import requests
from databento import FileBento, Historical
from databento.common.enums import HistoricalGateway, Schema
from tests.fixtures import get_test_data_path


class TestHistoricalClient:
    def test_key_returns_expected(self) -> None:
        # Arrange
        key = "DUMMY_ACCESS_KEY"

        # Act
        client = db.Historical(key=key)

        # Assert
        assert client.key == "DUMMY_ACCESS_KEY"

    def test_default_host_returns_expected(self) -> None:
        # Arrange, Act
        self.client = db.Historical(key="DUMMY_ACCESS_KEY")

        # Assert
        assert self.client.gateway == "https://hist.databento.com"

    @pytest.mark.parametrize(
        "gateway, expected",
        [
            [HistoricalGateway.BO1, "https://hist.databento.com"],
            [HistoricalGateway.NEAREST, "https://hist.databento.com"],
            ["bo1", "https://hist.databento.com"],
            ["nearest", "https://hist.databento.com"],
        ],
    )
    def test_gateway_nearest_and_bo1_map_to_hist_databento(
        self,
        gateway,
        expected,
    ):
        # Arrange, Act
        client = db.Historical(key="DUMMY_ACCESS_KEY", gateway=gateway)

        # Assert
        assert client.gateway == expected

    def test_custom_gateway_returns_expected(self):
        # Arrange
        ny4_gateway = "ny4.databento.com"

        # Act
        client = db.Historical(key="DUMMY_ACCESS_KEY", gateway=ny4_gateway)

        # Assert
        assert client.gateway == ny4_gateway

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_re_request_symbology_makes_expected_request(self, mocker) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        test_data_path = get_test_data_path(schema=Schema.MBO)
        data = FileBento(path=test_data_path)

        client = Historical(key="DUMMY_ACCESS_KEY")

        # Act
        client.request_symbology(data)

        # Assert
        call = mocked_get.call_args.kwargs
        assert (
            call["url"]
            == f"https://hist.databento.com/v{db.API_VERSION}/symbology.resolve"
        )
        assert call["params"] == [
            ("dataset", "glbx.mdp3"),
            ("symbols", "ESH1"),
            ("stype_in", "native"),
            ("stype_out", "product_id"),
            ("start", "2020-12-28"),
            ("end", "2020-12-29"),
            ("default_value", ""),
        ]
        assert call["headers"] == {"accept": "application/json"}
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_request_full_definitions_expected_request(self, mocker) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        test_data_path = get_test_data_path(schema=Schema.MBO)
        data = FileBento(path=test_data_path)

        client = Historical(key="DUMMY_ACCESS_KEY")

        # Act
        client.request_full_definitions(data)

        # Assert
        call = mocked_get.call_args.kwargs
        assert (
            call["url"]
            == f"https://hist.databento.com/v{db.API_VERSION}/timeseries.stream"
        )
        assert call["params"] == [
            ("dataset", "glbx.mdp3"),
            ("symbols", "ESH1"),
            ("schema", "definition"),
            ("start", "2020-12-28T13:00:00+00:00"),
            ("end", "2020-12-29T00:00:00+00:00"),
            ("stype_in", "native"),
            ("stype_out", "product_id"),
            ("encoding", "dbz"),
            ("compression", "zstd"),
        ]
        assert call["headers"] == {"accept": "application/json"}
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)
