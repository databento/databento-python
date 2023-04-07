import pathlib
import sys
from typing import Callable, Union
from unittest.mock import MagicMock

import databento as db
import pytest
import requests
from databento import DBNStore, Historical
from databento.common.enums import HistoricalGateway, Schema
from pytest_mock import MockerFixture


class TestHistoricalClient:
    def test_key_returns_expected(self) -> None:
        # Arrange
        key = "DUMMY_API_KEY"

        # Act
        client = db.Historical(key=key)

        # Assert
        assert client.key == "DUMMY_API_KEY"

    def test_default_host_returns_expected(self) -> None:
        # Arrange, Act
        self.client = db.Historical(key="DUMMY_API_KEY")

        # Assert
        assert self.client.gateway == "https://hist.databento.com"

    @pytest.mark.parametrize(
        "gateway, expected",
        [
            [HistoricalGateway.BO1, "https://hist.databento.com"],
            ["bo1", "https://hist.databento.com"],
        ],
    )
    def test_gateway_nearest_and_bo1_map_to_hist_databento(
        self,
        gateway: Union[HistoricalGateway, str],
        expected: str,
    ) -> None:
        # Arrange, Act
        client = db.Historical(key="DUMMY_API_KEY", gateway=gateway)

        # Assert
        assert client.gateway == expected

    def test_custom_gateway_returns_expected(self) -> None:
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
        self,
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
        self,
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

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_re_request_symbology_makes_expected_request(
        self,
        test_data_path: Callable[[Schema], pathlib.Path],
        mocker: MockerFixture,
    ) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        client = Historical(key="DUMMY_API_KEY")

        bento = DBNStore.from_file(path=test_data_path(Schema.MBO))

        # Act
        bento.request_symbology(client)

        # Assert
        call = mocked_get.call_args.kwargs
        assert (
            call["url"]
            == f"https://hist.databento.com/v{db.API_VERSION}/symbology.resolve"
        )
        assert call["params"] == [
            ("dataset", "GLBX.MDP3"),
            ("symbols", "ESH1"),
            ("stype_in", "native"),
            ("stype_out", "product_id"),
            ("start_date", "2020-12-28"),
            ("end_date", "2020-12-29"),
            ("default_value", ""),
        ]
        assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
        assert call["headers"]["accept"] == "application/json"
        assert all(
            v in call["headers"]["user-agent"] for v in ("Databento/", "Python/")
        )
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_request_full_definitions_expected_request(
        self,
        test_data: Callable[[Schema], bytes],
        test_data_path: Callable[[Schema], pathlib.Path],
        mocker: MockerFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")
        client = Historical(key="DUMMY_API_KEY")

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
        definition_bento = bento.request_full_definitions(client)

        # Assert
        call = mocked_get.call_args.kwargs
        assert (
            call["url"]
            == f"https://hist.databento.com/v{db.API_VERSION}/timeseries.get_range"
        )
        assert call["params"] == [
            ("dataset", "GLBX.MDP3"),
            ("start", "2020-12-28T13:00:00+00:00"),
            ("end", "2020-12-29T13:01:00+00:00"),
            ("symbols", "ESH1"),
            ("schema", "definition"),
            ("stype_in", "native"),
            ("stype_out", "product_id"),
            ("encoding", "dbn"),
            ("compression", "zstd"),
        ]
        assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
        assert call["headers"]["accept"] == "application/json"
        assert all(
            v in call["headers"]["user-agent"] for v in ("Databento/", "Python/")
        )
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)
        assert len(stream_bytes) == definition_bento.nbytes
