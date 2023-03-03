import sys
from typing import Union

import databento as db
import pytest
import requests
from databento.common.enums import Dataset, FeedMode, Schema
from pytest_mock import MockerFixture


class TestHistoricalMetadata:
    def setup_method(self) -> None:
        key = "DUMMY_API_KEY"
        self.client = db.Historical(key=key)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_list_publishers_sends_expected_request(
        self,
        mocker: MockerFixture,
    ) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.list_publishers()

        # Assert
        call = mocked_get.call_args.kwargs
        assert (
            call["url"]
            == f"https://hist.databento.com/v{db.API_VERSION}/metadata.list_publishers"
        )
        assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
        assert call["headers"]["accept"] == "application/json"
        assert all(
            v in call["headers"]["user-agent"] for v in ("Databento/", "Python/")
        )
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_list_datasets_sends_expected_request(self, mocker: MockerFixture) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.list_datasets(
            start_date="2018-01-01",
            end_date="2020-01-01",
        )

        # Assert
        call = mocked_get.call_args.kwargs
        assert (
            call["url"]
            == f"https://hist.databento.com/v{db.API_VERSION}/metadata.list_datasets"
        )
        assert ("start_date", "2018-01-01") in call["params"]
        assert ("end_date", "2020-01-01") in call["params"]
        assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
        assert call["headers"]["accept"] == "application/json"
        assert all(
            v in call["headers"]["user-agent"] for v in ("Databento/", "Python/")
        )
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_list_schemas_sends_expected_request(self, mocker: MockerFixture) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.list_schemas(
            dataset="GLBX.MDP3",
            start_date="2018-01-01",
            end_date="2021-01-01",
        )

        # Assert
        call = mocked_get.call_args.kwargs
        assert (
            call["url"]
            == f"https://hist.databento.com/v{db.API_VERSION}/metadata.list_schemas"
        )
        assert ("dataset", "GLBX.MDP3") in call["params"]
        assert ("start_date", "2018-01-01") in call["params"]
        assert ("end_date", "2021-01-01") in call["params"]
        assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
        assert call["headers"]["accept"] == "application/json"
        assert all(
            v in call["headers"]["user-agent"] for v in ("Databento/", "Python/")
        )
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_list_fields_sends_expected_request(self, mocker: MockerFixture) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.list_fields(
            dataset="GLBX.MDP3",
            schema="mbo",
            encoding="dbn",
        )

        # Assert
        call = mocked_get.call_args.kwargs
        assert (
            call["url"]
            == f"https://hist.databento.com/v{db.API_VERSION}/metadata.list_fields"
        )
        assert ("dataset", "GLBX.MDP3") in call["params"]
        assert ("schema", "mbo") in call["params"]
        assert ("encoding", "dbn") in call["params"]
        assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
        assert call["headers"]["accept"] == "application/json"
        assert all(
            v in call["headers"]["user-agent"] for v in ("Databento/", "Python/")
        )
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_list_encodings_sends_expected_request(self, mocker: MockerFixture) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.list_encodings()

        # Assert
        call = mocked_get.call_args.kwargs
        assert (
            call["url"]
            == f"https://hist.databento.com/v{db.API_VERSION}/metadata.list_encodings"
        )
        assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
        assert call["headers"]["accept"] == "application/json"
        assert all(
            v in call["headers"]["user-agent"] for v in ("Databento/", "Python/")
        )
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_list_compressions_sends_expected_request(
        self,
        mocker: MockerFixture,
    ) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.list_compressions()

        # Assert
        call = mocked_get.call_args.kwargs
        assert (
            call["url"]
            == f"https://hist.databento.com/v{db.API_VERSION}/metadata.list_compressions"  # noqa
        )
        assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
        assert call["headers"]["accept"] == "application/json"
        assert all(
            v in call["headers"]["user-agent"] for v in ("Databento/", "Python/")
        )
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    @pytest.mark.parametrize(
        "dataset, schema, mode",
        [
            ["GLBX.MDP3", "mbo", "live"],
            [Dataset.GLBX_MDP3, Schema.MBO, FeedMode.LIVE],
        ],
    )
    def test_list_unit_price_sends_expected_request(
        self,
        dataset: Union[str, Dataset],
        schema: Union[str, Schema],
        mode: Union[str, FeedMode],
        mocker: MockerFixture,
    ) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.list_unit_prices(
            dataset=dataset,
            schema=schema,
            mode=mode,
        )

        # Assert
        call = mocked_get.call_args.kwargs
        assert (
            call["url"]
            == f"https://hist.databento.com/v{db.API_VERSION}/metadata.list_unit_prices"
        )
        assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
        assert call["headers"]["accept"] == "application/json"
        assert all(
            v in call["headers"]["user-agent"] for v in ("Databento/", "Python/")
        )
        assert call["params"] == [
            ("dataset", "GLBX.MDP3"),
            ("mode", "live"),
            ("schema", "mbo"),
        ]
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_get_dataset_condition_sends_expected_request(
        self,
        mocker: MockerFixture,
    ) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.get_dataset_condition(
            dataset="GLBX.MDP3",
            start_date="2018-01-01",
            end_date="2020-01-01",
        )

        # Assert
        call = mocked_get.call_args.kwargs
        assert (
            call["url"]
            == f"https://hist.databento.com/v{db.API_VERSION}/metadata.get_dataset_condition"  # noqa
        )
        assert ("dataset", "GLBX.MDP3") in call["params"]
        assert ("start_date", "2018-01-01") in call["params"]
        assert ("end_date", "2020-01-01") in call["params"]
        assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
        assert call["headers"]["accept"] == "application/json"
        assert all(
            v in call["headers"]["user-agent"] for v in ("Databento/", "Python/")
        )
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_get_record_count_sends_expected_request(
        self,
        mocker: MockerFixture,
    ) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.get_record_count(
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
            == f"https://hist.databento.com/v{db.API_VERSION}/metadata.get_record_count"
        )
        assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
        assert call["headers"]["accept"] == "application/json"
        assert all(
            v in call["headers"]["user-agent"] for v in ("Databento/", "Python/")
        )
        assert call["params"] == [
            ("dataset", "GLBX.MDP3"),
            ("symbols", "ESH1"),
            ("schema", "mbo"),
            ("start", "2020-12-28T12:00:00"),
            ("end", "2020-12-29T00:00:00"),
            ("stype_in", "native"),
            ("limit", "1000000"),
        ]
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_get_billable_size_sends_expected_request(
        self,
        mocker: MockerFixture,
    ) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.get_billable_size(
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
            == f"https://hist.databento.com/v{db.API_VERSION}/metadata.get_billable_size"  # noqa
        )
        assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
        assert call["headers"]["accept"] == "application/json"
        assert all(
            v in call["headers"]["user-agent"] for v in ("Databento/", "Python/")
        )
        assert call["params"] == [
            ("dataset", "GLBX.MDP3"),
            ("start", "2020-12-28T12:00:00"),
            ("end", "2020-12-29T00:00:00"),
            ("symbols", "ESH1"),
            ("schema", "mbo"),
            ("stype_in", "native"),
            ("stype_out", "product_id"),
            ("limit", "1000000"),
        ]
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_get_cost_sends_expected_request(self, mocker: MockerFixture) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.get_cost(
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
            == f"https://hist.databento.com/v{db.API_VERSION}/metadata.get_cost"
        )
        assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
        assert call["headers"]["accept"] == "application/json"
        assert all(
            v in call["headers"]["user-agent"] for v in ("Databento/", "Python/")
        )
        assert call["params"] == [
            ("dataset", "GLBX.MDP3"),
            ("start", "2020-12-28T12:00:00"),
            ("end", "2020-12-29T00:00:00"),
            ("symbols", "ESH1"),
            ("schema", "mbo"),
            ("stype_in", "native"),
            ("stype_out", "product_id"),
            ("mode", "historical-streaming"),
            ("limit", "1000000"),
        ]
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)
