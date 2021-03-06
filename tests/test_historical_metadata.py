import sys

import databento as db
import pytest
import requests
from databento.common.enums import Dataset, FeedMode, Schema


class TestHistoricalMetadata:
    def setup(self) -> None:
        key = "DUMMY_ACCESS_KEY"
        self.client = db.Historical(key=key)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_list_datasets_sends_expected_request(self, mocker) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.list_datasets(start="2018-01-01", end="2020-01-01")

        # Assert
        call = mocked_get.call_args.kwargs
        assert call["url"] == "https://hist.databento.com/v1/metadata.list_datasets"
        assert ("start", "2018-01-01T00:00:00") in call["params"]
        assert ("end", "2020-01-01T00:00:00") in call["params"]
        assert call["headers"] == {"accept": "application/json"}
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_list_schemas_sends_expected_request(self, mocker) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.list_schemas(
            dataset="GLBX.MDP3",
            start="2018-01-01",
            end="2021-01-01",
        )

        # Assert
        call = mocked_get.call_args.kwargs
        assert call["url"] == "https://hist.databento.com/v1/metadata.list_schemas"
        assert ("start", "2018-01-01T00:00:00") in call["params"]
        assert ("end", "2021-01-01T00:00:00") in call["params"]
        assert call["headers"] == {"accept": "application/json"}
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_list_fields_sends_expected_request(self, mocker) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.list_fields(
            dataset="GLBX.MDP3",
            schema="mbo",
            encoding="dbz",
        )

        # Assert
        call = mocked_get.call_args.kwargs
        assert call["url"] == "https://hist.databento.com/v1/metadata.list_fields"
        assert ("schema", "mbo") in call["params"]
        assert ("encoding", "dbz") in call["params"]
        assert call["headers"] == {"accept": "application/json"}
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_list_encodings_sends_expected_request(self, mocker) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.list_encodings()

        # Assert
        call = mocked_get.call_args.kwargs
        assert call["url"] == "https://hist.databento.com/v1/metadata.list_encodings"
        assert call["headers"] == {"accept": "application/json"}
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_list_compressions_sends_expected_request(self, mocker) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.list_compressions()

        # Assert
        call = mocked_get.call_args.kwargs
        assert call["url"] == "https://hist.databento.com/v1/metadata.list_compressions"
        assert call["headers"] == {"accept": "application/json"}
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
        self, dataset, schema, mode, mocker
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
        assert call["url"] == "https://hist.databento.com/v1/metadata.list_unit_prices"
        assert call["headers"] == {"accept": "application/json"}
        assert call["params"] == [
            ("dataset", "glbx.mdp3"),
            ("mode", "live"),
            ("schema", "mbo"),
        ]
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_get_shape_sends_expected_request(self, mocker) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.get_shape(
            dataset="GLBX.MDP3",
            symbols=["ESH1"],
            schema="mbo",
            start="2020-12-28T12:00",
            end="2020-12-29",
            limit=1000000,
        )

        # Assert
        call = mocked_get.call_args.kwargs
        assert call["url"] == "https://hist.databento.com/v1/metadata.get_shape"
        assert call["headers"] == {"accept": "application/json"}
        assert call["params"] == [
            ("dataset", "glbx.mdp3"),
            ("symbols", "ESH1"),
            ("schema", "mbo"),
            ("start", "2020-12-28T12:00:00"),
            ("end", "2020-12-29T00:00:00"),
            ("encoding", "dbz"),
            ("stype_in", "native"),
            ("limit", "1000000"),
        ]
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_get_billable_size_sends_expected_request(self, mocker) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.get_billable_size(
            dataset="GLBX.MDP3",
            symbols=["ESH1"],
            schema="mbo",
            start="2020-12-28T12:00",
            end="2020-12-29",
            encoding="csv",
            limit=1000000,
        )

        # Assert
        call = mocked_get.call_args.kwargs
        assert call["url"] == "https://hist.databento.com/v1/metadata.get_billable_size"
        assert call["headers"] == {"accept": "application/json"}
        assert call["params"] == [
            ("dataset", "glbx.mdp3"),
            ("symbols", "ESH1"),
            ("schema", "mbo"),
            ("start", "2020-12-28T12:00:00"),
            ("end", "2020-12-29T00:00:00"),
            ("encoding", "csv"),
            ("compression", "none"),
            ("stype_in", "native"),
            ("stype_out", "product_id"),
            ("limit", "1000000"),
        ]
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_cost_sends_expected_request(self, mocker) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.metadata.get_cost(
            dataset="GLBX.MDP3",
            symbols=["ESH1"],
            schema="mbo",
            start="2020-12-28T12:00",
            end="2020-12-29",
            encoding="csv",
            compression="zstd",
            limit=1000000,
        )

        # Assert
        call = mocked_get.call_args.kwargs
        assert call["url"] == "https://hist.databento.com/v1/metadata.get_cost"
        assert call["headers"] == {"accept": "application/json"}
        assert call["params"] == [
            ("dataset", "glbx.mdp3"),
            ("symbols", "ESH1"),
            ("schema", "mbo"),
            ("start", "2020-12-28T12:00:00"),
            ("end", "2020-12-29T00:00:00"),
            ("encoding", "csv"),
            ("compression", "zstd"),
            ("stype_in", "native"),
            ("stype_out", "product_id"),
            ("limit", "1000000"),
            ("mode", "historical-streaming"),
        ]
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)
