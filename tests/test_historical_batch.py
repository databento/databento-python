import sys

import databento as db
import pytest
import requests


class TestHistoricalBatch:
    def setup(self) -> None:
        key = "DUMMY_ACCESS_KEY"
        self.client = db.Historical(key=key)

    def test_batch_submit_given_invalid_schema_raises_error(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(ValueError):
            self.client.batch.submit(
                dataset="GLBX.MDP3",
                symbols=["ES"],
                schema="ticks",  # <--- invalid
                start="2020-12-28",
                end="2020-12-28T23:00",
                encoding="csv",
            )

    def test_batch_submit_given_invalid_encoding_raises_error(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(ValueError):
            self.client.batch.submit(
                dataset="GLBX.MDP3",
                symbols=["ES"],
                schema="mbo",
                start="2020-12-28",
                end="2020-12-28T23:00",
                encoding="gzip",  # <--- invalid
            )

    def test_batch_submit_given_invalid_stype_in_raises_error(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(ValueError):
            self.client.batch.submit(
                dataset="GLBX.MDP3",
                symbols="ESH1",
                schema="mbo",
                start="2020-12-28",
                end="2020-12-28T23:00",
                encoding="bin",
                compression="zstd",
                stype_in="zzz",  # <--- invalid
            )

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_batch_submit_sends_expected_request(self, mocker) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.post")

        # Act
        self.client.batch.submit(
            dataset="GLBX.MDP3",
            symbols="ESH1",
            schema="trades",
            start="2020-12-28T12:00",
            end="2020-12-29",
            encoding="csv",
            delivery="http",
            compression="zstd",
        )

        # Assert
        call = mocked_get.call_args.kwargs
        assert call["url"] == "https://hist.databento.com/v1/batch.submit"
        assert call["headers"] == {"accept": "application/json"}
        assert ("dataset", "glbx.mdp3") in call["params"]
        assert ("symbols", "ESH1") in call["params"]
        assert ("schema", "trades") in call["params"]
        assert ("start", "2020-12-28T12:00:00") in call["params"]
        assert ("end", "2020-12-29T00:00:00") in call["params"]
        assert ("encoding", "csv") in call["params"]
        assert ("delivery", "http") in call["params"]
        assert ("compression", "zstd") in call["params"]
        assert ("stype_in", "native") in call["params"]
        assert ("stype_out", "product_id") in call["params"]
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_batch_query_sends_expected_request(self, mocker) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")
        job_id = "some_job_id"

        # Act
        self.client.batch.query(job_id)

        # Assert
        call = mocked_get.call_args.kwargs
        assert call["url"] == "https://hist.databento.com/v1/batch.query_job"
        assert call["headers"] == {"accept": "application/json"}
        assert ("job_id", job_id) in call["params"]
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)
