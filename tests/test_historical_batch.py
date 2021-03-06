import sys

import databento as db
import pytest
import requests


class TestHistoricalBatch:
    def setup(self) -> None:
        key = "DUMMY_ACCESS_KEY"
        self.client = db.Historical(key=key)

    def test_batch_timeseries_submit_given_invalid_schema_raises_error(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(ValueError):
            self.client.batch.timeseries_submit(
                dataset="GLBX.MDP3",
                symbols=["ES"],
                schema="ticks",  # <--- invalid
                start="2020-12-28",
                end="2020-12-28T23:00",
                encoding="csv",
            )

    def test_batch_timeseries_submit_given_invalid_encoding_raises_error(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(ValueError):
            self.client.batch.timeseries_submit(
                dataset="GLBX.MDP3",
                symbols=["ES"],
                schema="mbo",
                start="2020-12-28",
                end="2020-12-28T23:00",
                encoding="gzip",  # <--- invalid
            )

    def test_batch_timeseries_submit_given_invalid_stype_in_raises_error(self) -> None:
        # Arrange, Act, Assert
        with pytest.raises(ValueError):
            self.client.batch.timeseries_submit(
                dataset="GLBX.MDP3",
                symbols="ESH1",
                schema="mbo",
                start="2020-12-28",
                end="2020-12-28T23:00",
                encoding="dbz",
                compression="zstd",
                stype_in="zzz",  # <--- invalid
            )

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_batch_timeseries_submit_sends_expected_request(self, mocker) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.post")

        # Act
        self.client.batch.timeseries_submit(
            dataset="GLBX.MDP3",
            symbols="ESH1",
            schema="trades",
            start="2020-12-28T12:00",
            end="2020-12-29",
            encoding="csv",
            split_duration="day",
            split_size=10000000000,
            packaging="none",
            delivery="download",
            compression="zstd",
        )

        # Assert
        call = mocked_get.call_args.kwargs
        assert call["url"] == "https://hist.databento.com/v1/batch.timeseries_submit"
        assert call["headers"] == {"accept": "application/json"}
        assert call["params"] == [
            ("dataset", "glbx.mdp3"),
            ("symbols", "ESH1"),
            ("schema", "trades"),
            ("start", "2020-12-28T12:00:00"),
            ("end", "2020-12-29T00:00:00"),
            ("encoding", "csv"),
            ("compression", "zstd"),
            ("stype_in", "native"),
            ("stype_out", "product_id"),
            ("split_duration", "day"),
            ("packaging", "none"),
            ("delivery", "download"),
            ("split_size", "10000000000"),
        ]
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="incompatible mocking")
    def test_batch_list_jobs_sends_expected_request(self, mocker) -> None:
        # Arrange
        mocked_get = mocker.patch("requests.get")

        # Act
        self.client.batch.list_jobs(since="2022-01-01")

        # Assert
        call = mocked_get.call_args.kwargs
        assert call["url"] == "https://hist.databento.com/v1/batch.list_jobs"
        assert call["headers"] == {"accept": "application/json"}
        assert call["params"] == [
            ("states", "queued,processing,done"),
            ("since", "2022-01-01T00:00:00"),
        ]
        assert call["timeout"] == (100, 100)
        assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)
