from unittest.mock import MagicMock

import databento as db
import pytest
import requests
from databento.historical.client import Historical


def test_batch_submit_job_given_invalid_schema_raises_error(
    historical_client: Historical,
) -> None:
    # Arrange, Act, Assert
    with pytest.raises(ValueError):
        historical_client.batch.submit_job(
            dataset="GLBX.MDP3",
            symbols=["ES"],
            schema="ticks",  # <--- invalid
            start="2020-12-28",
            end="2020-12-28T23:00",
            encoding="csv",
        )


def test_batch_submit_job_given_invalid_encoding_raises_error(
    historical_client: Historical,
) -> None:
    # Arrange, Act, Assert
    with pytest.raises(ValueError):
        historical_client.batch.submit_job(
            dataset="GLBX.MDP3",
            symbols=["ES"],
            schema="mbo",
            start="2020-12-28",
            end="2020-12-28T23:00",
            encoding="gzip",  # <--- invalid
        )


def test_batch_submit_job_given_invalid_stype_in_raises_error(
    historical_client: Historical,
) -> None:
    # Arrange, Act, Assert
    with pytest.raises(ValueError):
        historical_client.batch.submit_job(
            dataset="GLBX.MDP3",
            symbols="ESH1",
            schema="mbo",
            start="2020-12-28",
            end="2020-12-28T23:00",
            encoding="dbn",
            compression="zstd",
            stype_in="zzz",  # <--- invalid
        )


def test_batch_submit_job_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "post", mocked_post := MagicMock())

    # Act
    historical_client.batch.submit_job(
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
    call = mocked_post.call_args.kwargs
    assert (
        call["url"] == f"{historical_client.gateway}/v{db.API_VERSION}/batch.submit_job"
    )
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["data"] == {
        "dataset": "GLBX.MDP3",
        "start": "2020-12-28T12:00",
        "end": "2020-12-29",
        "symbols": "ESH1",
        "schema": "trades",
        "stype_in": "raw_symbol",
        "stype_out": "instrument_id",
        "encoding": "csv",
        "compression": "zstd",
        "pretty_px": False,
        "pretty_ts": False,
        "map_symbols": False,
        "split_symbols": False,
        "split_duration": "day",
        "packaging": "none",
        "delivery": "download",
        "split_size": "10000000000",
    }
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


def test_batch_list_jobs_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())

    # Act
    historical_client.batch.list_jobs(since="2022-01-01")

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"] == f"{historical_client.gateway}/v{db.API_VERSION}/batch.list_jobs"
    )
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["params"] == [
        ("states", "received,queued,processing,done"),
        ("since", "2022-01-01"),
    ]
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


def test_batch_list_files_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())
    job_id = "GLBX-20220610-5DEFXVTMSM"

    # Act
    historical_client.batch.list_files(job_id=job_id)

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"] == f"{historical_client.gateway}/v{db.API_VERSION}/batch.list_files"
    )
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["params"] == [
        ("job_id", job_id),
    ]
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)


def test_batch_download_single_file_sends_expected_request(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
) -> None:
    # Arrange
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())
    job_id = "GLBX-20220610-5DEFXVTMSM"
    filename = "glbx-mdp3-20220610.mbo.csv.zst"

    # Act
    with pytest.raises(ValueError):
        # We expect this to fail since this is not a real batch job.
        historical_client.batch.download(
            job_id=job_id,
            output_dir="my_data",
            filename_to_download=filename,
        )

    # Assert
    call = mocked_get.call_args.kwargs
    assert (
        call["url"] == f"{historical_client.gateway}/v{db.API_VERSION}/batch.list_files"
    )
    assert sorted(call["headers"].keys()) == ["accept", "user-agent"]
    assert call["headers"]["accept"] == "application/json"
    assert all(v in call["headers"]["user-agent"] for v in ("Databento/", "Python/"))
    assert call["params"] == [
        ("job_id", job_id),
    ]
    assert call["timeout"] == (100, 100)
    assert isinstance(call["auth"], requests.auth.HTTPBasicAuth)
