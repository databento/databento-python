import hashlib
from pathlib import Path
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
    assert call["url"] == f"{historical_client.gateway}/v{db.API_VERSION}/batch.submit_job"
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
    assert call["url"] == f"{historical_client.gateway}/v{db.API_VERSION}/batch.list_jobs"
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
    assert call["url"] == f"{historical_client.gateway}/v{db.API_VERSION}/batch.list_files"
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
    tmp_path: Path,
) -> None:
    """
    Test batch download by setting up a MagicMock and checking the download
    request that will be sent.
    """
    # Arrange
    job_id = "GLBX-20220610-5DEFXVTMSM"
    filename = "glbx-mdp3-20220610.mbo.csv.zst"
    file_content = b""
    file_hash = f"sha256:{hashlib.sha256(file_content).hexdigest()}"
    file_size = len(file_content)

    # Mock the call to list files so it returns a test manifest
    monkeypatch.setattr(
        historical_client.batch,
        "list_files",
        mocked_batch_list_files := MagicMock(
            return_value=[
                {
                    "filename": filename,
                    "hash": file_hash,
                    "size": file_size,
                    "urls": {
                        "https": f"localhost:442/v0/batch/download/TESTUSER/{job_id}/{filename}",
                        "ftp": "",
                    },
                },
            ],
        ),
    )

    # Mock the call for get, so we can capture the download arguments
    monkeypatch.setattr(requests, "get", mocked_get := MagicMock())

    # Act
    historical_client.batch.download(
        job_id=job_id,
        output_dir=tmp_path,
        filename_to_download=filename,
    )

    # Assert
    assert mocked_batch_list_files.call_args.args == (job_id,)

    call = mocked_get.call_args.kwargs
    assert call["allow_redirects"]
    assert call["headers"]["accept"] == "application/json"
    assert call["stream"]
    assert call["url"] == f"localhost:442/v0/batch/download/TESTUSER/{job_id}/{filename}"


def test_batch_download_rate_limit_429(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
    tmp_path: Path,
) -> None:
    """
    Tests batch download by setting up a MagicMock which will return a 429
    status code followed by a 200 with the content "unittest".

    A file should be "downloaded" which contains this content, having
    retried the request due to the 429.

    """
    # Arrange
    job_id = "GLBX-20220610-5DEFXVTMSM"
    filename = "glbx-mdp3-20220610.mbo.csv.zst"
    file_content = b"unittest"
    file_hash = f"sha256:{hashlib.sha256(file_content).hexdigest()}"
    file_size = len(file_content)

    # Mock the call to list files so it returns a test manifest
    monkeypatch.setattr(
        historical_client.batch,
        "list_files",
        mocked_batch_list_files := MagicMock(
            return_value=[
                {
                    "filename": filename,
                    "hash": file_hash,
                    "size": file_size,
                    "urls": {
                        "https": f"localhost:442/v0/batch/download/TESTUSER/{job_id}/{filename}",
                        "ftp": "",
                    },
                },
            ],
        ),
    )

    # Mock the call for get, so we can simulate a 429 response
    rate_limit_response = requests.Response()
    rate_limit_response.status_code = 429
    rate_limit_response.headers["Retry-After"] = "0"
    ok_response = MagicMock()
    ok_response.__enter__.return_value = MagicMock(
        status_code=200,
        iter_content=MagicMock(return_value=iter([file_content])),
    )
    monkeypatch.setattr(
        requests,
        "get",
        MagicMock(
            side_effect=[rate_limit_response, ok_response],
        ),
    )

    # Act
    downloaded_files = historical_client.batch.download(
        job_id=job_id,
        output_dir=tmp_path,
        filename_to_download=filename,
    )

    # Assert
    assert mocked_batch_list_files.call_args.args == (job_id,)
    assert len(downloaded_files) == 1
    assert downloaded_files[0].read_bytes() == file_content
