import hashlib
from collections.abc import Callable
from pathlib import Path
from unittest.mock import MagicMock
from zipfile import ZipFile

import pytest
import requests
from databento_dbn import Schema

import databento as db
from databento.common.publishers import Dataset
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
        "map_symbols": True,
        "split_symbols": False,
        "split_duration": "day",
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
        ("states", "queued,processing,done"),
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


def test_batch_download_file_exists(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
    tmp_path: Path,
) -> None:
    """
    Tests batch download by setting up a MagicMock which will return the
    content "unittest".

    A subsequent call to batch.download should not fail.

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
    ok_response = MagicMock()
    ok_response.__enter__.return_value = MagicMock(
        status_code=200,
        iter_content=MagicMock(return_value=iter([file_content])),
    )
    monkeypatch.setattr(
        requests,
        "get",
        MagicMock(
            side_effect=[ok_response],
        ),
    )

    # Act
    historical_client.batch.download(
        job_id=job_id,
        output_dir=tmp_path,
        filename_to_download=filename,
    )

    downloaded_files = historical_client.batch.download(
        job_id=job_id,
        output_dir=tmp_path,
        filename_to_download=filename,
    )

    # Assert
    assert mocked_batch_list_files.call_args.args == (job_id,)
    assert len(downloaded_files) == 1
    assert downloaded_files[0].read_bytes() == file_content


def test_batch_download_file_larger_than_expected(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
    tmp_path: Path,
) -> None:
    """
    Tests batch download by setting up a MagicMock which will return the
    content "unittest".

    Then, write some extra bytes to that file, and ensure a subsequent
    call to batch.download will raise an exception.

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
        MagicMock(
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
    ok_response = MagicMock()
    ok_response.__enter__.return_value = MagicMock(
        status_code=200,
        iter_content=MagicMock(return_value=iter([file_content])),
    )
    monkeypatch.setattr(
        requests,
        "get",
        MagicMock(
            side_effect=[ok_response],
        ),
    )

    # Act
    downloaded_files = historical_client.batch.download(
        job_id=job_id,
        output_dir=tmp_path,
        filename_to_download=filename,
    )

    # Increase the existing file size with some junk
    with downloaded_files[-1].open(mode="ab") as out:
        out.write(b"junk")

    # Assert
    with pytest.raises(FileExistsError):
        historical_client.batch.download(
            job_id=job_id,
            output_dir=tmp_path,
            filename_to_download=filename,
        )


@pytest.mark.parametrize(
    "keep_zip",
    [
        True,
        False,
    ],
)
def test_batch_download_all_files(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
    test_data: Callable[[Dataset, Schema], bytes],
    tmp_path: Path,
    keep_zip: bool,
) -> None:
    """
    Test batch download for all files requests the on-demand ZIP which is then
    decompressed client side if keep_zip is True.
    """
    # Arrange
    testfile_data = test_data(Dataset.GLBX_MDP3, Schema.TRADES)
    stub_zip_path = tmp_path / "stub.zip"
    with ZipFile(tmp_path / "stub.zip", mode="w") as stub:
        stub.writestr("testfile.dbn", testfile_data)

    job_id = "GLBX-20220610-5DEFXVTMSM"
    file_content = stub_zip_path.read_bytes()
    file_hash = f"sha256:{hashlib.sha256(file_content).hexdigest()}"
    file_size = len(file_content)

    # Mock the call to list files so it returns a test manifest
    monkeypatch.setattr(
        historical_client.batch,
        "list_files",
        mocked_batch_list_files := MagicMock(
            return_value=[
                {
                    "filename": "testfile.dbn",
                    "hash": file_hash,
                    "size": file_size,
                    "urls": {
                        "https": f"localhost:442/v0/batch/download/TESTUSER/{job_id}/testfile.dbn",
                        "ftp": "",
                    },
                },
            ],
        ),
    )

    # Mock the call for get, so we can simulate a ZIP response
    zip_response = MagicMock()
    zip_response.status_code = 200
    zip_response.__enter__.return_value = MagicMock(
        status_code=200,
        iter_content=MagicMock(return_value=iter([stub_zip_path.read_bytes()])),
    )

    monkeypatch.setattr(
        requests,
        "get",
        mocked_get := MagicMock(
            side_effect=[zip_response],
        ),
    )

    # Act
    historical_client.batch.download(
        job_id=job_id,
        output_dir=tmp_path,
        keep_zip=keep_zip,
    )

    # Assert
    assert mocked_batch_list_files.call_args.args == (job_id,)

    call = mocked_get.call_args.kwargs
    assert call["allow_redirects"]
    assert call["headers"]["accept"] == "application/json"
    assert call["stream"]
    assert call["url"] == f"localhost:442/v0/batch/download/TESTUSER/{job_id}/{job_id}.zip"

    if keep_zip:
        assert (tmp_path / job_id / f"{job_id}.zip").exists()
        assert (tmp_path / job_id / f"{job_id}.zip").read_bytes() == stub_zip_path.read_bytes()
    else:
        assert (tmp_path / job_id / "testfile.dbn").exists()
        assert (tmp_path / job_id / "testfile.dbn").read_bytes() == testfile_data


@pytest.mark.parametrize(
    "keep_zip",
    [
        True,
        False,
    ],
)
@pytest.mark.asyncio
async def test_batch_download_all_files_async(
    monkeypatch: pytest.MonkeyPatch,
    historical_client: Historical,
    test_data: Callable[[Dataset, Schema], bytes],
    tmp_path: Path,
    keep_zip: bool,
) -> None:
    """
    Test batch download for all files requests the on-demand ZIP which is then
    decompressed client side if keep_zip is True.
    """
    # Arrange
    testfile_data = test_data(Dataset.GLBX_MDP3, Schema.TRADES)
    stub_zip_path = tmp_path / "stub.zip"
    with ZipFile(tmp_path / "stub.zip", mode="w") as stub:
        stub.writestr("testfile.dbn", testfile_data)

    job_id = "GLBX-20220610-5DEFXVTMSM"
    file_content = stub_zip_path.read_bytes()
    file_hash = f"sha256:{hashlib.sha256(file_content).hexdigest()}"
    file_size = len(file_content)

    # Mock the call to list files so it returns a test manifest
    monkeypatch.setattr(
        historical_client.batch,
        "list_files",
        mocked_batch_list_files := MagicMock(
            return_value=[
                {
                    "filename": "testfile.dbn",
                    "hash": file_hash,
                    "size": file_size,
                    "urls": {
                        "https": f"localhost:442/v0/batch/download/TESTUSER/{job_id}/testfile.dbn",
                        "ftp": "",
                    },
                },
            ],
        ),
    )

    # Mock the call for get, so we can simulate a ZIP response
    zip_response = MagicMock()
    zip_response.status_code = 200
    zip_response.__enter__.return_value = MagicMock(
        status_code=200,
        iter_content=MagicMock(return_value=iter([stub_zip_path.read_bytes()])),
    )

    monkeypatch.setattr(
        requests,
        "get",
        mocked_get := MagicMock(
            side_effect=[zip_response],
        ),
    )

    # Act
    await historical_client.batch.download_async(
        job_id=job_id,
        output_dir=tmp_path,
        keep_zip=keep_zip,
    )

    # Assert
    assert mocked_batch_list_files.call_args.args == (job_id,)

    call = mocked_get.call_args.kwargs
    assert call["allow_redirects"]
    assert call["headers"]["accept"] == "application/json"
    assert call["stream"]
    assert call["url"] == f"localhost:442/v0/batch/download/TESTUSER/{job_id}/{job_id}.zip"

    if keep_zip:
        assert (tmp_path / job_id / f"{job_id}.zip").exists()
        assert (tmp_path / job_id / f"{job_id}.zip").read_bytes() == stub_zip_path.read_bytes()
    else:
        assert (tmp_path / job_id / "testfile.dbn").exists()
        assert (tmp_path / job_id / "testfile.dbn").read_bytes() == testfile_data
