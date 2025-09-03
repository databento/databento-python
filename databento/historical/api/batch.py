from __future__ import annotations

import asyncio
import hashlib
import logging
import warnings
import zipfile
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from os import PathLike
from pathlib import Path
from time import sleep
from typing import Any
from typing import ClassVar
from typing import Final

import pandas as pd
import requests
from databento_dbn import Compression
from databento_dbn import Encoding
from databento_dbn import Schema
from databento_dbn import SType
from requests.auth import HTTPBasicAuth

from databento.common import API_VERSION
from databento.common.constants import HTTP_STREAMING_READ_SIZE
from databento.common.enums import Delivery
from databento.common.enums import JobState
from databento.common.enums import SplitDuration
from databento.common.error import BentoError
from databento.common.error import BentoHttpError
from databento.common.error import BentoWarning
from databento.common.http import BentoHttpAPI
from databento.common.http import check_http_error
from databento.common.parsing import datetime_to_string
from databento.common.parsing import optional_datetime_to_string
from databento.common.parsing import optional_states_list_to_string
from databento.common.parsing import symbols_list_to_list
from databento.common.publishers import Dataset
from databento.common.validation import validate_enum
from databento.common.validation import validate_path
from databento.common.validation import validate_semantic_string


logger = logging.getLogger(__name__)

BATCH_DOWNLOAD_MAX_RETRIES: Final = 5


class BatchHttpAPI(BentoHttpAPI):
    """
    Provides request methods for the batch HTTP API endpoints.
    """

    def __init__(self, key: str, gateway: str) -> None:
        super().__init__(key=key, gateway=gateway)
        self._base_url = gateway + f"/v{API_VERSION}/batch"

    def submit_job(
        self,
        dataset: Dataset | str,
        symbols: Iterable[str | int] | str | int,
        schema: Schema | str,
        start: pd.Timestamp | datetime | date | str | int,
        end: pd.Timestamp | datetime | date | str | int | None = None,
        encoding: Encoding | str = "dbn",
        compression: Compression | str = "zstd",
        pretty_px: bool = False,
        pretty_ts: bool = False,
        map_symbols: bool = False,
        split_symbols: bool = False,
        split_duration: SplitDuration | str = "day",
        split_size: int | None = None,
        delivery: Delivery | str = "download",
        stype_in: SType | str = "raw_symbol",
        stype_out: SType | str = "instrument_id",
        limit: int | None = None,
    ) -> dict[str, Any]:
        """
        Request a new time series data batch download from Databento.

        Makes a `POST /batch.submit_job` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset code (string identifier) for the request.
        symbols : Iterable[str | int] or str or int
            The instrument symbols to filter for. Takes up to 2,000 symbols per request.
            If more than 1 symbol is specified, the data is merged and sorted by time.
            If 'ALL_SYMBOLS' or `None` then will select **all** symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        start : pd.Timestamp, datetime, date, str, or int
            The inclusive start of the request range.
            Filters on `ts_recv` if it exists in the schema, otherwise `ts_event`.
            Assumes UTC as timezone unless passed a tz-aware object.
            If an integer is passed, then this represents nanoseconds since the UNIX epoch.
        end : pd.Timestamp, datetime, date, str, or int, optional
            The exclusive end of the request range.
            Filters on `ts_recv` if it exists in the schema, otherwise `ts_event`.
            Assumes UTC as timezone unless passed a tz-aware object.
            If an integer is passed, then this represents nanoseconds since the UNIX epoch.
            Defaults to the forward filled value of `start` based on the resolution provided.
        encoding : Encoding or str {'dbn', 'csv', 'json'}, default 'dbn'
            The data encoding.
        compression : Compression or str {'none', 'zstd'}, default 'zstd'
            The data compression format (if any).
        pretty_px : bool, default False
            If prices should be formatted to the correct scale (using the fixed-precision scalar 1e-9).
            Only applicable for 'csv' or 'json' encodings.
        pretty_ts : bool, default False
            If timestamps should be formatted as ISO 8601 strings.
            Only applicable for 'csv' or 'json' encodings.
        map_symbols : bool, default False
            If the requested symbol should be appended to every text encoded record.
            Only applicable for 'csv' or 'json' encodings.
        split_symbols : bool, default False
            If files should be split by raw symbol. Cannot be requested with `'ALL_SYMBOLS'`.
        split_duration : SplitDuration or str {'day', 'week', 'month', 'none'}, default 'day'
            The maximum time duration before batched data is split into multiple files.
            A week starts on Sunday UTC.
        split_size : int, optional
            The maximum size (bytes) of each batched data file before being split.
            Must be an integer between 1e9 and 10e9 inclusive (1GB - 10GB).
        delivery : Delivery or str {'download'}, default 'download'
            The delivery mechanism for the processed batched data files.
            Only 'download' is supported at this time.
        stype_in : SType or str, default 'raw_symbol'
            The input symbology type to resolve from.
        stype_out : SType or str, default 'instrument_id'
            The output symbology type to resolve to.
        limit : int, optional
            The maximum number of records to return. If `None` then no limit.

        Returns
        -------
        dict[str, Any]
            The job info for batch download request.

        Warnings
        --------
        Calling this method will incur a cost.

        """
        stype_in_valid = validate_enum(stype_in, SType, "stype_in")
        symbols_list = symbols_list_to_list(symbols, stype_in_valid)

        data: dict[str, object | None] = {
            "dataset": validate_semantic_string(dataset, "dataset"),
            "start": datetime_to_string(start),
            "end": optional_datetime_to_string(end),
            "symbols": ",".join(symbols_list),
            "schema": str(validate_enum(schema, Schema, "schema")),
            "stype_in": str(stype_in_valid),
            "stype_out": str(validate_enum(stype_out, SType, "stype_out")),
            "encoding": str(validate_enum(encoding, Encoding, "encoding")),
            "compression": (
                str(validate_enum(compression, Compression, "compression")) if compression else None
            ),
            "pretty_px": pretty_px,
            "pretty_ts": pretty_ts,
            "map_symbols": map_symbols,
            "split_symbols": split_symbols,
            "split_duration": str(
                validate_enum(split_duration, SplitDuration, "split_duration"),
            ),
            "delivery": str(validate_enum(delivery, Delivery, "delivery")),
        }

        # Optional Parameters
        if limit is not None:
            data["limit"] = str(limit)
        if split_size is not None:
            data["split_size"] = str(split_size)

        return self._post(
            url=self._base_url + ".submit_job",
            data=data,
            basic_auth=True,
        ).json()

    def list_jobs(
        self,
        states: Iterable[JobState | str] | JobState | str | None = "queued,processing,done",
        since: pd.Timestamp | datetime | date | str | int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Request all batch job details for the user account.

        The job details will be sorted in order of `ts_received`.

        Makes a `GET /batch.list_jobs` HTTP request.

        Parameters
        ----------
        states : Iterable[JobState | str] or JobState or str, optional {'queued', 'processing', 'done', 'expired'}  # noqa
            The filter for jobs states as an iterable of comma separated values.
            Defaults to all except 'expired'.
        since : pd.Timestamp, datetime, date, str, or int, optional
            The filter for timestamp submitted (will not include jobs prior to this).

        Returns
        -------
        list[dict[str, Any]]
            The batch job details.

        """
        params: list[tuple[str, str | None]] = [
            ("states", optional_states_list_to_string(states)),
            ("since", optional_datetime_to_string(since)),
        ]

        return self._get(
            url=self._base_url + ".list_jobs",
            params=params,
            basic_auth=True,
        ).json()

    def list_files(self, job_id: str) -> list[dict[str, Any]]:
        """
        Request details of all files for a specific batch job.

        Makes a `GET /batch.list_files` HTTP request.

        Parameters
        ----------
        job_id : str
            The batch job identifier.

        Returns
        -------
        list[dict[str, Any]]
            The file details for the batch job.

        """
        params: list[tuple[str, str | None]] = [
            ("job_id", job_id),
        ]

        return self._get(
            url=self._base_url + ".list_files",
            params=params,
            basic_auth=True,
        ).json()

    def download(
        self,
        job_id: str,
        output_dir: PathLike[str] | str | None = None,
        filename_to_download: str | None = None,
        keep_zip: bool = False,
    ) -> list[Path]:
        """
        Download a batch job or a specific file to `{output_dir}/{job_id}/`.

        Will automatically generate any necessary directories if they do not
        already exist.

        Makes one or many `GET /batch/download/{job_id}/{filename}` HTTP request(s).

        Parameters
        ----------
        job_id : str
            The batch job identifier.
        output_dir: PathLike[str] or str, optional
            The directory to download the file(s) to.
            If `None`, defaults to the current working directory.
        filename_to_download : str, optional
            The specific file to download.
            If `None` then will download all files for the batch job.
        keep_zip: bool, default False
            If `True`, and `filename_to_download` is `None`, files
            will be saved as a .zip archive in the `output_dir`.

        Returns
        -------
        list[Path]
            A list of paths to the downloaded files.

        Raises
        ------
        RuntimeError
            If no files were found for the batch job.
        ValueError
            If a file fails to download.
            If `keep_zip` is True and `filename_to_download` is not `None`

        """
        if keep_zip and filename_to_download:
            raise ValueError("Cannot specify an individual file to download when `keep_zip=True`")

        batch_download = _BatchJob(
            self,
            job_id=job_id,
            output_dir=output_dir,
        )

        if filename_to_download is None:
            filenames_to_download = None
        else:
            filenames_to_download = [filename_to_download]

        downloaded_files = batch_download.download(
            filenames_to_download=filenames_to_download,
        )

        if keep_zip:
            return downloaded_files

        extracted_files = []
        for file_path in downloaded_files:
            if file_path.suffix == ".zip":
                with zipfile.ZipFile(file_path) as zip_file:
                    for e in zip_file.namelist():
                        extracted_files.append(file_path.parent / e)
                    zip_file.extractall(path=file_path.parent)
                file_path.unlink()  # remove the zip archive
            else:
                extracted_files.append(file_path)

        return extracted_files

    async def download_async(
        self,
        output_dir: PathLike[str] | str,
        job_id: str,
        filename_to_download: str | None = None,
        keep_zip: bool = False,
    ) -> list[Path]:
        """
        Asynchronously download a batch job or a specific file to
        `{output_dir}/{job_id}/`.

        Will automatically generate any necessary directories if they do not
        already exist.

        Makes one or many `GET /batch/download/{job_id}/{filename}` HTTP request(s).

        Parameters
        ----------
        output_dir: PathLike[str] or str
            The directory to download the file(s) to.
        job_id : str
            The batch job identifier.
        filename_to_download : str, optional
            The specific file to download.
            If `None` then will download all files for the batch job.
        keep_zip: bool, default False
            If `True`, and `filename_to_download` is `None`, files
            will be saved as a .zip archive in the `output_dir`.

        Returns
        -------
        list[Path]
            A list of paths to the downloaded files.

        Raises
        ------
        RuntimeError
            If no files were found for the batch job.
        ValueError
            If a file fails to download.
            If `keep_zip` is True and `filename_to_download` is not `None`

        """
        if keep_zip and filename_to_download:
            raise ValueError("Cannot specify an individual file to download when `keep_zip=True`")

        batch_download = _BatchJob(
            self,
            job_id=job_id,
            output_dir=output_dir,
        )

        if filename_to_download is None:
            filenames_to_download = None
        else:
            filenames_to_download = [filename_to_download]

        downloaded_files = await batch_download.download_async(
            filenames_to_download=filenames_to_download,
        )

        if keep_zip:
            return downloaded_files

        extracted_files = []
        for file_path in downloaded_files:
            if file_path.suffix == ".zip":
                with zipfile.ZipFile(file_path) as zip_file:
                    for e in zip_file.namelist():
                        extracted_files.append(file_path.parent / e)
                    zip_file.extractall(path=file_path.parent)
                file_path.unlink()  # remove the zip archive
            else:
                extracted_files.append(file_path)

        return extracted_files

    def _download_batch_file(
        self,
        batch_download_file: _BatchJob._BatchJobFile,
        output_path: Path,
    ) -> Path:
        """
        Download a batch file.

        Parameters
        ----------
        batch_download_file : _BatchDownloadFile
            Instance of `_BatchDownloadFile` containing the data from the batch job manifest.
        output_path : Path
            The output path of the file.

        Returns
        -------
        Path

        Raises
        ------
        BentoError
            If the file fails to download.

        """
        attempts = 0
        logger.info(
            "Downloading batch job file to %s",
            output_path,
        )
        while True:
            headers: dict[str, str] = self._headers.copy()
            if output_path.exists():
                existing_size = output_path.stat().st_size
                if existing_size < batch_download_file.size:
                    headers["Range"] = f"bytes={existing_size}-{batch_download_file.size - 1}"
                    mode = "ab"
                elif existing_size == batch_download_file.size:
                    # File exists and is complete
                    break
                else:
                    raise FileExistsError(
                        f"Batch file {output_path.name} already exists and has a larger than expected size.",
                    )
            else:
                mode = "wb"
            try:
                with requests.get(
                    url=batch_download_file.https_url,
                    headers=headers,
                    auth=HTTPBasicAuth(username=self._key, password=""),
                    allow_redirects=True,
                    stream=True,
                ) as response:
                    check_http_error(response)
                    with open(output_path, mode=mode) as f:
                        for chunk in response.iter_content(chunk_size=HTTP_STREAMING_READ_SIZE):
                            f.write(chunk)

                            # Successfully wrote some data, reset attempts counter
                            if attempts > 0:
                                attempts = 0
                                logger.info(f"Resumed download of {output_path.name}.")
            except BentoHttpError as exc:
                if exc.http_status == 429:
                    wait_time = int(exc.headers.get("Retry-After", 1))
                    sleep(wait_time)
                    continue  # try again
                raise
            except Exception as exc:
                if attempts < BATCH_DOWNLOAD_MAX_RETRIES:
                    attempts += 1
                    logger.error(
                        f"Retrying download of {output_path.name} due to error, "
                        f"{attempts}/{BATCH_DOWNLOAD_MAX_RETRIES}: {exc}",
                    )
                    continue  # try again
                raise BentoError(f"Error downloading file: {exc}") from None
            else:
                break

        logger.debug("Download of %s completed", output_path.name)
        hash_algo, _, hash_hex = batch_download_file.hash_str.partition(":")

        if hash_algo == "sha256":
            output_hash = hashlib.new(hash_algo)
            with open(output_path, "rb") as fd:
                while chunk := fd.read(32_000_000):
                    output_hash.update(chunk)

            if output_hash.hexdigest() != hash_hex:
                warn_msg = f"Downloaded file failed checksum validation: {output_path.name}"
                logger.warning(warn_msg)
                warnings.warn(warn_msg, category=BentoWarning)
        else:
            logger.warning(
                "Skipping %s checksum because %s is not supported",
                output_path.name,
                hash_algo,
            )

        return output_path

    def _download_batch_zip(
        self,
        batch_download_url: str,
        output_path: Path,
    ) -> Path:
        """
        Download all batch files as a .zip archive.

        Parameters
        ----------
        batch_download_url : _BatchDownloadFile
            The batch download URL for the zipfile, should contain the job ID appended with .zip.
        output_path : Path
            The output path of the file.

        Returns
        -------
        Path

        Raises
        ------
        BentoError
            If the file fails to download.

        """
        attempts = 0
        logger.info(
            "Downloading batch job zip to %s",
            output_path,
        )
        while True:
            headers: dict[str, str] = self._headers.copy()

            try:
                with requests.get(
                    url=batch_download_url,
                    headers=headers,
                    auth=HTTPBasicAuth(username=self._key, password=""),
                    allow_redirects=True,
                    stream=True,
                ) as response:
                    check_http_error(response)
                    with open(output_path, mode="wb") as f:
                        for chunk in response.iter_content(chunk_size=HTTP_STREAMING_READ_SIZE):
                            f.write(chunk)
            except BentoHttpError as exc:
                if exc.http_status == 429:
                    wait_time = int(exc.headers.get("Retry-After", 1))
                    sleep(wait_time)
                    continue  # try again
                raise
            except Exception as exc:
                if attempts < BATCH_DOWNLOAD_MAX_RETRIES:
                    logger.error(
                        f"Retrying download of {output_path.name} due to error: {exc}",
                    )
                    attempts += 1
                    continue  # try again
                raise BentoError(f"Error downloading file: {exc}") from None
            else:
                logger.debug("Download of %s completed", output_path.name)
                return output_path


class _BatchJob:
    """
    Helper class for downloading multiple batch files from a job.

    Supports sync and async downloads using a shared `ThreadPoolExecutor`.

    """

    _executor: ClassVar = ThreadPoolExecutor(
        thread_name_prefix="databento_batch",
    )

    @dataclass
    class _BatchJobFile:
        filename: str
        hash_str: str
        https_url: str
        size: int

    def __init__(
        self,
        batch_http_api: BatchHttpAPI,
        job_id: str,
        output_dir: PathLike[str] | str | None = None,
    ):
        if output_dir is None:
            output_dir = Path.cwd()

        job_details = batch_http_api.list_files(job_id)
        if not job_details:
            error_message = f"No files found for batch job {job_id}"
            logger.error(error_message)
            raise RuntimeError(error_message)

        zip_filename = f"{job_id}.zip"
        zip_url = None

        batch_files = []
        for file_detail in job_details:
            try:
                filename = str(file_detail["filename"])
                hash_digest = str(file_detail["hash"])
                size = int(file_detail["size"])
                urls = file_detail["urls"]
            except KeyError as exc:
                missing_key = exc.args[0]
                raise BentoError(f"Batch job manifest missing key '{missing_key}'") from None
            except TypeError:
                raise BentoError("Error parsing job manifest") from None

            try:
                https_url = urls["https"]
            except KeyError:
                raise ValueError(
                    f"Cannot download {filename} over HTTPS, "
                    "'download' delivery is not available for this job.",
                ) from None

            if zip_url is None:
                zip_url = urls["https"].replace(filename, zip_filename)

            batch_files.append(
                _BatchJob._BatchJobFile(
                    filename=filename,
                    hash_str=hash_digest,
                    https_url=https_url,
                    size=size,
                ),
            )

        if not batch_files or not zip_url:
            raise ValueError(f"No job files for {job_id}.")

        self._zip_filename = zip_filename
        self._zip_url = zip_url
        self._batch_http_api = batch_http_api
        self._output_dir = validate_path(output_dir, "output_dir") / job_id
        self._batch_files = batch_files

    def download(
        self,
        filenames_to_download: Iterable[str] | None,
    ) -> list[Path]:
        self._output_dir.mkdir(
            exist_ok=True,
            parents=True,
        )

        file_paths = []
        tasks = []

        if filenames_to_download is None:
            tasks.append(
                self._executor.submit(
                    self._batch_http_api._download_batch_zip,
                    self._zip_url,
                    self._output_dir / self._zip_filename,
                ),
            )
        else:
            filenames_to_download = set(filenames_to_download)
            for batch_file in self._batch_files:
                if not filenames_to_download:
                    break

                if batch_file.filename not in filenames_to_download:
                    continue

                tasks.append(
                    self._executor.submit(
                        self._batch_http_api._download_batch_file,
                        batch_file,
                        self._output_dir / batch_file.filename,
                    ),
                )
                filenames_to_download.remove(batch_file.filename)

        for completed in as_completed(tasks):
            path = completed.result()
            file_paths.append(path)

        return file_paths

    async def download_async(
        self,
        filenames_to_download: Iterable[str] | None,
    ) -> list[Path]:
        self._output_dir.mkdir(
            exist_ok=True,
            parents=True,
        )

        file_paths: list[Path] = []
        tasks = []

        if filenames_to_download is None:
            tasks.append(
                asyncio.get_running_loop().run_in_executor(
                    self._executor,
                    self._batch_http_api._download_batch_zip,
                    self._zip_url,
                    self._output_dir / self._zip_filename,
                ),
            )
        else:
            filenames_to_download = set(filenames_to_download)

            tasks = []
            for batch_file in self._batch_files:
                if not filenames_to_download:
                    break

                if batch_file.filename not in filenames_to_download:
                    continue

                tasks.append(
                    asyncio.get_running_loop().run_in_executor(
                        self._executor,
                        self._batch_http_api._download_batch_file,
                        batch_file,
                        self._output_dir / batch_file.filename,
                    ),
                )

        for completed in asyncio.as_completed(tasks):
            try:
                path = await completed
            except Exception:
                for task in tasks:
                    task.cancel()
                raise
            file_paths.append(path)

        return file_paths
