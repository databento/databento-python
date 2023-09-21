from __future__ import annotations

import logging
import os
from datetime import date
from os import PathLike
from pathlib import Path
from typing import Any

import aiohttp
import pandas as pd
import requests
from databento_dbn import Compression
from databento_dbn import Encoding
from databento_dbn import Schema
from databento_dbn import SType
from requests.auth import HTTPBasicAuth

from databento.common.enums import Delivery
from databento.common.enums import Packaging
from databento.common.enums import SplitDuration
from databento.common.parsing import datetime_to_string
from databento.common.parsing import optional_datetime_to_string
from databento.common.parsing import optional_symbols_list_to_list
from databento.common.parsing import optional_values_list_to_string
from databento.common.publishers import Dataset
from databento.common.validation import validate_enum
from databento.common.validation import validate_path
from databento.common.validation import validate_semantic_string
from databento.historical.api import API_VERSION
from databento.historical.http import BentoHttpAPI
from databento.historical.http import check_http_error
from databento.historical.http import check_http_error_async


logger = logging.getLogger(__name__)


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
        symbols: list[str] | str,
        schema: Schema | str,
        start: pd.Timestamp | date | str | int,
        end: pd.Timestamp | date | str | int | None = None,
        encoding: Encoding | str = "dbn",
        compression: Compression | str = "zstd",
        pretty_px: bool = False,
        pretty_ts: bool = False,
        map_symbols: bool = False,
        split_symbols: bool = False,
        split_duration: SplitDuration | str = "day",
        split_size: int | None = None,
        packaging: Packaging | str | None = None,
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
        symbols : list[str | int] or str
            The instrument symbols to filter for. Takes up to 2,000 symbols per request.
            If more than 1 symbol is specified, the data is merged and sorted by time.
            If 'ALL_SYMBOLS' or `None` then will be for **all** symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        start : pd.Timestamp or date or str or int
            The start datetime of the request time range (inclusive).
            Assumes UTC as timezone unless passed a tz-aware object.
            If an integer is passed, then this represents nanoseconds since the UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime of the request time range (exclusive).
            Assumes UTC as timezone unless passed a tz-aware object.
            If an integer is passed, then this represents nanoseconds since the UNIX epoch.
            Values are forward filled based on the resolution provided.
            Defaults to the same value as `start`.
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
        packaging : Packaging or str {'none', 'zip', 'tar'}, optional
            The archive type to package all batched data files in.
        delivery : Delivery or str {'download', 's3', 'disk'}, default 'download'
            The delivery mechanism for the processed batched data files.
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
        symbols_list = optional_symbols_list_to_list(symbols, stype_in_valid)
        data: dict[str, object | None] = {
            "dataset": validate_semantic_string(dataset, "dataset"),
            "start": datetime_to_string(start),
            "end": optional_datetime_to_string(end),
            "symbols": ",".join(symbols_list),
            "schema": str(validate_enum(schema, Schema, "schema")),
            "stype_in": str(stype_in_valid),
            "stype_out": str(validate_enum(stype_out, SType, "stype_out")),
            "encoding": str(validate_enum(encoding, Encoding, "encoding")),
            "compression": str(validate_enum(compression, Compression, "compression"))
            if compression
            else None,
            "pretty_px": pretty_px,
            "pretty_ts": pretty_ts,
            "map_symbols": map_symbols,
            "split_symbols": split_symbols,
            "split_duration": str(
                validate_enum(split_duration, SplitDuration, "split_duration"),
            ),
            "packaging": str(validate_enum(packaging, Packaging, "packaging"))
            if packaging
            else None,
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
        states: list[str] | str = "received,queued,processing,done",
        since: pd.Timestamp | date | str | int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Request all batch job details for the user account.

        The job details will be sorted in order of `ts_received`.

        Makes a `GET /batch.list_jobs` HTTP request.

        Parameters
        ----------
        states : list[str] or str, optional {'received', 'queued', 'processing', 'done', 'expired'}  # noqa
            The filter for jobs states as a list of comma separated values.
        since : pd.Timestamp or date or str or int, optional
            The filter for timestamp submitted (will not include jobs prior to this).

        Returns
        -------
        list[dict[str, Any]]
            The batch job details.

        """
        params: list[tuple[str, str | None]] = [
            ("states", optional_values_list_to_string(states)),
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
        output_dir: PathLike[str] | str,
        job_id: str,
        filename_to_download: str | None = None,
        enable_partial_downloads: bool = True,
    ) -> list[Path]:
        """
        Download a batch job or a specific file to `{output_dir}/{job_id}/`.

        Will automatically generate any necessary directories if they do not
        already exist.

        Makes one or many `GET /batch/download/{job_id}/{filename}` HTTP request(s).

        Parameters
        ----------
        output_dir: PathLike or str
            The directory to download the file(s) to.
        job_id : str
            The batch job identifier.
        filename_to_download : str, optional
            The specific file to download.
            If `None` then will download all files for the batch job.
        enable_partial_downloads : bool, default True
            If partially downloaded files will be resumed using range request(s).

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

        """
        output_dir = validate_path(output_dir, "output_dir")
        self._check_api_key()

        params: list[tuple[str, str | None]] = [
            ("job_id", job_id),
        ]

        job_files: list[dict[str, Any]] = self._get(
            url=self._base_url + ".list_files",
            params=params,
            basic_auth=True,
        ).json()

        if not job_files:
            logger.error("Cannot download batch job %s (no files found).", job_id)
            raise RuntimeError(f"no files for batch job {job_id}")

        if filename_to_download:
            # A specific file is being requested
            is_file_found = False
            for details in job_files:
                if details["filename"] == filename_to_download:
                    # Reduce files to download only the single file
                    job_files = [details]
                    is_file_found = True
                    break
            if not is_file_found:
                logger.error(
                    "Cannot download batch job %s file (%s not found)",
                    job_id,
                    filename_to_download,
                )
                raise ValueError(
                    f"{filename_to_download} is not a file for batch job {job_id}",
                )

        # Prepare job directory
        job_dir = Path(output_dir) / job_id
        os.makedirs(job_dir, exist_ok=True)

        file_paths = []
        for details in job_files:
            filename = str(details["filename"])
            output_path = job_dir / filename
            logger.info(
                "Downloading batch job file to %s ...",
                output_path,
            )

            urls = details.get("urls")
            if not urls:
                raise ValueError(
                    f"Cannot download {filename}, URLs were not found in manifest.",
                )

            https_url = urls.get("https")
            if not https_url:
                raise ValueError(
                    f"Cannot download {filename} over HTTPS, "
                    "'download' delivery is not available for this job.",
                )

            self._download_file(
                url=https_url,
                filesize=int(details["size"]),
                output_path=output_path,
                enable_partial_downloads=enable_partial_downloads,
            )
            file_paths.append(output_path)

        return file_paths

    def _download_file(
        self,
        url: str,
        filesize: int,
        output_path: Path,
        enable_partial_downloads: bool,
    ) -> None:
        headers, mode = self._get_file_download_headers_and_mode(
            filesize=filesize,
            output_path=output_path,
            enable_partial_downloads=enable_partial_downloads,
        )

        with requests.get(
            url=url,
            headers=headers,
            auth=HTTPBasicAuth(username=self._key, password=""),
            allow_redirects=True,
            stream=True,
        ) as response:
            check_http_error(response)

            logger.debug("Starting download of file %s", output_path.name)
            with open(output_path, mode=mode) as f:
                for chunk in response.iter_content():
                    f.write(chunk)
            logger.debug("Download of %s completed", output_path.name)

    async def download_async(
        self,
        output_dir: PathLike[str] | str,
        job_id: str,
        filename_to_download: str | None = None,
        enable_partial_downloads: bool = True,
    ) -> list[Path]:
        """
        Asynchronously download a batch job or a specific file to
        `{output_dir}/{job_id}/`.

        Will automatically generate any necessary directories if they do not
        already exist.

        Makes one or many `GET /batch/download/{job_id}/{filename}` HTTP request(s).

        Parameters
        ----------
        output_dir: PathLike or str
            The directory to download the file(s) to.
        job_id : str
            The batch job identifier.
        filename_to_download : str, optional
            The specific file to download.
            If `None` then will download all files for the batch job.
        enable_partial_downloads : bool, default True
            If partially downloaded files will be resumed using range request(s).

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

        """
        output_dir = validate_path(output_dir, "output_dir")
        self._check_api_key()

        params: list[tuple[str, str | None]] = [
            ("job_id", job_id),
        ]

        job_files: list[dict[str, Any]] = await self._get_json_async(
            url=self._base_url + ".list_files",
            params=params,
            basic_auth=True,
        )

        if not job_files:
            logger.error("Cannot download batch job %s (no files found).", job_id)
            raise RuntimeError(f"no files for batch job {job_id}")

        if filename_to_download:
            # A specific file is being requested
            is_file_found = False
            for details in job_files:
                if details["filename"] == filename_to_download:
                    # Reduce files to download only the single file
                    job_files = [details]
                    is_file_found = True
                    break
            if not is_file_found:
                logger.error(
                    "Cannot download batch job %s file (%s not found)",
                    job_id,
                    filename_to_download,
                )
                raise ValueError(
                    f"{filename_to_download} is not a file for batch job {job_id}",
                )

        # Prepare job directory
        job_dir = Path(output_dir) / job_id
        os.makedirs(job_dir, exist_ok=True)

        file_paths = []
        for details in job_files:
            filename = str(details["filename"])
            output_path = job_dir / filename
            logger.info(
                "Downloading batch job file to %s ...",
                output_path,
            )

            urls = details.get("urls")
            if not urls:
                raise ValueError(
                    f"Cannot download {filename}, URLs were not found in manifest.",
                )

            https_url = urls.get("https")
            if not https_url:
                raise ValueError(
                    f"Cannot download {filename} over HTTPS, "
                    "'download' delivery is not available for this job.",
                )

            await self._download_file_async(
                url=https_url,
                filesize=int(details["size"]),
                output_path=output_path,
                enable_partial_downloads=enable_partial_downloads,
            )
            file_paths.append(output_path)

        return file_paths

    async def _download_file_async(
        self,
        url: str,
        filesize: int,
        output_path: Path,
        enable_partial_downloads: bool,
    ) -> None:
        headers, mode = self._get_file_download_headers_and_mode(
            filesize=filesize,
            output_path=output_path,
            enable_partial_downloads=enable_partial_downloads,
        )

        async with aiohttp.ClientSession() as session:
            async with session.get(
                url=url,
                headers=headers,
                auth=aiohttp.BasicAuth(login=self._key, password="", encoding="utf-8"),
                timeout=self.TIMEOUT,
            ) as response:
                await check_http_error_async(response)

                logger.debug("Starting async download of file %s", output_path.name)
                with open(output_path, mode=mode) as f:
                    async for chunk in response.content.iter_chunks():
                        data: bytes = chunk[0]
                        f.write(data)
                logger.debug("Download of %s completed", output_path.name)

    def _get_file_download_headers_and_mode(
        self,
        filesize: int,
        output_path: Path,
        enable_partial_downloads: bool,
    ) -> tuple[dict[str, str], str]:
        headers: dict[str, str] = self._headers.copy()
        mode = "wb"

        # Check if file already exists in partially downloaded state
        if enable_partial_downloads and output_path.is_file():
            existing_size = output_path.stat().st_size
            if existing_size < filesize:
                # Make range request for partial download,
                # will be from next byte to end of file.
                headers["Range"] = f"bytes={existing_size}-{filesize - 1}"
                mode = "ab"

        return headers, mode
