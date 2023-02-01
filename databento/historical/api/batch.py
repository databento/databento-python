import os
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import requests
from databento.common.enums import (
    Compression,
    Dataset,
    Delivery,
    Encoding,
    Packaging,
    Schema,
    SplitDuration,
    SType,
)
from databento.common.logging import log_error, log_info
from databento.common.parsing import (
    datetime_to_string,
    optional_datetime_to_string,
    optional_symbols_list_to_string,
    optional_values_list_to_string,
)
from databento.common.validation import validate_enum
from databento.historical.api import API_VERSION
from databento.historical.http import BentoHttpAPI, check_http_error
from requests.auth import HTTPBasicAuth


class BatchHttpAPI(BentoHttpAPI):
    """
    Provides request methods for the batch HTTP API endpoints.
    """

    def __init__(self, key: str, gateway: str) -> None:
        super().__init__(key=key, gateway=gateway)
        self._base_url = gateway + f"/v{API_VERSION}/batch"

    def submit_job(
        self,
        dataset: Union[Dataset, str],
        start: Union[pd.Timestamp, date, str, int],
        end: Union[pd.Timestamp, date, str, int],
        symbols: Optional[Union[List[str], str]],
        schema: Union[Schema, str],
        encoding: Union[Encoding, str] = "dbn",
        compression: Optional[Union[Compression, str]] = "none",
        split_duration: Union[SplitDuration, str] = "day",
        split_size: Optional[int] = None,
        packaging: Union[Packaging, str] = "none",
        delivery: Union[Delivery, str] = "download",
        stype_in: Union[SType, str] = "native",
        stype_out: Union[SType, str] = "product_id",
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Request a new time series data batch download from Databento.

        Makes a `POST /batch.submit_job` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset code (string identifier) for the request.
        start : pd.Timestamp or date or str or int
            The start datetime of the request time range (inclusive).
            Assumes UTC as timezone unless passed a tz-aware object.
            If an integer is passed, then this represents nanoseconds since UNIX epoch.
        end : pd.Timestamp or date or str or int
            The end datetime of the request time range (exclusive).
            Assumes UTC as timezone unless passed a tz-aware object.
            If an integer is passed, then this represents nanoseconds since UNIX epoch.
        symbols : List[Union[str, int]] or str
            The product symbols to filter for. Takes up to 2,000 symbols per request.
            If more than 1 symbol is specified, the data is merged and sorted by time.
            If 'ALL_SYMBOLS' or `None` then will be for **all** symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        encoding : Encoding or str {'dbn', 'csv', 'json'}, default 'dbn'
            The data encoding.
        compression : Compression or str {'none', 'zstd'}, optional
            The data compression format (if any).
        split_duration : SplitDuration or str {'day', 'week', 'month', 'none'}, default 'day'
            The maximum time duration before batched data is split into multiple files.
            A week starts on Sunday UTC.
        split_size : int, optional
            The maximum size (bytes) of each batched data file before being split.
        packaging : Packaging or str {'none', 'zip', 'tar'}, default 'none'
            The archive type to package all batched data files in.
        delivery : Delivery or str {'download', 's3', 'disk'}, default 'download'
            The delivery mechanism for the processed batched data files.
        stype_in : SType or str, default 'native'
            The input symbology type to resolve from.
        stype_out : SType or str, default 'product_id'
            The output symbology type to resolve to.
        limit : int, optional
            The maximum number of records for the request. If `None` then no limit.

        Returns
        -------
        Dict[str, Any]
            The job info for batch download request.

        Warnings
        --------
        Calling this method will incur a cost.

        """
        stype_in_valid = validate_enum(stype_in, SType, "stype_in")
        symbols_list = optional_symbols_list_to_string(symbols, stype_in_valid)
        params: List[Tuple[str, str]] = [
            ("dataset", dataset),
            ("start", datetime_to_string(start)),
            ("end", datetime_to_string(end)),
            ("symbols", str(symbols_list)),
            ("schema", str(validate_enum(schema, Schema, "schema"))),
            ("stype_in", str(stype_in_valid)),
            ("stype_out", str(validate_enum(stype_out, SType, "stype_out"))),
            ("encoding", str(validate_enum(encoding, Encoding, "encoding"))),
            (
                "compression",
                str(validate_enum(compression, Compression, "compression")),
            ),
            (
                "split_duration",
                str(validate_enum(split_duration, SplitDuration, "split_duration")),
            ),
            ("packaging", str(validate_enum(packaging, Packaging, "packaging"))),
            ("delivery", str(validate_enum(delivery, Delivery, "delivery"))),
        ]

        # Optional Parameters
        if limit is not None:
            params.append(("limit", str(limit)))
        if split_size is not None:
            params.append(("split_size", str(split_size)))

        return self._post(
            url=self._base_url + ".submit_job",
            params=params,
            basic_auth=True,
        ).json()

    def list_jobs(
        self,
        states: Optional[Union[List[str], str]] = "received,queued,processing,done",
        since: Optional[Union[pd.Timestamp, date, str, int]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Request all batch job details for the user account.

        The job details will be sorted in order of `ts_received`.

        Makes a `GET /batch.list_jobs` HTTP request.

        Parameters
        ----------
        states : List[str] or str, optional {'received', 'queued', 'processing', 'done', 'expired'}  # noqa
            The filter for jobs states as a list of comma separated values.
        since : pd.Timestamp or date or str or int, optional
            The filter for timestamp submitted (will not include jobs prior to this).

        Returns
        -------
        List[Dict[str, Any]]
            The batch job details.

        """
        params: List[Tuple[str, Optional[str]]] = [
            ("states", optional_values_list_to_string(states)),
            ("since", optional_datetime_to_string(since)),
        ]

        return self._get(
            url=self._base_url + ".list_jobs",
            params=params,
            basic_auth=True,
        ).json()

    def list_files(self, job_id: str) -> List[Dict[str, Any]]:
        """
        Request details of all files for a specific batch job.

        Makes a `GET /batch.list_files` HTTP request.

        Parameters
        ----------
        job_id : str
            The batch job identifier.

        Returns
        -------
        List[Dict[str, Any]]
            The file details for the batch job.

        """
        params: List[Tuple[str, str]] = [
            ("job_id", job_id),
        ]

        return self._get(
            url=self._base_url + ".list_files",
            params=params,
            basic_auth=True,
        ).json()

    def download(
        self,
        output_dir: Union[Path, str],
        job_id: str,
        filename_to_download: Optional[str] = None,
    ) -> None:
        """
        Download a batch job or a specific file to `{output_dir}/{job_id}/`.
         - Creates the directories if any do not already exist
         - Partially downloaded files will be retried using a range request

        Makes one or many `GET /batch.download` HTTP request(s).

        Parameters
        ----------
        output_dir: Path or str
            The directory to download the file(s) to.
        job_id : str
            The batch job identifier.
        filename_to_download : str, optional
            The specific file to download.
            If `None` then will download all files for the batch job.

        """
        params: List[Tuple[str, str]] = [
            ("job_id", job_id),
        ]

        job_files: List[Dict[str, Union[str, int]]] = self._get(
            url=self._base_url + ".list_files",
            params=params,
            basic_auth=True,
        ).json()

        if not job_files:
            log_error(f"Cannot download batch job {job_id} (no files found).")
            return

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
                log_error(
                    f"Cannot download batch job {job_id} file "
                    f"({filename_to_download} not found)",
                )
                return

        # Prepare job directory
        job_dir = os.path.join(output_dir, job_id)
        os.makedirs(job_dir, exist_ok=True)

        for details in job_files:
            filename = str(details["filename"])
            output_path = os.path.join(job_dir, filename)
            log_info(
                f"Downloading batch job file {filename} to {output_path} ...",
            )

            self._download_file(
                job_id=job_id,
                filename=filename,
                filesize=int(details["size"]),
                output_path=output_path,
            )

    def _download_file(
        self,
        job_id: str,
        filename: str,
        filesize: int,
        output_path: str,
    ) -> None:
        params: List[Tuple[str, str]] = [
            ("job_id", job_id),
            ("filename", filename),
        ]

        headers: Dict[str, str] = self._headers.copy()
        mode = "wb"

        # Check if file already exists in partially downloaded state
        if os.path.isfile(output_path):
            existing_size = os.path.getsize(output_path)
            if existing_size < filesize:
                # Make range request for partial download,
                # will be from next byte to end of file.
                headers["Range"] = f"bytes={existing_size}-{filesize - 1}"
                mode = "ab"

        with requests.get(
            url=self._base_url + ".download",
            params=params,
            headers=headers,
            auth=HTTPBasicAuth(username=self._key, password=None),
            allow_redirects=True,
            stream=True,
        ) as response:
            check_http_error(response)

            with open(output_path, mode=mode) as f:
                for chunk in response.iter_content():
                    f.write(chunk)
