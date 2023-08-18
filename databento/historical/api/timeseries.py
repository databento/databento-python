from __future__ import annotations

from datetime import date
from os import PathLike

import pandas as pd
from databento_dbn import Compression
from databento_dbn import Encoding
from databento_dbn import Schema
from databento_dbn import SType

from databento.common.dbnstore import DBNStore
from databento.common.parsing import datetime_to_string
from databento.common.parsing import optional_datetime_to_string
from databento.common.parsing import optional_symbols_list_to_list
from databento.common.publishers import Dataset
from databento.common.validation import validate_enum
from databento.common.validation import validate_file_write_path
from databento.common.validation import validate_semantic_string
from databento.historical.api import API_VERSION
from databento.historical.http import BentoHttpAPI


class TimeseriesHttpAPI(BentoHttpAPI):
    """
    Provides request methods for the time series HTTP API endpoints.
    """

    def __init__(self, key: str, gateway: str) -> None:
        super().__init__(key=key, gateway=gateway)
        self._base_url = gateway + f"/v{API_VERSION}/timeseries"

    def get_range(
        self,
        dataset: Dataset | str,
        start: pd.Timestamp | date | str | int,
        end: pd.Timestamp | date | str | int | None = None,
        symbols: list[str] | str | None = None,
        schema: Schema | str = "trades",
        stype_in: SType | str = "raw_symbol",
        stype_out: SType | str = "instrument_id",
        limit: int | None = None,
        path: PathLike[str] | str | None = None,
    ) -> DBNStore:
        """
        Request a historical time series data stream from Databento.

        Makes a `POST /timeseries.get_range` HTTP request.

        Primary method for getting historical intraday market data, daily data,
        instrument definitions and market status data directly into your application.

        This method only returns after all of the data has been downloaded,
        which can take a long time. For large requests, consider using a batch download.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset code (string identifier) for the request.
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
        symbols : list[str | instr | intt] or str, optional
            The instrument symbols to filter for. Takes up to 2,000 symbols per request.
            If more than 1 symbol is specified, the data is merged and sorted by time.
            If 'ALL_SYMBOLS' or `None` then will be for **all** symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, default 'trades'
            The data record schema for the request.
        stype_in : SType or str, default 'raw_symbol'
            The input symbology type to resolve from.
        stype_out : SType or str, default 'instrument_id'
            The output symbology type to resolve to.
        limit : int, optional
            The maximum number of records to return. If `None` then no limit.
        path : PathLike or str, optional
            The file path to stream the data to on disk (will then return a `DBNStore`).

        Returns
        -------
        DBNStore

        Notes
        -----
        The Databento Binary Encoding (DBN) will be streamed.

        Warnings
        --------
        Calling this method will incur a cost.

        """
        stype_in_valid = validate_enum(stype_in, SType, "stype_in")
        symbols_list = optional_symbols_list_to_list(symbols, stype_in_valid)
        schema_valid = validate_enum(schema, Schema, "schema")
        start_valid = datetime_to_string(start)
        end_valid = optional_datetime_to_string(end)
        data: dict[str, object | None] = {
            "dataset": validate_semantic_string(dataset, "dataset"),
            "start": start_valid,
            "symbols": ",".join(symbols_list),
            "schema": str(schema_valid),
            "stype_in": str(stype_in_valid),
            "stype_out": str(validate_enum(stype_out, SType, "stype_out")),
            "encoding": str(Encoding.DBN),  # Always request dbn
            "compression": str(Compression.ZSTD),  # Always request zstd
        }

        # Optional Parameters
        if limit is not None:
            data["limit"] = str(limit)
        if end is not None:
            data["end"] = end_valid
        if path is not None:
            path = validate_file_write_path(path, "path")

        return self._stream(
            url=self._base_url + ".get_range",
            data=data,
            basic_auth=True,
            path=path,
        )

    async def get_range_async(
        self,
        dataset: Dataset | str,
        start: pd.Timestamp | date | str | int,
        end: pd.Timestamp | date | str | int | None = None,
        symbols: list[str] | str | None = None,
        schema: Schema | str = "trades",
        stype_in: SType | str = "raw_symbol",
        stype_out: SType | str = "instrument_id",
        limit: int | None = None,
        path: PathLike[str] | str | None = None,
    ) -> DBNStore:
        """
        Asynchronously request a historical time series data stream from
        Databento.

        Makes a `POST /timeseries.get_range` HTTP request.

        Primary method for getting historical intraday market data, daily data,
        instrument definitions and market status data directly into your application.

        This coroutine will complete once all of the data has been downloaded,
        which can take a long time. For large requests, consider using a batch download.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset code (string identifier) for the request.
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
        symbols : list[str | int] or str, optional
            The instrument symbols to filter for. Takes up to 2,000 symbols per request.
            If more than 1 symbol is specified, the data is merged and sorted by time.
            If 'ALL_SYMBOLS' or `None` then will be for **all** symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        stype_in : SType or str, default 'raw_symbol'
            The input symbology type to resolve from.
        stype_out : SType or str, default 'instrument_id'
            The output symbology type to resolve to.
        limit : int, optional
            The maximum number of records to return. If `None` then no limit.
        path : PathLike or str, optional
            The file path to stream the data to on disk (will then return a `DBNStore`).

        Returns
        -------
        DBNStore

        Notes
        -----
        The Databento Binary Encoding (DBN) will be streamed.

        Warnings
        --------
        Calling this method will incur a cost.

        """
        stype_in_valid = validate_enum(stype_in, SType, "stype_in")
        symbols_list = optional_symbols_list_to_list(symbols, stype_in_valid)
        schema_valid = validate_enum(schema, Schema, "schema")
        start_valid = datetime_to_string(start)
        end_valid = optional_datetime_to_string(end)
        data: dict[str, object | None] = {
            "dataset": validate_semantic_string(dataset, "dataset"),
            "start": start_valid,
            "symbols": ",".join(symbols_list),
            "schema": str(schema_valid),
            "stype_in": str(stype_in_valid),
            "stype_out": str(validate_enum(stype_out, SType, "stype_out")),
            "encoding": str(Encoding.DBN),  # Always request dbn
            "compression": str(Compression.ZSTD),  # Always request zstd
        }

        # Optional Parameters
        if limit is not None:
            data["limit"] = str(limit)
        if end is not None:
            data["end"] = end_valid
        if path is not None:
            path = validate_file_write_path(path, "path")

        return await self._stream_async(
            url=self._base_url + ".get_range",
            data=data,
            basic_auth=True,
            path=path,
        )
