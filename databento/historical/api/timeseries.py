from __future__ import annotations

import warnings
from datetime import date
from io import BufferedIOBase
from io import BytesIO
from os import PathLike
from typing import List, Optional, Tuple, Union

import pandas as pd

from databento.common.dbnstore import DBNStore
from databento.common.deprecated import deprecated
from databento.common.enums import Compression
from databento.common.enums import Dataset
from databento.common.enums import Encoding
from databento.common.enums import Schema
from databento.common.enums import SType
from databento.common.error import BentoWarning
from databento.common.parsing import datetime_to_string
from databento.common.parsing import optional_datetime_to_string
from databento.common.parsing import optional_symbols_list_to_string
from databento.common.validation import validate_enum
from databento.common.validation import validate_semantic_string
from databento.historical.api import API_VERSION
from databento.historical.api.metadata import MetadataHttpAPI
from databento.historical.http import BentoHttpAPI


WARN_REQUEST_SIZE: int = 5 * 10**9  # 5 GB


class TimeSeriesHttpAPI(BentoHttpAPI):
    """
    Provides request methods for the time series HTTP API endpoints.
    """

    def __init__(self, key: str, gateway: str) -> None:
        super().__init__(key=key, gateway=gateway)
        self._base_url = gateway + f"/v{API_VERSION}/timeseries"

    def get_range(
        self,
        dataset: Union[Dataset, str],
        start: Union[pd.Timestamp, date, str, int],
        end: Optional[Union[pd.Timestamp, date, str, int]] = None,
        symbols: Optional[Union[List[str], str]] = None,
        schema: Union[Schema, str] = "trades",
        stype_in: Union[SType, str] = "raw_symbol",
        stype_out: Union[SType, str] = "instrument_id",
        limit: Optional[int] = None,
        path: Optional[Union[PathLike[str], str]] = None,
    ) -> DBNStore:
        """
        Request a historical time series data stream from Databento.

        Makes a `GET /timeseries.get_range` HTTP request.

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
        symbols : List[Union[str, int]] or str, optional
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
            The path to stream the data to on disk (will then return a `DBNStore`).

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
        symbols_list = optional_symbols_list_to_string(symbols, stype_in_valid)
        schema_valid = validate_enum(schema, Schema, "schema")
        start_valid = datetime_to_string(start)
        end_valid = optional_datetime_to_string(end)
        params: List[Tuple[str, Optional[str]]] = [
            ("dataset", validate_semantic_string(dataset, "dataset")),
            ("start", start_valid),
            ("end", end_valid),
            ("symbols", symbols_list),
            ("schema", str(schema_valid)),
            ("stype_in", str(stype_in_valid)),
            ("stype_out", str(validate_enum(stype_out, SType, "stype_out"))),
            ("encoding", str(Encoding.DBN)),  # Always request dbn
            ("compression", str(Compression.ZSTD)),  # Always request zstd
        ]

        # Optional Parameters
        if limit is not None:
            params.append(("limit", str(limit)))

        self._pre_check_data_size(
            dataset=dataset,
            stype_in=stype_in_valid,
            symbols=symbols_list,
            schema=schema_valid,
            start=start_valid,
            end=end_valid,
            limit=limit,
        )

        if path is not None:
            writer: BufferedIOBase = open(path, "x+b")
        else:
            writer = BytesIO()

        self._stream(
            url=self._base_url + ".get_range",
            params=params,
            basic_auth=True,
            writer=writer,
        )

        if path is not None:
            writer.close()
            return DBNStore.from_file(path)
        writer.seek(0)  # rewind for read
        return DBNStore.from_bytes(writer.read())

    @deprecated
    async def stream_async(
        self,
        dataset: Union[Dataset, str],
        start: Union[pd.Timestamp, date, str, int],
        end: Optional[Union[pd.Timestamp, date, str, int]] = None,
        symbols: Optional[Union[List[str], str]] = None,
        schema: Union[Schema, str] = "trades",
        stype_in: Union[SType, str] = "raw_symbol",
        stype_out: Union[SType, str] = "instrument_id",
        limit: Optional[int] = None,
        path: Optional[Union[PathLike[str], str]] = None,
    ) -> DBNStore:
        """
        The `.stream_async` method is deprecated and will be removed in a future
        version.
        The method has been renamed to `.get_range_async`, which you can now use.
        """
        return await self.get_range_async(
            dataset=dataset,
            start=start,
            end=end,
            symbols=symbols,
            schema=schema,
            stype_in=stype_in,
            stype_out=stype_out,
            limit=limit,
            path=path,
        )

    async def get_range_async(
        self,
        dataset: Union[Dataset, str],
        start: Union[pd.Timestamp, date, str, int],
        end: Optional[Union[pd.Timestamp, date, str, int]] = None,
        symbols: Optional[Union[List[str], str]] = None,
        schema: Union[Schema, str] = "trades",
        stype_in: Union[SType, str] = "raw_symbol",
        stype_out: Union[SType, str] = "instrument_id",
        limit: Optional[int] = None,
        path: Optional[Union[PathLike[str], str]] = None,
    ) -> DBNStore:
        """
        Asynchronously request a historical time series data stream from Databento.

        Makes a `GET /timeseries.get_range` HTTP request.

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
        symbols : List[Union[str, int]] or str, optional
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
            The path to stream the data to on disk (will then return a `DBNStore`).

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
        symbols_list = optional_symbols_list_to_string(symbols, stype_in_valid)
        schema_valid = validate_enum(schema, Schema, "schema")
        start_valid = datetime_to_string(start)
        end_valid = optional_datetime_to_string(end)
        params: List[Tuple[str, Optional[str]]] = [
            ("dataset", validate_semantic_string(dataset, "dataset")),
            ("start", start_valid),
            ("end", end_valid),
            ("symbols", symbols_list),
            ("schema", str(schema_valid)),
            ("stype_in", str(stype_in_valid)),
            ("stype_out", str(validate_enum(stype_out, SType, "stype_out"))),
            ("encoding", str(Encoding.DBN)),  # Always request dbn
            ("compression", str(Compression.ZSTD)),  # Always request zstd
        ]

        if limit is not None:
            params.append(("limit", str(limit)))

        self._pre_check_data_size(
            dataset=dataset,
            stype_in=stype_in_valid,
            symbols=symbols_list,
            schema=schema_valid,
            start=start_valid,
            end=end_valid,
            limit=limit,
        )

        if path is not None:
            writer: BufferedIOBase = open(path, "x+b")
        else:
            writer = BytesIO()

        await self._stream_async(
            url=self._base_url + ".get_range",
            params=params,
            basic_auth=True,
            writer=writer,
        )

        if path is not None:
            writer.close()
            return DBNStore.from_file(path)
        writer.seek(0)  # rewind for read
        return DBNStore.from_bytes(writer.read())

    def _pre_check_data_size(
        self,
        dataset: str,
        symbols: str,
        schema: Schema,
        start: str,
        end: Optional[str],
        stype_in: SType,
        limit: Optional[int],
    ) -> None:
        if _is_size_limited(
            schema=schema,
            limit=limit,
        ):
            return

        if _is_period_limited(
            schema=schema,
            symbols=symbols,
            start=start,
            end=end,
        ):
            return

        metadata_api = MetadataHttpAPI(
            key=self._key,
            gateway=self._gateway,
        )
        request_size = metadata_api.get_billable_size(
            dataset=dataset,
            start=start,
            end=end,
            symbols=symbols,
            schema=schema,
            stype_in=stype_in,
            limit=limit,
        )

        if request_size < WARN_REQUEST_SIZE:
            return

        warnings.warn(
            message="""The size of this streaming request is greater than 5GB.
            It is recommended to submit a batch download request for large volumes
            of data, or break this request into smaller requests.
            This warning can be suppressed:
            https://docs.python.org/3/library/warnings.html""",
            category=BentoWarning,
            stacklevel=3,  # This makes the error happen in user code
        )


def _is_size_limited(
    schema: Schema,
    limit: Optional[int],
    max_size: int = WARN_REQUEST_SIZE,
) -> bool:
    if limit is None:
        return False

    estimated_size = limit * schema.get_record_type().size_hint()
    return estimated_size < max_size


def _is_period_limited(
    schema: Schema,
    symbols: str,
    start: str,
    end: Optional[str],
    max_size: int = WARN_REQUEST_SIZE,
) -> bool:
    if end is None:
        return False

    if schema not in (
        Schema.OHLCV_1S,
        Schema.OHLCV_1M,
        Schema.OHLCV_1H,
        Schema.OHLCV_1D,
        Schema.DEFINITION,
    ):
        return False

    dt_start = pd.to_datetime(start, utc=True)
    dt_end = pd.to_datetime(end, utc=True)

    # default scale to one day for ohlcv_1d and definition
    scale = {
        Schema.OHLCV_1S: 1,
        Schema.OHLCV_1M: 60,
        Schema.OHLCV_1H: 60 * 60,
    }.get(schema, 60 * 60 * 24)

    num_symbols = len(symbols.split(","))
    num_records = num_symbols * (dt_end - dt_start).total_seconds() // scale
    estimated_size = num_records * schema.get_record_type().size_hint()
    return estimated_size < max_size
