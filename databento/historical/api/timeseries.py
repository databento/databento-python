import warnings
from datetime import date
from typing import Any, List, Optional, Tuple, Union

import pandas as pd
from databento.common.bento import Bento
from databento.common.enums import Compression, Dataset, Encoding, Schema, SType
from databento.common.logging import log_debug
from databento.common.validation import validate_enum
from databento.historical.api import API_VERSION
from databento.historical.http import BentoHttpAPI
from requests import Response


_5GB = 1024**3 * 5


class TimeSeriesHttpAPI(BentoHttpAPI):
    """
    Provides request methods for the time series HTTP API endpoints.
    """

    def __init__(self, key, gateway):
        super().__init__(key=key, gateway=gateway)
        self._base_url = gateway + f"/v{API_VERSION}/timeseries"

    def stream(
        self,
        dataset: Union[Dataset, str],
        symbols: Optional[Union[List[str], str]] = None,
        schema: Union[Schema, str] = "trades",
        start: Optional[Union[pd.Timestamp, date, str, int]] = None,
        end: Optional[Union[pd.Timestamp, date, str, int]] = None,
        stype_in: Union[SType, str] = "native",
        stype_out: Union[SType, str] = "product_id",
        limit: Optional[int] = None,
        path: Optional[str] = None,
    ) -> Bento:
        """
        Request a historical time series stream from the Databento API servers.

        Makes a `GET /timeseries.stream` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset name for the request.
        symbols : List[Union[str, int]] or str, optional
            The symbols for the request. If ``None`` then will be for ALL symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        start : pd.Timestamp or date or str or int, optional
            The start datetime (UTC) of the request time range (inclusive).
            If using an integer then this represents nanoseconds since UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime (UTC) of the request time range (exclusive).
            If using an integer then this represents nanoseconds since UNIX epoch.
        stype_in : SType or str, default 'native'
            The input symbology type to resolve from.
        stype_out : SType or str, default 'product_id'
            The output symbology type to resolve to.
        limit : int, optional
            The maximum number of records to return.
            If ``None`` is passed then no limit.
        path : str, optional
            The file path to write to on disk (if provided).

        Returns
        -------
        Bento
            If `path` provided then FileBento, otherwise MemoryBento.

        Notes
        -----
        The Databento Binary Encoding + Zstd Compression (DBZ) will be streamed.

        """
        validate_enum(schema, Schema, "schema")
        validate_enum(stype_in, SType, "stype_in")
        validate_enum(stype_out, SType, "stype_out")

        schema = Schema(schema)
        stype_in = SType(stype_in)
        stype_out = SType(stype_out)

        params: List[Tuple[str, str]] = BentoHttpAPI._timeseries_params(
            dataset=dataset,
            symbols=symbols,
            schema=schema,
            start=start,
            end=end,
            stype_in=stype_in,
            stype_out=stype_out,
            limit=limit,
        )

        params.append(("encoding", Encoding.DBZ.value))  # Always requests DBZ
        params.append(("compression", Compression.ZSTD.value))  # Always requests ZSTD

        self._pre_check_data_size(
            symbols=symbols,
            schema=schema,
            start=start,
            end=end,
            limit=limit,
            params=params,
        )

        bento: Bento = self._create_bento(path=path)

        self._stream(
            url=self._base_url + ".stream",
            params=params,
            basic_auth=True,
            bento=bento,
        )

        return bento

    async def stream_async(
        self,
        dataset: Union[Dataset, str],
        symbols: Optional[Union[List[str], str]] = None,
        schema: Union[Schema, str] = "trades",
        start: Optional[Union[pd.Timestamp, date, str, int]] = None,
        end: Optional[Union[pd.Timestamp, date, str, int]] = None,
        stype_in: Union[SType, str] = "native",
        stype_out: Union[SType, str] = "product_id",
        limit: Optional[int] = None,
        path: str = None,
    ) -> Bento:
        """
        Request a historical time series stream from the Databento API servers
        asynchronously.

        Makes a `GET /timeseries.stream` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset name for the request.
        symbols : List[Union[str, int]] or str, optional
            The symbols for the request. If ``None`` then will be for ALL symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        start : pd.Timestamp or date or str or int, optional
            The start datetime (UTC) of the request time range (inclusive).
            If using an integer then this represents nanoseconds since UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime (UTC) of the request time range (exclusive).
            If using an integer then this represents nanoseconds since UNIX epoch.
        stype_in : SType or str, default 'native'
            The input symbology type to resolve from.
        stype_out : SType or str, default 'product_id'
            The output symbology type to resolve to.
        limit : int, optional
            The maximum number of records to return.
            If ``None`` is passed then no limit.
        path : str, optional
            The file path to write to on disk (if provided).

        Returns
        -------
        Bento
            If `path` provided then FileBento, otherwise MemoryBento.

        Notes
        -----
        The Databento Binary Encoding + Zstd Compression (DBZ) will be streamed.

        """
        validate_enum(schema, Schema, "schema")
        validate_enum(stype_in, SType, "stype_in")
        validate_enum(stype_out, SType, "stype_out")

        schema = Schema(schema)
        stype_in = SType(stype_in)
        stype_out = SType(stype_out)

        params: List[Tuple[str, str]] = BentoHttpAPI._timeseries_params(
            dataset=dataset,
            symbols=symbols,
            schema=schema,
            start=start,
            end=end,
            stype_in=stype_in,
            stype_out=stype_out,
            limit=limit,
        )

        params.append(("encoding", Encoding.DBZ.value))  # Always requests DBZ
        params.append(("compression", Compression.ZSTD.value))  # Always requests ZSTD

        self._pre_check_data_size(
            symbols=symbols,
            schema=schema,
            start=start,
            end=end,
            limit=limit,
            params=params,
        )

        bento: Bento = self._create_bento(path=path)

        await self._stream_async(
            url=self._base_url + ".stream",
            params=params,
            basic_auth=True,
            bento=bento,
        )

        return bento

    def _pre_check_data_size(
        self,
        symbols: Optional[Union[List[str], str]],
        schema: Schema,
        start: Optional[Union[pd.Timestamp, date, str, int]],
        end: Optional[Union[pd.Timestamp, date, str, int]],
        limit: Optional[int],
        params: List[Tuple[str, Any]],
    ):
        if limit and limit < 10**7:
            return

        if (
            _is_large_data_size_schema(schema)
            or _is_greater_than_one_day(start, end)
            or _is_large_number_of_symbols(symbols)
        ):
            params = params[:]  # copy
            params.append(("mode", "historical-streaming"))
            params.append(("instruments", str(len(symbols))))
            log_debug(
                "Checking estimated data size for potentially large streaming "
                "request...",
            )
            response: Response = self._get(
                url=self._gateway + f"/v{API_VERSION}/metadata.get_size_estimation",
                params=params,
                basic_auth=True,
            )

            size: int = response.json()["size"]
            log_debug(
                f"Requesting data stream for {size:,} bytes (binary uncompressed)...",
            )
            if size > _5GB:
                warnings.warn(
                    f"\nThe size of the current streaming request is estimated "
                    f"to exceed 5GB ({_5GB:,} bytes). We recommend smaller "
                    f"individual streaming request sizes, or alternatively "
                    f"submit a batch data request."
                    f"\nYou can check the uncompressed binary size of a request "
                    f"through the metadata API (from the client library, or over "
                    f"HTTP).\nThis warning can be suppressed "
                    f"https://docs.python.org/3/library/warnings.html",
                )


def _is_large_number_of_symbols(symbols: Optional[Union[List[str], str]]):
    if not symbols:
        return True  # All symbols

    if isinstance(symbols, str):
        symbols = symbols.split(",")

    if len(symbols) >= 500:
        return True

    return False


def _is_large_data_size_schema(schema: Schema):
    return schema in (Schema.MBO, Schema.MBP_10)


def _is_greater_than_one_day(start, end):
    if start is None or end is None:
        return True

    if pd.to_datetime(end) - pd.to_datetime(start) > pd.Timedelta(days=1):
        return True

    return False
